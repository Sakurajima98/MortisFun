"""
画廊服务（GalleryService）

功能概述：
- 支持用户通过回复一条包含图片的消息，并发送命令 `/上传画廊 名称` 或 `/上传名称`，将图片保存到 `data/gallery/<名称>` 目录下；
- 支持名称别名解析，例如：`mnr`、`花里实乃里`、`实乃里` → `mnr`；`猪` → `pig`；
- 既支持从被回复的消息中提取图片（通过 OneBot/NapCat 的 `get_msg` 接口），也支持当前消息自带已解析的本地图片路径（由 NapCatIntegration 提供的 `images` 参数）。

实现要点：
- 通过上下文中的消息段查找 `reply` 段，获取被回复消息的 `id`（即 `message_id`），并调用 NapCat API `get_msg` 获取原消息；
- 从原消息的 `message` 段中提取图片的 `url` 或 `file://` 本地路径，进行下载/复制保存；
- 兼容 `base64://` 图片（如果出现），进行解码保存；
- 保存目标目录：项目根下 `data/gallery/<resolved_name>`（自动创建）；
- 返回用户可读的文本反馈，包含保存成功数量和目标目录。

使用示例：
- 用户先发送一张图片 → 回复该图片消息，发送 `/上传画廊 mnr`；
- 或者直接回复并发送 `/上传猪`；将保存到 `data/gallery/pig`。
"""

import os
import re
import base64
import shutil
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set

import aiohttp
import asyncio
import random
import math
from PIL import Image, ImageChops, ImageDraw, ImageFont


class GalleryService:
    """
    画廊服务类

    职责：
    - 解析并处理“上传到画廊”的用户命令；
    - 基于回复消息（reply）获取原消息图片并保存；
    - 支持别名解析与可选的配置覆盖；
    - 返回用户可读的操作结果文本。
    """

    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, napcat_api_caller, server):
        """
        初始化画廊服务

        Args:
            config (Dict[str, Any]): 全局配置
            data_manager: 数据管理器（保留以便未来扩展）
            text_formatter: 文本格式化器（保留以便未来扩展）
            napcat_api_caller (Callable): 调用 NapCat API 的回调（`app.call_napcat_api`）
            server: 服务器实例（用于统一日志）
        """
        self.config = config
        self.data_manager = data_manager
        self.text_formatter = text_formatter
        self.napcat_api_caller = napcat_api_caller
        self.server = server

        self.logger = logging.getLogger("GalleryService")

        # 画廊根目录（相对项目根）
        self.gallery_root = os.path.join('data', 'gallery')
        os.makedirs(self.gallery_root, exist_ok=True)

        # 临时目录：保存待对比图片与合成对比图
        self.temp_dir = os.path.join('data', 'gallery_tmp')
        os.makedirs(self.temp_dir, exist_ok=True)

        # 别名配置文件（独立）：data/gallery/gallerynameconfig.json
        self.alias_config_path = os.path.join(self.gallery_root, 'gallerynameconfig.json')
        # 别名分组：canonical -> set(aliases)
        self.alias_groups: Dict[str, Set[str]] = {}
        # 快速解析表：alias -> canonical
        self.alias_map: Dict[str, str] = {}
        self._load_alias_config()

        # 命令匹配：支持“/上传画廊 名称”、“/上传 名称”，以及“/上传别名”（无空格）
        # 例：/上传猪、/上传画廊 mnr、/上传mnr
        self.cmd_pattern = re.compile(r"^\s*/上传(?:画廊|图库)?\s*(.+?)\s*$")

        # 查看命令匹配：
        # /看 名称 或 /看名称id（无空格）
        self.watch_pattern = re.compile(r"^\s*/看\s*(.+?)\s*$")
        # /看所有 名称
        self.watch_all_pattern = re.compile(r"^\s*/看所有\s*(.+?)\s*$")
        # 来点名称 或 来点名称id（无空格）
        self.like_pattern = re.compile(r"^\s*来点\s*(.+?)\s*$")

        # /画廊别名 名称A 名称B（将B并入A）
        self.alias_cmd_pattern = re.compile(r"^\s*/画廊别名\s+(\S+)\s+(\S+)\s*$")

        # 待用户强制上传的临时状态：user_id -> {name, candidate_path, expires_at}
        self.pending_force: Dict[str, Dict[str, Any]] = {}

        # 用户最近一次成功上传记录：user_id -> saved_path
        self.last_saved: Dict[str, str] = {}

        self.logger.info("GalleryService 初始化完成：画廊根目录=%s", self.gallery_root)

    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理用户消息入口

        Args:
            message (str): 用户消息文本
            user_id (str): 用户ID
            **kwargs: 可能包含的扩展参数：
                - context (Dict[str, Any]): 原始 NapCat/OneBot 消息体（必须，用于读取 reply 段）
                - images (List[str]): 当前消息解析出的本地图片路径（可选）
                - group_id (str): 群号（可选）
                - message_type (str): 消息类型 'group' 或 'private'（可选）

        Returns:
            Optional[Dict[str, Any]]: 包含用户反馈文本的字典；不是相关命令则返回 None

        说明：
        - 当存在 reply 段时，优先从被回复的原消息中获取图片；
        - 否则，若当前消息携带解析图片路径（images），则使用这些图片进行保存；
        - 两者都不存在则返回提示信息。
        """
        try:
            # 指令分类：别名设置（优先）、查看（其次）、强制上传、退回上传、普通上传

            # 0) 设置别名
            alias_args = self._match_alias_command(message)
            if alias_args:
                a, b = alias_args
                result_text = self._apply_alias_command(a, b)
                return {'content': result_text}
            # 1) 查看所有
            watch_all_name = self._match_watch_all_command(message)
            if watch_all_name:
                resolved = self._resolve_alias(watch_all_name)
                gallery_dir = os.path.join(self.gallery_root, resolved)
                if not os.path.isdir(gallery_dir):
                    return {'content': f"❌ 画廊不存在：{resolved}"}
                context = kwargs.get('context', {})
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._background_send_watch_all(resolved, gallery_dir, context))
                except RuntimeError:
                    asyncio.create_task(self._background_send_watch_all(resolved, gallery_dir, context))
                return None

            # 1.5) 来点名称（与 /看 等价；若画廊不存在则不返回任何提示）
            like_token = self._match_like_command(message)
            if like_token:
                name_token, idx = self._parse_name_and_index(like_token)
                resolved = self._resolve_alias(name_token)
                gallery_dir = os.path.join(self.gallery_root, resolved)
                if not os.path.isdir(gallery_dir):
                    return None
                if idx is not None:
                    img_path = self._find_image_by_id(resolved, idx, gallery_dir)
                    if img_path:
                        return {
                            'content': "",
                            'image_path': img_path
                        }
                    else:
                        return {'content': f"❌ 未找到图片：{resolved}{idx}"}
                img_path = self._pick_random_image(gallery_dir)
                if img_path:
                    return {
                        'content': "",
                        'image_path': img_path
                    }
                return {'content': f"❌ {resolved} 画廊中没有可显示的图片"}

            # 2) 查看单张（随机或按id）
            watch_token = self._match_watch_command(message)
            if watch_token:
                name_token, idx = self._parse_name_and_index(watch_token)
                resolved = self._resolve_alias(name_token)
                gallery_dir = os.path.join(self.gallery_root, resolved)
                if not os.path.isdir(gallery_dir):
                    return {'content': f"❌ 画廊不存在：{resolved}"}
                if idx is not None:
                    # 按id精确返回
                    img_path = self._find_image_by_id(resolved, idx, gallery_dir)
                    if img_path:
                        return {
                            'content': "",
                            'image_path': img_path
                        }
                    else:
                        return {'content': f"❌ 未找到图片：{resolved}{idx}"}
                else:
                    # 随机返回
                    img_path = self._pick_random_image(gallery_dir)
                    if img_path:
                        return {
                            'content': "",
                            'image_path': img_path
                        }
                    else:
                        return {'content': f"❌ {resolved} 画廊中没有可显示的图片"}

            if self._match_force_upload_command(message):
                return await self._commit_force_upload(user_id)

            if self._match_rollback_command(message):
                return await self._rollback_last_upload(user_id)

            target_name = self._match_upload_command(message)
            if not target_name:
                return None

            context: Dict[str, Any] = kwargs.get('context', {}) or {}
            message_type: str = kwargs.get('message_type') or context.get('message_type', 'private')
            group_id: Optional[str] = kwargs.get('group_id') or context.get('group_id')
            images_from_current: List[str] = kwargs.get('images') or []

            # 解析别名
            resolved_name = self._resolve_alias(target_name)
            save_dir = os.path.join(self.gallery_root, resolved_name)
            os.makedirs(save_dir, exist_ok=True)

            # 优先：从 reply 段取被回复的消息图片
            reply_id = self._extract_reply_id(context)
            saved_files: List[str] = []
            if reply_id:
                fetched_images = await self._fetch_images_from_replied_message(reply_id)
                # 执行查重流程（不直接保存）
                compare_result = await self._deduplicate_and_prepare_save(fetched_images, resolved_name, save_dir, user_id)
                if compare_result:
                    return compare_result

            # 备选：如无 reply 或 reply 不含图片，则尝试当前消息已解析图片
            if not saved_files and images_from_current:
                # 将当前消息图片作为候选执行查重（不直接保存）
                compare_result = await self._deduplicate_local_and_prepare_save(images_from_current, resolved_name, save_dir, user_id)
                if compare_result:
                    return compare_result

            # 构建反馈
            # 如未进入查重提示（compare_result返回），说明没有候选或未找到图片
            if not (reply_id or images_from_current):
                usage = (
                    "❌ 没有找到可保存的图片。\n"
                    "请：先发送一张图片 → 回复那条图片消息，再发送命令：\n"
                    "- /上传画廊 名称\n"
                    "或\n"
                    "- /上传名称（例如：/上传猪 保存到 data/gallery/pig）"
                )
                msg = usage
                return {
                    'content': msg
                }

            # 若存在候选但没有重复（在查重流程中直接判定为不重复并落盘）
            # 这里不应进入，保留兜底
            return None

        except Exception as e:
            self.logger.error(f"处理画廊上传消息时出错: {e}")
            return {
                'content': f"❌ 处理失败：{str(e)}"
            }

    # ------------------ 内部工具方法 ------------------

    def _match_upload_command(self, message: str) -> Optional[str]:
        """
        解析上传命令，提取名称

        支持的格式：
        - /上传画廊 名称
        - /上传 名称
        - /上传名称（无空格）

        Returns:
            Optional[str]: 提取到的名称；不匹配则返回 None
        """
        try:
            s = str(message).strip()
            m = self.cmd_pattern.match(s)
            if not m:
                return None
            name = m.group(1).strip()
            return name if name else None
        except Exception:
            return None

    def _resolve_alias(self, name: str) -> str:
        """
        解析别名并返回最终目录名。

        - 先查 `alias_map`；如未命中，返回原名称（允许中文目录名）；
        - 去除名称首尾空白与非法文件字符；

        Args:
            name (str): 用户输入的目标名称或别名

        Returns:
            str: 画廊最终目录名
        """
        s = (name or '').strip()
        # Windows/Unix 通用的简单清理：移除路径分隔符与控制字符
        s = re.sub(r"[\\/:*?\"<>|]+", "_", s)
        if not s:
            s = "gallery"
        mapped = self.alias_map.get(s, s)
        return mapped

    def _match_alias_command(self, message: str) -> Optional[Tuple[str, str]]:
        """
        解析“/画廊别名 名称A 名称B”命令，返回 (A, B)。
        """
        try:
            s = str(message).strip()
            m = self.alias_cmd_pattern.match(s)
            if not m:
                return None
            a = m.group(1).strip()
            b = m.group(2).strip()
            if not a or not b:
                return None
            return a, b
        except Exception:
            return None

    def _apply_alias_command(self, name_a: str, name_b: str) -> str:
        """
        将名称B并入名称A所在集合，主名称采用 A 的主名称；
        同时将 B 目录下历史图片迁移至主目录并按顺序命名。
        """
        ca = self._get_canonical(name_a)
        cb = self._get_canonical(name_b)
        if ca == cb:
            aliases = sorted(list(self.alias_groups.get(ca, {ca})))
            return f"✅ 已在同一集合：{', '.join(aliases)}（主名称：{ca}）"
        # 合并集合（保留 ca）
        self._merge_alias_groups(ca, cb)
        self._save_alias_config()
        # 迁移历史图片目录
        try:
            self._merge_gallery_dirs(ca, cb)
        except Exception as e:
            self.logger.error(f"迁移历史图片失败：{e}")
        aliases = sorted(list(self.alias_groups.get(ca, {ca})))
        return f"✅ 已将『{name_b}』作为『{name_a}』的别名；当前集合：{', '.join(aliases)}（主名称：{ca}）"

    def _match_watch_all_command(self, message: str) -> Optional[str]:
        """
        解析“/看所有 名称”命令。
        """
        try:
            s = str(message).strip()
            m = self.watch_all_pattern.match(s)
            return m.group(1).strip() if m else None
        except Exception:
            return None

    def _match_watch_command(self, message: str) -> Optional[str]:
        """
        解析“/看 名称”或“/看名称id”命令，返回去掉空白的 token。
        """
        try:
            s = str(message).strip()
            m = self.watch_pattern.match(s)
            return m.group(1).replace(' ', '') if m else None
        except Exception:
            return None

    def _match_like_command(self, message: str) -> Optional[str]:
        """
        解析“来点名称”或“来点名称id”命令，返回去掉空白的 token。
        """
        try:
            s = str(message).strip()
            m = self.like_pattern.match(s)
            return m.group(1).replace(' ', '') if m else None
        except Exception:
            return None

    def _parse_name_and_index(self, token: str) -> Tuple[str, Optional[int]]:
        """
        解析名称与尾随的数字id，例如 token=mnr1 → ("mnr", 1)。
        """
        try:
            m = re.match(r"^(.*?)(\d+)$", token)
            if m:
                name = m.group(1)
                idx = int(m.group(2))
                return name, idx
            return token, None
        except Exception:
            return token, None

    def _pick_random_image(self, gallery_dir: str) -> Optional[str]:
        """
        随机返回画廊中的一张图片。
        """
        files = [f for f in os.listdir(gallery_dir) if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'))]
        if not files:
            return None
        choice = random.choice(files)
        return os.path.join(gallery_dir, choice)

    def _find_image_by_id(self, name: str, idx: int, gallery_dir: str) -> Optional[str]:
        """
        根据顺序命名查找图片，例如 mnr1.jpg / mnr1.png 等。
        """
        exts = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp']
        for ext in exts:
            fname = f"{name}{idx}.{ext}"
            path = os.path.join(gallery_dir, fname)
            if os.path.exists(path):
                return path
        # 宽松匹配（历史文件）：扫描所有文件，匹配不区分大小写的基名
        base = f"{name}{idx}".lower()
        for f in os.listdir(gallery_dir):
            if os.path.splitext(f)[0].lower() == base:
                return os.path.join(gallery_dir, f)
        return None

    def _extract_index_from_filename(self, name: str, filename: str) -> Optional[int]:
        """
        尝试从文件名中解析序号：name+数字。
        """
        try:
            m = re.match(rf"^{re.escape(name)}(\d+)\.[a-zA-Z0-9]+$", filename)
            if m:
                return int(m.group(1))
        except Exception:
            pass
        return None

    def _extract_reply_id(self, context: Dict[str, Any]) -> Optional[int]:
        """
        从上下文消息段中提取被回复消息的 ID（message_id）

        Args:
            context (Dict[str, Any]): 原始 NapCat/OneBot 消息体

        Returns:
            Optional[int]: 被回复的消息ID；未找到返回 None
        """
        try:
            segments = context.get('message', [])
            if isinstance(segments, list):
                for seg in segments:
                    if isinstance(seg, dict) and seg.get('type') == 'reply':
                        rid = seg.get('data', {}).get('id')
                        if rid is not None:
                            try:
                                return int(rid)
                            except Exception:
                                # id 可能是字符串，尝试直接返回
                                return rid
            return None
        except Exception:
            return None

    async def _fetch_images_from_replied_message(self, message_id: int) -> List[Dict[str, Any]]:
        """
        通过 NapCat API 获取被回复的消息，并提取图片段

        Args:
            message_id (int): 被回复的消息ID

        Returns:
            List[Dict[str, Any]]: 图片段列表（原始 `message` 段中的 `image` 类型字典）
        """
        try:
            request = {
                'action': 'get_msg',
                'params': {
                    'message_id': message_id
                }
            }
            result = await self.napcat_api_caller(request, timeout=10.0)
            images: List[Dict[str, Any]] = []
            if result and result.get('status') == 'ok':
                data = result.get('data') or {}
                msg_segments = data.get('message') or []
                if isinstance(msg_segments, list):
                    for seg in msg_segments:
                        if isinstance(seg, dict) and seg.get('type') == 'image':
                            images.append(seg)
            else:
                self.logger.warning(f"get_msg 返回异常: {result}")
            return images
        except Exception as e:
            self.logger.error(f"获取被回复消息时出错: {e}")
            return []

    async def _save_images(self, image_segments: List[Dict[str, Any]], save_dir: str) -> List[str]:
        """
        保存从 `get_msg` 提取的图片段到目标目录。

        Args:
            image_segments (List[Dict[str, Any]]): NapCat `image` 段列表
            save_dir (str): 目标保存目录

        Returns:
            List[str]: 成功保存的文件路径列表
        """
        saved: List[str] = []
        async with aiohttp.ClientSession() as session:
            for idx, seg in enumerate(image_segments, start=1):
                try:
                    data = seg.get('data', {})
                    url = data.get('url', '')
                    file_ref = data.get('file', '')

                    # 生成文件名（尽量保留原扩展名）
                    ext = self._guess_ext(url or file_ref)
                    filename = self._build_filename(idx, ext)
                    dest_path = os.path.join(save_dir, filename)

                    if url:
                        ok = await self._download_url(session, url, dest_path)
                        if ok:
                            saved.append(dest_path)
                            continue
                    if file_ref:
                        if str(file_ref).startswith('file://'):
                            src = str(file_ref)[7:]
                            if os.path.exists(src):
                                shutil.copy2(src, dest_path)
                                saved.append(dest_path)
                                continue
                        elif str(file_ref).startswith('base64://'):
                            b64 = str(file_ref)[9:]
                            if await self._save_base64(b64, dest_path):
                                saved.append(dest_path)
                                continue
                        # 其他未知 file 形式：直接跳过
                except Exception as e:
                    self.logger.error(f"保存图片段时出错: {e}")
        return saved

    async def _background_send_watch_all(self, name: str, gallery_dir: str, context: Dict[str, Any]) -> None:
        try:
            path = await asyncio.to_thread(self._compose_gallery_grid, name, gallery_dir)
            if not path:
                return
            mt = context.get('message_type')
            if mt == 'private':
                target_id = str(context.get('user_id', ''))
                if not target_id:
                    return
                payloads = [{
                    "action": "send_private_msg",
                    "params": {
                        "user_id": target_id,
                        "message": [{"type": "image", "data": {"file": f"file://{os.path.abspath(path)}"}}]
                    }
                }]
            else:
                group_id = str(context.get('group_id', ''))
                if not group_id:
                    return
                payloads = [{
                    "action": "send_group_msg",
                    "params": {
                        "group_id": group_id,
                        "message": [{"type": "image", "data": {"file": f"file://{os.path.abspath(path)}"}}]
                    }
                }]
            if hasattr(self, 'server') and self.server:
                for p in payloads:
                    await self.server.send_response_to_napcat(p)
        except Exception:
            pass

    async def _save_local_images(self, image_paths: List[str], save_dir: str) -> List[str]:
        """
        保存当前消息解析出的本地图片到画廊目录（通常来自 NapCatIntegration 缓存）。

        Args:
            image_paths (List[str]): 本地图片文件路径列表
            save_dir (str): 目标保存目录

        Returns:
            List[str]: 成功保存的文件路径列表
        """
        saved: List[str] = []
        for idx, src in enumerate(image_paths, start=1):
            try:
                if not src or not os.path.exists(src):
                    continue
                ext = self._guess_ext(src)
                filename = self._build_filename(idx, ext)
                dest_path = os.path.join(save_dir, filename)
                shutil.copy2(src, dest_path)
                saved.append(dest_path)
            except Exception as e:
                self.logger.error(f"复制本地图片时出错: {e}")
        return saved

    async def _download_url(self, session: aiohttp.ClientSession, url: str, dest_path: str) -> bool:
        """
        下载网络图片到指定路径。

        Args:
            session (aiohttp.ClientSession): HTTP 会话
            url (str): 图片 URL
            dest_path (str): 保存路径

        Returns:
            bool: 是否下载成功
        """
        try:
            async with session.get(url, timeout=30) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    with open(dest_path, 'wb') as f:
                        f.write(content)
                    return True
                else:
                    self.logger.warning(f"下载图片失败，HTTP {resp.status}: {url}")
                    return False
        except Exception as e:
            self.logger.error(f"下载图片时出错: {e}")
            return False

    async def _save_base64(self, b64: str, dest_path: str) -> bool:
        """
        保存 base64 编码的图片到指定路径。

        Args:
            b64 (str): base64 内容（不含前缀）
            dest_path (str): 保存路径

        Returns:
            bool: 是否保存成功
        """
        try:
            data = base64.b64decode(b64)
            with open(dest_path, 'wb') as f:
                f.write(data)
            return True
        except Exception as e:
            self.logger.error(f"保存 base64 图片时出错: {e}")
            return False

    def _guess_ext(self, ref: str) -> str:
        """
        猜测图片扩展名；若无法判断，默认 `jpg`。

        Args:
            ref (str): URL 或 文件路径字符串

        Returns:
            str: 扩展名（不含点）
        """
        try:
            s = (ref or '').lower()
            for ext in ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp']:
                if s.endswith('.' + ext):
                    return 'jpg' if ext == 'jpeg' else ext
            return 'jpg'
        except Exception:
            return 'jpg'

    # ------------------ 查重与命名逻辑 ------------------

    async def _deduplicate_and_prepare_save(self, image_segments: List[Dict[str, Any]], name: str, save_dir: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        针对从 get_msg 获得的图片段执行查重：
        - 如与库中某图高度相似，则生成对比图并提示，记录 3 分钟强制上传窗口；
        - 如不相似，则直接按顺序命名保存并返回成功消息。

        Returns:
            Optional[Dict[str, Any]]: 若检测到重复，返回提示消息与对比图；否则返回成功提示（已保存）。
        """
        if not image_segments:
            return None

        async with aiohttp.ClientSession() as session:
            for seg in image_segments:
                data = seg.get('data', {})
                candidate_path = None
                url = data.get('url')
                file_ref = data.get('file')

                # 将候选图片落到临时文件以便比对
                if url:
                    ext = self._guess_ext(url)
                    tmp_path = os.path.join(self.temp_dir, f"candidate_{user_id}_{datetime.now().strftime('%H%M%S%f')}.{ext}")
                    ok = await self._download_url(session, url, tmp_path)
                    candidate_path = tmp_path if ok else None
                elif file_ref:
                    if str(file_ref).startswith('file://'):
                        src = str(file_ref)[7:]
                        if os.path.exists(src):
                            candidate_path = src
                    elif str(file_ref).startswith('base64://'):
                        ext = self._guess_ext(file_ref)
                        tmp_path = os.path.join(self.temp_dir, f"candidate_{user_id}_{datetime.now().strftime('%H%M%S%f')}.{ext}")
                        b64 = str(file_ref)[9:]
                        ok = await self._save_base64(b64, tmp_path)
                        candidate_path = tmp_path if ok else None

                if not candidate_path:
                    continue

                is_dup, match_path, diff_path = self._check_duplicate(candidate_path, save_dir)
                if is_dup and match_path and diff_path:
                    # 记录强制上传窗口
                    self.pending_force[user_id] = {
                        'name': name,
                        'candidate_path': candidate_path,
                        'save_dir': save_dir,
                        'expires_at': datetime.now() + timedelta(minutes=3)
                    }
                    return {
                        'content': f"⚠️ 该图片与现有画廊图片高度相似，疑似重复。\n如需继续，请在3分钟内发送：/强制上传画廊",
                        'image_path': diff_path
                    }

                # 不重复：直接保存
                saved_path = self._save_with_sequential_name(candidate_path, name, save_dir)
                if saved_path:
                    # 如果是临时下载的候选，且保存后可删除临时文件
                    if candidate_path.startswith(self.temp_dir) and os.path.exists(candidate_path):
                        try:
                            os.remove(candidate_path)
                        except Exception:
                            pass
                    self.last_saved[user_id] = saved_path
                    return {
                        'content': f"✅ 已保存 1 张图片到目录：{save_dir}\n• 画廊名称：{name}\n• 文件：{os.path.basename(saved_path)}"
                    }

        return None

    async def _deduplicate_local_and_prepare_save(self, image_paths: List[str], name: str, save_dir: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        针对当前消息携带的本地图片执行查重与保存逻辑。
        """
        for src in image_paths:
            if not src or not os.path.exists(src):
                continue
            is_dup, match_path, diff_path = self._check_duplicate(src, save_dir)
            if is_dup and match_path and diff_path:
                self.pending_force[user_id] = {
                    'name': name,
                    'candidate_path': src,
                    'save_dir': save_dir,
                    'expires_at': datetime.now() + timedelta(minutes=3)
                }
                return {
                    'content': f"⚠️ 该图片与现有画廊图片高度相似，疑似重复。\n如需继续，请在3分钟内发送：/强制上传画廊",
                    'image_path': diff_path
                }

            saved_path = self._save_with_sequential_name(src, name, save_dir)
            if saved_path:
                self.last_saved[user_id] = saved_path
                return {
                    'content': f"✅ 已保存 1 张图片到目录：{save_dir}\n• 画廊名称：{name}\n• 文件：{os.path.basename(saved_path)}"
                }
        return None

    def _check_duplicate(self, candidate_path: str, gallery_dir: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        执行简单的 aHash 查重，并生成对比图。

        Returns:
            (is_dup, match_path, diff_path)
        """
        try:
            cand_hash = self._ahash(candidate_path)
            best_match = None
            best_dist = 1e9
            # 遍历画廊现有图片
            for fname in os.listdir(gallery_dir):
                if not fname.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')):
                    continue
                fpath = os.path.join(gallery_dir, fname)
                try:
                    h = self._ahash(fpath)
                except Exception:
                    continue
                dist = self._hamming_distance(cand_hash, h)
                if dist < best_dist:
                    best_dist = dist
                    best_match = fpath

            # 阈值（64位 aHash）：<= 5 视为高度相似
            if best_match is not None and best_dist <= 5:
                diff_img = self._compose_comparison(candidate_path, best_match)
                return True, best_match, diff_img
            return False, None, None
        except Exception as e:
            self.logger.error(f"查重流程异常: {e}")
            return False, None, None

    def _ahash(self, image_path: str) -> int:
        """
        计算图像的平均哈希（64位）。
        """
        with Image.open(image_path) as img:
            img = img.convert('L').resize((8, 8), Image.LANCZOS)
            pixels = list(img.getdata())
            avg = sum(pixels) / len(pixels)
            bits = ''.join('1' if p >= avg else '0' for p in pixels)
            return int(bits, 2)

    def _hamming_distance(self, h1: int, h2: int) -> int:
        """
        计算两个哈希的汉明距离。
        """
        return bin(h1 ^ h2).count('1')

    def _compose_comparison(self, path_a: str, path_b: str) -> str:
        """
        生成两张图片的左右对比图，并附加差异图（底部），返回合成文件路径。
        """
        try:
            with Image.open(path_a) as im_a, Image.open(path_b) as im_b:
                # 统一显示尺寸（宽 256，等比缩放）
                def to_disp(im: Image.Image) -> Image.Image:
                    w = 256
                    ratio = w / im.width
                    h = max(1, int(im.height * ratio))
                    return im.convert('RGB').resize((w, h), Image.LANCZOS)

                a = to_disp(im_a)
                b = to_disp(im_b)

                # 生成差异图（按统一尺寸对齐，取较小高）
                h_min = min(a.height, b.height)
                a_crop = a.crop((0, 0, a.width, h_min))
                b_crop = b.crop((0, 0, b.width, h_min))
                # 为差异图统一宽度（取较小宽）
                w_min = min(a_crop.width, b_crop.width)
                a_diff = a_crop.crop((0, 0, w_min, h_min))
                b_diff = b_crop.crop((0, 0, w_min, h_min))
                diff = ImageChops.difference(a_diff, b_diff)

                # 合成：上行左右对比，下行差异图
                top_h = max(a.height, b.height)
                top_w = a.width + b.width
                canvas_w = max(top_w, w_min)
                canvas_h = top_h + h_min
                canvas = Image.new('RGB', (canvas_w, canvas_h), (240, 240, 240))
                # 放置左右
                canvas.paste(a, (0, 0))
                canvas.paste(b, (a.width, 0))
                # 放置差异图居中
                offset_x = (canvas_w - w_min) // 2
                canvas.paste(diff, (offset_x, top_h))

                out_path = os.path.join(self.temp_dir, f"compare_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.jpg")
                canvas.save(out_path, 'JPEG', quality=90)
                return out_path
        except Exception as e:
            self.logger.error(f"生成对比图失败: {e}")
            return None

    def _compose_gallery_grid(self, name: str, gallery_dir: str) -> Optional[str]:
        """
        整合画廊所有图片为网格，并在每张图下方标注其ID（如 mnr1）。
        - 单元格宽度固定为 240 像素，按等比缩放；
        - 每个单元附带 28 像素的白色标签条，使用默认字体绘制文字；
        - 网格列数最多 4 列，根据图片数量动态计算行数。
        返回合成图路径（保存到 temp 目录）。
        """
        try:
            # 收集图片及序号
            items: List[Tuple[str, Optional[int]]] = []
            for f in os.listdir(gallery_dir):
                if not f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')):
                    continue
                idx = self._extract_index_from_filename(name, f)
                items.append((os.path.join(gallery_dir, f), idx))
            if not items:
                return None

            # 优先按 id 排序，其次按文件名
            items.sort(key=lambda x: (x[1] if x[1] is not None else 10**9, os.path.basename(x[0]).lower()))

            # 网格参数：适度缩小单元宽度并增加列数上限，避免合成图过大
            cell_w = 180
            label_h = 28
            max_cols = 24
            cols = min(max_cols, max(1, int(math.ceil(math.sqrt(len(items))))))
            rows = int(math.ceil(len(items) / cols))

            # 处理每个单元：缩放到 cell_w 宽，添加标签条
            tiles: List[Image.Image] = []
            for path, idx in items:
                with Image.open(path) as im:
                    im = im.convert('RGB')
                    ratio = cell_w / im.width
                    h = max(1, int(im.height * ratio))
                    disp = im.resize((cell_w, h), Image.LANCZOS)
                    # 标签条 + 绘制文字
                    tile = Image.new('RGB', (cell_w, h + label_h), (255, 255, 255))
                    tile.paste(disp, (0, 0))
                    draw = ImageDraw.Draw(tile)
                    try:
                        font = ImageFont.load_default()
                    except Exception:
                        font = None
                    # 只显示数字 ID；无法解析则显示文件名
                    text = f"{idx}" if idx is not None else os.path.basename(path)
                    tw, th = self._measure_text(draw, text, font)
                    draw.text(((cell_w - tw) // 2, h + (label_h - th) // 2), text, fill=(0, 0, 0), font=font)
                    tiles.append(tile)

            # 计算画布尺寸（每列取该列最大高度）
            col_width = cell_w
            # 先计算每行的最大高度（避免不同高度截断）
            row_heights: List[int] = []
            for r in range(rows):
                hmax = 0
                for c in range(cols):
                    idx = r * cols + c
                    if idx >= len(tiles):
                        break
                    hmax = max(hmax, tiles[idx].height)
                row_heights.append(hmax)
            canvas_w = cols * col_width
            canvas_h = sum(row_heights)
            canvas = Image.new('RGB', (canvas_w, canvas_h), (240, 240, 240))

            # 贴图
            y = 0
            k = 0
            for r in range(rows):
                x = 0
                for c in range(cols):
                    if k >= len(tiles):
                        break
                    tile = tiles[k]
                    canvas.paste(tile, (x, y))
                    x += col_width
                    k += 1
                y += row_heights[r]

            out_path = os.path.join(self.temp_dir, f"view_all_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg")
            canvas.save(out_path, 'JPEG', quality=90)
            return out_path
        except Exception as e:
            self.logger.error(f"合成整图库失败: {e}")
            return None

    def _measure_text(self, draw: ImageDraw.ImageDraw, text: str, font: Optional[ImageFont.ImageFont]) -> Tuple[int, int]:
        """
        文本尺寸测量兼容函数：
        - 优先使用 `ImageDraw.textbbox`（Pillow >= 8）；
        - 回退到 `font.getsize`；
        - 最后兜底按字符数估算。
        """
        try:
            bbox = draw.textbbox((0, 0), text, font=font)
            return bbox[2] - bbox[0], bbox[3] - bbox[1]
        except Exception:
            try:
                if font and hasattr(font, 'getsize'):
                    w, h = font.getsize(text)
                    return int(w), int(h)
            except Exception:
                pass
        # 兜底估算：按等宽字体近似
        return max(1, 8 * len(text)), 14

    # ------------------ 别名配置管理 ------------------
    def _load_alias_config(self) -> None:
        """
        加载 data/gallery/gallerynameconfig.json 为别名分组。
        结构：{"groups": {"mnr": ["实乃里", "花里实乃里"]}}
        若文件不存在则初始化为空配置。
        """
        try:
            if os.path.exists(self.alias_config_path):
                with open(self.alias_config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f) or {}
                groups = data.get('groups', {})
                self.alias_groups = {k: set(v) | {k} for k, v in groups.items()}
            else:
                self.alias_groups = {}
                self._save_alias_config()
        except Exception as e:
            self.logger.error(f"加载别名配置失败：{e}")
            self.alias_groups = {}
        self._rebuild_alias_map()

    def _save_alias_config(self) -> None:
        """
        保存别名分组到配置文件。
        """
        try:
            os.makedirs(os.path.dirname(self.alias_config_path), exist_ok=True)
            serializable = {k: sorted(list(v - {k})) for k, v in self.alias_groups.items()}
            with open(self.alias_config_path, 'w', encoding='utf-8') as f:
                json.dump({'groups': serializable}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存别名配置失败：{e}")

    def _rebuild_alias_map(self) -> None:
        """
        根据 alias_groups 重建别名解析表。
        """
        self.alias_map = {}
        for canonical, aliases in self.alias_groups.items():
            for a in aliases | {canonical}:
                self.alias_map[a] = canonical

    def _get_canonical(self, name: str) -> str:
        """
        返回名称所在集合的主名称；若未建组，则新建以自身为主名称。
        """
        n = (name or '').strip()
        if not n:
            return n
        c = self.alias_map.get(n)
        if c:
            return c
        self.alias_groups.setdefault(n, {n})
        self._rebuild_alias_map()
        return n

    def _merge_alias_groups(self, canonical_a: str, canonical_b: str) -> None:
        """
        合并两个别名集合，保留 canonical_a 为主名称。
        """
        set_a = self.alias_groups.get(canonical_a, {canonical_a})
        set_b = self.alias_groups.get(canonical_b, {canonical_b})
        self.alias_groups[canonical_a] = set_a | set_b | {canonical_a}
        if canonical_b in self.alias_groups:
            del self.alias_groups[canonical_b]
        self._rebuild_alias_map()

    def _merge_gallery_dirs(self, canonical: str, old_alias: str) -> None:
        """
        将旧别名目录 data/gallery/<old_alias> 中的图片迁移至 data/gallery/<canonical>。
        迁移时按主名称顺序命名（canonical+id）。
        """
        src_dir = os.path.join(self.gallery_root, old_alias)
        dst_dir = os.path.join(self.gallery_root, canonical)
        if not os.path.isdir(src_dir):
            return
        os.makedirs(dst_dir, exist_ok=True)
        exts = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')
        files = [f for f in os.listdir(src_dir) if f.lower().endswith(exts)]
        if not files:
            try:
                shutil.rmtree(src_dir)
            except Exception:
                pass
            return
        for f in files:
            src_path = os.path.join(src_dir, f)
            ext = os.path.splitext(f)[1].lstrip('.')
            next_id = self._get_next_index(canonical, dst_dir)
            dst_name = f"{canonical}{next_id}.{ext}"
            dst_path = os.path.join(dst_dir, dst_name)
            try:
                shutil.move(src_path, dst_path)
            except Exception as e:
                self.logger.error(f"迁移文件失败 {src_path} → {dst_path}: {e}")
        try:
            shutil.rmtree(src_dir)
        except Exception:
            pass

    def _get_next_index(self, name: str, gallery_dir: str) -> int:
        """
        获取顺序命名的下一个序号，例如 mnr1、mnr2。
        """
        pattern = re.compile(rf"^{re.escape(name)}(\d+)\.[a-zA-Z0-9]+$")
        max_idx = 0
        try:
            for fname in os.listdir(gallery_dir):
                m = pattern.match(fname)
                if m:
                    try:
                        idx = int(m.group(1))
                        max_idx = max(max_idx, idx)
                    except Exception:
                        continue
        except Exception:
            pass
        return max_idx + 1

    def _save_with_sequential_name(self, src_path: str, name: str, gallery_dir: str) -> Optional[str]:
        """
        以“<名称><序号>.<ext>”的格式保存图片。
        """
        try:
            ext = self._guess_ext(src_path)
            idx = self._get_next_index(name, gallery_dir)
            filename = f"{name}{idx}.{ext}"
            dest_path = os.path.join(gallery_dir, filename)
            shutil.copy2(src_path, dest_path)
            return dest_path
        except Exception as e:
            self.logger.error(f"顺序命名保存失败: {e}")
            return None

    # ------------------ 指令解析与状态操作 ------------------

    def _match_force_upload_command(self, message: str) -> bool:
        """
        匹配“/强制上传画廊”指令。
        """
        s = str(message).strip()
        return bool(re.match(r"^\s*/强制上传(?:画廊)?\s*$", s))

    def _match_rollback_command(self, message: str) -> bool:
        """
        匹配“/退回上传”指令。
        """
        s = str(message).strip()
        return bool(re.match(r"^\s*/退回上传\s*$", s))

    async def _commit_force_upload(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        提交强制上传：若用户存在未过期的重复候选，则落盘保存。
        """
        self._cleanup_expired_pending()
        info = self.pending_force.get(user_id)
        if not info:
            return {'content': "❌ 没有可强制上传的候选，或已过期。"}
        if datetime.now() > info['expires_at']:
            del self.pending_force[user_id]
            return {'content': "❌ 强制上传窗口已过期，请重新发起上传。"}

        name = info['name']
        save_dir = info['save_dir']
        candidate_path = info['candidate_path']
        saved_path = self._save_with_sequential_name(candidate_path, name, save_dir)
        if saved_path:
            self.last_saved[user_id] = saved_path
            # 若候选来自临时文件，保存后删除
            if candidate_path.startswith(self.temp_dir) and os.path.exists(candidate_path):
                try:
                    os.remove(candidate_path)
                except Exception:
                    pass
            del self.pending_force[user_id]
            return {
                'content': f"✅ 已强制保存图片到：{save_dir}\n• 文件：{os.path.basename(saved_path)}"
            }
        else:
            return {'content': "❌ 强制保存失败，请稍后重试。"}

    async def _rollback_last_upload(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        撤销用户最近一次成功上传：删除对应文件。
        """
        last = self.last_saved.get(user_id)
        if not last:
            return {'content': "❌ 没有可退回的上传记录。"}
        try:
            if os.path.exists(last):
                os.remove(last)
            self.last_saved.pop(user_id, None)
            return {'content': f"↩️ 已退回上传并删除文件：{os.path.basename(last)}"}
        except Exception as e:
            return {'content': f"❌ 退回失败：{e}"}

    def _cleanup_expired_pending(self):
        """
        清理过期的强制上传候选条目。
        """
        try:
            now = datetime.now()
            expired = [uid for uid, info in self.pending_force.items() if now > info.get('expires_at', now)]
            for uid in expired:
                # 删除临时候选文件
                path = self.pending_force[uid].get('candidate_path')
                if path and str(path).startswith(self.temp_dir) and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception:
                        pass
                del self.pending_force[uid]
        except Exception:
            pass

