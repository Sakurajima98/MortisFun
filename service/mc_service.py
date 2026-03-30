#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件说明:
- MC 消息发送服务: 识别用户输入的 `/send` 指令, 并通过 RCON 将文本广播到 Minecraft 服务器
- 集成到现有 service 体系, 不再使用独立的 mcbot 目录或 FastAPI 应用

配置说明:
- 从全局配置 `config['services']['mc']` 读取以下键 (均可选, 提供默认值):
  * host: 服务器 IP 地址, 默认 "43.136.81.133"
  * port: RCON 端口, 默认 8685
  * password: RCON 密码, 默认 "walnut"
  * allowed_groups: 允许的群号列表, 为空或缺省表示不限制
  * timeout: RCON 套接字超时(秒), 默认 5.0

使用说明:
- 在群聊或私聊中发送以下形式的消息将触发:
  * "/send 消息内容"
  * "/send\n消息内容"
  * "/send消息内容" (无空格, 紧随指令后为内容)
"""

from typing import Dict, Any, Optional, List
import socket
import struct
import time
import os
import re
import asyncio
import threading
import uuid
import aiohttp

from .base_service import BaseService


class RconClient:
    """
    类说明:
    - 轻量级 RCON 客户端, 适配 Minecraft 基于 Source RCON 协议的实现
    - 支持基本的 connect/auth/command/say/close 方法
    """

    SERVERDATA_AUTH = 3
    SERVERDATA_EXECCOMMAND = 2
    SERVERDATA_RESPONSE_VALUE = 0
    SERVERDATA_AUTH_RESPONSE = 2

    def __init__(self, host: str, port: int, password: str, timeout: float = 5.0) -> None:
        """
        函数说明:
        - 初始化 RCON 客户端
        参数:
            host: 服务器 IP 地址
            port: RCON 端口
            password: RCON 密码
            timeout: 套接字超时时间(秒)
        """
        self.host = host
        self.port = int(port)
        self.password = password
        self.timeout = float(timeout)
        self._sock: Optional[socket.socket] = None
        self._req_id: int = 0x6D63626F  # 默认请求ID

    def connect(self) -> bool:
        """
        函数说明:
        - 建立 TCP 连接并进行 RCON 认证
        返回:
            bool: True 表示认证成功, False 表示失败
        """
        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
            self._sock.settimeout(self.timeout)
            # 认证
            self._send_packet(self._req_id, self.SERVERDATA_AUTH, self.password.encode("utf-8"))
            rid, typ, _ = self._recv_packet()
            return rid != -1
        except Exception:
            self.close()
            return False

    def command(self, cmd: str) -> Optional[str]:
        """
        函数说明:
        - 发送命令并返回响应文本
        参数:
            cmd: 命令字符串, 例如 "say Hello"
        返回:
            Optional[str]: 响应文本; None 表示失败或无响应
        """
        if not self._sock:
            if not self.connect():
                return None
        try:
            self._send_packet(self._req_id, self.SERVERDATA_EXECCOMMAND, cmd.encode("utf-8"))
            rid, typ, body = self._recv_packet()
            if body is None:
                return None
            return body.decode("utf-8", errors="ignore")
        except Exception:
            return None

    def say(self, content: str) -> bool:
        """
        函数说明:
        - 在游戏内广播一条消息 (say)
        返回:
            bool: True 表示发送成功
        """
        resp = self.command(f"say {content}")
        return resp is not None

    def close(self) -> None:
        """
        函数说明:
        - 关闭连接
        """
        try:
            if self._sock:
                self._sock.close()
        finally:
            self._sock = None

    def _send_packet(self, request_id: int, typ: int, payload: bytes) -> None:
        """
        函数说明:
        - 发送一个 RCON 包
        包结构:
            length(4) + requestId(4) + type(4) + payload + 0x00 + 0x00
        """
        if not self._sock:
            raise RuntimeError("socket not connected")
        header = struct.pack("<ii", int(request_id), int(typ))
        packet = header + payload + b"\x00\x00"
        length = struct.pack("<i", len(packet))
        self._sock.sendall(length + packet)

    def _recv_packet(self) -> tuple:
        """
        函数说明:
        - 接收一个 RCON 包, 返回 (requestId, type, body)
        """
        if not self._sock:
            raise RuntimeError("socket not connected")
        raw_len = self._recv_exact(4)
        if not raw_len:
            return -1, -1, None
        length = struct.unpack("<i", raw_len)[0]
        raw = self._recv_exact(length)
        if not raw or len(raw) < 8:
            return -1, -1, None
        request_id, typ = struct.unpack("<ii", raw[:8])
        body = raw[8:-2] if len(raw) >= 10 else b""
        return request_id, typ, body

    def _recv_exact(self, n: int) -> Optional[bytes]:
        """
        函数说明:
        - 精确读取 n 字节, 超时或断开返回 None
        """
        buf = b""
        try:
            while len(buf) < n:
                chunk = self._sock.recv(n - len(buf))
                if not chunk:
                    return None
                buf += chunk
            return buf
        except Exception:
            return None


class McService(BaseService):
    """
    类说明:
    - 将 '/send' 指令集成为服务, 当用户发送对应消息时, 把文本通过 RCON 广播到 MC 服务器
    - 该类遵循 BaseService 接口, 以便被统一的消息路由器调用
    """

    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """
        函数说明:
        - 初始化MC服务并尝试调度日志监听的启动
        """
        super().__init__(config, data_manager, text_formatter, server)
        try:
            self._schedule_log_watch_start()
        except Exception:
            pass

    # 缓存结构：群ID -> (群名称, 过期时间戳)
    _group_name_cache: Dict[str, tuple] = {}
    _group_name_ttl_seconds: int = 3600
    # 成员名片缓存： (group_id, user_id) -> (display_name, 过期时间戳)
    _member_name_cache: Dict[tuple, tuple] = {}
    _member_name_ttl_seconds: int = 3600
    # 日志监听状态
    _log_task_started: bool = False
    _log_task: Optional[asyncio.Task] = None
    _last_log_pos: int = 0
    _log_path: Optional[str] = None
    _log_target_groups: List[str] = []
    _log_enabled: bool = False
    # 远程日志监听
    _remote_log_task_started: bool = False
    _remote_log_task: Optional[asyncio.Task] = None
    _remote_log_url: Optional[str] = None
    _remote_client_id: str = uuid.uuid4().hex

    def get_help_text(self) -> Dict[str, Any]:
        """
        函数说明:
        - 返回帮助文本, 指示用户如何使用 '/send' 指令
        """
        host = self.get_service_config("host", "43.136.81.133")
        port = int(self.get_service_config("port", 8685))
        return {
            "content": (
                "🎮 MC消息发送服务\n"
                "使用方法:\n"
                "1) /send 消息内容\n"
                "2) /send\\n消息内容\n"
                "3) /send消息内容\n"
                f"当前目标服务器: {host}:{port}\n"
            ),
            "image_path": None
        }

    async def start_log_watch(self) -> None:
        """
        函数说明:
        - 主动启动日志监听（远程/本地），无需等待任何消息触发
        - 在服务器启动阶段由主程序直接调用，以满足“无条件轮询并推送”的需求
        """
        await self._start_log_watch_if_needed()

    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        函数说明:
        - 处理用户输入消息, 若为 '/send' 指令则通过 RCON 发送到 MC 服务器
        参数:
            message: 用户消息
            user_id: 用户ID
            **kwargs: 其他参数, 可包含 'group_id'
        返回:
            Dict[str, Any] 或 None: 处理结果; 非本服务消息返回 None
        """
        try:
            # 任意消息到来时尝试启动日志监听（不依赖/send）
            await self._start_log_watch_if_needed()

            if not isinstance(message, str):
                return None
            text = message.strip()
            if not (text.startswith("/send")):
                return None

            group_id = str(kwargs.get("group_id", "")).strip()
            allowed_groups: List[str] = [str(x) for x in (self.get_service_config("allowed_groups", []) or [])]
            if allowed_groups and group_id and (group_id not in allowed_groups):
                # 群组未授权：拒绝发送，且不返回任何错误信息
                self.log_unified("WARNING", f"/send 被拒绝：群聊未授权 {group_id}", group_id, user_id)
                return None

            # 提取内容: 支持 "/send 内容" "/send\n内容" "/send内容"
            content = text[len("/send"):].strip()
            if not content:
                return {"content": "❌ /send 后未提供内容", "image_path": None}

            # 尝试启动日志监听（按需懒启动）
            await self._start_log_watch_if_needed()

            # 获取群名称（带缓存）
            group_name = await self._get_group_name_with_cache(group_id)
            # 获取成员在群内的显示名称（名片优先，失败退回昵称/用户ID）
            member_name = await self._get_member_display_name_with_cache(group_id, user_id)
            # 若群名称获取失败，使用群ID作为退路
            display_group = group_name if group_name else (f"群{group_id}" if group_id else "未知群")
            display_member = member_name if member_name else (str(user_id) if user_id else "未知用户")
            # 期望格式：相亲相爱一家人[A]:你好
            mc_text = f"{display_group}[{display_member}]:{content}"

            # 读取配置并发送
            host = str(self.get_service_config("host", "43.136.81.133"))
            port = int(self.get_service_config("port", 8685))
            password = str(self.get_service_config("password", "walnut"))
            timeout = float(self.get_service_config("timeout", 5.0))

            self.log_unified("INFO", f"RCON 发送: {mc_text}", group_id, user_id)
            client = RconClient(host=host, port=port, password=password, timeout=timeout)
            if not client.connect():
                self.log_unified("ERROR", f"RCON 认证失败: {host}:{port}", group_id, user_id)
                return {"content": "❌ 无法连接RCON或认证失败", "image_path": None}
            ok = client.say(mc_text)
            client.close()
            if ok:
                return {"content": "✅ 已发送到MC服务器", "image_path": None}
            else:
                return {"content": "⚠️ 发送失败, 服务器未响应", "image_path": None}

        except Exception as e:
            return self.handle_error(e, "MC消息发送")

    async def _start_log_watch_if_needed(self) -> None:
        """
        函数说明:
        - 懒启动日志监听任务：读取配置，若启用且路径存在则启动后台监听
        """
        try:
            # 读取配置
            # 默认开启日志监听（若配置未显式关闭）
            self._log_enabled = bool(self.get_service_config("log.enabled", True))
            self._log_path = str(self.get_service_config("log.path", "") or "").strip()
            self._remote_log_url = str(self.get_service_config("log.remote_url", self.get_service_config("log.url", "")) or "").strip()
            self._log_target_groups = [str(g) for g in (self.get_service_config("log.target_groups", []) or [])]
            # 优先使用远程日志监听
            if self._log_enabled and self._remote_log_url and not self._remote_log_task_started:
                loop = asyncio.get_running_loop()
                self._remote_log_task = loop.create_task(self._watch_remote_log_loop())
                self._remote_log_task_started = True
                self.log_unified("INFO", f"MC远程日志监听已启动: {self._remote_log_url}", group_id="system", user_id="system")
                return

            # 文件日志监听（本地或共享挂载）
            if self._log_task_started:
                return
            # 如果明确关闭或路径缺失则不启动
            if not self._log_enabled or not self._log_path:
                return
            # 初始化文件位置到末尾（只监听新增内容）
            try:
                self._last_log_pos = os.path.getsize(self._log_path) if os.path.exists(self._log_path) else 0
            except Exception:
                self._last_log_pos = 0
            loop = asyncio.get_running_loop()
            self._log_task = loop.create_task(self._watch_log_file_loop())
            self._log_task_started = True
            self.log_unified("INFO", f"MC日志监听已启动: {self._log_path}", group_id="system", user_id="system")
        except Exception:
            # 安静失败，避免影响主流程
            pass
    
    def _schedule_log_watch_start(self) -> None:
        """
        函数说明:
        - 尝试在事件循环可用时启动日志监听；如果当前无事件循环，延时重试
        """
        def _try_start():
            try:
                if self._log_task_started:
                    return
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self._start_log_watch_if_needed())
                else:
                    threading.Timer(2.0, _try_start).start()
            except RuntimeError:
                threading.Timer(2.0, _try_start).start()
            except Exception:
                pass
        _try_start()

    async def _watch_log_file_loop(self) -> None:
        """
        函数说明:
        - 后台循环读取MC服务器日志文件，解析关键信息并转发到配置的群聊
        """
        try:
            while True:
                try:
                    # 动态解析日志路径（支持目录输入）
                    self._log_path = self._resolve_log_path(self._log_path)
                    if not self._log_path or not os.path.exists(self._log_path):
                        try:
                            self.log_unified("WARNING", f"MC日志路径不可用: {self._log_path}", group_id="system", user_id="system")
                        except Exception:
                            pass
                        await asyncio.sleep(2.0)
                        continue
                    # 文件大小与位置管理（处理轮转/截断）
                    size = os.path.getsize(self._log_path)
                    if size < self._last_log_pos:
                        self._last_log_pos = 0
                    # 读取新增内容
                    with open(self._log_path, "r", encoding="utf-8", errors="ignore") as f:
                        f.seek(self._last_log_pos)
                        lines = f.readlines()
                        self._last_log_pos = f.tell()
                    for line in lines:
                        # 控制台打印原始日志
                        try:
                            print(line.rstrip())
                            self.log_unified("INFO", f"MC日志: {line.strip()}", group_id="system", user_id="system")
                        except Exception:
                            pass
                        msg = self._parse_log_line(line.strip())
                        if msg:
                            await self._send_log_to_groups(msg)
                except Exception:
                    # 解析或读取失败不影响后续循环
                    pass
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def _watch_remote_log_loop(self) -> None:
        """
        函数说明:
        - 后台循环轮询远程日志监听服务（HTTP接口），解析消息并转发到群聊
        远程接口规范：
        - GET {remote_url}/query?client_id=<uuid>
        - 返回数组，每个元素包含至少: id, ts, type, data
        """
        try:
            base_url = self._remote_log_url.rstrip("/")
            query_url = f"{base_url}/query?client_id={self._remote_client_id}"
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while True:
                    try:
                        async with session.get(query_url) as resp:
                            if resp.status == 200:
                                data = await resp.json(content_type=None)
                                if isinstance(data, list):
                                    for item in data:
                                        try:
                                            # 控制台打印原始事件
                                            print(f"[MC远程日志] {item}")
                                            self.log_unified("INFO", f"MC远程日志: {item}", group_id="system", user_id="system")
                                        except Exception:
                                            pass
                                        msg = self._map_remote_event_to_text(item)
                                        if msg:
                                            await self._send_log_to_groups(msg)
                            else:
                                try:
                                    self.log_unified("WARNING", f"远程日志HTTP状态异常: {resp.status}", group_id="system", user_id="system")
                                except Exception:
                                    pass
                    except Exception:
                        # 网络错误或解析错误，短暂休眠后重试
                        await asyncio.sleep(2.0)
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return
        except Exception:
            return

    def _resolve_log_path(self, configured_path: Optional[str]) -> Optional[str]:
        """
        函数说明:
        - 根据当前配置解析实际日志文件路径
        支持：
        - 直接文件路径
        - 目录路径（优先 latest.log，其次最近修改的 *.log）
        - 不存在时保持原值并由循环等待出现
        """
        try:
            if not configured_path:
                return None
            p = configured_path
            # 若是目录，则选择最新日志文件
            if os.path.isdir(p):
                latest_file = os.path.join(p, "latest.log")
                if os.path.exists(latest_file):
                    return latest_file
                # 选择目录中最近修改的 .log
                candidates = []
                for name in os.listdir(p):
                    full = os.path.join(p, name)
                    if os.path.isfile(full) and name.lower().endswith(".log"):
                        try:
                            candidates.append((full, os.path.getmtime(full)))
                        except Exception:
                            pass
                if candidates:
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    return candidates[0][0]
                return configured_path
            # 若是文件但不存在，尝试父目录选择 latest.log 或最近的 .log
            if not os.path.exists(p):
                parent = os.path.dirname(p)
                if parent and os.path.isdir(parent):
                    latest_file = os.path.join(parent, "latest.log")
                    if os.path.exists(latest_file):
                        return latest_file
                    candidates = []
                    for name in os.listdir(parent):
                        full = os.path.join(parent, name)
                        if os.path.isfile(full) and name.lower().endswith(".log"):
                            try:
                                candidates.append((full, os.path.getmtime(full)))
                            except Exception:
                                pass
                    if candidates:
                        candidates.sort(key=lambda x: x[1], reverse=True)
                        return candidates[0][0]
            return p
        except Exception:
            return configured_path

    def _map_remote_event_to_text(self, event: Any) -> Optional[str]:
        """
        函数说明:
        - 将远程日志服务的事件映射为群聊文本
        预期事件结构：
        {
          "id": "...",
          "ts": 1730000000,
          "type": "join|leave|advancement|goal|challenge|death|chat|info",
          "data": {
            "player": "Name",
            "title": "AdvancementName",
            "killer": "KillerName",
            "message": "原始文本"
          }
        }
        """
        try:
            if not isinstance(event, dict):
                return None
            typ = str(event.get("type", "")).lower()
            data = event.get("data") or {}
            if typ == "join":
                player = data.get("player") or data.get("name") or ""
                if player:
                    return f"🟢 玩家{player}加入游戏"
            elif typ == "leave":
                player = data.get("player") or data.get("name") or ""
                if player:
                    return f"🔴 玩家{player}离开游戏"
            elif typ in ("advancement", "goal", "challenge"):
                player = data.get("player") or ""
                title = data.get("title") or data.get("advancement") or ""
                if player and title:
                    if typ == "challenge":
                        return f"🏆 玩家{player}完成了挑战{title}"
                    elif typ == "goal":
                        return f"🏆 玩家{player}达成了目标{title}"
                    else:
                        return f"🏆 玩家{player}解锁了成就{title}"
            elif typ == "death":
                victim = (data.get("player") or data.get("victim") or "").strip()
                killer = (data.get("killer") or "").strip()
                cause = (data.get("cause") or data.get("reason") or "").strip()
                message = str(data.get("message") or "").strip()
                if not killer and message:
                    m = re.search(r"\bwas slain by\s+(.+)$", message, re.IGNORECASE)
                    if m:
                        killer = m.group(1).strip()
                    m = re.search(r"\bwas shot by\s+(.+)$", message, re.IGNORECASE)
                    if not killer and m:
                        killer = m.group(1).strip()
                    m = re.search(r"]:\s*\[([A-Za-z0-9_]{3,32}):\s*Killed\s+([A-Za-z0-9_]{3,32})\]", message, re.IGNORECASE)
                    if not killer and m and m.group(2).strip().lower() == victim.lower():
                        killer = m.group(1).strip()
                if not cause and message:
                    if re.search(r"\bdrowned\b", message, re.IGNORECASE):
                        cause = "溺亡"
                    elif re.search(r"\bfell\b", message, re.IGNORECASE):
                        cause = "高处坠落"
                    elif re.search(r"\bburned\b", message, re.IGNORECASE):
                        cause = "烧伤致死"
                    elif re.search(r"\bstarved\b", message, re.IGNORECASE):
                        cause = "饥饿致死"
                    elif re.search(r"\bsuffocated\b", message, re.IGNORECASE):
                        cause = "窒息致死"
                    elif re.search(r"\bwas blown up\b", message, re.IGNORECASE):
                        cause = "爆炸致死"
                    elif re.search(r"\bpricked to death\b", message, re.IGNORECASE):
                        cause = "扎死"
                    elif re.search(r"\bimpaled\b", message, re.IGNORECASE):
                        cause = "刺穿致死"
                    elif re.search(r"\bcrushed\b", message, re.IGNORECASE):
                        cause = "被压死"
                    elif re.search(r"\btried to swim in lava\b", message, re.IGNORECASE):
                        cause = "岩浆致死"
                if victim and killer:
                    return f"💀 玩家{victim}被{killer}击杀"
                if victim and cause:
                    return f"💀 玩家{victim}因{cause}"
                if victim:
                    return f"💀 玩家{victim}死亡"
            # 其他类型不转发或仅打印
            return None
        except Exception:
            return None

    def _parse_log_line(self, line: str) -> Optional[str]:
        """
        函数说明:
        - 解析一行MC日志，抽取关键信息并返回简洁消息文本
        识别要点：
        - 玩家加入/离开
        - 成就达成
        - 玩家死亡（常见模板）
        """
        try:
            if not line:
                return None
            # 玩家加入（支持常见日志格式：...]: Player joined the game）
            m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+joined the game", line)
            if m:
                player = m.group(1)
                return f"🟢 玩家{player}加入游戏"
            # 玩家离开
            m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+left the game", line)
            if m:
                player = m.group(1)
                return f"🔴 玩家{player}离开游戏"
            # 成就达成（多种模板）
            m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+has made the advancement\s+\[([^\]]+)\]", line)
            if m:
                player, adv = m.group(1), m.group(2)
                return f"🏆 玩家{player}解锁了成就{adv}"
            m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+has completed the challenge\s+\[([^\]]+)\]", line)
            if m:
                player, adv = m.group(1), m.group(2)
                return f"🏆 玩家{player}完成了挑战{adv}"
            m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+has reached the goal\s+\[([^\]]+)\]", line)
            if m:
                player, adv = m.group(1), m.group(2)
                return f"🏆 玩家{player}达成了目标{adv}"
            # 玩家死亡（更详细识别）
            # 1) ...]: PlayerName was killed
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+was\s+killed\b", line, re.IGNORECASE)
            if m:
                player = m.group(1)
                return f"💀 玩家{player}被击杀"
            # 2) 具体死亡原因与击杀者
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+was slain by\s+(.+)$", line, re.IGNORECASE)
            if m:
                victim, killer = m.group(1), m.group(2).strip()
                return f"💀 玩家{victim}被{killer}击杀"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+was shot by\s+(.+)$", line, re.IGNORECASE)
            if m:
                victim, killer = m.group(1), m.group(2).strip()
                return f"💀 玩家{victim}被{killer}射杀"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+drowned\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因溺亡"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+fell\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因高处坠落"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+burned\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因烧伤致死"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+starved\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因饥饿致死"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+suffocated\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因窒息致死"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+was blown up\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因爆炸致死"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+was pricked to death\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因扎死"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+was impaled\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因刺穿致死"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+was crushed\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因被压死"
            m = re.search(r"]:\s*([A-Za-z0-9_]{3,16})\s+tried to swim in lava\b", line, re.IGNORECASE)
            if m:
                victim = m.group(1)
                return f"💀 玩家{victim}因岩浆致死"
            # 3) 方括号插件格式： ...]: [Killer: Killed Victim]
            m = re.search(r"]:\s*\[([A-Za-z0-9_]{3,16}):\s*Killed\s+([A-Za-z0-9_]{3,16})\]", line, re.IGNORECASE)
            if m:
                killer, victim = m.group(1), m.group(2)
                return f"💀 玩家{victim}被{killer}击杀"
            return None
        except Exception:
            return None

    async def _send_log_to_groups(self, message: str) -> None:
        """
        函数说明:
        - 将解析到的日志关键消息发送到配置的群聊列表
        发送策略:
        1) 优先通过已连接的 NapCat WebSocket（OneBot v11）发送
        2) 若 NapCat 未连接或发送失败，且配置了 HTTP API，则使用 OneBot HTTP API 回退发送
        """
        try:
            if not self._log_target_groups:
                return
            text_segment = [{
                "type": "text",
                "data": {"text": f"【MC日志】{message}"}
            }]
            # 路径1：NapCat WebSocket
            if self.server and hasattr(self.server, "send_response_to_napcat"):
                for gid in self._log_target_groups:
                    payload = {
                        "action": "send_group_msg",
                        "params": {
                            "group_id": int(gid) if str(gid).isdigit() else gid,
                            "message": text_segment
                        }
                    }
                    try:
                        ok = await self.server.send_response_to_napcat(payload)
                        if ok:
                            continue
                    except Exception:
                        pass
            # 路径2：OneBot HTTP API 回退（可选配置）
            base_url = str(self.get_service_config("log.http_api.base_url", "") or "").strip()
            token = str(self.get_service_config("log.http_api.token", "") or "").strip()
            if base_url:
                try:
                    timeout = aiohttp.ClientTimeout(total=8)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        for gid in self._log_target_groups:
                            url = base_url.rstrip("/") + "/send_group_msg"
                            params = {}
                            headers = {}
                            if token:
                                # 兼容 OneBot v11 常见的 access_token 传递方式
                                params["access_token"] = token
                                headers["Authorization"] = f"Bearer {token}"
                            body = {
                                "group_id": int(gid) if str(gid).isdigit() else gid,
                                "message": text_segment
                            }
                            try:
                                async with session.post(url, json=body, params=params, headers=headers) as resp:
                                    # 不强制要求状态码为200，只要HTTP层成功即可
                                    _ = await resp.text()
                            except Exception:
                                pass
                except Exception:
                    pass
        except Exception:
            pass
    async def _get_group_name_with_cache(self, group_id: str) -> Optional[str]:
        """
        函数说明:
        - 获取群名称并进行1小时TTL缓存
        参数:
            group_id: QQ群ID
        返回:
            Optional[str]: 群名称; 失败返回 None
        """
        try:
            gid = str(group_id or "").strip()
            if not gid:
                return None
            now = time.time()
            cached = self._group_name_cache.get(gid)
            if cached:
                name, expire_at = cached
                if now < expire_at:
                    return name
            # 通过 NapCat OneBot API 获取群信息
            if not self.server or not hasattr(self.server, "call_napcat_api"):
                return None
            api_req = {
                "action": "get_group_info",
                "params": {
                    "group_id": int(gid)
                }
            }
            resp = await self.server.call_napcat_api(api_req, timeout=8.0)
            if resp and isinstance(resp, dict) and resp.get("status") == "ok":
                data = resp.get("data") or {}
                # OneBot v11 通常字段为 group_name
                name = data.get("group_name") or data.get("name") or ""
                name = str(name).strip()
                if name:
                    self._group_name_cache[gid] = (name, now + self._group_name_ttl_seconds)
                    return name
            return None
        except Exception:
            return None

    async def _get_member_display_name_with_cache(self, group_id: str, user_id: str) -> Optional[str]:
        """
        函数说明:
        - 获取某用户在指定群聊中的显示名称（群名片优先，昵称次之），并进行1小时TTL缓存
        参数:
            group_id: QQ群ID
            user_id: QQ用户ID
        返回:
            Optional[str]: 显示名称; 失败返回 None
        """
        try:
            gid = str(group_id or "").strip()
            uid = str(user_id or "").strip()
            if not gid or not uid or not uid.isdigit():
                return None
            now = time.time()
            key = (gid, uid)
            cached = self._member_name_cache.get(key)
            if cached:
                name, expire_at = cached
                if now < expire_at:
                    return name
            # 通过 NapCat OneBot API 获取群成员信息
            if not self.server or not hasattr(self.server, "call_napcat_api"):
                return None
            api_req = {
                "action": "get_group_member_info",
                "params": {
                    "group_id": int(gid),
                    "user_id": int(uid)
                }
            }
            resp = await self.server.call_napcat_api(api_req, timeout=8.0)
            if resp and isinstance(resp, dict) and resp.get("status") == "ok":
                data = resp.get("data") or {}
                card = str(data.get("card", "") or "").strip()
                nickname = str(data.get("nickname", "") or "").strip()
                # 名片优先，若为空则用昵称
                name = card if card else nickname
                if name:
                    self._member_name_cache[key] = (name, now + self._member_name_ttl_seconds)
                    return name
            return None
        except Exception:
            return None
