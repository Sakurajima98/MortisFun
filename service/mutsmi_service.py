#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
若叶睦互动服务（MutsmiService）

本文件实现与角色“若叶睦”的互动功能，包括：
- 触发摸摸指令：随机增加好感度（1-6），并实现每日首次摸摸金币奖励：
  - 连续第1天：10金币
  - 连续第2天：15金币
  - 连续第3天及以上：20金币
  - 新用户首次摸摸额外奖励：100金币（与当日奖励叠加）
- 商品系统：加载 data/mutsmi/shopping.json 的商品列表，支持“商品一览”和“买[商品名]”购买
- 购买后调用AI，根据礼物、若叶睦人设、用户好感度模拟回复，并返回好感度提升（2-12）

数据存储：
- 用户数据保存在 app_data/mutsmi/users/{user_id}.json 中，包含金币余额、好感度、连续摸摸天数、最后摸摸日期等
- 商品清单保存在项目相对路径 data/mutsmi/shopping.json 中

设计说明：
- 服务继承 BaseService，统一接入服务器日志格式和数据管理器
- AI调用通过 SiliconFlowClient(chat_completion) 实现，并约定返回严格JSON，失败时回退到默认回复和随机好感值

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025-11
"""

from typing import Dict, Any, Optional, List, Tuple
import asyncio
from datetime import datetime, timedelta
import os
import json
import random
import shutil
import re

from .base_service import BaseService
from .api_client import SiliconFlowClient


class MutsmiService(BaseService):
    """
    若叶睦互动服务类

    功能：
    - 解析并处理与“若叶睦”相关的指令（摸摸、商品一览、购买）
    - 维护用户的好感度与金币数据
    - 通过AI模拟若叶睦收到礼物后的回复与好感变化

    主要数据字段：
    - favorability: 当前好感度（int）
    - gold: 金币余额（int）
    - last_touch_date: 最近一次“摸摸”的日期（YYYY-MM-DD）
    - consecutive_days: 连续“摸摸”天数（int）
    - total_touches: 累计“摸摸”次数（int）
    """

    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """
        初始化若叶睦服务

        Args:
            config: 全局配置字典
            data_manager: 数据管理器（用于读写 app_data 下数据）
            text_formatter: 文本格式化器
            server: 服务器实例（用于统一日志）
        """
        super().__init__(config, data_manager, text_formatter, server)

        # 触发指令集合（简体中文多别名）
        self.touch_triggers = {
            "摸摸睦头人", "摸摸莫莫", "摸摸木木", "摸摸睦睦", "摸摸若叶睦"
        }

        # 用户数据目录（迁移至 wwwroot/walnutmortis.top/data/mutsmi/users）
        # 使用绝对路径，确保不受工作目录影响
        self.user_data_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 'data', 'mutsmi', 'users')
        )
        os.makedirs(self.user_data_dir, exist_ok=True)

        # 商品清单路径（使用绝对路径）
        self.shopping_file = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 'data', 'mutsmi', 'shopping.json')
        )

        # 初始化AI客户端（支持从siliconflow节或回退到services.chat节）
        # 说明：
        # - 优先使用顶层 `siliconflow` 配置中的 `api_key` 等参数
        # - 若顶层缺失密钥，则回退使用 `services.chat` 中的API配置并映射到siliconflow节
        # - 启动时进行密钥校验并输出明确的日志，便于定位配置问题
        sf_cfg = self.config.get('siliconflow', {})
        chat_cfg = self.config.get('services', {}).get('chat', {})

        # 构造SiliconFlow配置
        ai_config = self.config.copy()
        if not sf_cfg.get('api_key'):
            # 回退到chat服务配置（与ChatService保持一致的映射规则）
            api_url = chat_cfg.get('api_url', 'https://api.siliconflow.cn/v1/chat/completions')
            ai_config['siliconflow'] = {
                'api_key': chat_cfg.get('api_key', ''),
                'base_url': api_url.replace('/chat/completions', ''),
                'model': chat_cfg.get('model', 'deepseek-ai/DeepSeek-V3'),
                'max_tokens': chat_cfg.get('max_tokens', 2000),
                'temperature': chat_cfg.get('temperature', 0.7),
                'enable_thinking': chat_cfg.get('enable_thinking', False)
            }
            self.log_unified(
                "WARNING",
                "siliconflow.api_key缺失，已回退使用services.chat配置",
                group_id="system",
                user_id="system"
            )
        else:
            # 显式规范化siliconflow配置（避免缺字段时默认值不一致）
            ai_config['siliconflow'] = {
                'api_key': sf_cfg.get('api_key', ''),
                'base_url': sf_cfg.get('base_url', 'https://api.siliconflow.cn/v1'),
                'model': sf_cfg.get('model', 'deepseek-ai/DeepSeek-V3'),
                'max_tokens': sf_cfg.get('max_tokens', 2000),
                'temperature': sf_cfg.get('temperature', 0.7),
                'enable_thinking': sf_cfg.get('enable_thinking', False)
            }

        # 创建AI客户端实例
        self.ai_client = SiliconFlowClient(ai_config, self.logger)

        # 启动时密钥校验与提示
        if not ai_config.get('siliconflow', {}).get('api_key'):
            self.log_unified(
                "ERROR",
                "AI密钥未配置：请在config.json的siliconflow或services.chat中设置api_key",
                group_id="system",
                user_id="system"
            )
        else:
            self.log_unified(
                "INFO",
                "AI客户端初始化完成（已检测到API密钥）",
                group_id="system",
                user_id="system"
            )

        # 若叶睦人设（可在配置中覆盖）
        default_persona = (
            "若叶睦，性格温柔善良、略微害羞，喜欢清新自然的礼物，"
            "对细心体贴的行为会有积极回应。她讲话委婉而真诚，"
            "会根据与对方的熟悉程度（好感度）调整态度与称呼。"
        )
        self.persona = self.config.get('services', {}).get('mutsmi', {}).get('persona', default_persona)

        # 分时段“摸摸”文案模板配置（可选）：
        # 从 config.json 的 services.mutsmi.templates.touch 读取；
        # 缺失时在运行期回退到内置默认模板。
        self.touch_templates: Dict[str, Any] = (
            self.config.get('services', {}).get('mutsmi', {}).get('templates', {}).get('touch', {})
        )

        # 执行用户数据目录迁移（从 app_data/mutsmi/users → data/mutsmi/users）
        try:
            self._migrate_user_data_dir()
            self.log_unified("INFO", "若叶睦互动服务初始化完成（已检查并迁移用户数据目录）", group_id="system", user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"若叶睦服务初始化迁移失败: {e}", group_id="system", user_id="system")

    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理进入的消息并返回响应

        支持的指令：
        - 摸摸睦头人 / 摸摸莫莫 / 摸摸木木 / 摸摸睦睦 / 摸摸若叶睦
        - 商品一览
        - 买[商品名称]
        - /种黄瓜（两段式：种植→1小时后再次输入结算）
        - /打工（两段式：开始→3小时后再次输入结算）
        - /live 或 /演唱会（两段式：开始→12小时后再次输入结算）

        Args:
            message: 原始文本消息
            user_id: 发送者QQ号
            **kwargs: 上下文（如 group_id, message_type, context等）

        Returns:
            dict 或 None: 返回内容字典以供统一响应构建，或None表示不处理
        """
        try:
            msg = message.strip()

            # 摸摸触发
            if msg in self.touch_triggers:
                # 提取昵称（优先群名片card，其次nickname，最后回退“用户{user_id}”）
                nickname = self._extract_nickname(kwargs.get('context'), user_id)
                content = self._handle_touch(user_id, nickname)
                # 随机挑选“摸摸”图片（data/mutsmi/touch），混合消息返回
                touch_image = self._get_touch_image()
                if touch_image:
                    return {"content": content, "image_path": touch_image, "mixed_message": True}
                else:
                    return {"content": content}

            # 商品一览
            if msg == "商品一览":
                items = self._load_shopping_items()
                if not items:
                    return {"content": "当前没有可购买的商品哦~ 请稍后再试。"}
                lines = ["商品列表："]
                for it in items:
                    lines.append(f"- {it.get('name','未知')}：{it.get('price',0)} 金币")
                return {"content": "\n".join(lines)}

            # 购买商品：买[商品名称]
            if msg.startswith("买"):
                item_name = msg[1:].strip()
                if not item_name:
                    return {"content": "请输入要购买的商品名称，例如：买黄瓜"}
                nickname = self._extract_nickname(kwargs.get('context'), user_id)
                context = kwargs.get('context', {})
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._background_buy_and_send(user_id, item_name, nickname, context))
                except RuntimeError:
                    asyncio.create_task(self._background_buy_and_send(user_id, item_name, nickname, context))
                return None

            # 额外金币相关指令：/种黄瓜
            if msg == "/种黄瓜":
                nickname = self._extract_nickname(kwargs.get('context'), user_id)
                content = self._handle_cucumber(user_id, nickname)
                return {"content": content}

            # 额外金币相关指令：/打工
            if msg == "/打工":
                nickname = self._extract_nickname(kwargs.get('context'), user_id)
                content = self._handle_work(user_id, nickname)
                return {"content": content}

            # 额外金币相关指令：/live 或 /演唱会
            if msg in ("/live", "/演唱会"):
                nickname = self._extract_nickname(kwargs.get('context'), user_id)
                content = self._handle_live(user_id, nickname)
                return {"content": content}

            # 解除当前活动：/放弃活动
            if msg == "/放弃活动":
                nickname = self._extract_nickname(kwargs.get('context'), user_id)
                content = self._handle_abort_activity(user_id, nickname)
                return {"content": content}

            return None
        except Exception as e:
            err = self.handle_error(e, context="MutsmiService.process_message")
            return err

    def _ensure_user_data(self, user_id: str) -> Dict[str, Any]:
        """
        确保用户数据存在，如不存在则初始化默认结构并返回

        数据结构包含：
        - favorability: 初始好感度 0
        - gold: 初始金币 0
        - last_touch_date: 最近摸摸日期（字符串）
        - consecutive_days: 连续摸摸天数
        - total_touches: 累计摸摸次数

        Args:
            user_id: 用户QQ号

        Returns:
            dict: 用户数据字典
        """
        path = os.path.join(self.user_data_dir, f"{user_id}.json")
        data: Dict[str, Any] = {}
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f) or {}
        except Exception:
            data = {}

        if not data:
            data = {
                "favorability": 0,
                "gold": 0,
                "last_touch_date": None,
                "consecutive_days": 0,
                "total_touches": 0,
                # 额外获取金币任务的临时状态（两段式任务）
                # 示例：{"type": "farm|work|live", "start_ts": "ISO8601", "duration_hours": 1|3|12}
                "extra_task": None
            }
            self.data_manager.save_data(path, data)
        return data

    def _save_user_data(self, user_id: str, data: Dict[str, Any]) -> None:
        """
        将用户数据保存到 app_data/mutsmi/users/{user_id}.json

        Args:
            user_id: 用户QQ号
            data: 用户数据字典
        """
        try:
            path = os.path.join(self.user_data_dir, f"{user_id}.json")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存用户数据失败: {e}")

    def _extract_nickname(self, context: Optional[Dict[str, Any]], user_id: str) -> str:
        """
        提取用户昵称用于个性化文案：
        - 优先使用群名片 `sender.card`
        - 其次使用 `sender.nickname`
        - 均缺失时回退为 `用户{user_id}`

        Args:
            context: 原始消息上下文（NapCat/OneBot消息体）
            user_id: 用户QQ号

        Returns:
            str: 昵称字符串
        """
        try:
            sender = (context or {}).get('sender', {})
            card = str(sender.get('card', '')).strip()
            nickname = str(sender.get('nickname', '')).strip()
            return card or nickname or f"用户{user_id}"
        except Exception:
            return f"用户{user_id}"

    def _get_time_segment(self) -> str:
        """
        根据当前时间划分到五个时段，用于选择对应的“摸摸”模板。

        时段划分（Asia/Shanghai）：
        - 凌晨：00:00-05:59
        - 白天：06:00-10:59（体现“早安”元素）
        - 中午：11:00-12:59
        - 下午：13:00-17:59
        - 夜晚：18:00-23:59

        Returns:
            str: 其中之一："凌晨"/"白天"/"中午"/"下午"/"夜晚"
        """
        try:
            hour = datetime.now().hour
            if 0 <= hour < 6:
                return "凌晨"
            if 6 <= hour < 11:
                return "白天"
            if 11 <= hour < 13:
                return "中午"
            if 13 <= hour < 18:
                return "下午"
            return "夜晚"
        except Exception:
            # 异常时安全回退到“白天”
            return "白天"

    def _choose_touch_template(self, kind: str, segment: str, nickname: str) -> str:
        """
        选择“摸摸”文案模板：按类别（成功/已摸过）+ 时段，从配置或默认集合随机挑选。

        Args:
            kind: 模板类型，"success"（首次/成功）或 "already"（当天已摸过）
            segment: 时段标签（"凌晨"/"白天"/"中午"/"下午"/"夜晚"）
            nickname: 用户昵称，用于插入到模板中（模板可包含 {nickname} 占位符）

        Returns:
            str: 已渲染的模板文本（插入昵称后）
        """
        # 默认模板（当配置缺失时使用），保持若叶睦口吻与轻柔语气
        default_success = {
            "白天": [
                "早安，{nickname}…今天的第一下摸摸…让我有精神起来…",
                "{nickname}，清晨的手心好暖…摸摸…我会加油的…",
                "阳光很柔和…{nickname}的摸摸也很柔和…谢谢…",
                "早上的摸摸…像清新的风…{nickname}…我喜欢…",
                "嗯…{nickname}…早安摸摸…我会把这份温度记在心里…",
                "新的一天从摸摸开始…{nickname}…一起前进吧…"
            ],
            "中午": [
                "{nickname}，午间的摸摸…让我安定下来…",
                "中午稍微有点犯困…{nickname}摸摸后…又精神了一些…",
                "午日的光很懒散…{nickname}的摸摸很认真…谢谢…",
                "午休前的摸摸…让我更放松…{nickname}…",
                "{nickname}…午间的小小鼓励…我收到了…",
                "咚…{nickname}的摸摸…让我不再紧绷…"
            ],
            "下午": [
                "下午的风…和{nickname}的摸摸一样温柔…",
                "有点疲惫的时候…{nickname}的摸摸…刚刚好…",
                "嗯…下午的摸摸…让我继续努力…谢谢你，{nickname}…",
                "{nickname}…轻轻的…我就不那么慌张了…",
                "像茶香一样…{nickname}的摸摸很舒心…",
                "我会把这份安稳…留在今天的日记里…{nickname}…"
            ],
            "夜晚": [
                "夜里安静…{nickname}的摸摸…让我更安心…",
                "{nickname}…晚上的摸摸…像星光一样点亮心里…",
                "嗯…今天也辛苦了…摸摸…谢谢你一直在…{nickname}…",
                "晚风里…我会记住这份温度…{nickname}…",
                "{nickname}…晚安之前的摸摸…让我更勇敢…",
                "我会带着这份温柔入睡…谢谢你，{nickname}…"
            ],
            "凌晨": [
                "有点困困的…{nickname}的摸摸…让我不再迷糊…",
                "嗯…凌晨很安静…{nickname}…摸摸后…我想再休息一下…",
                "迷迷糊糊地…但我知道…这是{nickname}的温度…",
                "{nickname}…我会小声地说谢谢…因为现在很安静…",
                "困意里…摸摸…让我更安心…谢谢你，{nickname}…",
                "我会继续眯一会儿…带着这份温度…{nickname}…"
            ]
        }
        default_already = {
            "白天": [
                "早安…{nickname}…今天已经摸过啦…我还在回味…",
                "{nickname}，小本本上已记下今天的摸摸…嘿嘿…",
                "今天的第一下摸摸已经完成…明天再来吧…{nickname}…",
                "嗯…记录一下…今天摸摸√…谢谢你，{nickname}…"
            ],
            "中午": [
                "{nickname}…午间已经摸过一次啦…要适当休息哦…",
                "我还在享受午后的余温…今天就到这里吧…{nickname}…",
                "今天的摸摸额度用完啦…{nickname}…我们明天继续…",
                "小提醒：今天摸摸已完成…谢谢你，{nickname}…"
            ],
            "下午": [
                "{nickname}…今天已经摸过…我会记住这份安稳…",
                "记录完成…摸摸√…{nickname}…我们明天见…",
                "今天的温度我收到了…{nickname}…再多一点我会害羞…",
                "嗯…今天的摸摸已完成…谢谢你关心，{nickname}…"
            ],
            "夜晚": [
                "{nickname}…今天已经摸过啦…晚风里我会好好珍惜…",
                "晚上的温柔我记住了…今天就到这里…{nickname}…",
                "嗯…今天的摸摸完成…明天也请多关照…{nickname}…",
                "我会带着这份温度入睡…谢谢你，{nickname}…"
            ],
            "凌晨": [
                "有点困困的…不过…{nickname}…今天已经摸过…我会继续休息…",
                "迷糊地说一句…今天摸摸完成…谢谢你，{nickname}…",
                "嗯…现在很安静…今天的摸摸…我记住了…{nickname}…",
                "我会再眯一下…明天再来摸我吧…{nickname}…"
            ]
        }

        try:
            seg_list: List[str] = (
                self.touch_templates.get(kind, {}).get(segment, []) if isinstance(self.touch_templates, dict) else []
            )
            candidates = seg_list if seg_list else default_success[segment] if kind == "success" else default_already[segment]
            template = random.choice(candidates)
            return template.format(nickname=nickname)
        except Exception:
            # 任何异常都安全回退到通用一条
            return f"{nickname}…摸摸…我会把这份温度记在心里…"

    def _get_touch_image(self) -> Optional[str]:
        """
        随机选择“摸摸”配图用于混合消息发送。

        资源路径：data/mutsmi/touch（绝对路径构造）
        支持扩展名：.jpg/.jpeg/.png/.gif/.webp

        Returns:
            Optional[str]: 图片绝对路径；无可用图片或出错时返回None
        """
        try:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'mutsmi', 'touch'))
            if not os.path.isdir(base_dir):
                self.logger.warning(f"摸摸图片目录不存在: {base_dir}")
                return None
            files = [f for f in os.listdir(base_dir) if f.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp"))]
            if not files:
                self.logger.info(f"摸摸图片目录为空: {base_dir}")
                return None
            return os.path.join(base_dir, random.choice(files))
        except Exception as e:
            self.logger.error(f"选择摸摸图片失败: {e}")
            return None

    def _handle_touch(self, user_id: str, nickname: str) -> str:
        """
        处理“摸摸”逻辑：
        - 首次触发与每日首次奖励金币
        - 随机增加好感度 1-6（当日仅第一次有效）
        - 维护连续天数

        奖励规则：
        - 新用户首次 +100 金币（仅首次）
        - 连续第1天 +10，连续第2天 +15，连续>=3天 +20

        返回信息设计：
        - 文案包含对用户昵称的称呼，如“啊，是{nickname}啊，很温暖呢~”
        - 奖励金币描述为“若叶睦从自己的零花钱中取出一部分送给了你”

        Returns:
            str: 给用户的响应文本
        """
        data = self._ensure_user_data(user_id)
        today = datetime.now().strftime('%Y-%m-%d')
        last_date = data.get("last_touch_date")

        # 是否新用户首次（在本次触发前尚未有任何摸摸）
        is_first_ever = (data.get("total_touches", 0) == 0)

        # 计算是否是新的连续
        consecutive = data.get("consecutive_days", 0)
        if last_date:
            try:
                last = datetime.strptime(last_date, '%Y-%m-%d')
                if (datetime.strptime(today, '%Y-%m-%d') - last).days == 1:
                    consecutive += 1
                elif today != last_date:
                    consecutive = 1
                # 如果今天已摸过，则不再增加好感与金币，但提示状态
                else:
                    # 当天已摸过：按时段选择“已摸过”模板
                    segment = self._get_time_segment()
                    prefix = self._choose_touch_template("already", segment, nickname)
                    return (
                        f"{prefix}\n"
                        f"当前好感度：{data.get('favorability',0)}，金币余额：{data.get('gold',0)}。明天再来摸我吧～"
                    )
            except Exception:
                consecutive = 1
        else:
            consecutive = 1

        # 当日首次：计算金币奖励
        bonus = 10 if consecutive == 1 else (15 if consecutive == 2 else 20)
        if is_first_ever:
            bonus += 100

        # 增加好感 1-6（当日首次）
        favor_delta = random.randint(1, 6)
        data['favorability'] = int(data.get('favorability', 0)) + favor_delta
        data['gold'] = int(data.get('gold', 0)) + bonus

        # 更新状态
        data['last_touch_date'] = today
        data['consecutive_days'] = consecutive
        data['total_touches'] = int(data.get('total_touches', 0)) + 1
        self._save_user_data(user_id, data)

        # 反馈文本（10+条成功文案模板，随机选择一条）
        tip_new = "（含新用户首次+100）" if is_first_ever else ""
        metrics_block = (
            f"💝 零花钱奖励：+{bonus} 金币{tip_new}\n"
            f"💗 好感度提升：+{favor_delta}\n"
            f"📈 当前好感度：{data['favorability']}\n"
            f"💰 金币余额：{data['gold']}\n"
            f"📅 连续摸摸：{consecutive} 天"
        )

        # 首次/成功：按时段选择模板，并追加奖励统计信息
        segment = self._get_time_segment()
        core = self._choose_touch_template("success", segment, nickname)
        header = f"【若叶睦】\n{core}"
        return f"{header}\n{metrics_block}"

    def _load_shopping_items(self) -> List[Dict[str, Any]]:
        """
        加载商品清单 data/mutsmi/shopping.json

        Returns:
            List[Dict[str, Any]]: 商品列表（name, price）
        """
        try:
            # 自动创建目录与默认文件（若缺失）
            os.makedirs(os.path.dirname(self.shopping_file), exist_ok=True)
            if not os.path.exists(self.shopping_file):
                with open(self.shopping_file, 'w', encoding='utf-8') as f:
                    json.dump([{"name": "黄瓜", "price": 5}], f, ensure_ascii=False, indent=2)

            with open(self.shopping_file, 'r', encoding='utf-8') as f:
                items = json.load(f)
                if isinstance(items, list):
                    return items
                return []
        except Exception as e:
            self.logger.error(f"加载商品清单失败: {e}")
            return []

    async def _handle_buy(self, user_id: str, item_name: str, nickname: str) -> Optional[str]:
        """
        处理购买流程：
        - 校验商品存在与价格
        - 校验用户金币余额并扣除
        - 调用AI模拟若叶睦的回复（包含用户昵称）；好感度提升：
          * 若礼物为“黄瓜”：随机 [2, 12]
          * 其他礼物：favor_delta = round(price * 1.5)

        Args:
            user_id: 用户QQ号
            item_name: 商品名称

        Returns:
            Optional[str]: 回复文本；当商品不存在时返回 None（静默）
        """
        items = self._load_shopping_items()
        item = next((it for it in items if str(it.get('name', '')).strip() == item_name), None)
        if not item:
            # 未找到商品时不返回任何消息（静默处理）
            return None

        price = int(item.get('price', 0))
        if price <= 0:
            return f"商品价格异常：{item_name}。请联系管理员。"

        data = self._ensure_user_data(user_id)
        balance = int(data.get('gold', 0))
        if balance < price:
            return f"金币不足（需要 {price}，当前 {balance}）。可通过摸摸获取金币奖励哦～"

        # 扣除金币
        data['gold'] = balance - price

        # 计算好感度提升：黄瓜随机，其它按价格1.5倍
        if item_name == "黄瓜":
            favor_delta = random.randint(2, 12)
        else:
            favor_delta = int(round(price * 1.5))

        # 调用AI生成若叶睦的回复（带昵称）
        ai_reply = await self._simulate_mutsmi_response(
            item_name=item_name,
            favorability=int(data.get('favorability', 0)),
            user_id=user_id,
            nickname=nickname
        )

        # 应用好感提升
        data['favorability'] = int(data.get('favorability', 0)) + favor_delta
        self._save_user_data(user_id, data)

        return (
            f"已购买并赠予若叶睦：{item_name}（-{price} 金币）\n"
            f"她的回复：{ai_reply}\n"
            f"好感度 +{favor_delta}（当前：{data['favorability']}），金币余额：{data['gold']}"
        )

    # ============================================
    # 额外金币获取任务：工具方法与三类指令处理
    # ============================================
    def _get_extra_task(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        获取当前用户的额外金币任务状态。

        说明：
        - 额外金币任务采用两段式（启动→冷却完成后再次输入结算）。
        - 该方法仅返回结构正确的任务字典，否则返回 None。

        Returns:
            Optional[Dict[str, Any]]: 当前任务字典或 None
        """
        task = data.get("extra_task")
        return task if isinstance(task, dict) else None

    def _set_extra_task(self, data: Dict[str, Any], task_type: str, duration_hours: int) -> None:
        """
        启动额外金币任务，记录任务类型与开始时间及持续时长。

        Args:
            data: 用户数据字典
            task_type: 任务类型（"farm" | "work" | "live"）
            duration_hours: 冷却时长（单位：小时）
        """
        data["extra_task"] = {
            "type": task_type,
            "start_ts": datetime.now().isoformat(),
            "duration_hours": int(duration_hours),
        }

    def _clear_extra_task(self, data: Dict[str, Any]) -> None:
        """
        清除当前的额外金币任务（在结算后调用）。
        """
        data["extra_task"] = None

    def _extra_task_remaining_seconds(self, task: Dict[str, Any]) -> int:
        """
        计算额外任务的剩余时间（秒）。若已到期，返回 0。

        Args:
            task: 任务字典，包含 "start_ts" 与 "duration_hours"

        Returns:
            int: 剩余秒数（到期时为 0）
        """
        try:
            start = datetime.fromisoformat(str(task.get("start_ts")))
            duration_hours = int(task.get("duration_hours", 0))
            end = start + timedelta(hours=duration_hours)
            now = datetime.now()
            remaining = (end - now).total_seconds()
            return int(remaining) if remaining > 0 else 0
        except Exception:
            # 若结构异常或时间解析失败，视为到期
            return 0

    def _format_remaining(self, seconds: int) -> str:
        """
        将剩余秒数格式化为中文可读的时间长度。

        规则：
        - 小于 1 小时：返回 "X分钟"
        - 大于等于 1 小时：返回 "X小时Y分钟"

        Args:
            seconds: 剩余秒数

        Returns:
            str: 格式化后的剩余时间
        """
        if seconds <= 0:
            return "0分钟"
        mins = seconds // 60
        hours = mins // 60
        rem_mins = mins % 60
        if hours > 0:
            return f"{hours}小时{rem_mins}分钟"
        return f"{rem_mins}分钟"

    # ------------------
    # 指令：/种黄瓜（farm）
    # ------------------
    def _handle_cucumber(self, user_id: str, nickname: str) -> str:
        """
        处理“/种黄瓜”两段式任务：
        1) 第一次输入：开始种植，锁定 1 小时内不允许其他额外金币指令；
        2) 1 小时后再次输入：进行收获并结算奖励。

        结算奖励：
        - 90% 概率：+10 金币
        - 10% 概率：+5 金币，并以若叶睦口吻提示“黄瓜看着特别鲜嫩，她也想要一根”
        - 额外好感度（仅 5 金币时触发）：随机 +2 ~ +12

        Args:
            user_id: 用户QQ号
            nickname: 用户昵称（用于文案）

        Returns:
            str: 文本回复（中文）
        """
        data = self._ensure_user_data(user_id)
        task = self._get_extra_task(data)

        # 若已有其他类型任务，则提示剩余时间并禁止切换
        if task and task.get("type") != "farm":
            remain = self._extra_task_remaining_seconds(task)
            tmap = {"work": "打工", "live": "演出"}
            other = tmap.get(task.get("type"), "其他任务")
            return (
                f"还在{other}呢！…还需要 {self._format_remaining(remain)}这么久！"
                f"等结束后我们再来一起种黄瓜吧，{nickname}…"
            )

        # 未有任务：启动种植
        if not task:
            self._set_extra_task(data, "farm", duration_hours=1)
            self._save_user_data(user_id, data)
            return (
                f"已经把黄瓜种下了…需要一小时才能收获…{nickname}…\n"
                f"这一小时内先不要安排其他活动哦…很累的"
            )

        # 已有 farm 任务：检查是否到期
        remain = self._extra_task_remaining_seconds(task)
        if remain > 0:
            return (
                f"还没成熟呢…还需要 {self._format_remaining(remain)}。{nickname}…\n"
                f"我们耐心等一等…黄瓜会更鲜嫩…"
            )

        # 到期结算
        coin = 10 if random.random() < 0.9 else 5
        data['gold'] = int(data.get('gold', 0)) + coin

        # 仅在 5 金币的情况下增加额外好感度
        favor_line = ""
        if coin == 5:
            favor_delta = random.randint(2, 12)
            data['favorability'] = int(data.get('favorability', 0)) + favor_delta
            favor_line = f"另外…我感觉我更喜欢你了… \n好感度 +{favor_delta}。\n"

        self._clear_extra_task(data)
        self._save_user_data(user_id, data)

        if coin == 5:
            prefix = (
                f"嗯…今天的黄瓜看着特别鲜嫩…我也…想要一根…{nickname}…"
            )
        else:
            prefix = f"收获完成…黄瓜很新鲜…我们分到了 {coin} 金币…谢谢你，{nickname}…"

        return (
            f"{prefix}\n"
            f"{favor_line}"
            f"当前好感度：{data['favorability']}，金币余额：{data['gold']}。"
        )

    # ------------------
    # 指令：/打工（work）
    # ------------------
    def _handle_work(self, user_id: str, nickname: str) -> str:
        """
        处理“/打工”两段式任务：
        1) 第一次输入：开始打工，锁定 3 小时内不允许其他额外金币指令；
        2) 3 小时后再次输入：进行结算。

        结算奖励：
        - 正常情况：随机 +20 ~ 25 金币
        - 5% 概率：不给金币，直接 +50 好感度（遭遇黑心老板，感谢用户安慰）

        Args:
            user_id: 用户QQ号
            nickname: 用户昵称

        Returns:
            str: 文本回复（中文）
        """
        data = self._ensure_user_data(user_id)
        task = self._get_extra_task(data)

        # 已有其他任务则提示剩余时间
        if task and task.get("type") != "work":
            remain = self._extra_task_remaining_seconds(task)
            tmap = {"farm": "种黄瓜", "live": "演出"}
            other = tmap.get(task.get("type"), "其他任务")
            return (
                f"还在{other}呢！…还需要 {self._format_remaining(remain)}这么久！"
                f"等结束后再来一起打工吧，{nickname}…"
            )

        # 未有任务：启动打工
        if not task:
            self._set_extra_task(data, "work", duration_hours=3)
            self._save_user_data(user_id, data)
            return f"去打工啦…需要三小时…这期间做不了其他的事情了…{nickname}…"

        # 已有 work 任务：检查剩余
        remain = self._extra_task_remaining_seconds(task)
        if remain > 0:
            return f"还在打工啦…还需要 {self._format_remaining(remain)}。{nickname}…"

        # 到期结算
        if random.random() < 0.05:
            # 5%：不给金币 +50好感度
            favor_delta = 50
            data['favorability'] = int(data.get('favorability', 0)) + favor_delta
            # 不增金币
            self._clear_extra_task(data)
            self._save_user_data(user_id, data)
            text = (
                f"今天…有点不顺…遇到了黑心老板…没有拿到打工的报酬…\n"
                f"不过…谢谢你的安慰…我心里变得更勇敢了…\n"
                f"好感度 +{favor_delta}。\n"
                f"当前好感度：{data['favorability']}，金币余额：{data.get('gold', 0)}。"
            )
            return text

        # 正常：20-25金币
        coin = random.randint(20, 25)
        data['gold'] = int(data.get('gold', 0)) + coin
        self._clear_extra_task(data)
        self._save_user_data(user_id, data)
        return (
            f"打工结束啦…虽然有点累…但我们拿到了 {coin} 金币…谢谢你的鼓励，{nickname}…\n"
            f"当前好感度：{data['favorability']}，金币余额：{data['gold']}。"
        )

    # ------------------
    # 指令：/live 或 /演唱会（live）
    # ------------------
    def _handle_live(self, user_id: str, nickname: str) -> str:
        """
        处理“/live /演唱会”两段式任务：
        1) 第一次输入：开始演出，锁定 12 小时；
        2) 12 小时后再次输入：进行结算（固定 +60 金币）。

        Args:
            user_id: 用户QQ号
            nickname: 用户昵称

        Returns:
            str: 文本回复（中文）
        """
        data = self._ensure_user_data(user_id)
        task = self._get_extra_task(data)

        if task and task.get("type") != "live":
            remain = self._extra_task_remaining_seconds(task)
            tmap = {"farm": "种黄瓜", "work": "打工"}
            other = tmap.get(task.get("type"), "其他任务")
            return (
                f"还在{other}呢！…还需要 {self._format_remaining(remain)}这么久！"
                f"等结束后再开一场盛大的演出吧，{nickname}…"
            )

        if not task:
            self._set_extra_task(data, "live", duration_hours=12)
            self._save_user_data(user_id, data)
            return (
                f"我要去舞台啦…这次演出加筹备需要十二小时…一定要来看…{nickname}…\n"
                f"要精心准备live，就先不做其他的…"
            )

        remain = self._extra_task_remaining_seconds(task)
        if remain > 0:
            return f"演出还在进行中…还需要 {self._format_remaining(remain)}。{nickname}…"

        # 到期结算：固定 +60 金币
        coin = 60
        data['gold'] = int(data.get('gold', 0)) + coin
        self._clear_extra_task(data)
        self._save_user_data(user_id, data)
        return (
            f"演出结束…大获成功…谢谢你的鼓励…{nickname}…我们获得了 {coin} 金币！\n"
            f"当前好感度：{data['favorability']}，金币余额：{data['gold']}。"
        )

    def _handle_abort_activity(self, user_id: str, nickname: str) -> str:
        """
        处理“/放弃活动”指令：解除当前用户的额外金币任务，使其立即转为空闲状态。

        行为说明：
        - 若用户当前没有进行额外金币任务：提示已处于空闲状态。
        - 若用户正在进行（种黄瓜/打工/演出）之一：清除任务并提示“已放弃当前活动”。
        - 不改变金币与好感度，仅解除任务锁定。

        Args:
            user_id: 用户QQ号
            nickname: 用户昵称

        Returns:
            str: 文本回复（中文）
        """
        data = self._ensure_user_data(user_id)
        task = self._get_extra_task(data)
        if not task:
            return (
                f"现在没有正在进行的额外活动…你已经处于空闲状态，{nickname}…"
            )

        tmap = {"farm": "种黄瓜", "work": "打工", "live": "演出"}
        current = tmap.get(task.get("type"), "活动")
        self._clear_extra_task(data)
        self._save_user_data(user_id, data)
        return (
            f"已放弃当前活动（{current}）…现在处于空闲状态…{nickname}…\n"
            f"当前好感度：{data['favorability']}，金币余额：{data['gold']}。"
        )

    async def _simulate_mutsmi_response(self, item_name: str, favorability: int, user_id: str, nickname: str) -> str:
        """
        调用硅基流动AI，根据礼物、对方昵称与当前好感度模拟若叶睦的回复；
        并进行清洗与双语格式化（优先中文，括号内附原日文）。

        输出规则：
        - 强制生成「日文」原文；随后尝试将日文机翻为「简体中文」。
        - 最终返回「中文（日本語）」的单行文本；若翻译失败，回退仅日文或默认温柔回复。
        - 兼容历史：若上游仍返回 JSON（{"reply": "..."}），会先清洗为纯文本。

        Args:
            item_name: 礼物名称
            favorability: 当前好感度
            user_id: 用户QQ号（用于上下文日志）

        Returns:
            str: 已清洗并格式化的双语回复文本（中文（日文））
        """
        system_prompt = (
            "你是《BanG Dream!》中的若叶睦(Mutsumi Wakaba)，Ave Mujica的节奏吉他手。"
            "请用她的口吻，对收到的礼物做出自然真诚的回复。"
            "语句简洁、温柔、略微害羞，适度使用省略号。"
            "仅用日文（日语）生成一段自然回复；不要输出JSON；不要包含任何解释或附加说明。"
        )
        user_prompt = (
            f"人设：{self.persona}\n"
            f"对方昵称：{nickname}\n"
            f"当前好感度：{favorability}\n"
            f"收到礼物：{item_name}\n"
            "请生成若叶睦的回复，仅返回纯文本（日文）。"
        )

        try:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            result = await asyncio.to_thread(self.ai_client.chat_completion, messages)

            if result.get('success'):
                raw = result.get('content', '').strip()
                # 统一清洗：剔除JSON包装、代码围栏、前后空白
                ja_text = self._clean_ai_reply_text(raw)
                # 若检测到日文，则尝试翻译为中文
                if ja_text:
                    if self._is_japanese_text(ja_text):
                        zh_text = await self._translate_ja_to_zh(ja_text) or "谢谢你的礼物…我会好好珍惜的。"
                        return self._format_bilingual(zh_text, ja_text)
                    else:
                        # 若未检测到日文（极少数情况），直接作为中文处理
                        return ja_text
            else:
                self.logger.warning(f"AI调用失败: {result}")
        except Exception as e:
            self.logger.error(f"AI回复解析失败: {e}")
        # 回退文本（温柔感谢与温暖氛围）
        ja_fallback = "ありがとうございます…大切に使わせていただきますね…"
        zh_fallback = f"谢谢你送我的{item_name}……我会好好珍惜的。"
        return self._format_bilingual(zh_fallback, ja_fallback)

    def _clean_ai_reply_text(self, content: str) -> str:
        """
        清洗AI返回的文本：
        - 若为JSON字符串（包含 reply 字段），解析并取其值；
        - 去除代码块围栏（```）与可能的前后标记；
        - 统一为单行文本（保留省略号与日文/中文字符）。

        Args:
            content: 原始AI响应文本

        Returns:
            str: 清洗后的纯文本
        """
        if not content:
            return ""
        s = content.strip()
        # 去除三引号代码块
        if s.startswith("```") and s.endswith("```"):
            s = s.strip('`').strip()
        # 尝试解析JSON包装
        try:
            parsed = json.loads(s)
            if isinstance(parsed, dict) and 'reply' in parsed:
                v = str(parsed.get('reply', '')).strip()
                if v:
                    return v
        except Exception:
            pass
        # 去除可能的前缀键名（如 reply: ...）
        m = re.match(r"^reply\s*[:：]\s*(.+)$", s, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        # 归一化空白
        return s.replace('\n', ' ').strip()

    def _is_japanese_text(self, s: str) -> bool:
        """
        简单检测文本是否包含日文字符：
        - 识别平假名(\u3040-\u309F)、片假名(\u30A0-\u30FF)或半宽片假名(\uFF65-\uFF9F)。
        - 该检测为启发式，足以区分绝大多数日文回复。

        Args:
            s: 待检测文本

        Returns:
            bool: 是否包含典型日文字符
        """
        return bool(re.search(r"[\u3040-\u309F\u30A0-\u30FF\uFF65-\uFF9F]", s))

    async def _translate_ja_to_zh(self, ja_text: str) -> Optional[str]:
        """
        将日文翻译为简体中文。

        翻译策略：
        - 使用已配置的 SiliconFlowClient 进行一次短文本翻译；
        - 要求输出纯文本中文，不含解释或JSON；
        - 若失败，返回 None，由上游回退处理。

        Args:
            ja_text: 日文原文

        Returns:
            Optional[str]: 中文译文，失败时为 None
        """
        try:
            system_prompt = "你是一个专业的翻译助手。把输入的日文翻译为简体中文，输出纯文本，不要任何解释。"
            user_prompt = f"请翻译为简体中文：\n{ja_text}"
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            result = await asyncio.to_thread(self.ai_client.chat_completion, messages)
            if result.get('success'):
                zh = self._clean_ai_reply_text(str(result.get('content', '')).strip())
                return zh or None
            else:
                self.logger.warning(f"翻译调用失败: {result}")
        except Exception as e:
            self.logger.error(f"翻译异常: {e}")
        return None

    async def _background_buy_and_send(self, user_id: str, item_name: str, nickname: str, context: Dict[str, Any]) -> None:
        try:
            content = await self._handle_buy(user_id, item_name, nickname)
            if content is None:
                return
            mt = context.get('message_type')
            if mt == 'private':
                target_id = str(context.get('user_id', user_id))
                payload = {
                    "action": "send_private_msg",
                    "params": {
                        "user_id": target_id,
                        "message": [{"type": "text", "data": {"text": content}}]
                    }
                }
            else:
                group_id = str(context.get('group_id', ''))
                if not group_id:
                    return
                payload = {
                    "action": "send_group_msg",
                    "params": {
                        "group_id": group_id,
                        "message": [{"type": "text", "data": {"text": content}}]
                    }
                }
            if hasattr(self, 'server') and self.server:
                await self.server.send_response_to_napcat(payload)
        except Exception:
            pass

    def _format_bilingual(self, zh_text: str, ja_text: str) -> str:
        """
        将中文与对应的日文原文组合为：中文（日本語）。

        展示要求：
        - 优先显示中文；括号内显示日文原文；
        - 保留日文与中文的省略号与语气；
        - 统一为单行输出，便于前端展示。

        Args:
            zh_text: 中文译文
            ja_text: 日文原文

        Returns:
            str: 组合后的展示文本
        """
        zh = (zh_text or "").replace('\n', ' ').strip()
        ja = (ja_text or "").replace('\n', ' ').strip()
        if not zh and not ja:
            return ""
        if zh and ja:
            return f"{zh}（{ja}）"
        return zh or ja

    def _migrate_user_data_dir(self) -> None:
        """
        将用户数据目录从 app_data/mutsmi/users 迁移到 data/mutsmi/users。

        迁移策略：
        - 检测旧目录（基于 DataManager.base_path）是否存在
        - 将所有 .json 文件移动到新目录（存在则覆盖同名文件）
        - 保留原有目录结构，失败不会影响服务运行
        """
        try:
            old_dir = os.path.join(str(self.data_manager.base_path), 'mutsmi', 'users')
            new_dir = self.user_data_dir
            if os.path.isdir(old_dir):
                os.makedirs(new_dir, exist_ok=True)
                for filename in os.listdir(old_dir):
                    if not filename.lower().endswith('.json'):
                        continue
                    src = os.path.join(old_dir, filename)
                    dst = os.path.join(new_dir, filename)
                    try:
                        shutil.move(src, dst)
                        self.log_unified("INFO", f"迁移用户数据: {filename}", group_id="system", user_id="system")
                    except Exception as ie:
                        self.log_unified("ERROR", f"迁移文件失败 {filename}: {ie}", group_id="system", user_id="system")
        except Exception as e:
            # 迁移失败不阻断服务
            self.log_unified("ERROR", f"用户数据目录迁移异常: {e}", group_id="system", user_id="system")

    def get_help_text(self) -> Dict[str, Any]:
        """
        返回服务帮助文本

        Returns:
            Dict[str, Any]: 帮助内容字典
        """
        return {
            "content": (
                "若叶睦互动：\n"
                "- 摸摸睦头人 / 摸摸莫莫 / 摸摸木木 / 摸摸睦睦 / 摸摸若叶睦\n"
                "  当日首次随机+好感（1-6），并获得金币奖励（连续天数递增），新用户首次另加100。\n"
                "- 商品一览：查看可购买礼物（来自 data/mutsmi/shopping.json）。\n"
                "- 买[商品名称]：用金币购买并赠予若叶睦，AI回复并提升好感（2-12）。\n"
                "- /种黄瓜：两段式任务，开始后锁定1小时；到期再次输入收获（90%+10金币；10%+5金币且随机+2~12好感，仅在5金币时增加好感）。\n"
                "- /打工：两段式任务，开始后锁定3小时；到期再次输入结算（20~25金币；5%不给金币但+50好感）。\n"
                "- /live 或 /演唱会：两段式任务，开始后锁定12小时；到期再次输入结算（固定+60金币）。\n"
                "- /放弃活动：立即解除当前额外金币任务（种黄瓜/打工/演出），转为空闲状态（不保留进度）。\n"
            )
        }
