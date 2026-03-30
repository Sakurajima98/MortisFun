#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
会话统计与总结服务

功能说明：
- 基于 DataManager 的聊天记录统计，统计群聊最近若干天的消息活跃情况
- 支持按天统计群聊消息总数与活跃成员消息数，展示“话痨榜”
- 结合 ChatService 的对话历史和大模型，生成用户与机器人的会话总结
- 可选生成带有统计内容的图片，并通过 napcat 发送到 QQ

作者：Assistant
创建时间：2026-01-24
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import os
from pathlib import Path
import time
import json
import html as html_lib
import base64

from .base_service import BaseService
from utils.image_generator import ImageGenerator


class ConversationStatisticsService(BaseService):
    """
    会话统计与总结服务类
    
    负责：
    - 提供「/会话统计」「/对话统计」指令，统计群聊最近若干天的聊天消息情况
    - 提供「/会话总结」「/对话总结」指令，结合历史对话生成总结报告
    - 按需生成统计图片，便于在群聊中直观展示结果
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """
        初始化会话统计服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例，用于访问其他服务与日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 读取服务配置
        self.service_config: Dict[str, Any] = config.get('services', {}).get('conversation_statistics', {})
        self.enabled: bool = self.service_config.get('enabled', True)
        
        # 统计窗口配置
        self.default_days: int = int(self.service_config.get('default_days', 7))
        self.max_days: int = int(self.service_config.get('max_days', 30))
        
        # 是否启用 AI 总结
        self.enable_ai_summary: bool = bool(self.service_config.get('enable_ai_summary', True))
        
        # 图片生成器
        try:
            self.image_generator: Optional[ImageGenerator] = ImageGenerator()
        except Exception:
            self.image_generator = None
        
        # 群聊 HTML 日报提示词模板（来自 log_promote.txt）
        self.group_html_prompt: Optional[str] = None
        try:
            service_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(service_dir)
            prompt_path = os.path.join(project_root, "log_promote.txt")
            if os.path.exists(prompt_path):
                with open(prompt_path, "r", encoding="utf-8") as f:
                    self.group_html_prompt = f.read()
        except Exception:
            self.group_html_prompt = None
        
        # HTML 截图辅助服务实例（按需懒加载）
        self._html_capture_helper = None
        
        self.log_unified(
            "INFO",
            "会话统计服务初始化完成",
            group_id="system",
            user_id="system"
        )
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理会话统计相关指令
        
        支持的指令：
        - /会话统计 [天数]
        - /对话统计 [天数]
        - /会话总结
        - /对话总结
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 其他上下文参数（保留以便扩展）
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果；无法处理时返回 None
        """
        try:
            if not self.is_enabled():
                return None
            
            text = (message or "").strip()
            if not text:
                return None
            
            # 统计指令：/会话统计 或 /对话统计
            if text.startswith("/会话统计") or text.startswith("/对话统计"):
                group_id = kwargs.get("group_id")
                context = kwargs.get("context") or {}
                if not group_id:
                    group_id = context.get("group_id")
                return self._handle_usage_stats_command(text, user_id, group_id=group_id)
            
            # 总结指令：/会话总结 或 /对话总结（支持时间段参数）
            if text.startswith("/会话总结") or text.startswith("/对话总结"):
                group_id = kwargs.get("group_id")
                context = kwargs.get("context") or {}
                if not group_id:
                    group_id = context.get("group_id")
                return self._handle_summary_command(text, user_id, group_id=group_id)
            
            return None
        
        except Exception as e:
            return self.handle_error(e, context="会话统计服务处理指令")
    
    def _handle_usage_stats_command(self, text: str, user_id: str, group_id: Optional[str] = None) -> Dict[str, Any]:
        """
        处理会话统计指令
        
        支持格式：
        - /会话统计
        - /会话统计 7
        - /对话统计
        - /对话统计 14
        """
        # 解析天数参数
        parts = text.split()
        days = self.default_days
        if len(parts) >= 2:
            try:
                days = int(parts[1])
            except ValueError:
                return {
                    "content": "❌ 统计天数必须是数字，例如：/会话统计 7",
                    "image_path": None
                }
        
        if days <= 0:
            days = self.default_days
        if days > self.max_days:
            days = self.max_days
        
        # 记录服务使用
        self.log_service_usage(user_id, "conversation_statistics", action=f"stats_{days}d")
        
        # 优先统计群聊消息；若无群聊上下文则退回到个人服务使用统计
        if group_id:
            group_id_str = str(group_id)
            stats = self.data_manager.get_group_chat_stats(group_id_str, days=days)
            content, image_path = self._build_group_usage_stats_response(group_id_str, stats, days)
        else:
            usage_stats = self.data_manager.get_user_usage_stats(user_id, days=days)
            content, image_path = self._build_user_usage_stats_response(user_id, usage_stats, days)
        return {
            "content": content,
            "image_path": image_path
        }
    
    def _handle_summary_command(self, text: str, user_id: str, group_id: Optional[str] = None) -> Dict[str, Any]:
        """
        处理会话总结指令
        """
        # 记录服务使用
        self.log_service_usage(user_id, "conversation_statistics", action="summary")
        
        parts = text.split(maxsplit=1)
        time_range_str = parts[1].strip() if len(parts) == 2 else None
        
        if group_id:
            content, image_path = self._build_group_conversation_summary(str(group_id), time_range_str)
        else:
            content, image_path = self._build_conversation_summary(user_id)
        return {
            "content": content,
            "image_path": image_path
        }
    
    def _build_group_usage_stats_response(
        self,
        group_id: str,
        stats: Dict[str, Any],
        days: int
    ) -> (str, Optional[str]):
        """
        根据 DataManager 返回的群聊统计数据构建群聊使用统计文本与图片
        """
        daily_stats: List[Dict[str, Any]] = stats.get("daily_stats") or []
        if not daily_stats:
            content = f"📊 最近 {days} 天内，本群还没有记录到聊天数据。"
            return content, None
        
        total_messages = int(stats.get("total_messages", 0))
        user_totals: Dict[str, int] = {
            str(uid): int(count) for uid, count in (stats.get("user_totals") or {}).items()
        }
        
        lines: List[str] = []
        lines.append(f"📊 最近 {days} 天群聊会话统计（群：{group_id}）")
        lines.append("")
        lines.append(f"✅ 总消息数：{total_messages} 条")
        if days > 0:
            avg_per_day = total_messages / days
            lines.append(f"📈 日均消息数：{avg_per_day:.1f} 条/天")
        lines.append(f"👥 活跃成员数：{len(user_totals)} 人")
        
        # 构建话痨榜（按消息数倒序，最多展示前10名）
        if user_totals:
            lines.append("")
            lines.append("🏆 话痨榜（按消息数排序）：")
            sorted_users = sorted(user_totals.items(), key=lambda kv: kv[1], reverse=True)
            for idx, (uid, count) in enumerate(sorted_users[:10], start=1):
                lines.append(f"{idx}. 用户 {uid}：{count} 条消息")
        
        # 构建每日消息数统计
        if daily_stats:
            lines.append("")
            lines.append("📅 每日消息数：")
            # 按日期正序显示
            sorted_daily = sorted(daily_stats, key=lambda x: x.get("date", ""))
            for day_info in sorted_daily:
                date_str = day_info.get("date", "")
                day_total = int(day_info.get("total_messages", 0))
                lines.append(f"- {date_str}：{day_total} 条")
        
        content = "\n".join(lines)
        
        image_path = self._generate_text_image(
            title=f"最近 {days} 天群聊统计",
            lines=lines
        )
        
        return content, image_path
    
    def _build_user_usage_stats_response(
        self,
        user_id: str,
        usage_stats: Dict[str, Any],
        days: int
    ) -> (str, Optional[str]):
        """
        在没有群聊上下文时，回退到个人服务使用统计
        """
        if not usage_stats or not usage_stats.get("daily_stats"):
            content = (
                f"📊 最近 {days} 天内，没有记录到你的服务使用数据。\n"
                f"如果想统计群聊中的聊天活跃情况，请在群聊中使用 /会话统计。"
            )
            return content, None
        
        total_usage = int(usage_stats.get("total_usage", 0))
        service_totals: Dict[str, int] = usage_stats.get("service_totals", {})
        
        chat_usage = int(service_totals.get("chat", 0))
        
        lines: List[str] = []
        lines.append(f"📊 最近 {days} 天服务使用统计（用户：{user_id}）")
        lines.append("")
        lines.append(f"✅ 总交互次数：{total_usage} 次")
        lines.append(f"💬 其中 AI 对话服务(chat)：{chat_usage} 次")
        
        if service_totals:
            lines.append("")
            lines.append("📌 各服务使用次数：")
            sorted_services = sorted(
                service_totals.items(),
                key=lambda kv: kv[1],
                reverse=True
            )
            for name, count in sorted_services:
                display_name = name
                if name == "chat":
                    display_name = "AI 对话(chat)"
                elif name == "help":
                    display_name = "帮助(help)"
                elif name == "team":
                    display_name = "车队(team)"
                lines.append(f"- {display_name}: {int(count)} 次")
        
        content = "\n".join(lines)
        
        image_path = self._generate_text_image(
            title=f"最近 {days} 天服务使用统计",
            lines=lines
        )
        
        return content, image_path
    
    def _build_conversation_summary(self, user_id: str) -> (str, Optional[str]):
        """
        构建会话总结文本与图片
        
        优先调用 ChatService 的对话历史与 AI 接口；
        若不可用，则使用简单的本地总结。
        """
        # 获取 ChatService 实例
        chat_service = self._get_chat_service()
        if not chat_service:
            return (
                "⚠️ 当前未启用 AI 对话服务，无法生成会话总结。\n"
                "你仍然可以使用 /会话统计 查看基础使用统计。",
                None
            )
        
        try:
            # 获取对话历史
            history = chat_service.get_conversation_history(user_id)
        except Exception:
            history = []
        
        if not history:
            return (
                "📭 当前没有可用的对话历史记录，无法生成会话总结。\n"
                "可以先使用 /对话 或 /开始对话 与 AI 互动一段时间后再试。",
                None
            )
        
        # 仅使用最近若干条消息，避免提示词过长
        max_messages = 20
        recent_messages = history[-max_messages:]
        
        # 构建用于 AI 的对话文本
        conversation_lines: List[str] = []
        for msg in recent_messages:
            role = msg.get("role", "user")
            role_label = "用户" if role == "user" else "AI"
            content = str(msg.get("content", "")).replace("\n", " ").strip()
            if not content:
                continue
            conversation_lines.append(f"{role_label}：{content}")
        
        if not conversation_lines:
            return (
                "📭 对话历史中没有有效文本内容，无法生成会话总结。",
                None
            )
        
        header = "📊 会话总结报告\n\n"
        
        # 默认的本地概要（在 AI 不可用或被关闭时使用）
        fallback_preview = "\n".join(conversation_lines[-6:])
        fallback_text = (
            header
            + "当前启用了会话总结功能，但 AI 总结暂时不可用。\n"
            + "以下是最近对话的简单预览（最多 6 条）：\n\n"
            + fallback_preview
        )
        
        if not self.enable_ai_summary:
            image_path = self._generate_text_image(
                title="会话总结",
                lines=fallback_text.splitlines()
            )
            return fallback_text, image_path
        
        # 尝试调用 ChatService 的 AI 接口生成总结
        ai_summary: Optional[str] = None
        try:
            messages_for_ai: List[Dict[str, Any]] = [
                {
                    "role": "system",
                    "content": (
                        "你是一个会话分析助手。请根据给出的用户与 AI 的对话记录，"
                        "用简体中文生成一份简洁的总结报告，内容包括：\n"
                        "1. 本轮对话的主要话题和用户关心点（使用条目列出）；\n"
                        "2. 用户的整体情绪与倾向性（1-2 句）；\n"
                        "3. 给用户的 2 条后续建议或可以继续深入的问题。\n"
                        "请控制在 300 字以内，适合直接发送到 QQ 群聊。"
                    )
                },
                {
                    "role": "user",
                    "content": (
                        "以下是用户最近的对话记录（从旧到新，最多 20 条）：\n\n"
                        + "\n".join(conversation_lines)
                        + "\n\n请按照要求输出总结报告。"
                    )
                }
            ]
            
            if hasattr(chat_service, "_call_ai_api"):
                ai_summary = chat_service._call_ai_api(messages_for_ai)
        except Exception:
            ai_summary = None
        
        if not ai_summary:
            image_path = self._generate_text_image(
                title="会话总结",
                lines=fallback_text.splitlines()
            )
            return fallback_text, image_path
        
        full_text = header + ai_summary.strip()
        image_path = self._generate_text_image(
            title="会话总结",
            lines=[line for line in full_text.splitlines() if line.strip()]
        )
        return full_text, image_path
    
    def _get_chat_service(self):
        """
        从服务器中获取 ChatService 实例
        """
        try:
            if self.server and hasattr(self.server, "services"):
                return self.server.services.get("chat")
        except Exception:
            return None
        return None
    
    def _generate_text_image(self, title: str, lines: List[str]) -> Optional[str]:
        """
        使用 ImageGenerator 将多行文本渲染为图片
        
        Args:
            title (str): 图片标题
            lines (List[str]): 要渲染的文本行列表
            
        Returns:
            Optional[str]: 生成的图片路径；生成失败或未配置图片生成器时返回 None
        """
        if not self.image_generator:
            return None
        
        try:
            # 为了避免图片过长，对行数做一个上限裁剪
            max_lines = 30
            safe_lines = lines[:max_lines]
            return self.image_generator.generate_conversation_summary_image(title, safe_lines)
        except Exception as e:
            self.log_unified(
                "ERROR",
                f"生成会话统计图片失败：{str(e)}",
                group_id="system",
                user_id="system"
            )
            return None
    
    def _build_group_conversation_summary(self, group_id: str, time_range_str: Optional[str]) -> (str, Optional[str]):
        """
        构建群聊在指定时间段内的会话总结
        """
        from datetime import time as dt_time
        
        today = datetime.now().date()
        date_str = today.strftime("%Y-%m-%d")
        
        start_hour = 0
        end_hour = 24
        
        if time_range_str:
            try:
                raw = time_range_str.replace("：", ":").strip()
                if "-" not in raw:
                    raise ValueError("invalid range")
                start_part, end_part = raw.split("-", 1)
                start_hour = int(start_part.split(":")[0])
                end_hour = int(end_part.split(":")[0])
                if not (0 <= start_hour < 24 and 0 <= end_hour <= 24 and start_hour < end_hour):
                    raise ValueError("hour out of range")
            except Exception:
                return (
                    "❌ 时间段格式错误，请使用形如：/会话总结 12-15，表示今天12点到15点。",
                    None
                )
        
        start_dt = datetime.combine(today, dt_time(hour=start_hour, minute=0, second=0))
        end_dt = datetime.combine(today, dt_time(hour=end_hour, minute=0, second=0))
        
        messages = self.data_manager.get_group_chat_messages(group_id, start_dt)
        filtered: List[Dict[str, Any]] = []
        for msg in messages:
            try:
                t_str = str(msg.get("time", ""))
                if not t_str:
                    continue
                msg_dt = datetime.fromisoformat(t_str)
                if start_dt <= msg_dt < end_dt:
                    filtered.append(msg)
            except Exception:
                continue
        
        if not filtered:
            return (
                f"📭 本群在今天 {start_hour:02d}:00 - {end_hour:02d}:00 时间段内没有记录到聊天消息，无法生成会话总结。",
                None
            )
        
        filtered = filtered[-200:]
        
        member_name_map: Dict[str, str] = {}
        try:
            service_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(service_dir)
            csv_path = os.path.join(project_root, "data", "group_members", f"group_members_{str(group_id)}.csv")
            if os.path.exists(csv_path):
                import csv
                with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        uid = str(row.get("用户ID") or row.get("QQ号") or "").strip()
                        name = str(row.get("群名称") or row.get("昵称") or "").strip()
                        if uid and name:
                            member_name_map[uid] = name
        except Exception:
            member_name_map = {}
        
        alias_map: Dict[str, str] = {}
        conversation_lines: List[str] = []
        for msg in filtered:
            uid = str(msg.get("user_id", "") or "").strip()
            display_name = str(msg.get("display_name") or "").strip()
            if (
                uid
                and member_name_map
                and (
                    (not display_name)
                    or display_name == uid
                    or display_name.isdigit()
                    or (display_name.startswith("用户") and display_name[2:].isdigit())
                )
            ):
                mapped = str(member_name_map.get(uid) or "").strip()
                if mapped:
                    display_name = mapped
            if not display_name or display_name == uid or display_name.isdigit() or (display_name.startswith("用户") and display_name[2:].isdigit()):
                if uid:
                    if uid not in alias_map:
                        alias_map[uid] = f"群友{len(alias_map) + 1}"
                    display_name = alias_map[uid]
                else:
                    display_name = "群友"
            content = str(msg.get("content", "")).replace("\n", " ").strip()
            if not content:
                continue
            conversation_lines.append(f"{display_name}：{content}")
        
        if not conversation_lines:
            return (
                f"📭 本群在指定时间段内没有有效文本消息，无法生成会话总结。",
                None
            )
        
        header = (
            "📊 群聊会话总结报告\n\n"
            f"群号：{group_id}\n"
            f"时间范围：今天 {start_hour:02d}:00 - {end_hour:02d}:00\n\n"
        )
        
        fallback_preview = "\n".join(conversation_lines[-20:])
        fallback_text = (
            header
            + "当前启用了会话总结功能，但 AI 总结暂时不可用。\n"
            + "以下是该时间段内部分聊天内容预览（最多 20 条）：\n\n"
            + fallback_preview
        )
        
        if not self.enable_ai_summary:
            image_path = self._generate_text_image(
                title="群聊会话总结",
                lines=fallback_text.splitlines()
            )
            return fallback_text, image_path
        
        chat_service = self._get_chat_service()
        ai_summary: Optional[str] = None
        try:
            style_guide = self.group_html_prompt or ""
            system_content = (
                "你是一个群聊数据分析助手。根据给定的群聊消息记录，生成一个 JSON 对象，"
                "用于后端渲染固定风格的群聊日报 HTML 页面。\n"
                "必须严格输出合法的 JSON 文本，且顶层是一个对象，不要输出任何解释、注释、Markdown 或额外内容。\n"
                "JSON 顶层结构要求如下（字段可以为空，但必须存在）：\n"
                "{\n"
                '  \"meta\": {\n'
                '    \"group_id\": \"群号字符串\",\n'
                '    \"group_name\": \"群名称\",\n'
                '    \"date\": \"日期字符串，例如 2026-01-24\",\n'
                '    \"time_range\": \"时间范围描述，例如 12:00-15:00\",\n'
                '    \"total_messages\": 统计窗口内的消息总数,\n'
                '    \"active_members\": 活跃成员数量,\n'
                '    \"overall_summary\": \"用 2-4 句总结今日群聊的整体情况\"\n'
                "  },\n"
                '  \"hot_topics\": [\n'
                "    {\n"
                '      \"title\": \"话题标题\",\n'
                '      \"category\": \"话题类别，例如 技术/游戏/日常/其他\",\n'
                '      \"keywords\": [\"关键词1\", \"关键词2\"],\n'
                '      \"message_count\": 与该话题相关的大致消息条数,\n'
                '      \"summary\": \"1-3 句描述该话题的主要讨论内容\"\n'
                "    }\n"
                "  ],\n"
                '  \"important_messages\": [\n'
                "    {\n"
                '      \"title\": \"简短标题，例如 重要公告/版本更新/活动通知\",\n'
                '      \"sender\": \"发送者昵称\",\n'
                '      \"time\": \"大致时间，例如 14:35\",\n'
                '      \"type\": \"消息类型，例如 公告/问题/答复/其他\",\n'
                '      \"priority\": \"high/medium/low\",\n'
                '      \"content\": \"消息的核心内容，1-3 句\"\n'
                "    }\n"
                "  ],\n"
                '  \"qa_list\": [\n'
                "    {\n"
                '      \"questioner\": \"提问者昵称\",\n'
                '      \"question\": \"问题内容\",\n'
                '      \"answerer\": \"主要回答者昵称，如果没有就写 空\",\n'
                '      \"answer\": \"回答要点的总结，如果没有就写 空\",\n'
                '      \"tags\": [\"标签1\", \"标签2\"]\n'
                "    }\n"
                "  ],\n"
                '  \"active_members\": [\n'
                "    {\n"
                '      \"name\": \"成员昵称\",\n'
                '      \"message_count\": 该成员的大致发言条数,\n'
                '      \"remark\": \"一句话描述该成员今日表现，例如 话痨王者/气氛担当 等\"\n'
                "    }\n"
                "  ]\n"
                "}\n"
                "所有字段内容必须与提供的群聊记录相符，不要虚构严重偏离记录的内容。\n"
                "以下是日报页面的风格与内容偏好说明，仅供你理解，不影响输出格式：\n"
                + style_guide
            )
            messages_for_ai: List[Dict[str, Any]] = [
                {
                    "role": "system",
                    "content": system_content
                },
                {
                    "role": "user",
                    "content": (
                        f"群聊标识：{group_id}\n"
                        f"时间范围：今天 {start_hour:02d}:00 - {end_hour:02d}:00\n"
                        f"本次统计窗口内共记录到 {len(filtered)} 条消息（已按时间升序截取至最多 200 条）。\n\n"
                        "下面是该时间段内的部分群聊消息记录，从旧到新，每行格式为“昵称：内容”：\n\n"
                        + "\n".join(conversation_lines)
                    )
                }
            ]
            if chat_service and hasattr(chat_service, "_call_ai_api"):
                ai_summary = chat_service._call_ai_api(messages_for_ai)
        except Exception:
            ai_summary = None
        if not ai_summary:
            image_path = self._generate_text_image(
                title="群聊会话总结",
                lines=fallback_text.splitlines()
            )
            return fallback_text, image_path
        summary_data: Optional[Dict[str, Any]] = None
        try:
            parsed = json.loads(str(ai_summary))
            if isinstance(parsed, dict):
                summary_data = parsed
        except Exception:
            summary_data = None
        if not summary_data:
            image_path = self._generate_text_image(
                title="群聊会话总结",
                lines=fallback_text.splitlines()
            )
            return fallback_text, image_path
        html_content = self._build_group_summary_html_from_json(
            group_id=str(group_id),
            date_str=date_str,
            start_hour=start_hour,
            end_hour=end_hour,
            total_messages=len(filtered),
            data=summary_data
        )
        if not html_content.strip():
            image_path = self._generate_text_image(
                title="群聊会话总结",
                lines=fallback_text.splitlines()
            )
            return fallback_text, image_path
        screenshot_path = self._render_html_to_image(html_content)
        if screenshot_path:
            return "", screenshot_path
        return fallback_text, None

    def _build_group_summary_html_from_json(
        self,
        group_id: str,
        date_str: str,
        start_hour: int,
        end_hour: int,
        total_messages: int,
        data: Dict[str, Any]
    ) -> str:
        def esc(value: Any) -> str:
            if value is None:
                return ""
            return html_lib.escape(str(value), quote=True)
        meta = data.get("meta") or {}
        meta_group_name = str(meta.get("group_name") or "").strip()
        if meta_group_name:
            group_name = meta_group_name
        else:
            group_name = f"群聊 {group_id}"
        group_name_html = esc(group_name)
        group_id_html = esc(meta.get("group_id") or group_id)
        date_html = esc(meta.get("date") or date_str)
        time_range_html = esc(meta.get("time_range") or f"{start_hour:02d}:00 - {end_hour:02d}:00")
        total_messages_value = meta.get("total_messages")
        if isinstance(total_messages_value, int) and total_messages_value > 0:
            total_messages_html = esc(total_messages_value)
        else:
            total_messages_html = esc(total_messages)
        active_members_value = meta.get("active_members")
        if isinstance(active_members_value, int) and active_members_value > 0:
            active_members_html = esc(active_members_value)
        else:
            active_members_html = ""
        overall_summary_html = esc(meta.get("overall_summary") or "")
        hot_topics = data.get("hot_topics") or []
        hot_topic_cards: List[str] = []
        for item in hot_topics:
            title = esc(item.get("title") or "")
            if not title:
                continue
            category = esc(item.get("category") or "")
            keywords_source = item.get("keywords") or []
            keywords_list: List[str] = []
            for kw in keywords_source:
                text = str(kw).strip()
                if text:
                    keywords_list.append(esc(text))
            keywords_html = "".join(
                f'<span class="keyword">{kw}</span>' for kw in keywords_list
            )
            message_count_html = esc(item.get("message_count") or "")
            summary_html = esc(item.get("summary") or "")
            hot_topic_cards.append(
                f'''
                <article class="topic-card">
                    <div class="topic-category">{category}</div>
                    <h3>{title}</h3>
                    <div class="topic-keywords">{keywords_html}</div>
                    <p class="topic-mentions">相关消息数：{message_count_html}</p>
                    <p>{summary_html}</p>
                </article>
                '''
            )
        if hot_topic_cards:
            hot_topics_html = "\n".join(hot_topic_cards)
        else:
            hot_topics_html = "<p>今日暂无明显讨论热点。</p>"
        important_messages = data.get("important_messages") or []
        important_cards: List[str] = []
        for item in important_messages:
            title = esc(item.get("title") or "")
            content = esc(item.get("content") or "")
            if not title and not content:
                continue
            sender = esc(item.get("sender") or "")
            time_text = esc(item.get("time") or "")
            msg_type = esc(item.get("type") or "")
            priority = str(item.get("priority") or "").lower().strip()
            if priority not in ("high", "medium", "low"):
                priority = "medium"
            priority_class = f"priority-{priority}"
            priority_label_map = {
                "high": "高",
                "medium": "中",
                "low": "低",
            }
            priority_label = priority_label_map.get(priority, "中")
            important_cards.append(
                f'''
                <article class="message-card">
                    <div class="message-meta">
                        <span>{sender}</span>
                        <span>{time_text}</span>
                        <span class="message-type">{msg_type}</span>
                        <span class="priority {priority_class}">优先级：{priority_label}</span>
                    </div>
                    <h3>{title}</h3>
                    <p>{content}</p>
                </article>
                '''
            )
        if important_cards:
            important_html = "\n".join(important_cards)
        else:
            important_html = "<p>今日暂无需要特别关注的重要消息。</p>"
        qa_list = data.get("qa_list") or []
        qa_cards: List[str] = []
        for item in qa_list:
            question = esc(item.get("question") or "")
            answer = esc(item.get("answer") or "")
            if not question and not answer:
                continue
            questioner = esc(item.get("questioner") or "")
            answerer = esc(item.get("answerer") or "")
            tags_source = item.get("tags") or []
            tag_spans: List[str] = []
            for tag in tags_source:
                text = str(tag).strip()
                if text:
                    tag_spans.append(f'<span class="tag">{esc(text)}</span>')
            tags_html = "".join(tag_spans)
            qa_cards.append(
                f'''
                <article class="qa-card">
                    <div class="question">
                        <div class="question-meta">提问者：{questioner}</div>
                        <h3>问题</h3>
                        <p>{question}</p>
                        <div class="question-tags">{tags_html}</div>
                    </div>
                    <div class="answer">
                        <div class="answer-meta">主要回答者：{answerer}</div>
                        <h3>回答要点</h3>
                        <p>{answer}</p>
                    </div>
                </article>
                '''
            )
        if qa_cards:
            qa_html = "\n".join(qa_cards)
        else:
            qa_html = "<p>今日暂无典型的问答交流。</p>"
        active_members = data.get("active_members") or []
        participant_items: List[str] = []
        for item in active_members:
            name = esc(item.get("name") or "")
            if not name:
                continue
            count_value = item.get("message_count")
            if isinstance(count_value, int):
                count_html = esc(count_value)
            else:
                count_html = esc(str(count_value or ""))
            remark_html = esc(item.get("remark") or "")
            participant_items.append(
                f'''
                <div class="participant-item">
                    <h3>{name}</h3>
                    <p>消息条数：{count_html}</p>
                    <p>{remark_html}</p>
                </div>
                '''
            )
        if participant_items:
            participants_html = "\n".join(participant_items)
        else:
            participants_html = "<p>今日暂无明显活跃成员统计。</p>"
        css = """
:root {
    --bg-primary: #0f0e17;
    --bg-secondary: #1a1925;
    --bg-tertiary: #252336;
    --text-primary: #fffffe;
    --text-secondary: #a7a9be;
    --accent-primary: #ff8906;
    --accent-secondary: #f25f4c;
    --accent-tertiary: #e53170;
    --accent-blue: #3da9fc;
    --accent-purple: #7209b7;
    --accent-cyan: #00b4d8;
}
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}
body {
    font-family: 'SF Pro Display', 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', sans-serif;
    background-color: var(--bg-primary);
    color: var(--text-primary);
    line-height: 1.6;
    font-size: 16px;
    width: 1200px;
    margin: 0 auto;
    padding: 20px;
}
header {
    text-align: center;
    padding: 30px 0;
    background-color: var(--bg-secondary);
    margin-bottom: 30px;
}
h1 {
    font-size: 36px;
    font-weight: 700;
    color: var(--accent-primary);
    margin-bottom: 10px;
}
.date {
    font-size: 18px;
    color: var(--text-secondary);
    margin-bottom: 20px;
}
.meta-info {
    display: flex;
    justify-content: center;
    gap: 20px;
}
.meta-info span {
    background-color: var(--bg-tertiary);
    padding: 5px 15px;
    border-radius: 20px;
    font-size: 14px;
}
section {
    background-color: var(--bg-secondary);
    margin-bottom: 30px;
    padding: 25px;
}
h2 {
    font-size: 28px;
    font-weight: 600;
    color: var(--accent-blue);
    margin-bottom: 20px;
    padding-bottom: 10px;
    border-bottom: 2px solid var(--accent-blue);
}
h3 {
    font-size: 22px;
    font-weight: 600;
    color: var(--accent-primary);
    margin: 15px 0 10px 0;
}
p {
    margin-bottom: 15px;
}
.topics-container,
.messages-container,
.qa-container,
.participants-container {
    display: grid;
    grid-template-columns: 1fr;
    gap: 20px;
}
.topic-card,
.message-card,
.qa-card,
.participant-item {
    background-color: var(--bg-tertiary);
    padding: 20px;
}
.topic-category {
    display: inline-block;
    background-color: var(--accent-blue);
    color: var(--text-primary);
    padding: 3px 10px;
    border-radius: 15px;
    font-size: 14px;
    margin-bottom: 10px;
}
.topic-keywords {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 10px 0;
}
.keyword {
    background-color: rgba(61, 169, 252, 0.2);
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 14px;
}
.topic-mentions {
    color: var(--accent-cyan);
    font-weight: 600;
}
.message-meta span {
    margin-right: 15px;
    font-size: 14px;
}
.message-type {
    background-color: var(--accent-tertiary);
    color: var(--text-primary);
    padding: 3px 10px;
    border-radius: 15px;
}
.priority {
    padding: 3px 10px;
    border-radius: 15px;
}
.priority-high {
    background-color: var(--accent-secondary);
}
.priority-medium {
    background-color: var(--accent-primary);
}
.priority-low {
    background-color: var(--accent-blue);
}
.question-meta,
.answer-meta {
    color: var(--text-secondary);
    margin-bottom: 5px;
    font-size: 14px;
}
.question-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 10px;
}
.tag {
    background-color: rgba(114, 9, 183, 0.2);
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 14px;
}
"""
        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>群聊日报 - {date_html}</title>
    <style>
{css}
    </style>
</head>
<body>
    <header>
        <h1>群聊日报</h1>
        <div class="date">{date_html}</div>
        <div class="meta-info">
            <span>群名称：{group_name_html}</span>
            <span>群号：{group_id_html}</span>
            <span>时间范围：{time_range_html}</span>
            <span>消息总数：{total_messages_html}</span>
        </div>
    </header>
    <main>
        <section>
            <h2>今日概览</h2>
            <p>{overall_summary_html}</p>
        </section>
        <section>
            <h2>今日讨论热点</h2>
            <div class="topics-container">
                {hot_topics_html}
            </div>
        </section>
        <section>
            <h2>重要消息汇总</h2>
            <div class="messages-container">
                {important_html}
            </div>
        </section>
        <section>
            <h2>典型问答与讨论</h2>
            <div class="qa-container">
                {qa_html}
            </div>
        </section>
        <section>
            <h2>活跃成员一览</h2>
            <div class="participants-container">
                {participants_html}
            </div>
        </section>
    </main>
</body>
</html>"""
        return html

    def _render_html_to_image(self, html_content: str) -> Optional[str]:
        try:
            html_text = (html_content or "").strip()
            if not html_text:
                return None
            
            # 保存 HTML 到临时文件
            service_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(service_dir)
            html_dir = os.path.join(project_root, "data", "web_capture", "summary_html")
            os.makedirs(html_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            html_path = os.path.join(html_dir, f"group_summary_{ts}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_text)
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            out_dir = os.path.join(project_root, "data", "web_capture", "screenshots")
            os.makedirs(out_dir, exist_ok=True)
            screenshot_path = os.path.join(out_dir, f"screenshot_{ts}.png")
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--hide-scrollbars")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1400,900")
            driver = webdriver.Chrome(options=options)
            file_url = Path(html_path).as_uri()
            driver.get(file_url)
            time.sleep(1)
            try:
                scroll_width = driver.execute_script("return document.documentElement.scrollWidth || document.body.scrollWidth || 1280;")
                scroll_height = driver.execute_script("return document.documentElement.scrollHeight || document.body.scrollHeight || 720;")
                max_height = 20000
                height = max(720, min(int(scroll_height), max_height))
                width = max(1400, int(scroll_width))
                try:
                    driver.execute_cdp_cmd(
                        "Emulation.setDeviceMetricsOverride",
                        {
                            "mobile": False,
                            "width": int(width),
                            "height": int(height),
                            "deviceScaleFactor": 1,
                        },
                    )
                    png = driver.execute_cdp_cmd(
                        "Page.captureScreenshot",
                        {
                            "format": "png",
                            "fromSurface": True,
                            "captureBeyondViewport": True,
                        },
                    )
                    if isinstance(png, dict) and png.get("data"):
                        with open(screenshot_path, "wb") as out_f:
                            out_f.write(base64.b64decode(png["data"]))
                        driver.quit()
                        return screenshot_path if os.path.exists(screenshot_path) else None
                except Exception:
                    pass
                driver.set_window_size(width, height)
                time.sleep(0.5)
            except Exception:
                pass
            driver.save_screenshot(screenshot_path)
            driver.quit()
            return screenshot_path if os.path.exists(screenshot_path) else None
        except Exception:
            return None
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取会话统计服务的帮助文本
        
        Returns:
            Dict[str, Any]: 帮助信息字典
        """
        return {
            "title": "📊 会话统计与总结功能指南",
            "description": (
                "统计你最近的服务使用情况，并基于 AI 对话历史生成总结报告，"
                "帮助你回顾近期与机器人的互动。"
            ),
            "commands": {
                "会话统计": {
                    "format": "/会话统计 [天数]\n/对话统计 [天数]",
                    "examples": [
                        "/会话统计",
                        "/会话统计 7",
                        "/对话统计 14"
                    ],
                    "description": (
                        "在群聊中统计最近若干天的聊天活跃情况，展示总消息数、"
                        "日均消息数、活跃成员数量以及话痨榜等信息。"
                    )
                },
                "会话总结": {
                    "format": "/会话总结 [起始小时-结束小时]\n/对话总结 [起始小时-结束小时]",
                    "examples": [
                        "/会话总结",
                        "/会话总结 12-15"
                    ],
                    "description": (
                        "在群聊中，对今天指定时间段内的聊天内容进行总结，"
                        "包括主要话题、聊天氛围和后续建议；在私聊中，则基于"
                        "你与机器人的最近对话记录生成总结报告。"
                    )
                }
            },
            "tips": [
                "会话统计需要在群聊中使用，默认统计最近 7 天，最多支持 30 天。",
                "如果在私聊中使用，会退回到个人服务使用统计结果。",
                "会话总结在群聊中支持时间段参数，例如：/会话总结 12-15。",
                "私聊中使用会话总结依赖 chat 对话服务，请确保其已启用并有历史记录。",
                "图片生成失败时，仍会返回完整的文本统计结果。"
            ]
        }

