#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
选择服务

当用户输入包含“或者/或/还是/or/、”等连接词的多选内容时，
随机选择其中一个选项，并随机返回预设语录。

本服务不需要前缀"/"，仅在识别到选择语境时响应。
"""

import re
import random
from typing import Dict, Any, Optional, List

from .base_service import BaseService


class ChoiceService(BaseService):
    """无前缀选择服务"""

    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        super().__init__(config, data_manager, text_formatter, server)
        extra_config = self.config.get('choice')
        if isinstance(extra_config, dict) and extra_config:
            merged = self.service_config.copy()
            merged.update(extra_config)
            self.service_config = merged
        self.enabled = self.service_config.get('enabled', True)

        default_phrases = [
            "我觉得{opt}更好。",
            "如果是我的话，我会选择{opt}哦。",
            "要不要试试{opt}呢？",
            "选{opt}可能更稳。",
            "我更偏向{opt}。",
            "直觉告诉我选{opt}！",
            "不如先从{opt}开始试试。",
            "先选{opt}，之后再看效果。",
        ]
        self.phrases: List[str] = self.service_config.get('phrases', default_phrases)

        default_connectors = ["或者", "或是", "或", "还是", "or"]
        default_separators = ["、"]
        self.connector_keywords: List[str] = self.service_config.get('connectors', default_connectors)
        self.enumeration_separators: List[str] = self.service_config.get('separators', default_separators)

        connector_parts = []
        for kw in self.connector_keywords:
            if kw.lower() == 'or':
                connector_parts.append(r"\bor\b")
            else:
                connector_parts.append(re.escape(kw))
        separator_parts = [re.escape(s) for s in self.enumeration_separators]
        # (\s*(连接词)\s* | 分隔符)
        self.connectors_pattern = r'(?:\s*(?:' + '|'.join(connector_parts) + r')\s*|' + '|'.join(separator_parts) + r')'
        # 检测用（不包含分隔符）：出现任一连接词即可认定为选择语境
        self.connectors_detection_pattern = r'(?:' + '|'.join(connector_parts) + r')'

        # 询问提示词（从配置读取，可扩展）
        default_question_keywords = [
            '选', '选择', '哪个', '更好',
            '哪个好', '怎么选', '推荐哪个', '选哪个', '哪个更好'
        ]
        # 如果配置提供空数组，仍回退到默认集合，避免检测失效
        configured_qk = self.service_config.get('question_keywords', default_question_keywords)
        self.question_keywords: List[str] = configured_qk if configured_qk else default_question_keywords
        qk_parts = [re.escape(k) for k in self.question_keywords if isinstance(k, str) and k.strip()]
        self.question_keywords_pattern = r'(?:' + '|'.join(qk_parts) + r')' if qk_parts else r'(?:选|选择|哪个|更好)'

    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        识别并处理选择类消息

        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 额外上下文（如 group_id 等）

        Returns:
            Optional[Dict[str, Any]]: 返回回复字典；无法处理则返回None
        """
        try:
            if not self.validate_input(message):
                return None

            text = message.strip()

            # 前缀命令交给其它服务
            if text.startswith('/'):
                return None

            # 判定是否可能为选择语境
            if not self._looks_like_choice(text):
                return None

            options = self._extract_options(text)
            options = self._dedup_and_filter(options)

            if len(options) < 2:
                return None

            chosen = random.choice(options)
            template = random.choice(self.phrases)
            reply = template.format(opt=chosen)

            # 记录使用日志
            group_id = kwargs.get('group_id')
            self.log_service_usage(user_id, self.service_name, 'choose')
            self.log_unified('INFO', f"识别到选项: {options} -> 选择: {chosen}", group_id, user_id)

            return {
                'content': reply,
                'image_path': None,
                'mixed_message': False
            }

        except Exception as e:
            group_id = kwargs.get('group_id')
            self.log_unified('ERROR', f"选择服务处理异常: {e}", group_id, user_id)
            return None

    def _looks_like_choice(self, text: str) -> bool:
        """判断文本是否具备选择语境"""
        # 包含核心连接词（可配置）
        if re.search(self.connectors_detection_pattern, text, flags=re.IGNORECASE):
            return True
        # 使用枚举分隔符列举并包含询问语气
        has_separator = any(sep in text for sep in (self.enumeration_separators + ['，', ',']))
        if has_separator and re.search(self.question_keywords_pattern, text, flags=re.IGNORECASE):
            return True
        return False

    def _extract_options(self, text: str) -> List[str]:
        """基于连接词与列举标点拆分选项，并做基础清洗"""
        # 将全角/半角逗号统一为第一个分隔符（默认'、'），便于统一拆分
        primary_sep = self.enumeration_separators[0] if self.enumeration_separators else '、'
        normalized = re.sub(r'[，,]', primary_sep, text)
        parts = re.split(self.connectors_pattern, normalized, flags=re.IGNORECASE)

        cleaned: List[str] = []
        for p in parts:
            s = p.strip()
            if not s:
                continue
            # 移除常见疑问尾词与标点
            s = re.sub(r'(选哪个|哪个好|哪个更好|更好吗?|吗|呢|啊|吧|嘛|～|!|！|\.|。|\?|？)$', '', s).strip()
            # 移除前导提示词
            s = re.sub(r'^(选|选择|要不要|要不|不如|试试|试试看)\s*', '', s).strip()
            # 进一步裁剪可能的多余修饰
            s = s.strip('“”"\'（）()<>『』【】')
            if s:
                cleaned.append(s)
        return cleaned

    def _dedup_and_filter(self, options: List[str]) -> List[str]:
        """去重、过滤异常项并保留原顺序"""
        seen = set()
        result: List[str] = []
        for o in options:
            if len(o) > 50:  # 过长文本忽略，避免误伤整段描述
                continue
            if o not in seen:
                seen.add(o)
                result.append(o)
        return result

    def get_help_text(self) -> Dict[str, Any]:
        """
        获取选择服务的帮助文本（委托 HelpService 统一格式化）

        Returns:
            Dict[str, Any]: 帮助信息字典
        """
        try:
            # 延迟导入以避免循环依赖
            from .help_service import HelpService
            helper = HelpService(self.config, self.data_manager, self.text_formatter)
            return helper.get_service_help("选择服务")
        except Exception as e:
            self.log_unified('ERROR', f"获取选择服务帮助失败: {e}", group_id="system", user_id="system")
            return {"error": "获取帮助失败", "message": "请稍后重试"}
