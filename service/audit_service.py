#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""审核服务（AuditService）"""

import os
import json
import random
from typing import Any, Dict, List, Optional

from .base_service import BaseService


class AuditService(BaseService):
    """审核服务类"""

    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """初始化审核服务"""
        super().__init__(config, data_manager, text_formatter, server)
        service_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(service_dir)
        self.questions_file: str = os.path.join(project_root, 'data', 'SIN', 'question.json')
        self.commands: List[str] = ['/审核问答', '/审核题目']
        self.log_unified('INFO', f"审核服务初始化完成，题库路径: {self.questions_file}", group_id='system', user_id='system')

    def get_help_text(self) -> Dict[str, Any]:
        """获取审核服务的帮助文本"""
        return {
            'name': '审核服务',
            'description': '提供审核题库查询，支持随机抽取与全部列出',
            'commands': [
                {'command': '/审核问答', 'description': '随机返回三条题目'},
                {'command': '/审核题目', 'description': '返回全部题目'}
            ],
            'examples': ['/审核问答', '/审核题目'],
            'status': 'enabled' if self.is_enabled() else 'disabled',
        }

    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """处理审核指令消息"""
        text = str(message).strip()
        if text not in self.commands:
            return None
        self.log_service_usage(user_id, '审核服务', 'audit_query')
        if text == '/审核问答':
            questions = self._load_questions()
            if not questions:
                return {
                    'content': self.text_formatter.format_error_message('data_error', '未找到审核题库或内容为空'),
                    'image_path': None
                }
            sample_count = 3 if len(questions) >= 3 else len(questions)
            picked = random.sample(questions, sample_count)
            content = self._format_question_list(picked, title='随机审核问答（3条）')
            return {'content': content, 'image_path': None}
        if text == '/审核题目':
            questions = self._load_questions()
            if not questions:
                return {
                    'content': self.text_formatter.format_error_message('data_error', '未找到审核题库或内容为空'),
                    'image_path': None
                }
            content = self._format_question_list(questions, title='全部审核题目')
            return {'content': content, 'image_path': None}
        return None

    def _load_questions(self) -> List[str]:
        """加载题目列表"""
        try:
            if not os.path.exists(self.questions_file):
                return []
            with open(self.questions_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                return [str(x).strip() for x in data if str(x).strip()]
            if isinstance(data, dict):
                if 'questions' in data and isinstance(data['questions'], list):
                    return [str(x).strip() for x in data['questions'] if str(x).strip()]
                values = [v for v in data.values() if isinstance(v, str)]
                if values:
                    return [v.strip() for v in values if v.strip()]
            return []
        except Exception:
            return []

    def _format_question_list(self, questions: List[str], title: str) -> str:
        """格式化题目列表为可读文本"""
        lines: List[str] = []
        lines.append(f"📋 {title}")
        lines.append("")
        for idx, q in enumerate(questions, start=1):
            clean_q = self.text_formatter.clean_text(q) if hasattr(self.text_formatter, 'clean_text') else q
            lines.append(f"{idx}. {clean_q}")
        return "\n".join(lines)

