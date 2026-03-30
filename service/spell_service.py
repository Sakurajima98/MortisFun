#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun 项目 施法服务

功能：
- 新增指令 /施法
- 随机发送项目数据目录下 data/shifa 文件夹中的一张图片

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

from typing import Dict, Any, Optional, List
import os
import random
from pathlib import Path

from .base_service import BaseService


class SpellService(BaseService):
    """
    施法服务：接收指令并随机发送图片
    
    指令：
    - /施法 或 施法
    """

    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        super().__init__(config, data_manager, text_formatter, server)
        self.service_name = "spell"  # 内部服务键
        # 支持的图片扩展名
        self.supported_exts: List[str] = [".jpg", ".jpeg", ".png", ".gif", ".webp"]
        # 图片目录（项目根的 data/shifa）
        self.image_dir: Path = Path(self.data_manager.base_path) / "shifa"
        self.log_unified("INFO", "施法服务初始化完成", group_id="system", user_id="system")

    def _collect_images(self) -> List[Path]:
        try:
            if not self.image_dir.exists() or not self.image_dir.is_dir():
                return []
            files = []
            for entry in self.image_dir.iterdir():
                if entry.is_file():
                    ext = entry.suffix.lower()
                    if ext in self.supported_exts:
                        files.append(entry)
            return files
        except Exception as e:
            self.logger.error(f"扫描施法图片目录失败: {e}")
            return []

    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理施法指令。
        支持群聊与私聊。
        """
        try:
            msg = (message or "").strip()
            if msg not in ["/施法", "施法"]:
                return None

            # 使用次数限制（如配置了）
            if not self.check_daily_limit(user_id, action="spell"):
                return {
                    "content": "⏰ 今日施法次数已达上限，请明日再试。",
                    "mixed_message": False
                }

            images = self._collect_images()
            if not images:
                # 明确提示目录位置
                return {
                    "content": "❌ 未找到施法图片。请将图片放到 data/shifa 目录下。",
                    "mixed_message": False
                }

            chosen = random.choice(images)
            content = "🪄 施法成功！随机召唤一张图片～"
            response = {
                "content": content,
                "image_path": str(chosen),
                "mixed_message": True
            }

            # 记录使用
            self.log_service_usage(user_id, "spell", action="spell")
            return response
        except Exception as e:
            return self.handle_error(e, context="施法服务处理")

    def get_help_text(self) -> Dict[str, Any]:
        """
        返回施法服务的帮助信息。
        """
        dir_tip = str(self.image_dir)
        help_content = f"""🪄 施法服务帮助

📋 可用指令：
• /施法 - 随机发送一张施法图片

🖼️ 图片来源：
• 把图片放在 {dir_tip} 目录下
• 支持格式：{', '.join(self.supported_exts)}
"""
        return {"content": help_content, "image_path": None}