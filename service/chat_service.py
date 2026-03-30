#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目对话服务

本文件实现AI对话功能，包括：
1. 持续性对话管理
2. 对话历史记录存储
3. 对话状态控制
4. 大模型API调用
5. 对话超时处理

支持的指令：
- /对话 [消息] - 与AI进行对话
- /开始对话 - 开启对话模式
- /关闭对话 - 关闭对话模式
- /开始新对话 - 重置对话历史并开始新对话
- /对话历史 - 查看对话历史摘要

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

import json
import asyncio
import hashlib
import os
import base64
import mimetypes
from .cache_manager import cache_manager
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from .base_service import BaseService
from .api_client import SiliconFlowClient


class ChatService(BaseService):
    """
    AI对话服务类
    
    负责处理与AI的对话交互，包括对话状态管理、
    历史记录存储、大模型API调用等功能。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """
        初始化对话服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 获取聊天服务配置
        chat_config = config.get('services', {}).get('chat', {})
        
        # API配置
        self.api_provider: str = chat_config.get('api_provider', 'siliconflow')
        self.api_key: str = chat_config.get('api_key', '')
        self.api_url: str = chat_config.get('api_url', 'https://api.siliconflow.cn/v1/chat/completions')
        self.model_name: str = chat_config.get('model', 'deepseek-ai/DeepSeek-V3')
        
        # 对话参数
        self.max_history_length: int = chat_config.get('max_history_length', 10)
        self.conversation_timeout_hours: int = chat_config.get('conversation_timeout_hours', 2)
        self.max_tokens: int = chat_config.get('max_tokens', 2000)
        self.temperature: float = chat_config.get('temperature', 0.7)
        self.enable_thinking: bool = chat_config.get('enable_thinking', False)
        
        # 初始化API客户端 - 构建包含chat服务配置的完整配置
        api_config = config.copy()
        # 将chat服务的API配置映射到siliconflow配置节
        api_config['siliconflow'] = {
            'api_key': self.api_key,
            'base_url': self.api_url.replace('/chat/completions', ''),
            'model': self.model_name,
            'max_tokens': self.max_tokens,
            'temperature': self.temperature,
            'enable_thinking': self.enable_thinking
        }
        self.api_client = SiliconFlowClient(api_config, self.logger)
        
        # 缓存配置
        self.enable_cache: bool = chat_config.get('enable_cache', True)
        self.cache_ttl: int = chat_config.get('cache_ttl_minutes', 30) * 60  # 转换为秒
        
        # 机器人人设
        self.bot_personality: str = self._get_bot_personality()
        
        # 用户对话状态缓存
        self.active_conversations: Dict[str, Any] = {}

        self.chat_data_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 'data', 'chat')
        )

        # 确保数据目录存在
        self._ensure_chat_directories()
    
    def process_message(self, message: str, user_id: str, images: Optional[List[str]] = None, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理对话请求消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            images (Optional[List[str]]): 图片路径或URL列表（支持本地与网络）
            **kwargs: 其他参数
            
        Returns:
            Optional[Dict[str, Any]]: 对话回复，如果无法处理则返回None
        """
        try:
            # 兼容通过 kwargs 传入的 images
            if images is None:
                images_kw = kwargs.get("images")
                if images_kw is not None:
                    images = images_kw
            # 规范化 images 为列表
            if images is not None and not isinstance(images, list):
                images = [images]
            # 使用上下文进行图片规范化（将 file 映射为 url，过滤无效项）
            if images:
                context = kwargs.get("context")
                images = self._normalize_images(images, context)
            # 解析对话指令
            command, content = self._parse_chat_command(message)
            
            if command == "single_chat":
                context = kwargs.get("context", {})
                chat_role = self._get_current_chat_role(user_id)
                prepared = self._prepare_messages_for_ai(user_id, content or "", images, chat_role=chat_role)
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._process_and_send_ai_reply(prepared, context, user_id, chat_role=chat_role))
                except RuntimeError:
                    asyncio.create_task(self._process_and_send_ai_reply(prepared, context, user_id, chat_role=chat_role))
                return None
            
            elif command == "start_conversation":
                # 开始对话：/开始对话
                return self.start_conversation(user_id)
            
            elif command == "end_conversation":
                # 结束对话：/关闭对话
                return self.end_conversation(user_id)
            
            elif command == "reset_conversation":
                # 开始新对话：/开始新对话
                return self.reset_conversation(user_id)
            
            elif command == "conversation_history":
                # 对话历史：/对话历史
                return self.get_conversation_summary(user_id)

            elif command == "switch_role":
                return self._handle_switch_role(user_id, content)

            elif command == "continue_chat":
                if self.is_conversation_active(user_id):
                    context = kwargs.get("context", {})
                    chat_role = self._get_current_chat_role(user_id)
                    prepared = self._prepare_messages_for_ai(user_id, message, images, chat_role=chat_role)
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(self._process_and_send_ai_reply(prepared, context, user_id, images=images, update_history=True, user_message=message, chat_role=chat_role))
                    except RuntimeError:
                        asyncio.create_task(self._process_and_send_ai_reply(prepared, context, user_id, images=images, update_history=True, user_message=message, chat_role=chat_role))
                    return None
                else:
                    return None
            
            else:
                return None
                
        except Exception as e:
            return {"content": f"对话处理出错了：{str(e)}", "image_path": None}

    async def _process_and_send_ai_reply(self, messages: List[Dict[str, Any]], context: Dict[str, Any], user_id: str, images: Optional[List[str]] = None, update_history: bool = False, user_message: Optional[str] = None, chat_role: Optional[str] = None) -> None:
        try:
            response = await asyncio.to_thread(self._call_ai_api, messages)
            if update_history and self.is_conversation_active(user_id, chat_role=chat_role):
                if images and not (user_message and user_message.strip()):
                    self._add_message_to_history(user_id, "user", "[图片消息]", chat_role=chat_role)
                elif images:
                    self._add_message_to_history(user_id, "user", f"{user_message} [图片]", chat_role=chat_role)
                else:
                    self._add_message_to_history(user_id, "user", user_message or "", chat_role=chat_role)
                self._add_message_to_history(user_id, "assistant", response, chat_role=chat_role)
            message_type = context.get("message_type")
            if message_type == "private":
                target_id = str(context.get("user_id", user_id))
                payload = {
                    "action": "send_private_msg",
                    "params": {
                        "user_id": target_id,
                        "message": [{"type": "text", "data": {"text": response}}]
                    }
                }
            else:
                group_id = str(context.get("group_id", ""))
                if not group_id:
                    return
                payload = {
                    "action": "send_group_msg",
                    "params": {
                        "group_id": group_id,
                        "message": [{"type": "text", "data": {"text": response}}]
                    }
                }
            if hasattr(self, "server") and self.server:
                await self.server.send_response_to_napcat(payload)
        except Exception:
            pass
    
    def _single_chat(self, user_id: str, content: str, images: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        处理单次对话
        
        Args:
            user_id (str): 用户ID
            content (str): 对话内容
            images (Optional[List[str]]): 图片路径或URL列表
            
        Returns:
            Dict[str, Any]: 包含AI回复的字典
        """
        try:
            # 准备消息（不包含历史记录）
            messages: List[Dict[str, Any]] = [
                {
                    "role": "system",
                    "content": self._get_bot_personality(user_id=user_id)
                }
            ]

            # 构建用户消息（支持文本+图片）
            if images:
                user_content_blocks: List[Dict[str, Any]] = []
                if content and content.strip():
                    user_content_blocks.append({"type": "text", "text": content})
                user_content_blocks.extend(self._images_to_ai_content_blocks(images))
                messages.append({"role": "user", "content": user_content_blocks})
            else:
                messages.append({"role": "user", "content": content})
            
            # 调用AI API
            response = self._call_ai_api(messages)
            return {"content": response, "image_path": None}
            
        except Exception as e:
            error_message = self._handle_api_error(e)
            return {"content": error_message, "image_path": None}
    
    def _ensure_chat_directories(self):
        """
        确保聊天数据目录存在
        """
        try:
            # 通过DataManager确保目录存在，避免路径问题
            self.data_manager.ensure_directories()
            os.makedirs(self.chat_data_root, exist_ok=True)
            self.log_unified("DEBUG", "聊天数据目录检查完成", group_id="system", user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"创建聊天目录失败：{str(e)}", group_id="system", user_id="system")

    def _sanitize_path_component(self, value: str, default_value: str = "default") -> str:
        if value is None:
            return default_value
        s = str(value).strip()
        if not s:
            return default_value
        invalid_chars = '<>:"/\\\\|?*'
        for ch in invalid_chars:
            s = s.replace(ch, "_")
        s = s.replace("\n", "_").replace("\r", "_").replace("\t", "_")
        while "__" in s:
            s = s.replace("__", "_")
        return s[:80] if len(s) > 80 else s

    def _conversation_cache_key(self, user_id: str, chat_role: str) -> str:
        return f"{user_id}::{chat_role}"

    def _get_profile_file_path(self, user_id: str) -> str:
        safe_user_id = self._sanitize_path_component(user_id, default_value="unknown_user")
        return os.path.join(self.chat_data_root, safe_user_id, "profile.json")

    def _get_conversation_file_path(self, user_id: str, chat_role: str) -> str:
        safe_user_id = self._sanitize_path_component(user_id, default_value="unknown_user")
        safe_role = self._sanitize_path_component(chat_role, default_value="default")
        return os.path.join(self.chat_data_root, safe_user_id, safe_role, "chat.json")

    def _get_chat_roles(self) -> Dict[str, str]:
        prompts = self.config.get("ai_prompts", {}) if isinstance(self.config, dict) else {}
        roles = prompts.get("chat_roles", {})
        if not isinstance(roles, dict):
            return {}

        normalized: Dict[str, str] = {}
        for k, v in roles.items():
            if k is None:
                continue
            role_key = str(k).strip()
            if not role_key:
                continue
            if isinstance(v, str):
                prompt = v
            elif isinstance(v, dict):
                if v.get("use_legacy") is True:
                    prompt = prompts.get("chat_bot_personality", "")
                else:
                    prompt = v.get("prompt", "")
            else:
                continue
            prompt = str(prompt or "").strip()
            if not prompt:
                continue
            normalized[role_key] = prompt
        return normalized

    def _normalize_role_lookup_key(self, name: str) -> str:
        return str(name or "").strip().casefold()

    def _get_chat_role_alias_map(self) -> Dict[str, str]:
        prompts = self.config.get("ai_prompts", {}) if isinstance(self.config, dict) else {}
        roles = prompts.get("chat_roles", {})
        if not isinstance(roles, dict):
            return {}

        alias_to_role: Dict[str, str] = {}
        for role_key, v in roles.items():
            canonical = str(role_key or "").strip()
            if not canonical:
                continue
            if not isinstance(v, dict):
                continue
            aliases = v.get("aliases")
            if not isinstance(aliases, list):
                continue
            for alias in aliases:
                alias_norm = self._normalize_role_lookup_key(alias)
                if not alias_norm:
                    continue
                alias_to_role[alias_norm] = canonical
        return alias_to_role

    def _resolve_chat_role_name(self, name: str, roles: Optional[Dict[str, str]] = None) -> Optional[str]:
        roles = roles if roles is not None else self._get_chat_roles()
        if not roles:
            return None

        raw = str(name or "").strip()
        if not raw:
            return None

        if raw in roles:
            return raw

        raw_norm = self._normalize_role_lookup_key(raw)
        role_ci_lookup: Dict[str, str] = {self._normalize_role_lookup_key(k): k for k in roles.keys()}
        if raw_norm in role_ci_lookup:
            return role_ci_lookup[raw_norm]

        alias_map = self._get_chat_role_alias_map()
        canonical = alias_map.get(raw_norm)
        if canonical and canonical in roles:
            return canonical

        return None

    def _get_default_chat_role(self) -> str:
        prompts = self.config.get("ai_prompts", {}) if isinstance(self.config, dict) else {}
        default_role = str(prompts.get("chat_default_role", "") or "").strip()
        roles = self._get_chat_roles()
        if default_role and default_role in roles:
            return default_role
        if roles:
            return next(iter(roles.keys()))
        return "默认"

    def _load_user_chat_profile(self, user_id: str) -> Dict[str, Any]:
        profile_path = self._get_profile_file_path(user_id)
        profile = self.data_manager.load_data(profile_path) or {}
        return profile if isinstance(profile, dict) else {}

    def _save_user_chat_profile(self, user_id: str, profile: Dict[str, Any]) -> bool:
        profile_path = self._get_profile_file_path(user_id)
        return bool(self.data_manager.save_data(profile_path, profile))

    def _get_current_chat_role(self, user_id: str) -> str:
        roles = self._get_chat_roles()
        default_role = self._get_default_chat_role()
        if not roles:
            return default_role

        profile = self._load_user_chat_profile(user_id)
        current = str(profile.get("current_role", "") or "").strip()
        if current and current in roles:
            return current
        if current:
            resolved = self._resolve_chat_role_name(current, roles=roles)
            if resolved and resolved in roles:
                profile["current_role"] = resolved
                self._save_user_chat_profile(user_id, profile)
                return resolved

        if default_role in roles:
            profile["current_role"] = default_role
            self._save_user_chat_profile(user_id, profile)
            return default_role

        first_role = next(iter(roles.keys()))
        profile["current_role"] = first_role
        self._save_user_chat_profile(user_id, profile)
        return first_role

    def _set_current_chat_role(self, user_id: str, chat_role: str) -> Tuple[bool, str]:
        roles = self._get_chat_roles()
        if not roles:
            return False, "未配置可用角色，请管理员在 config.json 的 ai_prompts.chat_roles 添加角色。"
        role_name = str(chat_role or "").strip()
        if not role_name:
            return False, "用法：/切换角色 角色名"
        resolved = self._resolve_chat_role_name(role_name, roles=roles)
        if not resolved:
            available = "、".join(list(roles.keys())[:30])
            return False, f"未找到角色：{role_name}\n可用角色：{available}\n用法：/切换角色 角色名"
        profile = self._load_user_chat_profile(user_id)
        profile["current_role"] = resolved
        self._save_user_chat_profile(user_id, profile)
        return True, resolved

    def _handle_switch_role(self, user_id: str, content: str) -> Dict[str, Any]:
        roles = self._get_chat_roles()
        if not roles:
            return {"content": "当前未配置可用角色，请管理员在 config.json 的 ai_prompts.chat_roles 添加。", "image_path": None}

        arg = str(content or "").strip()
        if not arg:
            current = self._get_current_chat_role(user_id)
            available = "、".join(list(roles.keys())[:30])
            return {
                "content": f"🎭 当前角色：{current}\n可用角色：{available}\n\n用法：/切换角色 角色名",
                "image_path": None
            }

        ok, msg = self._set_current_chat_role(user_id, arg)
        if not ok:
            return {"content": msg, "image_path": None}

        new_role = msg
        active = self.is_conversation_active(user_id, chat_role=new_role)
        extra = "（该角色对话模式进行中）" if active else "（该角色未开启对话模式，可用 /开始对话）"
        return {"content": f"✅ 已切换角色：{new_role} {extra}", "image_path": None}
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取对话服务的帮助文本
        
        Returns:
            Dict[str, Any]: 包含帮助文本的字典
        """
        help_text = """
🤖 AI对话服务帮助

📝 基本指令：
• /对话 [消息] - 与AI进行单次对话
  示例：/对话 今天天气怎么样

💬 持续对话：
• /开始对话 - 开启持续对话模式
• /关闭对话 - 关闭持续对话模式
• /开始新对话 - 清空历史并开始新对话
• /对话历史 - 查看对话历史摘要
• /切换角色 [角色名] - 切换对话人设（不同角色分别保存历史）

🔧 功能说明：
• 单次对话：每次独立对话，不保存历史
• 持续对话：保存对话历史，支持上下文理解
• 历史记录：最多保存{max_history}轮对话
• 自动清理：{timeout}小时后自动清理过期对话

💡 使用技巧：
• 持续对话模式下，直接发送消息即可对话
• AI会根据对话历史提供更准确的回复
• 可随时使用/关闭对话退出持续模式
""".format(
            max_history=self.max_history_length,
            timeout=self.conversation_timeout_hours
        )
        return {"content": help_text, "image_path": None}
    
    def start_conversation(self, user_id: str) -> Dict[str, Any]:
        """
        开始新对话
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Dict[str, Any]: 包含开始对话确认消息的字典
        """
        try:
            chat_role = self._get_current_chat_role(user_id)
            # 检查是否已经在对话中
            if self.is_conversation_active(user_id, chat_role=chat_role):
                return {"content": "您已经在对话模式中了，可以直接发送消息与我聊天，或使用 /关闭对话 退出", "image_path": None}
            
            # 创建新的对话状态
            conversation_data = {
                'user_id': user_id,
                'chat_role': chat_role,
                'start_time': datetime.now().isoformat(),
                'last_activity': datetime.now().isoformat(),
                'messages': [],
                'active': True
            }
            
            # 保存对话状态
            self.save_conversation_state(user_id, conversation_data, chat_role=chat_role)
            self.active_conversations[self._conversation_cache_key(user_id, chat_role)] = conversation_data
            
            return {"content": "🤖 对话模式已开启\n\n现在您可以直接发送消息与我聊天，我会记住我们的对话内容\n使用 /关闭对话 可以退出对话模式", "image_path": None}
            
        except Exception as e:
            return {"content": f"开启对话失败了：{str(e)}", "image_path": None}
    
    def end_conversation(self, user_id: str) -> Dict[str, Any]:
        """
        结束对话
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Dict[str, Any]: 包含结束对话确认消息的字典
        """
        try:
            chat_role = self._get_current_chat_role(user_id)
            # 检查是否在对话中
            if not self.is_conversation_active(user_id, chat_role=chat_role):
                return {"content": "您当前不在对话模式中", "image_path": None}
            
            # 获取对话数据
            conversation_data = self.load_conversation_state(user_id, chat_role=chat_role)
            if conversation_data:
                # 标记对话为非活跃状态
                conversation_data['active'] = False
                conversation_data['end_time'] = datetime.now().isoformat()
                
                # 保存最终状态
                self.save_conversation_state(user_id, conversation_data, chat_role=chat_role)
            
            # 从活跃对话缓存中移除
            cache_key = self._conversation_cache_key(user_id, chat_role)
            if cache_key in self.active_conversations:
                del self.active_conversations[cache_key]
            
            return {"content": "👋 对话模式已关闭\n\n感谢与我聊天，您的对话历史已保存\n如需重新开始对话，请使用 /开始对话 指令", "image_path": None}
            
        except Exception as e:
            return {"content": f"关闭对话失败了：{str(e)}", "image_path": None}
    
    def reset_conversation(self, user_id: str) -> Dict[str, Any]:
        """
        重置对话历史
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Dict[str, Any]: 包含重置对话确认消息的字典
        """
        try:
            chat_role = self._get_current_chat_role(user_id)
            # 先结束当前对话（如果存在）
            if self.is_conversation_active(user_id, chat_role=chat_role):
                self.end_conversation(user_id)
            
            # 删除历史记录文件
            chat_file = self._get_conversation_file_path(user_id, chat_role)
            if self.data_manager.file_exists(chat_file):
                self.data_manager.delete_file(chat_file)

            legacy_file = f"data/chat/chat_{user_id}.json"
            if self.data_manager.file_exists(legacy_file):
                self.data_manager.delete_file(legacy_file)
            
            # 开始新对话
            start_response = self.start_conversation(user_id)
            return {"content": start_response["content"] + "\n\n🔄 历史记录已清空，这是一个全新的对话", "image_path": None}
            
        except Exception as e:
            return {"content": f"重置对话失败了：{str(e)}", "image_path": None}
    
    def chat_with_ai(self, user_id: str, message: str, images: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        与AI进行对话
        
        Args:
            user_id (str): 用户ID
            message (str): 用户消息
            images (Optional[List[str]]): 图片路径或URL列表
            
        Returns:
            Optional[Dict[str, Any]]: 包含AI回复的字典，如果不需要回复则返回None
        """
        try:
            # 准备消息格式（支持多模态）
            chat_role = self._get_current_chat_role(user_id)
            messages = self._prepare_messages_for_ai(user_id, message, images, chat_role=chat_role)
            
            # 调用AI API
            ai_response = self._call_ai_api(messages)
            
            # 检查是否需要回复（用于持续对话模式）
            if ai_response.strip() == "不需要进行回复":
                return None
            
            # 添加消息到历史记录
            if self.is_conversation_active(user_id):
                # 图片消息历史记录占位（仅保存文本）
                if images and not (message and message.strip()):
                    self._add_message_to_history(user_id, "user", "[图片消息]", chat_role=chat_role)
                elif images:
                    self._add_message_to_history(user_id, "user", f"{message} [图片]", chat_role=chat_role)
                else:
                    self._add_message_to_history(user_id, "user", message, chat_role=chat_role)
                self._add_message_to_history(user_id, "assistant", ai_response, chat_role=chat_role)
            
            return {"content": ai_response, "image_path": None}
            
        except Exception as e:
            error_message = self._handle_api_error(e)
            return {"content": error_message, "image_path": None}
    
    def get_conversation_history(self, user_id: str) -> List[Dict[str, Any]]:
        """
        获取用户对话历史
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            List[Dict[str, Any]]: 对话历史列表
        """
        try:
            chat_role = self._get_current_chat_role(user_id)
            conversation_data = self.load_conversation_state(user_id, chat_role=chat_role)
            if conversation_data and 'messages' in conversation_data:
                return conversation_data['messages']
            return []
        except Exception:
            return []
    
    def get_conversation_summary(self, user_id: str) -> Dict[str, Any]:
        """
        获取对话历史摘要
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Dict[str, Any]: 包含对话历史摘要的字典
        """
        try:
            chat_role = self._get_current_chat_role(user_id)
            conversation_data = self.load_conversation_state(user_id, chat_role=chat_role)
            if not conversation_data:
                return {"content": "您还没有对话历史记录", "image_path": None}
            
            messages = conversation_data.get('messages', [])
            if not messages:
                return {"content": "对话历史为空", "image_path": None}
            
            start_time = conversation_data.get('start_time', '')
            active = conversation_data.get('active', False)
            
            summary = f"📊 对话历史摘要\n\n"
            summary += f"🕐 开始时间：{start_time[:19] if start_time else '未知'}\n"
            summary += f"📝 消息数量：{len(messages)} 条\n"
            summary += f"🔄 状态：{'进行中' if active else '已结束'}\n\n"
            
            # 显示最近几条消息
            recent_messages = messages[-6:] if len(messages) > 6 else messages
            summary += "💬 最近对话：\n"
            
            for msg in recent_messages:
                role = "👤" if msg.get('role') == 'user' else "🤖"
                content = msg.get('content', '')[:50]
                if len(msg.get('content', '')) > 50:
                    content += "..."
                summary += f"{role} {content}\n"
            
            return {"content": summary, "image_path": None}
            
        except Exception as e:
            return {"content": f"获取对话历史失败了：{str(e)}", "image_path": None}
    
    def is_conversation_active(self, user_id: str, chat_role: Optional[str] = None) -> bool:
        """
        检查用户是否处于活跃对话状态
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            bool: 是否处于对话状态
        """
        try:
            resolved_role = chat_role or self._get_current_chat_role(user_id)
            cache_key = self._conversation_cache_key(user_id, resolved_role)
            # 首先检查内存缓存
            if cache_key in self.active_conversations:
                return self.active_conversations[cache_key].get('active', False)
            
            # 从文件加载状态
            conversation_data = self.load_conversation_state(user_id, chat_role=resolved_role)
            if not conversation_data:
                return False
            
            # 检查是否活跃
            active = conversation_data.get('active', False)
            if not active:
                return False
            
            # 检查是否超时
            last_activity = conversation_data.get('last_activity', '')
            if last_activity:
                try:
                    last_time = datetime.fromisoformat(last_activity)
                    timeout_delta = timedelta(hours=self.conversation_timeout_hours)
                    if datetime.now() - last_time > timeout_delta:
                        # 超时，标记为非活跃
                        conversation_data['active'] = False
                        self.save_conversation_state(user_id, conversation_data, chat_role=resolved_role)
                        return False
                except ValueError:
                    pass
            
            # 更新内存缓存
            self.active_conversations[cache_key] = conversation_data
            return True
            
        except Exception:
            return False
    
    def cleanup_expired_conversations(self):
        """
        清理过期的对话会话
        """
        try:
            # 清理内存缓存中的过期对话
            expired_users = []
            for cache_key in list(self.active_conversations.keys()):
                try:
                    user_id, role = cache_key.split("::", 1)
                except ValueError:
                    user_id, role = cache_key, None
                if not self.is_conversation_active(user_id, chat_role=role):
                    expired_users.append(cache_key)
            
            for cache_key in expired_users:
                if cache_key in self.active_conversations:
                    del self.active_conversations[cache_key]
                    
        except Exception as e:
            self.log_unified("ERROR", f"清理过期对话失败：{str(e)}", group_id="system", user_id="system")
    
    def save_conversation_state(self, user_id: str, conversation_data: Dict[str, Any], chat_role: Optional[str] = None):
        """
        保存对话状态
        
        Args:
            user_id (str): 用户ID
            conversation_data (Dict[str, Any]): 对话数据
        """
        try:
            resolved_role = chat_role or conversation_data.get("chat_role") or self._get_current_chat_role(user_id)
            # 更新最后活动时间
            conversation_data['last_activity'] = datetime.now().isoformat()
            conversation_data['chat_role'] = resolved_role
            
            # 保存到文件
            chat_file = self._get_conversation_file_path(user_id, resolved_role)
            self.data_manager.save_data(chat_file, conversation_data)
            self.log_unified("DEBUG", f"对话状态已保存，消息数量: {len(conversation_data.get('messages', []))}", "system", user_id)
            
            # 更新内存缓存
            self.active_conversations[self._conversation_cache_key(user_id, resolved_role)] = conversation_data
            
        except Exception as e:
            self.log_unified("ERROR", f"保存对话状态失败：{str(e)}", group_id="system", user_id=user_id)
    
    def load_conversation_state(self, user_id: str, chat_role: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        加载对话状态
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Optional[Dict[str, Any]]: 对话数据，如果不存在则返回None
        """
        try:
            resolved_role = chat_role or self._get_current_chat_role(user_id)
            cache_key = self._conversation_cache_key(user_id, resolved_role)
            # 首先检查内存缓存
            if cache_key in self.active_conversations:
                self.log_unified("DEBUG", "从内存缓存加载对话状态", "system", user_id)
                return self.active_conversations[cache_key]
            
            # 从文件加载
            chat_file = self._get_conversation_file_path(user_id, resolved_role)
            conversation_data = self.data_manager.load_data(chat_file)

            if not conversation_data:
                conversation_data = self._try_migrate_legacy_conversation(user_id, resolved_role)
            
            if conversation_data:
                # 更新内存缓存
                self.active_conversations[cache_key] = conversation_data
                self.log_unified("DEBUG", f"从文件加载对话状态，消息数量: {len(conversation_data.get('messages', []))}", "system", user_id)
                return conversation_data
            
            return None
            
        except Exception as e:
            self.log_unified("ERROR", f"加载对话状态失败：{str(e)}", group_id="system", user_id=user_id)
            return None

    def _try_migrate_legacy_conversation(self, user_id: str, chat_role: str) -> Optional[Dict[str, Any]]:
        default_role = self._get_default_chat_role()
        if chat_role != default_role:
            return None

        legacy_relative = f"data/chat/chat_{user_id}.json"
        legacy_data = None
        try:
            if self.data_manager.file_exists(legacy_relative):
                legacy_data = self.data_manager.load_data(legacy_relative)
        except Exception:
            legacy_data = None

        if not legacy_data:
            try:
                old_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'chat'))
                legacy_abs = os.path.join(old_root, f"chat_{user_id}.json")
                if os.path.exists(legacy_abs):
                    legacy_data = self.data_manager.load_data(legacy_abs)
            except Exception:
                legacy_data = None

        if not isinstance(legacy_data, dict):
            return None

        legacy_data["chat_role"] = chat_role
        try:
            self.save_conversation_state(user_id, legacy_data, chat_role=chat_role)
        except Exception:
            pass
        return legacy_data
    
    def _call_ai_api(self, messages: List[Dict[str, Any]]) -> str:
        """
        调用AI API获取回复
        
        Args:
            messages (List[Dict[str, Any]]): 对话消息列表（支持字符串或多模态内容块）
            
        Returns:
            str: AI回复内容
        """
        try:
            # 检查缓存
            if self.enable_cache:
                cache_key = self._generate_cache_key(messages)
                api_cache = cache_manager.get_cache("api_responses")
                if api_cache:
                    cached_response = api_cache.get(f"chat_response:{cache_key}")
                    if cached_response:
                        self.logger.info("使用缓存的AI回复")
                        return cached_response
            
            # 调用API客户端
            result = self.api_client.chat_completion(
                messages=messages,
                model=self.model_name,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                enable_thinking=self.enable_thinking
            )
            
            if result.get('success', False):
                content = result.get('content', '')
                
                # 缓存成功的响应
                if self.enable_cache and content:
                    api_cache = cache_manager.get_cache("api_responses")
                    if api_cache:
                        api_cache.set(f"chat_response:{cache_key}", content, ttl=self.cache_ttl)
                
                # 记录API使用统计
                usage = result.get('usage', {})
                response_time = result.get('response_time', 0)
                self.logger.info(f"AI API调用成功 - 响应时间: {response_time:.2f}s, 使用token: {usage}")
                
                return content
            else:
                # 处理API错误
                error_type = result.get('error', 'unknown')
                error_message = result.get('message', 'AI服务调用失败')
                
                self.logger.error(f"AI API调用失败: {error_type} - {error_message}")
                
                # 返回用户友好的错误消息
                return self._get_fallback_response(error_type)
                
        except Exception as e:
            self.logger.error(f"AI API调用异常: {str(e)}")
            return "……AI服务暂时不可用。稍后再试吧。"
    
    def _prepare_messages_for_ai(self, user_id: str, user_message: str, images: Optional[List[str]] = None, chat_role: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        为AI API准备消息格式
        
        Args:
            user_id (str): 用户ID
            user_message (str): 用户消息
            images (Optional[List[str]]): 图片路径或URL列表
            
        Returns:
            List[Dict[str, Any]]: 格式化的消息列表（支持多模态）
        """
        try:
            resolved_role = chat_role or self._get_current_chat_role(user_id)
            messages: List[Dict[str, Any]] = []
            
            # 添加系统人设
            bot_personality = self._get_bot_personality(user_id=user_id, chat_role=resolved_role)
            messages.append({
                "role": "system",
                "content": bot_personality
            })
            
            # 获取历史对话
            conversation_data = self.load_conversation_state(user_id, chat_role=resolved_role)
            if conversation_data and 'messages' in conversation_data:
                history_messages = conversation_data['messages']
                
                # 限制历史消息数量
                if len(history_messages) > self.max_history_length:
                    # 保留最近的消息
                    history_messages = history_messages[-self.max_history_length:]
                
                # 添加历史消息
                for msg in history_messages:
                    if msg.get('role') in ['user', 'assistant']:
                        messages.append({
                            "role": msg['role'],
                            "content": msg['content']
                        })
            
            # 添加当前用户消息（支持文本+图片）
            if images:
                user_content_blocks: List[Dict[str, Any]] = []
                if user_message and user_message.strip():
                    user_content_blocks.append({"type": "text", "text": user_message})
                user_content_blocks.extend(self._images_to_ai_content_blocks(images))
                messages.append({"role": "user", "content": user_content_blocks})
            else:
                messages.append({
                    "role": "user",
                    "content": user_message
                })
            
            return messages
            
        except Exception:
            # 如果出错，至少返回基本的消息结构
            if images:
                user_content_blocks: List[Dict[str, Any]] = []
                if user_message and user_message.strip():
                    user_content_blocks.append({"type": "text", "text": user_message})
                user_content_blocks.extend(self._images_to_ai_content_blocks(images))
                return [
                    {"role": "system", "content": self._get_bot_personality(user_id=user_id, chat_role=chat_role)},
                    {"role": "user", "content": user_content_blocks}
                ]
            else:
                return [
                    {
                        "role": "system",
                        "content": self._get_bot_personality(user_id=user_id, chat_role=chat_role)
                    },
                    {
                        "role": "user",
                        "content": user_message
                    }
                ]
    
    def _images_to_ai_content_blocks(self, images: Optional[List[str]]) -> List[Dict[str, Any]]:
        """
        将图片路径或URL转换为AI可识别的多模态内容块。
        
        兼容两种输入：
        - 远程URL：直接作为 `image_url.url` 传入
        - 本地路径：读取文件并转为 `data:<mime>;base64,<content>` 形式
        
        Args:
            images (Optional[List[str]]): 图片路径或URL列表
        
        Returns:
            List[Dict[str, Any]]: 多模态内容块列表
        """
        blocks: List[Dict[str, Any]] = []
        if not images:
            return blocks

        for img in images:
            if not img:
                continue
            try:
                img_str = str(img).strip()
                # 兼容 CQ:image 原始串或包含 url= 的文本
                if 'url=' in img_str:
                    try:
                        # 提取 url 参数（直到下一个逗号或空白）
                        start = img_str.find('url=') + 4
                        end_candidates = [
                            (img_str.find(',', start), ','),
                            (img_str.find('\n', start), '\n'),
                            (img_str.find(' ', start), ' ')
                        ]
                        # 选择最小的非 -1 位置作为结束
                        ends = [pos for (pos, ch) in end_candidates if pos != -1]
                        end = min(ends) if ends else len(img_str)
                        extracted = img_str[start:end].strip()
                        if extracted.lower().startswith('http://') or extracted.lower().startswith('https://'):
                            blocks.append({
                                "type": "image_url",
                                "image_url": {"url": extracted}
                            })
                            continue
                    except Exception:
                        # 提取失败则继续采用后续逻辑
                        pass
                # 远程图片URL：直接使用
                if img_str.lower().startswith("http://") or img_str.lower().startswith("https://"):
                    blocks.append({
                        "type": "image_url",
                        "image_url": {"url": img_str}
                    })
                    continue

                # 本地文件：转为 data:URL
                file_path = img_str
                if not os.path.exists(file_path):
                    # 许多QQ消息的 file 字段是文件ID而非本地路径，这里仅记录调试信息并跳过
                    self.log_unified("DEBUG", f"图片本地路径不存在或为文件ID，跳过本地读取：{file_path}", group_id="system", user_id="system")
                    continue

                mime_type, _ = mimetypes.guess_type(file_path)
                if not mime_type:
                    mime_type = "image/png"
                with open(file_path, "rb") as f:
                    b64_data = base64.b64encode(f.read()).decode("ascii")
                data_url = f"data:{mime_type};base64,{b64_data}"
                blocks.append({
                    "type": "image_url",
                    "image_url": {"url": data_url}
                })
            except Exception as e:
                self.log_unified("ERROR", f"转换图片为内容块失败：{str(e)}", group_id="system", user_id="system")
                continue
        return blocks

    def _normalize_images(self, images: List[str], context: Optional[Dict[str, Any]]) -> List[str]:
        """
        标准化图片输入列表：
        - 将 QQ 段落里的 file 映射为对应的 url（若存在）
        - 保留本地存在的文件路径与已是 http(s) 的 URL
        - 过滤空值和无法解析的文件ID，避免误读为本地文件
        
        Args:
            images (List[str]): 原始图片列表（可能为文件ID、相对/绝对路径或URL）
            context (Optional[Dict[str, Any]]): 上下文（包含原始消息段），用于建立 file→url 映射
        
        Returns:
            List[str]: 规范化后的图片列表（尽量为可直接使用的路径或URL）
        """
        normalized: List[str] = []
        if not images:
            return normalized

        # 从上下文构建 file→url 映射
        file_to_url = {}
        try:
            if context and isinstance(context.get('message'), list):
                for seg in context['message']:
                    try:
                        if seg.get('type') == 'image':
                            data = seg.get('data', {})
                            f = str(data.get('file', '')).strip()
                            u = str(data.get('url', '')).strip()
                            if f and u:
                                file_to_url[f] = u
                    except Exception:
                        pass
        except Exception:
            pass

        for item in images:
            s = str(item or '').strip()
            if not s:
                continue
            # 已是URL：直接保留
            if s.lower().startswith('http://') or s.lower().startswith('https://'):
                normalized.append(s)
                continue
            # 本地存在的文件路径：保留
            if os.path.exists(s):
                normalized.append(s)
                continue
            # 尝试使用上下文映射 file→url
            if s in file_to_url and file_to_url[s]:
                normalized.append(file_to_url[s])
                continue
            # 无法解析的文件ID：丢弃并记录调试
            self.log_unified("DEBUG", f"无法解析的图片标识（既非URL也非本地路径）：{s}", group_id="system", user_id="system")
        return normalized

    def _get_bot_personality(self, user_id: Optional[str] = None, chat_role: Optional[str] = None) -> str:
        """
        获取机器人人设提示词
        
        Returns:
            str: 机器人人设描述
        """
        # 从配置文件读取AI提示词
        try:
            roles = self._get_chat_roles()
            if roles:
                resolved_role = chat_role
                if not resolved_role and user_id is not None:
                    resolved_role = self._get_current_chat_role(user_id)
                if resolved_role and resolved_role in roles:
                    return roles[resolved_role]

            return self.config.get('ai_prompts', {}).get(
                'chat_bot_personality',
                '你是一个友善的AI助手，请用简洁有趣的方式与用户对话。'
            )
        except Exception:
            return '你是一个友善的AI助手，请用简洁有趣的方式与用户对话。'
    
    def _add_message_to_history(self, user_id: str, role: str, content: str, chat_role: Optional[str] = None):
        """
        添加消息到对话历史
        
        Args:
            user_id (str): 用户ID
            role (str): 消息角色（user/assistant）
            content (str): 消息内容
        """
        try:
            resolved_role = chat_role or self._get_current_chat_role(user_id)
            # 加载现有对话数据
            conversation_data = self.load_conversation_state(user_id, chat_role=resolved_role)
            if not conversation_data:
                conversation_data = {
                    'messages': [],
                    'active': True,
                    'start_time': datetime.now().isoformat(),
                    'last_activity': datetime.now().isoformat()
                }
            conversation_data['chat_role'] = resolved_role
            
            # 添加新消息
            message = {
                'role': role,
                'content': content,
                'timestamp': datetime.now().isoformat()
            }
            
            conversation_data['messages'].append(message)
            
            # 修剪历史记录
            self._trim_conversation_history(conversation_data)
            
            # 保存更新后的数据
            self.save_conversation_state(user_id, conversation_data, chat_role=resolved_role)
            
        except Exception as e:
            self.log_unified("ERROR", f"添加消息到历史失败：{str(e)}", group_id="system", user_id="system")
    
    def _trim_conversation_history(self, conversation_data: Dict[str, Any]):
        """
        修剪对话历史，保持在限制长度内
        
        Args:
            conversation_data (Dict[str, Any]): 对话数据
        """
        try:
            if 'messages' not in conversation_data:
                return
            
            messages = conversation_data['messages']
            
            # 如果消息数量超过限制，保留最近的消息
            max_messages = self.max_history_length * 2  # 允许存储更多历史，但API调用时会限制
            if len(messages) > max_messages:
                # 保留最近的消息
                conversation_data['messages'] = messages[-max_messages:]
                
        except Exception as e:
            self.log_unified("ERROR", f"修剪对话历史失败：{str(e)}", group_id="system", user_id="system")
    
    def _parse_chat_command(self, message: str) -> Tuple[str, str]:
        """
        解析对话指令
        
        Args:
            message (str): 用户消息
            
        Returns:
            Tuple[str, str]: (命令类型, 消息内容)
        """
        try:
            message = message.strip()
            
            # 检查各种命令
            if message == '/开始对话':
                return 'start_conversation', ''
            elif message == '/关闭对话':
                return 'end_conversation', ''
            elif message == '/开始新对话':
                return 'reset_conversation', ''
            elif message == '/对话历史':
                return 'conversation_history', ''
            elif message == '/切换角色':
                return 'switch_role', ''
            elif message.startswith('/切换角色'):
                content = message[len('/切换角色'):].strip()
                return 'switch_role', content
            elif message.startswith('/对话 '):
                # 单次对话
                content = message[3:].strip()  # 移除 '/对话 '
                return 'single_chat', content
            elif message.startswith('/聊天 '):
                # 单次对话的别名
                content = message[3:].strip()  # 移除 '/聊天 '
                return 'single_chat', content
            else:
                # 普通消息（在持续对话模式下）
                return 'continue_chat', message
                
        except Exception:
            return 'continue_chat', message
    
    def _generate_cache_key(self, messages: List[Dict[str, str]]) -> str:
        """
        生成缓存键
        
        Args:
            messages: 消息列表
            
        Returns:
            str: 缓存键
        """
        # 只使用最后几条消息生成缓存键，避免过长
        recent_messages = messages[-3:] if len(messages) > 3 else messages
        content = json.dumps(recent_messages, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    

    
    def _get_fallback_response(self, error_type: str) -> str:
        """
        根据错误类型返回用户友好的错误消息
        
        Args:
            error_type: 错误类型
            
        Returns:
            str: 用户友好的错误消息
        """
        fallback_messages = {
            'timeout': '请求超时了。稍后再试吧。',
            'rate_limit': '请求过于频繁。稍后再试吧。',
            'auth_error': 'API认证失败了。请检查配置。',
            'server_error': 'AI服务暂时不可用。稍后再试吧。',
            'network_error': '网络连接失败了。请检查网络设置。',
            'quota_exceeded': 'API配额已用完。稍后再试吧。'
        }
        
        return fallback_messages.get(error_type, '……AI服务暂时不可用。稍后再试吧。')
    
    def _handle_api_error(self, error: Exception) -> str:
        """
        处理API调用错误
        
        Args:
            error (Exception): 异常对象
            
        Returns:
            str: 错误处理后的用户友好消息
        """
        try:
            error_msg = str(error).lower()
            
            if 'timeout' in error_msg:
                return "⏰ 请求超时了。AI服务响应较慢，稍后再试吧。"
            elif 'connection' in error_msg:
                return "🔌 网络连接失败了。请检查网络状态后重试。"
            elif 'unauthorized' in error_msg or '401' in error_msg:
                return "🔑 API密钥验证失败了。请联系管理员检查配置。"
            elif 'rate limit' in error_msg or '429' in error_msg:
                return "🚦 API调用频率过高。稍后再试吧。"
            elif 'quota' in error_msg or 'billing' in error_msg:
                return "💳 API配额不足了。请联系管理员充值。"
            elif '500' in error_msg or 'server error' in error_msg:
                return "🔧 AI服务暂时不可用。稍后再试吧。"
            else:
                return f"❌ AI服务调用失败了：{str(error)[:100]}"
                
        except Exception:
            return "❌ 未知错误。稍后再试吧。"
