#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目用户管理服务

本文件实现用户管理相关功能，包括：
1. 用户注册功能
2. 密码修改功能
3. 群聊添加功能
4. 用户数据验证和管理

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

import os
import json
import re
from typing import Dict, Any, Optional, List
from datetime import datetime
from service.base_service import BaseService


class UserManagementService(BaseService):
    """
    用户管理服务类
    
    负责处理用户注册、密码修改、群聊管理等功能。
    提供完整的用户生命周期管理服务。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None):
        """
        初始化用户管理服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 用户数据存储路径
        self.users_dir = os.path.join("app_data", "web_user", "users")
        
        # 确保用户目录存在
        os.makedirs(self.users_dir, exist_ok=True)
        
        # 支持的指令
        self.commands = {
            "/注册": self._handle_register,
            "/修改密码": self._handle_change_password,
            "/增加群聊": self._handle_add_group
        }
        
        self.log_unified("INFO", "用户管理服务初始化完成")
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理用户消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户QQ号
            **kwargs: 其他参数，包含群聊信息等
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果
        """
        try:
            # 获取群聊ID
            group_id = kwargs.get('group_id')
            
            # 检查是否为支持的指令
            for command, handler in self.commands.items():
                if message.startswith(command):
                    # 提取指令参数
                    params = message[len(command):].strip()
                    
                    self.log_unified("INFO", f"处理用户管理指令: {command}", group_id, user_id)
                    
                    # 调用对应的处理函数
                    return handler(user_id, params, group_id)
            
            return None
            
        except Exception as e:
            self.log_unified("ERROR", f"处理用户管理消息时发生错误: {str(e)}", kwargs.get('group_id'), user_id)
            return self.handle_error(e, "处理用户管理消息")
    
    def _handle_register(self, user_id: str, params: str, group_id: str) -> Dict[str, Any]:
        """
        处理用户注册指令
        
        指令格式: /注册 群聊 CN 密码
        
        Args:
            user_id (str): 用户QQ号
            params (str): 指令参数
            group_id (str): 当前群聊ID
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            # 解析参数
            parts = params.split()
            if len(parts) != 3:
                return {
                    "content": "❌ 注册指令格式错误！\n正确格式：/注册 群聊 CN 密码\n示例：/注册 123456789 张三 mypassword"
                }
            
            target_group_id, cn_name, password = parts
            
            # 验证参数
            if not self._validate_group_id(target_group_id):
                return {
                    "content": "❌ 群聊ID格式错误！群聊ID应为数字。"
                }
            
            if not self._validate_cn_name(cn_name):
                return {
                    "content": "❌ 中文名格式错误！中文名长度应在1-20个字符之间。"
                }
            
            if not self._validate_password(password):
                return {
                    "content": "❌ 密码格式错误！密码长度应在6-50个字符之间。"
                }
            
            # 检查用户是否已存在
            user_file_path = os.path.join(self.users_dir, f"{user_id}.json")
            if os.path.exists(user_file_path):
                return {
                    "content": f"❌ 用户已存在！\n如需修改密码，请使用：/修改密码 新密码\n如需增加群聊，请使用：/增加群聊 {target_group_id}"
                }
            
            # 创建用户数据
            user_data = {
                "qq": user_id,
                "cn_name": cn_name,
                "password": password,
                "groups": [
                    {
                        "group_id": target_group_id,
                        "is_admin": False
                    }
                ],
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_login": None,
                "is_active": True
            }
            
            # 保存用户数据
            with open(user_file_path, 'w', encoding='utf-8') as f:
                json.dump(user_data, f, ensure_ascii=False, indent=4)
            
            self.log_unified("INFO", f"用户注册成功: {cn_name} (QQ: {user_id})", group_id, user_id)
            
            return {
                "content": f"✅ 注册成功！\n👤 用户名：{cn_name}\n🆔 QQ号：{user_id}\n👥 群聊：{target_group_id}\n🔑 密码：{password}\n📅 注册时间：{user_data['created_at']}\n\n注意：您当前为普通用户，无管理员权限。"
            }
            
        except Exception as e:
            self.log_unified("ERROR", f"用户注册失败: {str(e)}", group_id, user_id)
            return self.handle_error(e, "用户注册")
    
    def _handle_change_password(self, user_id: str, params: str, group_id: str) -> Dict[str, Any]:
        """
        处理密码修改指令
        
        指令格式: /修改密码 新密码
        
        Args:
            user_id (str): 用户QQ号
            params (str): 新密码
            group_id (str): 当前群聊ID
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            new_password = params.strip()
            
            # 验证新密码
            if not self._validate_password(new_password):
                return {
                    "content": "❌ 密码格式错误！密码长度应在6-50个字符之间。"
                }
            
            # 检查用户是否存在
            user_file_path = os.path.join(self.users_dir, f"{user_id}.json")
            if not os.path.exists(user_file_path):
                return {
                    "content": "❌ 用户不存在！请先使用 /注册 指令进行注册。"
                }
            
            # 读取用户数据
            with open(user_file_path, 'r', encoding='utf-8') as f:
                user_data = json.load(f)
            
            # 更新密码
            old_password = user_data.get("password", "")
            user_data["password"] = new_password
            user_data["last_login"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 保存更新后的用户数据
            with open(user_file_path, 'w', encoding='utf-8') as f:
                json.dump(user_data, f, ensure_ascii=False, indent=4)
            
            self.log_unified("INFO", f"用户密码修改成功: {user_data.get('cn_name', 'Unknown')} (QQ: {user_id})", group_id, user_id)
            
            return {
                "content": f"✅ 密码修改成功！\n👤 用户名：{user_data.get('cn_name', 'Unknown')}\n🆔 QQ号：{user_id}\n🔑 新密码：{new_password}\n📅 修改时间：{user_data['last_login']}"
            }
            
        except Exception as e:
            self.log_unified("ERROR", f"密码修改失败: {str(e)}", group_id, user_id)
            return self.handle_error(e, "密码修改")
    
    def _handle_add_group(self, user_id: str, params: str, group_id: str) -> Dict[str, Any]:
        """
        处理增加群聊指令
        
        指令格式: /增加群聊 群聊
        
        Args:
            user_id (str): 用户QQ号
            params (str): 群聊ID
            group_id (str): 当前群聊ID
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            target_group_id = params.strip()
            
            # 验证群聊ID
            if not self._validate_group_id(target_group_id):
                return {
                    "content": "❌ 群聊ID格式错误！群聊ID应为数字。"
                }
            
            # 检查用户是否存在
            user_file_path = os.path.join(self.users_dir, f"{user_id}.json")
            if not os.path.exists(user_file_path):
                return {
                    "content": "❌ 用户不存在！请先使用 /注册 指令进行注册。"
                }
            
            # 读取用户数据
            with open(user_file_path, 'r', encoding='utf-8') as f:
                user_data = json.load(f)
            
            # 检查群聊是否已存在
            existing_groups = [group["group_id"] for group in user_data.get("groups", [])]
            if target_group_id in existing_groups:
                return {
                    "content": f"❌ 群聊 {target_group_id} 已存在于您的群聊列表中！"
                }
            
            # 添加新群聊
            user_data["groups"].append({
                "group_id": target_group_id,
                "is_admin": False
            })
            user_data["last_login"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 保存更新后的用户数据
            with open(user_file_path, 'w', encoding='utf-8') as f:
                json.dump(user_data, f, ensure_ascii=False, indent=4)
            
            self.log_unified("INFO", f"用户群聊添加成功: {user_data.get('cn_name', 'Unknown')} (QQ: {user_id}) 添加群聊 {target_group_id}", group_id, user_id)
            
            # 构建群聊列表显示
            groups_list = "\n".join([f"  • {group['group_id']} {'(管理员)' if group['is_admin'] else '(普通用户)'}" for group in user_data["groups"]])
            
            return {
                "content": f"✅ 群聊添加成功！\n👤 用户名：{user_data.get('cn_name', 'Unknown')}\n🆔 QQ号：{user_id}\n➕ 新增群聊：{target_group_id}\n📅 添加时间：{user_data['last_login']}\n\n📋 当前群聊列表：\n{groups_list}\n\n注意：您在新群聊中为普通用户，无管理员权限。"
            }
            
        except Exception as e:
            self.log_unified("ERROR", f"群聊添加失败: {str(e)}", group_id, user_id)
            return self.handle_error(e, "群聊添加")
    
    def _validate_group_id(self, group_id: str) -> bool:
        """
        验证群聊ID格式
        
        Args:
            group_id (str): 群聊ID
            
        Returns:
            bool: 是否有效
        """
        return group_id.isdigit() and len(group_id) >= 6 and len(group_id) <= 15
    
    def _validate_cn_name(self, cn_name: str) -> bool:
        """
        验证中文名格式
        
        Args:
            cn_name (str): 中文名
            
        Returns:
            bool: 是否有效
        """
        return 1 <= len(cn_name) <= 20 and cn_name.strip() != ""
    
    def _validate_password(self, password: str) -> bool:
        """
        验证密码格式
        
        Args:
            password (str): 密码
            
        Returns:
            bool: 是否有效
        """
        return 6 <= len(password) <= 50 and password.strip() != ""
    
    def get_user_info(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        获取用户信息
        
        Args:
            user_id (str): 用户QQ号
            
        Returns:
            Optional[Dict[str, Any]]: 用户信息，如果用户不存在则返回None
        """
        try:
            user_file_path = os.path.join(self.users_dir, f"{user_id}.json")
            if not os.path.exists(user_file_path):
                return None
            
            with open(user_file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
                
        except Exception as e:
            self.log_unified("ERROR", f"获取用户信息失败: {str(e)}", user_id=user_id)
            return None
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取帮助文本
        
        Returns:
            Dict[str, Any]: 帮助信息
        """
        return {
            "content": """🔧 用户管理服务帮助

📝 支持的指令：

1️⃣ /注册 群聊 CN 密码
   • 注册新用户账号
   • 示例：/注册 123456789 张三 mypassword
   • 注意：注册后为普通用户，无管理员权限

2️⃣ /修改密码 新密码
   • 修改当前用户的密码
   • 示例：/修改密码 newpassword123
   • 注意：需要先注册才能修改密码

3️⃣ /增加群聊 群聊
   • 为当前用户添加新的群聊
   • 示例：/增加群聊 987654321
   • 注意：在新群聊中仍为普通用户

📋 使用说明：
• 所有通过此服务注册的用户均为普通用户
• 群聊ID必须为6-15位数字
• 中文名长度为2-20个字符
• 密码长度为6-50个字符
• 重复注册会提示使用相应的修改指令

❓ 如有问题，请联系管理员。"""
        }