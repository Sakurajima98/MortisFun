#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目今日老婆服务 - 重构版

本文件实现今日老婆功能，包括：
1. 通过napcat API获取群成员列表
2. 按群聊分别记录用户QQ号
3. 随机选择群成员作为今日老婆
4. 通过QQ头像API获取用户头像
5. 整合信息生成响应

支持的指令：
- /今日老婆 - 获取今日老婆

功能特性：
- 每个群聊独立记录成员信息
- 每日随机选择，同一天返回相同结果
- 自动获取用户昵称和头像
- 支持混合消息格式（文字+图片）

作者: Mortisfun Team
版本: 2.0.0
创建时间: 2025
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import random
import os
import json
import re
from .base_service import BaseService


class DailyWifeService(BaseService):
    """
    今日老婆服务类 - 重构版
    
    主要功能：
    - 通过napcat API获取群成员信息
    - 按群聊分别记录和管理用户数据
    - 每日随机选择群成员作为今日老婆
    - 获取用户头像并整合信息
    - 支持混合消息格式输出
    
    数据结构：
    - 群成员数据：按群ID存储成员QQ号、昵称等信息
    - 每日选择记录：记录每个群每天的选择结果
    - 支持数据持久化和自动清理
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, message_sender=None, server=None) -> None:
        """
        初始化今日老婆服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            message_sender: 消息发送回调函数（用于调用napcat API）
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 消息发送回调函数
        self.message_sender = message_sender
        
        # 确保数据目录存在
        self._ensure_wife_directories()
        
        # QQ头像API地址模板 <mcreference link="https://www.napcat.wiki/onebot/api" index="1">1</mcreference>
        self.avatar_url_template = "http://q.qlogo.cn/headimg_dl?dst_uin={}&spec=640"
        
        # 数据文件路径
        self.group_members_file = os.path.join(self.data_manager.base_path, 'daily_wife', 'group_members.json')
        self.daily_records_file = os.path.join(self.data_manager.base_path, 'daily_wife', 'daily_records.json')
        self.relationships_file = os.path.join(self.data_manager.base_path, 'daily_wife', 'relationships.json')
        
        # 加载数据
        self.group_members_data = self._load_group_members_data()
        self.daily_records_data = self._load_daily_records_data()
        self.relationships_data = self._load_relationships_data()
        
        # 跨平台字体路径
        self.font_paths = self._get_system_fonts()
        
        self.log_unified("INFO", "今日老婆服务初始化完成（重构版）", group_id="system", user_id="system")
    
    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理今日老婆请求消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 其他参数，包含group_id等
            
        Returns:
            Optional[Dict[str, Any]]: 包含内容和图片路径的响应字典，如果无法处理则返回None
        """
        message = message.strip()
        group_id = kwargs.get('group_id')
        
        # 检查是否在群聊中
        if not group_id:
            if message in ["/今日老婆", "今日老婆", "/分手"] or message.startswith("/今日老婆@"):
                return {
                    'content': '今日老婆功能仅支持在群聊中使用哦~',
                    'mixed_message': False
                }
            return None
        
        # 处理分手请求
        if message == "/分手":
            self.logger.info(f"今日老婆服务 - 分手请求")
            return await self.handle_breakup(user_id, group_id)
        
        # 处理指定今日老婆请求 - 检查CQ码和普通@格式
        raw_message = kwargs.get('context', {}).get('raw_message', '')
        
        # 判断是否为指定请求：消息以/今日老婆开头且包含@符号或CQ码
        is_target_request = (message.startswith("/今日老婆") and 
                           ("@" in message or "CQ:at" in raw_message))
        
        if is_target_request:
            target_user = None
            
            # 优先从CQ码中提取QQ号
            if "CQ:at" in raw_message:
                cq_pattern = r'\[CQ:at,qq=([0-9]+)\]'
                cq_match = re.search(cq_pattern, raw_message)
                if cq_match:
                    target_user = cq_match.group(1)
            
            # 如果CQ码解析失败，尝试从普通@格式解析
            if not target_user and "@" in message:
                # 使用正则表达式提取@用户信息，支持格式：@昵称 (QQ号)
                at_pattern = r'@([^(]+)\s*\(([0-9]+)\)'
                match = re.search(at_pattern, message)
                
                if match:
                    nickname = match.group(1).strip()
                    qq_number = match.group(2).strip()
                    target_user = qq_number
                else:
                    # 兼容旧格式：直接提取@后的内容
                    at_index = message.find("@")
                    if at_index != -1:
                        target_user = message[at_index + 1:].strip()
            
            if target_user:
                self.logger.info(f"今日老婆服务 - 指定请求")
                return await self.get_daily_wife_with_target(user_id, group_id, target_user)
        
        # 处理普通今日老婆请求
        if message in ["/今日老婆", "今日老婆"]:
            self.logger.info(f"今日老婆服务 - 普通请求")
            return await self.get_daily_wife_for_group(user_id, group_id)
        
        return None
    
    def _load_group_members_data(self) -> Dict[str, Any]:
        """
        加载群成员数据
        
        Returns:
            Dict[str, Any]: 群成员数据，格式为 {group_id: {members: [...], last_update: timestamp}}
        """
        try:
            if os.path.exists(self.group_members_file):
                with open(self.group_members_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"加载群成员数据失败: {e}")
        return {}
    
    def _save_group_members_data(self) -> None:
        """
        保存群成员数据
        """
        try:
            os.makedirs(os.path.dirname(self.group_members_file), exist_ok=True)
            with open(self.group_members_file, 'w', encoding='utf-8') as f:
                json.dump(self.group_members_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存群成员数据失败: {e}")
    
    def _load_daily_records_data(self) -> Dict[str, Any]:
        """
        加载每日选择记录数据
        
        Returns:
            Dict[str, Any]: 每日记录数据，格式为 {group_id: {date: {user_id: qq, nickname: name}}}
        """
        try:
            if os.path.exists(self.daily_records_file):
                with open(self.daily_records_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"加载每日记录数据失败: {e}")
        return {}
    
    def _save_daily_records_data(self) -> None:
        """
        保存每日选择记录数据
        """
        try:
            os.makedirs(os.path.dirname(self.daily_records_file), exist_ok=True)
            with open(self.daily_records_file, 'w', encoding='utf-8') as f:
                json.dump(self.daily_records_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存每日记录数据失败: {e}")
    
    def _load_relationships_data(self) -> Dict[str, Any]:
        """
        加载恋爱关系数据
        
        Returns:
            Dict[str, Any]: 恋爱关系数据
        """
        try:
            if os.path.exists(self.relationships_file):
                with open(self.relationships_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"加载恋爱关系数据失败: {e}")
        
        # 返回默认结构
        return {
            "_schema_version": "2.0",
            "_description": "今日老婆恋爱关系数据 - 支持双向绑定和分手功能",
            "relationships": {},
            "breakup_history": {},
            "daily_couples": {}
        }
    
    def _save_relationships_data(self) -> None:
        """
        保存恋爱关系数据到文件
        """
        try:
            with open(self.relationships_file, 'w', encoding='utf-8') as f:
                json.dump(self.relationships_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"保存恋爱关系数据失败: {e}")
    
    def _get_system_fonts(self) -> List[str]:
        """
        获取系统字体路径（跨平台支持）
        
        Returns:
            List[str]: 字体文件路径列表
        """
        import platform
        
        system = platform.system()
        font_paths = []
        
        if system == "Windows":
            font_paths = [
                "C:/Windows/Fonts/msyh.ttc",  # 微软雅黑
                "C:/Windows/Fonts/simhei.ttf",  # 黑体
                "C:/Windows/Fonts/simsun.ttc",  # 宋体
            ]
        elif system == "Linux":
            font_paths = [
                "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",  # 文泉驿微米黑
                "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",   # 文泉驿正黑
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", # DejaVu Sans
            ]
        elif system == "Darwin":  # macOS
            font_paths = [
                "/System/Library/Fonts/PingFang.ttc",  # 苹方
                "/System/Library/Fonts/Helvetica.ttc", # Helvetica
            ]
        
        return font_paths
    
    async def get_daily_wife_for_group(self, user_id: str, group_id: str) -> Dict[str, Any]:
        """
        获取群聊的今日老婆（纯爱版本）
        
        Args:
            user_id (str): 请求用户ID
            group_id (str): 群组ID
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        try:
            # 检查是否已有恋爱关系
            existing_partner = self._get_current_partner(user_id, group_id)
            if existing_partner:
                # 已有恋爱关系，返回现有伴侣
                partner_info = await self._get_user_info(existing_partner, group_id)
                if partner_info:
                    return await self._format_wife_response({
                        'user_id': existing_partner,
                        'nickname': partner_info['nickname'],
                        'selected_by': user_id,
                        'selected_time': datetime.now().isoformat(),
                        'is_existing_relationship': True
                    }, is_repeat=True)
            
            # 获取群成员列表
            members = await self._get_group_members(group_id)
            if not members:
                return {
                    'content': '❌ 无法获取群成员列表，napcat API调用失败。\n请检查：\n1. napcat是否正常运行\n2. 机器人是否在该群聊中\n3. 是否有获取群成员的权限',
                    'mixed_message': False
                }
            
            # 过滤掉自己和已有恋爱关系的用户
            available_members = self._filter_available_members(members, user_id, group_id)
            if not available_members:
                return {
                    'content': '❌ 没有可选择的群成员了~\n可能的原因：\n1. 群里只有你一个人\n2. 其他成员都已经有恋爱关系了\n\n💡 提示：使用 /分手 可以解除现有关系',
                    'mixed_message': False
                }
            
            # 随机选择一个成员
            selected_member = random.choice(available_members)
            
            # 建立双向恋爱关系
            self._create_relationship(user_id, selected_member['user_id'], group_id)
            
            # 记录今日选择
            today = datetime.now().strftime('%Y-%m-%d')
            self._record_daily_couple(user_id, selected_member['user_id'], group_id, today)
            
            # 返回选择结果
            return await self._format_wife_response({
                'user_id': selected_member['user_id'],
                'nickname': selected_member['nickname'],
                'selected_by': user_id,
                'selected_time': datetime.now().isoformat(),
                'is_new_relationship': True
            }, is_repeat=False)
            
        except Exception as e:
            self.logger.error(f"获取今日老婆失败: {e}")
            return {
                'content': f'❌ 获取今日老婆失败: {str(e)}',
                'mixed_message': False
            }
    
    async def _get_group_members(self, group_id: str) -> List[Dict[str, Any]]:
        """
        获取群成员列表（通过napcat API）
        
        Args:
            group_id (str): 群组ID
            
        Returns:
            List[Dict[str, Any]]: 群成员列表
        """
        try:
            # 检查缓存是否有效（1小时内）
            current_time = datetime.now().timestamp()
            if (group_id in self.group_members_data and 
                'last_update' in self.group_members_data[group_id] and
                current_time - self.group_members_data[group_id]['last_update'] < 3600):
                
                return self.group_members_data[group_id]['members']
            
            # 通过napcat API获取群成员列表
            if self.message_sender:
                api_request = {
                    "action": "get_group_member_list",
                    "params": {
                        "group_id": group_id
                    }
                }
                
                # 发送API请求
                response = await self._call_napcat_api(api_request)
                
                if response and isinstance(response, dict):
                    # 检查API调用是否成功
                    if response.get('status') == 'ok' and response.get('retcode') == 0:
                        if 'data' in response and isinstance(response['data'], list):
                            members = []
                            for member in response['data']:
                                if isinstance(member, dict):
                                    # 提取成员信息，支持多种字段名
                                    user_id = member.get('user_id') or member.get('qq') or member.get('uin')
                                    nickname = (member.get('nickname') or 
                                               member.get('card') or 
                                               member.get('nick') or 
                                               member.get('name') or 
                                               f"用户{user_id}" if user_id else "未知用户")
                                    
                                    if user_id:
                                        members.append({
                                            'user_id': str(user_id),
                                            'nickname': nickname
                                        })
                                    else:
                                        self.logger.warning(f"成员数据缺少用户ID: {member}")
                                else:
                                    self.logger.warning(f"无效的成员数据格式: {member}")
                            
                            if members:
                                # 更新缓存
                                self.group_members_data[group_id] = {
                                    'members': members,
                                    'last_update': current_time
                                }
                                self._save_group_members_data()
                                
                                return members

                        else:
                            self.logger.error(f"API响应中缺少data字段或data不是数组: {response}")
                    else:
                        self.logger.error(f"napcat API调用失败: status={response.get('status')}, retcode={response.get('retcode')}, message={response.get('message')}")
                else:
                    self.logger.error(f"napcat API返回无效响应: {response}")
            
            return []
            
        except Exception as e:
            self.logger.error(f"获取群成员列表失败: {e}")
            return []
    
    async def _call_napcat_api(self, api_request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        调用napcat API
        
        Args:
            api_request (Dict[str, Any]): API请求数据
            
        Returns:
            Optional[Dict[str, Any]]: API响应数据
        """
        try:
            if self.message_sender:
                response = await self.message_sender(api_request)
                return response
            else:
                self.logger.error("napcat API调用器未设置，无法调用API")
                return None
        except Exception as e:
            self.logger.error(f"调用napcat API失败: {e}")

            return None
    
    def _get_current_partner(self, user_id: str, group_id: str) -> Optional[str]:
        """
        获取用户当前的恋爱伴侣
        
        Args:
            user_id (str): 用户ID
            group_id (str): 群组ID
            
        Returns:
            Optional[str]: 伴侣用户ID，如果没有则返回None
        """
        relationships = self.relationships_data.get('relationships', {})
        group_relationships = relationships.get(group_id, {})
        return group_relationships.get(user_id)
    
    def _create_relationship(self, user_a: str, user_b: str, group_id: str) -> None:
        """
        创建双向恋爱关系
        
        Args:
            user_a (str): 用户A的ID
            user_b (str): 用户B的ID
            group_id (str): 群组ID
        """
        if 'relationships' not in self.relationships_data:
            self.relationships_data['relationships'] = {}
        
        if group_id not in self.relationships_data['relationships']:
            self.relationships_data['relationships'][group_id] = {}
        
        # 建立双向关系
        self.relationships_data['relationships'][group_id][user_a] = user_b
        self.relationships_data['relationships'][group_id][user_b] = user_a
        
        self._save_relationships_data()
    
    def _break_relationship(self, user_id: str, group_id: str) -> Optional[str]:
        """
        解除恋爱关系
        
        Args:
            user_id (str): 用户ID
            group_id (str): 群组ID
            
        Returns:
            Optional[str]: 前任的用户ID，如果没有关系则返回None
        """
        partner = self._get_current_partner(user_id, group_id)
        if not partner:
            return None
        
        # 移除双向关系
        relationships = self.relationships_data.get('relationships', {})
        group_relationships = relationships.get(group_id, {})
        
        if user_id in group_relationships:
            del group_relationships[user_id]
        if partner in group_relationships:
            del group_relationships[partner]
        
        # 记录分手历史
        self._record_breakup_history(user_id, partner, group_id)
        self._record_breakup_history(partner, user_id, group_id)
        
        self._save_relationships_data()
        
        return partner
    
    def _record_breakup_history(self, user_id: str, ex_partner: str, group_id: str) -> None:
        """
        记录分手历史
        
        Args:
            user_id (str): 用户ID
            ex_partner (str): 前任ID
            group_id (str): 群组ID
        """
        if 'breakup_history' not in self.relationships_data:
            self.relationships_data['breakup_history'] = {}
        
        if group_id not in self.relationships_data['breakup_history']:
            self.relationships_data['breakup_history'][group_id] = {}
        
        if user_id not in self.relationships_data['breakup_history'][group_id]:
            self.relationships_data['breakup_history'][group_id][user_id] = []
        
        if ex_partner not in self.relationships_data['breakup_history'][group_id][user_id]:
            self.relationships_data['breakup_history'][group_id][user_id].append(ex_partner)
    
    def _record_daily_couple(self, user_a: str, user_b: str, group_id: str, date: str) -> None:
        """
        记录每日情侣
        
        Args:
            user_a (str): 用户A的ID
            user_b (str): 用户B的ID
            group_id (str): 群组ID
            date (str): 日期
        """
        if 'daily_couples' not in self.relationships_data:
            self.relationships_data['daily_couples'] = {}
        
        if group_id not in self.relationships_data['daily_couples']:
            self.relationships_data['daily_couples'][group_id] = {}
        
        if date not in self.relationships_data['daily_couples'][group_id]:
            self.relationships_data['daily_couples'][group_id][date] = []
        
        couple = [user_a, user_b]
        couple.sort()  # 确保顺序一致
        
        if couple not in self.relationships_data['daily_couples'][group_id][date]:
            self.relationships_data['daily_couples'][group_id][date].append(couple)
        
        self._save_relationships_data()
    
    def _filter_available_members(self, members: List[Dict[str, Any]], user_id: str, group_id: str) -> List[Dict[str, Any]]:
        """
        过滤可选择的成员（排除自己和已有恋爱关系的用户）
        
        Args:
            members (List[Dict[str, Any]]): 群成员列表
            user_id (str): 请求用户ID
            group_id (str): 群组ID
            
        Returns:
            List[Dict[str, Any]]: 可选择的成员列表
        """
        available_members = []
        relationships = self.relationships_data.get('relationships', {}).get(group_id, {})
        
        for member in members:
            member_id = member['user_id']
            # 排除自己
            if member_id == user_id:
                continue
            # 排除已有恋爱关系的用户
            if member_id in relationships:
                continue
            available_members.append(member)
        
        return available_members
    
    async def _get_user_info(self, user_id: str, group_id: str) -> Optional[Dict[str, Any]]:
        """
        获取用户信息
        
        Args:
            user_id (str): 用户ID
            group_id (str): 群组ID
            
        Returns:
            Optional[Dict[str, Any]]: 用户信息，如果找不到则返回None
        """
        # 先从缓存的群成员数据中查找
        if group_id in self.group_members_data:
            for member in self.group_members_data[group_id].get('members', []):
                if member['user_id'] == user_id:
                    return member
        
        # 如果缓存中没有，尝试重新获取群成员列表
        members = await self._get_group_members(group_id)
        if members:
            for member in members:
                if member['user_id'] == user_id:
                    return member
        
        return None
    
    async def handle_breakup(self, user_id: str, group_id: str) -> Dict[str, Any]:
        """
        处理分手请求
        
        Args:
            user_id (str): 用户ID
            group_id (str): 群组ID
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        try:
            ex_partner = self._break_relationship(user_id, group_id)
            
            if not ex_partner:
                return {
                    'content': '💔 你目前没有恋爱关系哦~\n\n💡 使用 /今日老婆 开始寻找你的另一半吧！',
                    'mixed_message': False
                }
            
            # 获取前任信息
            ex_info = await self._get_user_info(ex_partner, group_id)
            ex_nickname = ex_info['nickname'] if ex_info else f'用户{ex_partner}'
            
            return {
                'content': f'很遗憾，你与 {ex_nickname} 的关系已经结束了。\n\n愿你们都能找到属于自己的幸福。',
                'mixed_message': False
            }
            
        except Exception as e:
            self.logger.error(f"处理分手请求失败: {e}")
            return {
                'content': f'❌ 分手处理失败: {str(e)}',
                'mixed_message': False
            }
    
    async def get_daily_wife_with_target(self, user_id: str, group_id: str, target_user: str) -> Dict[str, Any]:
        """
        获取指定目标的今日老婆（40%概率）
        
        Args:
            user_id (str): 请求用户ID
            group_id (str): 群组ID
            target_user (str): 目标用户（可能是昵称或QQ号）
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        try:
            # 检查是否已有恋爱关系
            existing_partner = self._get_current_partner(user_id, group_id)
            if existing_partner:
                partner_info = await self._get_user_info(existing_partner, group_id)
                if partner_info:
                    return {
                        'content': f'💕 你已经和 {partner_info["nickname"]} 在一起了~\n\n专一是美德哦！如果想要分手，请使用 /分手 指令。',
                        'mixed_message': False
                    }
            
            # 获取群成员列表
            members = await self._get_group_members(group_id)
            if not members:
                return {
                    'content': '❌ 无法获取群成员列表，napcat API调用失败。\n请检查：\n1. napcat是否正常运行\n2. 机器人是否在该群聊中\n3. 是否有获取群成员的权限',
                    'mixed_message': False
                }
            
            # 查找目标用户
            target_member = self._find_target_member(members, target_user, user_id)
            if not target_member:
                return {
                    'content': f'❌ 找不到目标用户 "{target_user}"\n\n请检查：\n1. 用户是否在群聊中\n2. 昵称或QQ号是否正确\n3. 不能指定自己',
                    'mixed_message': False
                }
            
            # 检查目标用户是否已有恋爱关系
            if self._get_current_partner(target_member['user_id'], group_id):
                return {
                    'content': f'💔 {target_member["nickname"]} 已经有恋爱关系了~\n\n换个目标试试吧！',
                    'mixed_message': False
                }
            
            # 过滤可选择的成员
            available_members = self._filter_available_members(members, user_id, group_id)
            if not available_members:
                return {
                    'content': '❌ 没有可选择的群成员了~\n可能的原因：\n1. 群里只有你一个人\n2. 其他成员都已经有恋爱关系了\n\n💡 提示：使用 /分手 可以解除现有关系',
                    'mixed_message': False
                }
            
            # 指定用户抽取逻辑：为@的目标用户增加40%抽取概率
            selected_member = None
            is_target_selected = False
            
            # 如果指定了目标用户且目标用户在可选列表中，40%概率选择目标用户
            if target_member in available_members and random.random() < 0.4:
                selected_member = target_member
                is_target_selected = True
            else:
                # 随机选择其他用户
                selected_member = random.choice(available_members)
                is_target_selected = (target_member and selected_member['user_id'] == target_member['user_id'])
            
            # 建立双向恋爱关系
            self._create_relationship(user_id, selected_member['user_id'], group_id)
            
            # 记录今日选择
            today = datetime.now().strftime('%Y-%m-%d')
            self._record_daily_couple(user_id, selected_member['user_id'], group_id, today)
            
            # 返回选择结果
            return await self._format_wife_response({
                'user_id': selected_member['user_id'],
                'nickname': selected_member['nickname'],
                'selected_by': user_id,
                'selected_time': datetime.now().isoformat(),
                'is_new_relationship': True,
                'is_target_selection': True,
                'target_hit': is_target_selected
            }, is_repeat=False)
            
        except Exception as e:
            self.logger.error(f"指定今日老婆失败: {e}")
            return {
                'content': f'❌ 指定今日老婆失败: {str(e)}',
                'mixed_message': False
            }
    
    def _find_target_member(self, members: List[Dict[str, Any]], target_user: str, requester_id: str) -> Optional[Dict[str, Any]]:
        """
        查找目标用户
        
        Args:
            members (List[Dict[str, Any]]): 群成员列表
            target_user (str): 目标用户（昵称或QQ号）
            requester_id (str): 请求者ID（用于排除自己）
            
        Returns:
            Optional[Dict[str, Any]]: 目标用户信息，如果找不到或是自己则返回None
        """
        for member in members:
            # 不能指定自己
            if member['user_id'] == requester_id:
                continue
            
            # 匹配QQ号或昵称
            if (member['user_id'] == target_user or 
                member['nickname'] == target_user or
                target_user in member['nickname']):
                return member
        
        return None
    
    async def _format_wife_response(self, wife_data: Dict[str, Any], is_repeat: bool = False) -> Dict[str, Any]:
        """
        格式化今日老婆响应（简化版本）
        
        Args:
            wife_data (Dict[str, Any]): 老婆数据
            is_repeat (bool): 是否为重复查询
            
        Returns:
            Dict[str, Any]: 格式化的响应数据
        """
        try:
            user_id = wife_data['user_id']
            nickname = wife_data['nickname']
            
            # 构建头像URL（直接使用网络链接，无需下载）
            avatar_url = self.avatar_url_template.format(user_id)
            
            # 构建文本内容
            content = ""
            
            # 处理指定功能的特殊前缀
            if wife_data.get('is_target_selection'):
                if wife_data.get('target_hit'):
                    content += "何其有幸，让你实现了指定的愿望。\n\n"
                else:
                    content += "经历一段时间的发展，我们认为随机到的用户更适合你。\n\n"
            
            # 主要内容 - 统一格式
            content += f"你今天的老婆是：\n{nickname}\n请好好对她吧~"
            
            # 返回包含网络图片链接的响应
            return {
                'content': content,
                'image_url': avatar_url,  # 使用网络链接而不是本地路径
                'mixed_message': True
            }
            
        except Exception as e:
            self.logger.error(f"格式化老婆响应失败: {e}")
            return {
                'content': f'❌ 生成响应失败: {str(e)}',
                'mixed_message': False
            }
    

    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取今日老婆服务的帮助文本（纯爱版本）
        
        Returns:
            Dict[str, Any]: 包含帮助信息的字典
        """
        help_content = """
💕 今日老婆功能 - 纯爱版本 💕

📝 可用指令：
• /今日老婆 - 获取今日老婆（建立恋爱关系）
• /今日老婆@用户 - 指定目标用户（40%概率成功）
• /分手 - 解除当前恋爱关系

💖 纯爱特性：
• 双向绑定：A抽到B时，B使用功能结果也是A
• 专一关系：一旦确定关系就不会改变
• 防止自恋：不会抽到自己
• 排他性：已有恋爱关系的用户不会被选中

✨ 功能说明：
• 通过napcat API获取真实群成员信息
• 每个群聊独立管理恋爱关系
• 支持分手和重新开始
• 指定功能增加成功概率
• 自动获取用户昵称和头像
• 支持网络头像显示

⚠️ 使用要求：
• 需要napcat正常运行并连接
• 机器人需要在目标群聊中
• 需要获取群成员列表的权限

💡 使用示例：
• /今日老婆 - 随机选择或返回现有恋人
• /今日老婆@小明 - 40%概率选中小明
• /今日老婆@123456789 - 可以使用QQ号指定
• /分手 - 结束当前恋爱关系

🎯 小贴士：
• 恋爱关系是双向的，彼此专一
• 分手后可以重新寻找真爱
• 指定功能不保证100%成功，增加趣味性
• 头像获取可能需要一定时间
• 每个群聊独立记录成员信息
        """.strip()
        
        return {
            'content': help_content,
            'mixed_message': False
        }
    

    
    def _ensure_wife_directories(self) -> None:
        """
        确保今日老婆功能所需的目录存在
        """
        try:
            # 创建主目录
            wife_dir = os.path.join(self.data_manager.base_path, 'daily_wife')
            os.makedirs(wife_dir, exist_ok=True)
            
            # 创建图片目录
            images_dir = os.path.join(wife_dir, 'images')
            os.makedirs(images_dir, exist_ok=True)
            
            # 创建记录目录
            records_dir = os.path.join(wife_dir, 'records')
            os.makedirs(records_dir, exist_ok=True)
            
            self.log_unified("INFO", "今日老婆目录结构创建完成", group_id="system", user_id="system")
            
        except Exception as e:
            self.log_unified("ERROR", f"创建今日老婆目录失败: {e}", group_id="system", user_id="system")
    
    def cleanup_old_wife_data(self, days_to_keep: int = 7) -> None:
        """
        清理过期的老婆数据
        
        Args:
            days_to_keep (int): 保留的天数，默认7天
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # 清理过期的头像文件
            images_dir = os.path.join(self.data_manager.base_path, 'daily_wife', 'images')
            if os.path.exists(images_dir):
                for filename in os.listdir(images_dir):
                    if filename.startswith('wife_avatar_'):
                        file_path = os.path.join(images_dir, filename)
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_mtime < cutoff_date:
                            os.remove(file_path)
            
            # 清理过期的记录数据
            cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
            
            # 清理每日记录数据
            for group_id in list(self.daily_records_data.keys()):
                group_records = self.daily_records_data[group_id]
                for date_str in list(group_records.keys()):
                    if date_str < cutoff_date_str:
                        del group_records[date_str]
                
                # 如果群组没有记录了，删除整个群组
                if not group_records:
                    del self.daily_records_data[group_id]
            
            # 保存清理后的数据
            self._save_daily_records_data()
            

            
        except Exception as e:
            self.logger.error(f"清理老婆数据失败: {e}")