#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目群成员获取服务

本文件实现群成员获取功能，包括：
1. 通过napcat API获取群成员列表
2. 将群成员信息记录到Excel表格中
3. 支持按群聊分别记录成员信息
4. 提供群成员数据的导出功能

支持的指令：
- /成员获取 - 获取当前群聊的成员列表并记录到Excel

功能特性：
- 自动获取群成员的QQ号和群名称
- 将数据保存到Excel表格中
- 支持多群聊独立记录
- 提供详细的成员信息统计

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import os
import asyncio
import time
import pandas as pd
from .base_service import BaseService


class GroupMemberService(BaseService):
    """
    群成员获取服务类
    
    主要功能：
    - 通过napcat API获取群成员信息
    - 将成员信息记录到Excel表格中
    - 支持按群聊分别管理成员数据
    - 提供成员信息的导出和统计功能
    
    数据结构：
    - 群成员数据：包含QQ号、群名称、获取时间等信息
    - Excel表格：按群ID分别保存成员信息
    - 支持数据更新和历史记录
    """

    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, message_sender=None, server=None) -> None:
        """
        初始化群成员获取服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            message_sender: napcat消息发送器
            server: 服务器实例
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.message_sender = message_sender
        
        # 创建数据目录
        self.data_dir = os.path.join("data", "group_members")
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.logger.info("群成员获取服务初始化完成")
    
    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理群成员获取相关消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 其他参数，包含group_id等
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果，如果不是相关消息则返回None
        """
        group_id = kwargs.get('group_id')
        if not group_id:
            return None
        
        # 处理成员获取指令
        if message in ["/成员获取", "成员获取"]:
            self.logger.info(f"群成员获取服务 - 用户 {user_id} 在群 {group_id} 请求获取成员列表")
            return await self.get_group_members_and_save(user_id, group_id, full_info=False)
        
        # 处理完整成员获取指令
        if message in ["/成员获取 全", "成员获取 全"]:
            self.logger.info(f"群成员获取服务 - 用户 {user_id} 在群 {group_id} 请求获取完整成员列表")
            return await self.get_group_members_and_save(user_id, group_id, full_info=True)
        
        return None
    
        return None
    
    async def get_group_members_and_save(self, user_id: str, group_id: str, full_info: bool = False) -> Dict[str, Any]:
        """
        获取群成员列表并保存到Excel表格
        
        Args:
            user_id (str): 请求用户ID
            group_id (str): 群组ID
            full_info (bool): 是否获取完整信息，默认False
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            # 获取群成员列表
            members = await self._get_group_members(group_id, full_info)
            if not members:
                return {
                    'content': '❌ 无法获取群成员列表，napcat API调用失败。\n请检查：\n1. napcat是否正常运行\n2. 机器人是否在该群聊中\n3. 是否有获取群成员的权限',
                    'mixed_message': False
                }
            
            # 保存到Excel表格
            excel_path = self._save_members_to_excel(members, group_id, full_info)
            if not excel_path:
                return {
                    'content': '❌ 保存群成员信息到Excel失败，请检查文件权限和磁盘空间',
                    'mixed_message': False
                }
            
            # 生成统计信息
            stats = self._generate_member_stats(members, group_id)
            
            # 直接返回Excel文件，不包含任何文本信息
            return {
                'content': '',
                'file_path': excel_path,
                'mixed_message': False
            }
            
        except Exception as e:
            self.logger.error(f"获取群成员列表失败: {e}")
            return {
                'content': f'❌ 获取群成员列表失败: {str(e)}',
                'mixed_message': False
            }
    
    async def _get_group_members(self, group_id: str, full_info: bool = False) -> List[Dict[str, Any]]:
        """
        获取群成员列表（通过napcat API）
        
        Args:
            group_id (str): 群组ID
            full_info (bool): 是否获取完整信息，默认False
            
        Returns:
            List[Dict[str, Any]]: 群成员列表
        """
        try:
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
                                    # 提取成员信息
                                    user_id = member.get('user_id') or member.get('qq') or member.get('uin')
                                    
                                    if full_info:
                                        # 获取完整信息 - 需要单独调用get_group_member_info获取详细信息
                                        if user_id:
                                            # 先获取基本信息
                                            card = member.get('card', '')  # 群名片
                                            nickname = member.get('nickname', '')  # 昵称
                                            role = member.get('role', '成员')  # 角色
                                            join_time = member.get('join_time', 0)  # 加入时间
                                            last_sent_time = member.get('last_sent_time', 0)  # 最后发言时间
                                            level = member.get('level', 0)  # 等级
                                            
                                            # 调用get_group_member_info获取详细信息
                                            member_detail_request = {
                                                "action": "get_group_member_info",
                                                "params": {
                                                    "group_id": group_id,
                                                    "user_id": user_id
                                                }
                                            }
                                            
                                            detail_response = await self._call_napcat_api(member_detail_request)
                                            
                                            # 初始化默认值
                                            age = 0
                                            area = '未知'
                                            sex = '未知'
                                            title = ''
                                            title_expire_time = 0
                                            card_changeable = True
                                            shut_up_timestamp = 0
                                            
                                            # 如果获取到详细信息，则使用详细信息
                                            if detail_response and isinstance(detail_response, dict):
                                                if detail_response.get('status') == 'ok' and detail_response.get('retcode') == 0:
                                                    detail_data = detail_response.get('data', {})
                                                    if isinstance(detail_data, dict):
                                                        age = detail_data.get('age', 0)
                                                        area = detail_data.get('area', '未知')
                                                        sex = detail_data.get('sex', 'unknown')
                                                        title = detail_data.get('title', '')
                                                        title_expire_time = detail_data.get('title_expire_time', 0)
                                                        card_changeable = detail_data.get('card_changeable', True)
                                                        shut_up_timestamp = detail_data.get('shut_up_timestamp', 0)
                                                        
                                                        # 更新基本信息（详细接口可能返回更准确的信息）
                                                        card = detail_data.get('card', card)
                                                        nickname = detail_data.get('nickname', nickname)
                                                        role = detail_data.get('role', role)
                                                        join_time = detail_data.get('join_time', join_time)
                                                        last_sent_time = detail_data.get('last_sent_time', last_sent_time)
                                                        level = detail_data.get('level', level)
                                            
                                            # 如果群名片为空，使用昵称作为群名称
                                            display_name = card if card.strip() else nickname
                                            
                                            members.append({
                                                'user_id': str(user_id),
                                                'card': display_name,
                                                'nickname': nickname,
                                                'role': role,
                                                'join_time': join_time,
                                                'last_sent_time': last_sent_time,
                                                'level': level,
                                                'age': age,
                                                'area': area,
                                                'sex': sex,
                                                'title': title,
                                                'title_expire_time': title_expire_time,
                                                'card_changeable': card_changeable,
                                                'shut_up_timestamp': shut_up_timestamp,
                                                'group_id': group_id,
                                                'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                            })
                                    else:
                                        # 只获取基本信息（群名称）
                                        card = member.get('card', '')  # 群名片
                                        nickname = member.get('nickname', '')  # 昵称
                                        
                                        # 如果群名片为空，使用昵称作为群名称
                                        display_name = card if card.strip() else nickname
                                        
                                        if user_id:
                                            members.append({
                                                'user_id': str(user_id),
                                                'card': display_name,
                                                'group_id': group_id,
                                                'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                            })
                                        else:
                                            self.logger.warning(f"成员数据缺少用户ID: {member}")
                                else:
                                    self.logger.warning(f"无效的成员数据格式: {member}")
                            
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
    
    def _excel_file_path(self, group_id: str) -> str:
        """
        函数说明:
        - 根据群ID生成Excel文件路径（与现有成员获取实现的保存位置一致）
        参数:
        - group_id: 群聊ID
        返回:
        - Excel文件完整路径，例如 output/group_members/群成员_{group_id}.xlsx
        """
        output_dir = os.path.join(os.getcwd(), 'output', 'group_members')
        filename = f"群成员_{group_id}.xlsx"
        return os.path.join(output_dir, filename)
    
    def excel_exists(self, group_id: str) -> bool:
        """
        函数说明:
        - 检查指定群聊的成员Excel是否已存在
        参数:
        - group_id: 群聊ID
        返回:
        - True/False
        """
        return os.path.exists(self._excel_file_path(group_id))
    
    async def update_single_member_in_excel(self, group_id: str, user_id: str) -> bool:
        """
        函数说明:
        - 复用现有NapCat调用与Excel结构，仅对单一成员进行合并更新，减少全量拉取
        参数:
        - group_id: 群聊ID
        - user_id: 成员QQ号/用户ID
        返回:
        - 是否更新成功
        """
        try:
            # 获取单成员详细信息（复用 get_group_member_info）
            request = {
                "action": "get_group_member_info",
                "params": {
                    "group_id": group_id,
                    "user_id": user_id
                }
            }
            resp = await self._call_napcat_api(request)
            if not resp or resp.get('status') != 'ok' or resp.get('retcode') != 0:
                self.logger.error(f"单成员详细信息获取失败: 群 {group_id}, 成员 {user_id}, 响应: {resp}")
                return False
            d = resp.get('data', {}) or {}
            card = str(d.get('card', '') or '').strip()
            nickname = str(d.get('nickname', '') or '').strip()
            role = str(d.get('role', '成员') or '').strip()
            join_time = d.get('join_time', 0)
            last_sent_time = d.get('last_sent_time', 0)
            level = d.get('level', 0)
            display = card if card else nickname
            
            # 读取或初始化Excel
            file_path = self._excel_file_path(group_id)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            import pandas as _pd
            existing_df = None
            if os.path.exists(file_path):
                try:
                    existing_df = _pd.read_excel(file_path, engine='openpyxl')
                except Exception as e:
                    self.logger.warning(f"读取现有Excel失败，将重建: {e}")
                    existing_df = None
            
            # 构造新行（尽量兼容现有完整信息结构）
            new_row = {
                '用户ID': str(user_id),
                '群名称': display,
                '昵称': nickname,
                '角色': role,
                '加入时间': datetime.fromtimestamp(join_time).strftime('%Y-%m-%d %H:%M:%S') if join_time > 0 else '未知',
                '最后发言时间': datetime.fromtimestamp(last_sent_time).strftime('%Y-%m-%d %H:%M:%S') if last_sent_time > 0 else '未发言',
                '等级': level,
                '群组ID': group_id,
                '获取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 合并更新到Excel
            if existing_df is not None and not existing_df.empty:
                # 若不存在用户ID列，初始化为仅包含新行
                if '用户ID' not in existing_df.columns:
                    final_df = _pd.DataFrame([new_row])
                else:
                    mask = existing_df['用户ID'].astype(str) == str(user_id)
                    if mask.any():
                        for col, val in new_row.items():
                            if col in existing_df.columns:
                                existing_df.loc[mask, col] = val
                            else:
                                # 补充缺失列
                                existing_df[col] = ''
                                existing_df.loc[mask, col] = val
                        final_df = existing_df
                    else:
                        # 对齐列集合
                        for col in new_row.keys():
                            if col not in existing_df.columns:
                                existing_df[col] = ''
                        final_df = _pd.concat([existing_df, _pd.DataFrame([new_row])], ignore_index=True)
            else:
                final_df = _pd.DataFrame([new_row])
            
            # 保存Excel
            final_df.to_excel(file_path, index=False, engine='openpyxl')
            self.logger.info(f"单成员已合并更新至Excel: 群 {group_id}, 成员 {user_id}, 文件 {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"单成员Excel更新失败: 群 {group_id}, 成员 {user_id}, 错误: {e}")
            return False
    def _csv_file_path(self, group_id: str) -> str:
        """
        文件级说明:
        - 群成员CSV存储路径生成函数
        函数说明:
        - 根据群ID生成CSV完整路径，目录固定为 data/group_members
        参数:
        - group_id: 群聊ID
        返回:
        - 完整CSV文件路径，例如 data/group_members/group_members_123456.csv
        """
        filename = f"group_members_{str(group_id)}.csv"
        return os.path.join(self.data_dir, filename)
    
    def csv_exists(self, group_id: str) -> bool:
        """
        函数说明:
        - 检查指定群聊的成员CSV是否存在
        参数:
        - group_id: 群聊ID
        返回:
        - True/False
        """
        return os.path.exists(self._csv_file_path(group_id))
    
    async def save_full_members_to_csv(self, group_id: str) -> Optional[str]:
        """
        函数说明:
        - 通过 NapCat API 拉取群内所有成员的完整信息，并以CSV格式保存
        - 字段包含: 用户ID, QQ号(同用户ID), 群名称(card优先, 否则nickname), 昵称, 获取时间
        参数:
        - group_id: 群聊ID
        返回:
        - CSV文件路径，失败返回None
        """
        try:
            members = await self._get_group_members(group_id, full_info=True)
            if not members:
                self.logger.error(f"群 {group_id} 未获取到成员列表，CSV保存失败")
                return None
            csv_path = self._csv_file_path(group_id)
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            rows = []
            for m in members:
                uid = str(m.get('user_id', '')).strip()
                card = str(m.get('card', '')).strip()
                nickname = str(m.get('nickname', '')).strip()
                display = card if card else nickname
                rows.append({
                    '用户ID': uid,
                    'QQ号': uid,
                    '群名称': display,
                    '昵称': nickname,
                    '获取时间': m.get('fetch_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                })
            # 写入CSV（UTF-8 BOM 兼容）
            import csv as _csv
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = _csv.DictWriter(f, fieldnames=['用户ID', 'QQ号', '群名称', '昵称', '获取时间'])
                writer.writeheader()
                writer.writerows(rows)
            self.logger.info(f"群 {group_id} 成员CSV已创建: {csv_path}")
            return csv_path
        except Exception as e:
            self.logger.error(f"保存群成员CSV失败: {e}")
            return None
    
    async def update_member_to_csv_after_delay(self, group_id: str, user_id: str, delay_seconds: int = 300) -> bool:
        """
        函数说明:
        - 延迟指定秒数后获取单个成员的详细信息，并更新到CSV中（存在则覆盖，不存在则追加）
        参数:
        - group_id: 群聊ID
        - user_id: 成员QQ号/用户ID
        - delay_seconds: 延迟时间，默认300秒
        返回:
        - 更新是否成功
        """
        try:
            await asyncio.sleep(max(0, int(delay_seconds)))
            detail = await self._fetch_single_member_detail(group_id, user_id)
            if not detail:
                self.logger.warning(f"群 {group_id} 成员 {user_id} 详细信息获取失败，跳过CSV更新")
                return False
            csv_path = self._csv_file_path(group_id)
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            # 使用pandas读写，便于覆盖更新
            try:
                import pandas as _pd
                if os.path.exists(csv_path):
                    try:
                        df = _pd.read_csv(csv_path, encoding='utf-8-sig')
                    except Exception:
                        df = _pd.DataFrame(columns=['用户ID', 'QQ号', '群名称', '昵称', '获取时间'])
                else:
                    df = _pd.DataFrame(columns=['用户ID', 'QQ号', '群名称', '昵称', '获取时间'])
                new_row = {
                    '用户ID': str(user_id),
                    'QQ号': str(user_id),
                    '群名称': detail.get('display_name', ''),
                    '昵称': detail.get('nickname', ''),
                    '获取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                if not df.empty and '用户ID' in df.columns:
                    mask = df['用户ID'].astype(str) == str(user_id)
                    if mask.any():
                        df.loc[mask, ['QQ号', '群名称', '昵称', '获取时间']] = [
                            new_row['QQ号'], new_row['群名称'], new_row['昵称'], new_row['获取时间']
                        ]
                    else:
                        df = _pd.concat([df, _pd.DataFrame([new_row])], ignore_index=True)
                else:
                    df = _pd.DataFrame([new_row])
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            except Exception as e:
                # pandas失败则使用csv模块追加
                import csv as _csv
                header = ['用户ID', 'QQ号', '群名称', '昵称', '获取时间']
                exists = os.path.exists(csv_path)
                with open(csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = _csv.DictWriter(f, fieldnames=header)
                    if not exists:
                        writer.writeheader()
                    writer.writerow({
                        '用户ID': str(user_id),
                        'QQ号': str(user_id),
                        '群名称': detail.get('display_name', ''),
                        '昵称': detail.get('nickname', ''),
                        '获取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
            self.logger.info(f"群 {group_id} 成员 {user_id} 已更新至CSV: {csv_path}")
            return True
        except Exception as e:
            self.logger.error(f"更新成员CSV失败: 群 {group_id}, 成员 {user_id}, 错误: {e}")
            return False
    
    async def _fetch_single_member_detail(self, group_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        函数说明:
        - 获取单个成员的详细信息，参考 get_group_member_info 的逻辑
        - 组装 display_name = card 优先，否则 nickname
        参数:
        - group_id: 群聊ID
        - user_id: 成员QQ号/用户ID
        返回:
        - dict: {'display_name','nickname'}，失败返回None
        """
        try:
            member_detail_request = {
                "action": "get_group_member_info",
                "params": {
                    "group_id": group_id,
                    "user_id": user_id
                }
            }
            detail_response = await self._call_napcat_api(member_detail_request)
            if detail_response and isinstance(detail_response, dict):
                if detail_response.get('status') == 'ok' and detail_response.get('retcode') == 0:
                    d = detail_response.get('data', {}) or {}
                    card = str(d.get('card', '') or '').strip()
                    nickname = str(d.get('nickname', '') or '').strip()
                    display = card if card else nickname
                    return {'display_name': display, 'nickname': nickname}
            return None
        except Exception as e:
            self.logger.error(f"获取单成员详细信息失败: 群 {group_id}, 成员 {user_id}, 错误: {e}")
            return None
    
    def lookup_name_by_qq_in_csv(self, group_id: str, qq_number: str) -> Optional[str]:
        """
        函数说明:
        - 在群成员CSV中根据QQ号查找展示名称（群名称优先，其次昵称）
        参数:
        - group_id: 群聊ID
        - qq_number: QQ号
        返回:
        - 展示名称字符串，未找到则返回None
        """
        try:
            csv_path = self._csv_file_path(group_id)
            if not os.path.exists(csv_path):
                return None
            import csv as _csv
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = _csv.DictReader(f)
                for row in reader:
                    if str(row.get('QQ号', '')).strip() == str(qq_number).strip():
                        name = str(row.get('群名称', '')).strip()
                        if name:
                            return name
                        nn = str(row.get('昵称', '')).strip()
                        return nn if nn else None
            return None
        except Exception:
            return None
    
    def _save_members_to_excel(self, members: List[Dict[str, Any]], group_id: str, full_info: bool = False) -> Optional[str]:
        """
        将群成员信息保存到Excel文件，更新现有文件而非新增
        
        Args:
            members (List[Dict[str, Any]]): 群成员列表
            group_id (str): 群组ID
            full_info (bool): 是否保存完整信息，默认False
            
        Returns:
            Optional[str]: Excel文件路径，失败时返回None
        """
        try:
            if not members:
                self.logger.warning(f"群 {group_id} 没有成员数据可保存")
                return None
            
            # 确保输出目录存在
            output_dir = os.path.join(os.getcwd(), 'output', 'group_members')
            os.makedirs(output_dir, exist_ok=True)
            
            # 固定文件名（不使用时间戳）
            filename = f"群成员_{group_id}.xlsx"
            file_path = os.path.join(output_dir, filename)
            
            # 创建新的成员数据DataFrame
            new_df = pd.DataFrame(members)
            
            # 检查文件是否已存在
            existing_df = None
            if os.path.exists(file_path):
                try:
                    # 读取现有文件
                    existing_df = pd.read_excel(file_path, engine='openpyxl')
                    self.logger.info(f"找到现有文件，将更新群名称字段: {file_path}")
                except Exception as e:
                    self.logger.warning(f"读取现有文件失败，将创建新文件: {e}")
                    existing_df = None
            
            if full_info:
                # 完整信息模式：包含所有字段
                column_mapping = {
                    'user_id': '用户ID',
                    'card': '群名称',
                    'nickname': '昵称',
                    'role': '角色',
                    'join_time': '加入时间',
                    'last_sent_time': '最后发言时间',
                    'level': '等级',
                    'age': '年龄',
                    'area': '地区',
                    'sex': '性别',
                    'title': '头衔',
                    'title_expire_time': '头衔过期时间',
                    'card_changeable': '可修改名片',
                    'shut_up_timestamp': '禁言到期时间',
                    'group_id': '群组ID',
                    'fetch_time': '获取时间'
                }
                
                # 重命名列
                new_df = new_df.rename(columns=column_mapping)
                
                # 格式化时间戳列
                if '加入时间' in new_df.columns:
                    new_df['加入时间'] = new_df['加入时间'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '未知')
                if '最后发言时间' in new_df.columns:
                    new_df['最后发言时间'] = new_df['最后发言时间'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '未发言')
                if '头衔过期时间' in new_df.columns:
                    new_df['头衔过期时间'] = new_df['头衔过期时间'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '永久')
                if '禁言到期时间' in new_df.columns:
                    new_df['禁言到期时间'] = new_df['禁言到期时间'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '未禁言')
                
                # 格式化性别
                if '性别' in new_df.columns:
                    sex_mapping = {'male': '男', 'female': '女', 'unknown': '未知'}
                    new_df['性别'] = new_df['性别'].map(sex_mapping).fillna('未知')
                
                # 格式化角色
                if '角色' in new_df.columns:
                    role_mapping = {'owner': '群主', 'admin': '管理员', 'member': '成员'}
                    new_df['角色'] = new_df['角色'].map(role_mapping).fillna('成员')
                
                # 格式化布尔值
                if '可修改名片' in new_df.columns:
                    new_df['可修改名片'] = new_df['可修改名片'].apply(lambda x: '是' if x else '否')
                
                # 如果存在现有文件，进行智能合并
                if existing_df is not None and not existing_df.empty:
                    # 以用户ID为键进行合并，优先使用新数据的群名称，保留现有数据的其他字段
                    if '用户ID' in existing_df.columns and '用户ID' in new_df.columns:
                        # 创建用户ID到群名称的映射
                        name_mapping = dict(zip(new_df['用户ID'], new_df['群名称']))
                        
                        # 更新现有数据中的群名称
                        existing_df['群名称'] = existing_df['用户ID'].map(name_mapping).fillna(existing_df['群名称'])
                        
                        # 添加新用户（如果有的话）
                        existing_user_ids = set(existing_df['用户ID'].tolist())
                        new_users = new_df[~new_df['用户ID'].isin(existing_user_ids)]
                        
                        if not new_users.empty:
                            # 确保新用户数据包含所有现有列
                            for col in existing_df.columns:
                                if col not in new_users.columns:
                                    new_users[col] = ''  # 或其他默认值
                            
                            # 合并数据
                            final_df = pd.concat([existing_df, new_users[existing_df.columns]], ignore_index=True)
                        else:
                            final_df = existing_df
                    else:
                        # 如果没有用户ID列，直接使用新数据
                        final_df = new_df
                else:
                    final_df = new_df
            else:
                # 基本信息模式：只保留群名称
                new_df = new_df[['card']].rename(columns={'card': '群名称'})
                
                # 如果存在现有文件且包含更多字段，只更新群名称字段
                if existing_df is not None and not existing_df.empty:
                    if '群名称' in existing_df.columns:
                        # 假设第一列是某种ID或索引，用于匹配
                        if len(existing_df) == len(new_df):
                            # 如果行数相同，直接更新群名称列
                            existing_df['群名称'] = new_df['群名称'].values
                            final_df = existing_df
                        else:
                            # 行数不同，使用新数据但保留现有结构
                            final_df = new_df
                            # 如果现有文件有额外列，为新数据添加空列
                            for col in existing_df.columns:
                                if col not in final_df.columns and col != '群名称':
                                    final_df[col] = ''
                    else:
                        final_df = new_df
                else:
                    final_df = new_df
            
            # 保存到Excel
            final_df.to_excel(file_path, index=False, engine='openpyxl')
            
            action = "更新" if existing_df is not None else "创建"
            self.logger.info(f"群成员信息已{action}: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"保存群成员信息到Excel失败: {e}")
            return None
    
    def _generate_member_stats(self, members: List[Dict[str, Any]], group_id: str) -> str:
        """
        生成群成员统计信息
        
        Args:
            members (List[Dict[str, Any]]): 群成员列表
            group_id (str): 群组ID
            
        Returns:
            str: 统计信息文本
        """
        try:
            # 统计角色分布
            role_stats = {}
            for member in members:
                role = member.get('role', '成员')
                role_stats[role] = role_stats.get(role, 0) + 1
            
            # 生成统计文本
            stats_lines = ['📈 角色分布：']
            for role, count in role_stats.items():
                stats_lines.append(f'• {role}: {count}人')
            
            # 添加其他统计信息
            active_members = sum(1 for member in members if member.get('last_sent_time', 0) > 0)
            stats_lines.append(f'• 有发言记录: {active_members}人')
            stats_lines.append(f'• 无发言记录: {len(members) - active_members}人')
            
            return '\n'.join(stats_lines)
            
        except Exception as e:
            self.logger.error(f"生成统计信息失败: {e}")
            return '统计信息生成失败'
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取群成员获取服务的帮助文本
        
        Returns:
            Dict[str, Any]: 包含帮助信息的字典
        """
        help_content = """
📋 群成员获取功能

📝 可用指令：
• /成员获取 - 获取当前群聊的成员列表并保存到Excel

✨ 功能说明：
• 通过napcat API获取真实群成员信息
• 自动保存成员信息到Excel表格中
• 包含QQ号、群名称、角色、等级等详细信息
• 支持时间戳格式化显示
• 提供成员统计和角色分布信息

📊 记录信息：
• QQ号 - 成员的QQ号码
• 群名称 - 成员在群中的昵称
• 角色 - 群主/管理员/成员
• 群ID - 所属群聊的ID
• 等级 - 群等级信息
• 加入时间 - 加入群聊的时间
• 最后发言时间 - 最近一次发言的时间
• 获取时间 - 数据获取的时间

⚠️ 使用要求：
• 需要napcat正常运行并连接
• 机器人需要在目标群聊中
• 需要获取群成员列表的权限
• 需要安装pandas和openpyxl库

💡 使用示例：
• /成员获取 - 获取当前群的所有成员信息

🎯 小贴士：
• Excel文件保存在data/group_members目录下
• 文件名包含群ID和时间戳便于区分
• 支持自动调整列宽以便查看
• 时间戳会自动格式化为可读格式
        """.strip()
        
        return {
            'content': help_content,
            'mixed_message': False
        }
