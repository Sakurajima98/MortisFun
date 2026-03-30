#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
车队服务模块 - 全新实现

功能特性：
- 支持跨天报班（如 23:00-3:00）
- 支持多天报班（如 8.3-8.7）
- 基于时间戳的精确人数计算
- 每个时间段最多5人限制
- 权限管理和自动过期处理

作者: Assistant
创建时间: 2025-08-14
"""

import json
import os
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set, Union
from .base_service import BaseService

# 导入图片生成相关模块
try:
    from utils.team_query import TeamQuery
    from utils.image_generator import ImageGenerator
    IMAGE_GENERATION_AVAILABLE = True
except ImportError as e:
    logging.warning(f"图片生成模块导入失败: {e}")
    IMAGE_GENERATION_AVAILABLE = False


class TeamService(BaseService):
    """
    车队服务类 - 全新实现
    
    主要功能：
    - 车队创建和管理
    - 成员加入和退出
    - 基于时间戳的精确人数统计
    - 跨天和多天报班支持
    - 权限验证和数据持久化
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, message_sender=None, server=None, reminder_service=None):
        """
        初始化车队服务
        
        Args:
            config: 配置信息
            data_manager: 数据管理器
            text_formatter: 文本格式化器
            message_sender: 消息发送回调函数
            server: 服务器实例，用于日志格式化
            reminder_service: 提醒服务实例，用于清理提醒记录
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.logger = logging.getLogger(__name__)
        self.max_team_size = 5  # 每个时间段最大人数
        self.teams_file = os.path.join(self.data_manager.base_path, 'team', 'teams.json')
        self.teams_record_file = os.path.join(self.data_manager.base_path, 'team', 'teams_record.json')
        self.message_sender = message_sender  # 消息发送回调函数
        self.reminder_service = reminder_service  # 提醒服务实例
        # 待确认的修改缓存（key: group_id:user_id）
        self.pending_modifications: Dict[str, Dict[str, Any]] = {}
        
        # 确保数据目录存在
        os.makedirs(os.path.dirname(self.teams_file), exist_ok=True)
        
        # 初始化数据结构
        self.teams_data = self._load_teams()
        self.teams_record = self._load_teams_record()
        
        # 启动时清理过期车队
        self._cleanup_expired_teams_on_startup()
        
        # 初始化推车时长统计服务
        try:
            from .push_time_statistics_service import PushTimeStatisticsService
            self.push_time_service = PushTimeStatisticsService(config, data_manager, text_formatter)
        except Exception as e:
            self.log_unified("WARNING", f"初始化推车统计服务失败: {e}", group_id="system", user_id="system")
            self.push_time_service = None
        
        # 初始化图片生成器
        if IMAGE_GENERATION_AVAILABLE:
            try:
                self.team_query = TeamQuery(data_dir=os.path.join(self.data_manager.base_path, 'team'))
                self.image_generator = ImageGenerator()
            except Exception as e:
                self.log_unified("WARNING", f"初始化图像/车队查询组件失败: {e}", group_id="system", user_id="system")
                self.team_query = None
                self.image_generator = None
        else:
            self.team_query = None
            self.image_generator = None
    
    def _load_teams(self) -> Dict[str, Any]:
        """
        加载车队数据
        
        Returns:
            车队数据字典
        """
        try:
            if os.path.exists(self.teams_file):
                with open(self.teams_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 验证数据结构
                    if isinstance(data, dict):
                        return data
            return {}
        except Exception:
            return {}
    
    def _save_teams(self) -> None:
        """
        保存车队数据到文件
        """
        try:
            with open(self.teams_file, 'w', encoding='utf-8') as f:
                json.dump(self.teams_data, f, ensure_ascii=False, indent=2)
            self.log_unified("DEBUG", "车队数据保存成功", group_id="system", user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"保存车队数据失败: {e}", group_id="system", user_id="system")
    
    def _load_teams_record(self) -> Dict[str, Any]:
        """
        加载车队记录数据
        
        Returns:
            车队记录数据字典
        """
        try:
            if os.path.exists(self.teams_record_file):
                with open(self.teams_record_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.log_unified("ERROR", f"加载车队记录失败: {e}", group_id="system", user_id="system")
            return {}
    
    def _save_teams_record(self) -> None:
        """
        保存车队记录到文件
        """
        try:
            with open(self.teams_record_file, 'w', encoding='utf-8') as f:
                json.dump(self.teams_record, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log_unified("ERROR", f"保存车队记录失败: {e}", group_id="system", user_id="system")
    
    def _cleanup_expired_teams_on_startup(self) -> None:
        """
        启动时清理过期车队
        """
        try:
            current_time = datetime.now()
            expired_count = 0
            
            for group_id in list(self.teams_data.keys()):
                group_data = self.teams_data[group_id]
                if 'teams' not in group_data:
                    continue
                
                for team_id in list(group_data['teams'].keys()):
                    team = group_data['teams'][team_id]
                    try:
                        # 检查车队是否过期（立即清理过期车队）
                        end_timestamp = team.get('end_timestamp', 0)
                        if current_time.timestamp() > end_timestamp:
                            self._archive_expired_team(group_id, team_id, team)
                            expired_count += 1
                    except Exception as e:
                        # 使用统一日志记录
                        self.log_unified("ERROR", f"清理过期车队失败 {team_id}: {e}", group_id="system", user_id="system")
            
            if expired_count > 0:
                # 使用统一日志记录
                self.log_unified("INFO", f"启动时清理了 {expired_count} 个过期车队", group_id="system", user_id="system")
                self._save_teams()
        except Exception as e:
            self.log_unified("ERROR", f"启动清理过期车队失败: {e}", group_id="system", user_id="system")
    
    def _cleanup_expired_teams(self, group_id: str) -> None:
        """
        清理指定群组的过期车队
        将过期车队移动到teams_record中，释放车队号供重新分配
        
        Args:
            group_id: 群组ID
        """
        try:
            current_time = datetime.now()
            
            if group_id not in self.teams_data or 'teams' not in self.teams_data[group_id]:
                return

            expired_count = self._cleanup_expired_teams_until(group_id, current_time.timestamp())
            if expired_count > 0:
                self.log_unified("INFO", f"清理了 {expired_count} 个过期车队，释放了车队号供重新分配", group_id, "system")
        
        except Exception as e:
            # 使用统一日志记录
            self.log_unified("ERROR", f"清理过期车队失败: {e}", group_id, "system")

    def _cleanup_expired_teams_until(self, group_id: str, cutoff_timestamp: float) -> int:
        try:
            if group_id not in self.teams_data or 'teams' not in self.teams_data[group_id]:
                return 0

            expired_team_ids: List[str] = []
            for team_id, team in list(self.teams_data[group_id]['teams'].items()):
                end_ts = team.get('end_timestamp', 0)
                if end_ts and float(end_ts) <= float(cutoff_timestamp):
                    expired_team_ids.append(team_id)

            for team_id in expired_team_ids:
                team = self.teams_data[group_id]['teams'][team_id]
                self._archive_expired_team(group_id, team_id, team)

            if expired_team_ids:
                self._save_teams()

            return len(expired_team_ids)
        except Exception as e:
            self.log_unified("ERROR", f"按截止时间清理过期车队失败: {e}", group_id, "system")
            return 0

    def archive_expired_teams_until(self, group_id: str, cutoff_timestamp: float) -> int:
        try:
            return self._cleanup_expired_teams_until(group_id, cutoff_timestamp)
        except Exception as e:
            self.log_unified("ERROR", f"归档截止时间前过期车队失败: {e}", group_id, "system")
            return 0
    
    def _archive_expired_team(self, group_id: str, team_id: str, team: Dict[str, Any], expire_reason: str = 'natural') -> None:
        """
        归档过期车队到记录文件
        
        Args:
            group_id: 群组ID
            team_id: 车队ID
            team: 车队数据
            expire_reason: 过期原因，'natural'表示自然过期，'cancelled'表示撤回
        """
        try:
            # 确保记录结构存在
            if group_id not in self.teams_record:
                self.teams_record[group_id] = {'archived_teams': []}
            if 'archived_teams' not in self.teams_record[group_id]:
                self.teams_record[group_id]['archived_teams'] = []
            
            # 添加归档时间戳和过期原因
            team['archived_at'] = datetime.now().isoformat()
            team['team_id'] = team_id
            team['expire_reason'] = expire_reason
            
            # 归档车队
            self.teams_record[group_id]['archived_teams'].append(team)
            
            # 从活跃车队中删除
            if group_id in self.teams_data and 'teams' in self.teams_data[group_id]:
                if team_id in self.teams_data[group_id]['teams']:
                    del self.teams_data[group_id]['teams'][team_id]
                
                # 释放车队号
                team_number = team.get('team_number')
                if team_number and 'team_numbers' in self.teams_data[group_id]:
                    # 确保类型转换正确 - team_numbers的键是字符串
                    team_number_str = str(team_number)
                    if team_number_str in self.teams_data[group_id]['team_numbers']:
                        del self.teams_data[group_id]['team_numbers'][team_number_str]
                        self.log_unified("INFO", f"释放车队号: {team_number_str} (归档原因: {expire_reason})", group_id, "system")
                    else:
                        self.log_unified("WARNING", f"车队号 {team_number_str} 不在team_numbers中", group_id, "system")
            
            self._save_teams_record()
            reason_text = '自然过期' if expire_reason == 'natural' else '撤回'
            self.log_unified("INFO", f"车队 {team_id} 已归档 ({reason_text})", group_id, "system")
        except Exception as e:
            self.log_unified("ERROR", f"归档车队失败: {e}", group_id=group_id)
    
    def _parse_date_range(self, date_str: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        解析日期范围字符串
        
        Args:
            date_str: 日期字符串，如 "8.3" 或 "8.3-8.7"
            
        Returns:
            (开始日期, 结束日期) 元组，失败返回(None, None)
        """
        try:
            if '-' in date_str:
                # 日期范围
                start_str, end_str = date_str.split('-', 1)
                start_date = self._parse_single_date(start_str.strip())
                end_date = self._parse_single_date(end_str.strip())
                return start_date, end_date
            else:
                # 单个日期
                single_date = self._parse_single_date(date_str)
                return single_date, single_date
        except Exception as e:
            self.logger.error(f"解析日期范围失败 {date_str}: {e}")
            return None, None
    
    def _parse_single_date(self, date_str: str) -> Optional[datetime]:
        """
        解析单个日期字符串
        
        Args:
            date_str: 日期字符串，如 "8.3" 或 "2024.8.3"
            
        Returns:
            解析后的日期对象，失败返回None
        """
        try:
            current_date = datetime.now()
            current_year = current_date.year
            
            if '.' in date_str:
                parts = date_str.split('.')
                if len(parts) == 2:
                    # "8.3" 格式 - 使用当前年份，不自动推到下一年
                    month, day = int(parts[0]), int(parts[1])
                    return datetime(current_year, month, day)
                elif len(parts) == 3:
                    # "2024.8.3" 格式
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                    return datetime(year, month, day)
            
            return None
        except (ValueError, IndexError) as e:
            self.logger.error(f"解析单个日期失败 {date_str}: {e}")
            return None
    
    def _parse_time_range(self, time_str: str) -> Tuple[Optional[int], Optional[int]]:
        """
        解析时间范围字符串，转换为分钟数
        
        Args:
            time_str: 时间字符串，如 "13:00-15:00" 或 "23:00-3:00"
            
        Returns:
            (开始分钟数, 结束分钟数) 元组，失败返回(None, None)
            分钟数从0开始计算，跨天时结束时间会大于1440
        """
        try:
            if '-' not in time_str:
                return None, None
            
            start_str, end_str = time_str.split('-', 1)
            start_minutes = self._time_to_minutes(start_str.strip())
            end_minutes = self._time_to_minutes(end_str.strip())
            
            if start_minutes is None or end_minutes is None:
                return None, None
            
            # 处理跨天情况
            if end_minutes <= start_minutes:
                end_minutes += 1440  # 加一天的分钟数
            
            return start_minutes, end_minutes
        except Exception as e:
            self.logger.error(f"解析时间范围失败 {time_str}: {e}")
            return None, None
    
    def _time_to_minutes(self, time_str: str) -> Optional[int]:
        """
        将时间字符串转换为分钟数
        
        Args:
            time_str: 时间字符串，如 "13:00" 或 "13"
            
        Returns:
            分钟数，失败返回None
        """
        try:
            if ':' in time_str:
                # "13:00" 格式
                hour, minute = time_str.split(':', 1)
                return int(hour) * 60 + int(minute)
            else:
                # "13" 格式
                return int(time_str) * 60
        except (ValueError, IndexError) as e:
            self.logger.error(f"时间转换失败 {time_str}: {e}")
            return None
    
    def _minutes_to_time(self, minutes: int) -> str:
        """
        将分钟数转换为时间字符串
        
        Args:
            minutes: 分钟数
            
        Returns:
            时间字符串，如 "13:00" 或 "01:00(次日)"
        """
        try:
            # 处理跨天情况
            if minutes >= 1440:
                actual_minutes = minutes - 1440
                hour = actual_minutes // 60
                minute = actual_minutes % 60
                return f"{hour:02d}:{minute:02d}(次日)"
            else:
                hour = minutes // 60
                minute = minutes % 60
                return f"{hour:02d}:{minute:02d}"
        except Exception as e:
            self.logger.error(f"分钟转时间失败 {minutes}: {e}")
            return "00:00"
    
    def _create_timestamp_range(self, date_start: datetime, date_end: datetime, 
                               time_start_minutes: int, time_end_minutes: int) -> List[Tuple[float, float]]:
        """
        创建时间戳范围列表
        
        Args:
            date_start: 开始日期
            date_end: 结束日期
            time_start_minutes: 开始时间（分钟）
            time_end_minutes: 结束时间（分钟）
            
        Returns:
            时间戳范围列表，每个元素为(开始时间戳, 结束时间戳)
        """
        try:
            timestamp_ranges = []
            current_date = date_start
            
            while current_date <= date_end:
                # 计算当天的开始和结束时间戳
                day_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
                
                # 开始时间戳
                start_timestamp = day_start.timestamp() + time_start_minutes * 60
                
                # 结束时间戳（处理跨天）
                if time_end_minutes >= 1440:
                    # 跨天情况
                    end_timestamp = day_start.timestamp() + time_end_minutes * 60
                else:
                    # 同天情况
                    end_timestamp = day_start.timestamp() + time_end_minutes * 60
                
                timestamp_ranges.append((start_timestamp, end_timestamp))
                current_date += timedelta(days=1)
            
            return timestamp_ranges
        except Exception as e:
            self.logger.error(f"创建时间戳范围失败: {e}")
            return []
    
    def _check_timestamp_overlap(self, range1: List[Tuple[float, float]], 
                                range2: List[Tuple[float, float]]) -> bool:
        """
        检查两个时间戳范围是否有重叠
        
        Args:
            range1: 时间戳范围1
            range2: 时间戳范围2
            
        Returns:
            是否有重叠
        """
        try:
            for start1, end1 in range1:
                for start2, end2 in range2:
                    # 检查是否有重叠：开始时间小于对方结束时间，且结束时间大于对方开始时间
                    if start1 < end2 and end1 > start2:
                        return True
            return False
        except Exception as e:
            self.logger.error(f"检查时间戳重叠失败: {e}")
            return False
    
    def _check_timestamp_within(self, user_ranges: List[Tuple[float, float]], 
                               team_ranges: List[Tuple[float, float]]) -> bool:
        """
        检查用户时间范围是否完全在车队时间范围内
        
        Args:
            user_ranges: 用户时间戳范围
            team_ranges: 车队时间戳范围
            
        Returns:
            用户时间是否完全在车队时间内
        """
        try:
            # 用户的每个时间段都必须完全在车队的某个时间段内
            for user_start, user_end in user_ranges:
                is_within = False
                for team_start, team_end in team_ranges:
                    # 检查用户时间段是否完全在车队时间段内
                    if user_start >= team_start and user_end <= team_end:
                        is_within = True
                        break
                # 如果用户的某个时间段不在车队任何时间段内，返回False
                if not is_within:
                    return False
            return True
        except Exception as e:
            self.logger.error(f"检查时间戳包含关系失败: {e}")
            return False
    
    def _init_group_data(self, group_id: str) -> None:
        """
        初始化群组数据结构
        
        Args:
            group_id: 群组ID
        """
        if group_id not in self.teams_data:
            self.teams_data[group_id] = {
                'teams': {},
                'team_numbers': {},  # 车队号到team_id的映射
                'next_team_number': 1
            }
        else:
            # 如果群组数据已存在，确保team_numbers映射正确
            group_data = self.teams_data[group_id]
            if 'team_numbers' not in group_data:
                group_data['team_numbers'] = {}
            
            # 重建team_numbers映射，确保与现有车队数据一致
            if 'teams' in group_data:
                team_numbers = {}
                for team_id, team in group_data['teams'].items():
                    if 'team_number' in team:
                        team_numbers[team['team_number']] = team_id
                group_data['team_numbers'] = team_numbers
                self.log_unified("INFO", f"群组 {group_id} 重建车队号映射，共 {len(team_numbers)} 个车队号", group_id=group_id)
    
    def _allocate_team_number(self, group_id: str) -> int:
        """
        分配新的车队号
        优先重用已过期车队释放的号码
        
        Args:
            group_id: 群组ID
            
        Returns:
            新的车队号
        """
        self._init_group_data(group_id)
        
        # 先清理过期车队，释放车队号
        self._cleanup_expired_teams(group_id)
        
        group_data = self.teams_data[group_id]
        
        # 从1开始寻找最小的可用车队号（重用已释放的号码）
        team_number = 1
        while team_number in group_data['team_numbers']:
            team_number += 1
        
        return team_number
    
    def _generate_team_id(self, captain: str, date_start: datetime, date_end: datetime, 
                         time_start: int, time_end: int) -> str:
        """
        生成车队ID
        
        Args:
            captain: 队长名称
            date_start: 开始日期
            date_end: 结束日期
            time_start: 开始时间（分钟）
            time_end: 结束时间（分钟）
            
        Returns:
            车队ID
        """
        try:
            date_str = f"{date_start.strftime('%Y%m%d')}-{date_end.strftime('%Y%m%d')}"
            time_str = f"{time_start}-{time_end}"
            return f"{captain}_{date_str}_{time_str}"
        except Exception as e:
            # 获取当前时间戳
            current_time = datetime.now()
            timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            self.log_unified("ERROR", f"生成车队ID失败: {e}", group_id="system", user_id="system")
            return f"{captain}_{datetime.now().timestamp()}"
    
    async def create_team(self, date_range: str, time_range: str, song: str, 
                         captain: str, group_id: str, captain_user_id: str = None, description: str = "",
                         laps: Optional[float] = None, multiplier: Optional[float] = None, comprehensive_power: Optional[float] = None) -> str:
        """
        创建新车队
        
        Args:
            date_range: 日期范围，如 "8.3" 或 "8.3-8.7"
            time_range: 时间范围，如 "13:00-15:00" 或 "23:00-3:00"
            song: 歌曲名称
            captain: 队长名称
            group_id: 群组ID
            captain_user_id: 队长用户ID
            description: 车队描述信息（可选）
            
        Returns:
            创建结果消息
        """
        try:
            # 解析日期范围
            date_start, date_end = self._parse_date_range(date_range)
            if not date_start or not date_end:
                return "❌ 日期格式错误！请使用格式：8.3 或 8.3-8.7"
            
            # 解析时间范围
            time_start, time_end = self._parse_time_range(time_range)
            if time_start is None or time_end is None:
                return "❌ 时间格式错误！请使用格式：HH:MM-HH:MM"
            
            # 检查创建车队的时间是否合法 - 不能创建已过期的时间
            current_time = datetime.now()
            
            # 创建时间戳范围来检查是否过期
            temp_timestamp_ranges = self._create_timestamp_range(date_start, date_end, time_start, time_end)
            if temp_timestamp_ranges:
                # 检查车队时间的开始时间是否已过期
                if current_time.timestamp() > temp_timestamp_ranges[0][0]:
                    return "❌ 不能创建已过期的时间段的车队！"
                # 检查车队时间的结束时间是否已过期
                if current_time.timestamp() > temp_timestamp_ranges[-1][1]:
                    return "❌ 不能创建已过期的时间段的车队！"
            
            # 初始化群组数据
            self._init_group_data(group_id)
            
            # 生成车队ID
            team_id = self._generate_team_id(captain, date_start, date_end, time_start, time_end)
            
            # 检查是否已存在相同车队
            if team_id in self.teams_data[group_id]['teams']:
                return f"❌ 车队已存在！队长 {captain} 在该时间段已有车队"
            
            # 检查队长是否有时间重叠的车队
            new_timestamp_ranges = self._create_timestamp_range(date_start, date_end, time_start, time_end)
            
            for existing_team in self.teams_data[group_id]['teams'].values():
                if existing_team['captain'] == captain:
                    existing_ranges = existing_team['timestamp_ranges']
                    # 将存储的时间戳范围转换为元组列表
                    existing_ranges_tuples = [(r[0], r[1]) for r in existing_ranges]
                    
                    if self._check_timestamp_overlap(new_timestamp_ranges, existing_ranges_tuples):
                        return f"❌ 时间冲突！队长 {captain} 在该时间段已有重叠的车队"
            
            # 分配车队号
            team_number = self._allocate_team_number(group_id)
            
            # 格式化日期显示 - 对于跨日时间，显示完整的日期范围
            if date_start == date_end and time_start < time_end:
                # 同一天且不跨日
                date_display = date_start.strftime('%m.%d')
            elif date_start == date_end and time_start >= time_end:
                # 同一天但跨日（如23:00-5:00）
                next_day = date_start + timedelta(days=1)
                date_display = f"{date_start.strftime('%m.%d')}-{next_day.strftime('%m.%d')}"
            else:
                # 多天范围
                date_display = f"{date_start.strftime('%m.%d')}-{date_end.strftime('%m.%d')}"
            
            # 创建车队数据
            team_data = {
                'team_number': team_number,
                'captain': captain,
                'captain_user_id': captain_user_id or captain,
                'date_range': date_range,
                'date_display': date_display,
                'time_range': time_range,
                'time_start_minutes': time_start,
                'time_end_minutes': time_end,
                'song': song,
                'description': description,  # 新增描述字段
                'comprehensive_power': comprehensive_power,
                'laps': laps,
                'multiplier': multiplier,
                'members': [],
                'substitutes': [],  # 替补成员列表
                'timestamp_ranges': new_timestamp_ranges,
                'start_timestamp': new_timestamp_ranges[0][0],
                'end_timestamp': new_timestamp_ranges[-1][1],
                'created_at': current_time.isoformat(),
                'group_id': group_id,
                'license_plate': None
            }
            
            # 保存车队
            self.teams_data[group_id]['teams'][team_id] = team_data
            self.teams_data[group_id]['team_numbers'][team_number] = team_id
            self._save_teams()
            
            # 记录车队创建成功日志
            self.log_unified("INFO", f"车队创建成功 - 车队号:{team_number}, 队长:{captain}, 歌曲:{song}", group_id, captain_user_id or captain)
            
            # 发送@全体成员的车队创建通知
            await self._send_team_creation_notification(team_data, group_id)
            
            # 自动记录车队创建的推车时长统计
            if self.push_time_service:
                try:
                    await self.push_time_service.auto_record_team_creation(
                        captain,
                        captain_user_id or captain,
                        group_id,
                        team_data['timestamp_ranges'],
                        date_display=team_data.get('date_display'),
                        time_range=team_data.get('time_range')
                    )
                except Exception as e:
                    self.log_unified("ERROR", f"自动记录车队创建统计失败: {e}", group_id, captain_user_id or captain)
            
            # 构建返回消息
            result_msg = (f"✅ 车队创建成功！\n"
                         f"🚗 车队号：{team_number}\n"
                         f"👨‍✈️ 队长：{captain}\n"
                         f"📅 日期：{date_display}\n"
                         f"⏰ 时间：{time_range}\n"
                         f"🎵 歌曲：{song}")

            # 如果有综合力、要求周回或要求倍率，添加到消息中
            if comprehensive_power is not None:
                try:
                    v = float(comprehensive_power)
                    if v >= 10000:
                        s = f"{(v/10000.0):.2f}".rstrip('0').rstrip('.')
                        result_msg += f"\n💪 车主综合：{s}w"
                    else:
                        result_msg += f"\n💪 车主综合：{int(v) if v.is_integer() else v}"
                except Exception:
                    result_msg += f"\n💪 车主综合：{comprehensive_power}"
            if laps is not None:
                result_msg += f"\n🔁 要求周回：{laps}"
            if multiplier is not None:
                result_msg += f"\n✖️ 要求倍率：{multiplier}"
            
            # 如果有描述，添加到消息中
            if description.strip():
                result_msg += f"\n📝 描述：{description}"
            
            return result_msg
        
        except Exception as e:
            self.log_unified("ERROR", f"创建车队失败: {e}", group_id=group_id)
            return f"❌ 创建车队失败: {str(e)}"
    
    def get_help_text(self) -> str:
        """
        获取帮助文本
        
        Returns:
            帮助文本
        """
        return """🚗 车队报班功能 🚗

📝 报班指令：
/报班 日期 时间 歌曲名称 车队队长名称 车主综合 [周回] [倍率] [描述]
车主综合可输入纯数字或带w后缀（表示万），如 29w、31.6w、29657
例如：/报班 8.3 13:00-15:00 龙 Mortis 31.6w
例如：/报班 8.3-8.7 13:00-15:00 龙 Mortis 29w (多天报班)
例如：/报班 8.3 23:00-3:00 龙 Mortis 29657 (跨天报班)

🚌 上车指令：
/推车 车队队长名称/班号 日期 时间 车队队员名称
/跑推 车队队长名称/班号 日期 时间 车队队员名称
/共跑 车队队长名称 日期 时间 车队队员名称
例如：/推车 Mortis 8.3 13:00-15:00 小明
例如：/跑推 Mortis 8.3 13:00-15:00 小明
例如：/推车 1 8.3 13:00-15:00 小明 (按班号推车)
例如：/跑推 1 8.3 13:00-15:00 小明 (按班号跑推)
例如：/推车 Mortis 8.3-8.7 13:00-15:00 小明 (多天车队)
例如：/跑推 Mortis 8.3-8.7 13:00-15:00 小明 (多天车队)

🔍 查询指令：
/车队查询 - 查看所有车队信息
/车队查询 车队号 - 查看指定车队号的车队信息
/车队查询 队长名称 - 查看指定队长的所有车队
/可报车队查询 - 查看可报名的车队
/看班 - 查看所有未过期车队的班次信息
/看班 名称 - 查看指定名称的班次信息

🚙 车牌管理：
/上传车牌 车队号 车牌号 (队长和成员都可以上传)
例如：/上传车牌 1 12345

❌ 撤回班车指令：
/撤回班车 车队队长名称/班号 日期 时间
例如：/撤回班车 Mortis 8.3 13:00-15:00
例如：/撤回班车 1 8.3 13:00-15:00

👤 队员撤回指令：
/队员撤回 车队队长名称/班号 日期 时间 队员名称
例如：/队员撤回 Mortis 8.3 13:00-15:00 小明
例如：/队员撤回 1 8.3 13:00-15:00 小明

💡 特性说明：
• 支持跨天报班（如23:00-3:00）
• 支持多天报班（如8.3-8.7）
• 每个时间段最多5人
• 自动过期管理
• 权限验证"""
    
    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理消息的入口方法
        
        Args:
            message: 用户消息
            user_id: 用户ID
            **kwargs: 其他参数
            
        Returns:
            处理结果字典或None
        """
        try:
            # 检查是否是车队相关指令
            team_commands = ['/报班', '/推车', '/跑推', '/共跑', '/车队查询', '/车队图片查询', '/可报车队查询', 
                           '/撤回班车', '/队员撤回', '/上传车牌', '/修改车队', '/确认修改车队', '/看班', '/替补推车', '/替补共跑']
            
            if not any(message.startswith(cmd) for cmd in team_commands):
                return None
            
            # 获取群组ID，确保转换为字符串类型
            group_id = str(kwargs.get('group_id', 'default'))
            
            # 处理消息并获取响应
            response = await self.handle_message(message, user_id, {'group_id': group_id})
            
            if response:
                # 如果响应是字典格式（如图片响应），直接返回
                if isinstance(response, dict):
                    return response
                # 否则包装为文本响应
                return {
                    'type': 'text',
                    'content': response
                }
            
            return None
        
        except Exception as e:
            # 获取当前时间戳
            current_time = datetime.now()
            timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            self.log_unified("ERROR", f"处理消息失败: {e}", "system", user_id)
            return {
                'type': 'text',
                'content': f"❌ 处理消息失败: {str(e)}"
            }
    
    async def handle_message(self, message: str, user_id: str, context: Dict[str, Any]) -> Optional[str]:
        """
        处理具体的车队指令
        
        Args:
            message: 用户消息
            user_id: 用户ID
            context: 上下文信息
            
        Returns:
            响应消息或None
        """
        try:
            group_id = str(context.get('group_id', 'default'))
            
            def _extract_first_at_qq(ctx: Dict[str, Any]) -> Optional[str]:
                """
                从原始OneBot事件中提取首个@的QQ号
                """
                try:
                    msg = ctx.get('message')
                    if isinstance(msg, list):
                        for seg in msg:
                            if seg.get('type') == 'at':
                                qq = str(seg.get('data', {}).get('qq') or '').strip()
                                if qq and qq != 'all':
                                    return qq
                except Exception:
                    pass
                return None
            
            # 报班指令: /报班 日期 时间 歌曲名称 车队队长名称 综合力 [周回] [倍率] [描述]
            if message.startswith('/报班'):
                tokens = message.split()
                if len(tokens) < 6:
                    return "❌ 报班格式错误！请使用：/报班 日期 时间 歌曲名称 车队队长名称 车主综合 [周回] [倍率] [描述]"

                # 必填参数
                _, date_range, time_range, song, captain, comp_str = tokens[:6]
                # CN归一化：删除前导空格
                captain = captain.lstrip()
                def _parse_comprehensive_power(s: str):
                    s1 = s.strip().lower()
                    if s1.endswith('w'):
                        num = s1[:-1]
                        try:
                            return float(num) * 10000
                        except Exception:
                            return None
                    try:
                        return float(s1)
                    except Exception:
                        return None
                comprehensive_power = _parse_comprehensive_power(comp_str)
                if comprehensive_power is None:
                    return "❌ 车主综合格式错误！可填纯数字或带w后缀，如 29w、31.6w、29657"

                # 可选参数：周回、倍率、描述
                optional = tokens[6:]
                laps: Optional[float] = None
                multiplier: Optional[float] = None
                description = ""

                def parse_int_in_range(s: str, min_v: int, max_v: int) -> Optional[int]:
                    try:
                        if '.' in s:
                            return None
                        v = int(s)
                        return v if (min_v <= v <= max_v) else None
                    except Exception:
                        return None

                def parse_float_in_range(s: str, min_v: float, max_v: float) -> Optional[float]:
                    try:
                        v = float(s)
                        return v if (min_v <= v <= max_v) else None
                    except Exception:
                        return None

                i = 0
                while i < len(optional):
                    tok = optional[i]
                    matched = False
                    if laps is None:
                        lap_val = parse_float_in_range(tok, 10.0, 40.0)
                        if lap_val is not None:
                            laps = lap_val
                            matched = True
                            i += 1
                            continue
                    if multiplier is None:
                        mul_val = parse_float_in_range(tok, 2.0, 4.0)
                        if mul_val is not None:
                            multiplier = mul_val
                            matched = True
                            i += 1
                            continue
                    if not matched:
                        description = " ".join(optional[i:])
                        break
                
                return await self.create_team(date_range, time_range, song, captain, group_id, user_id, description, laps=laps, multiplier=multiplier, comprehensive_power=comprehensive_power)
            
            # 推车指令: /推车 车队队长名称/班号 日期 时间 车队队员名称
            elif message.startswith('/推车'):
                parts = message.split(' ', 4)
                if len(parts) != 5:
                    return "❌ 推车格式错误！请使用：/推车 车队队长名称/班号 日期 时间 车队队员名称"
                
                _, leader_or_number, date_range, time_range, member_name = parts
                # CN归一化：删除前导空格
                member_name = member_name.lstrip()
                
                # 尝试解析为车队号
                try:
                    team_number = int(leader_or_number)
                    return await self.join_team_by_number(team_number, date_range, time_range, 
                                                        member_name, group_id, '推车', user_id)
                except ValueError:
                    return await self.join_team_by_captain(leader_or_number.lstrip(), date_range, time_range, 
                                                         member_name, group_id, '推车', user_id)
            
            # 跑推指令: /跑推 车队队长名称/班号 日期 时间 车队队员名称
            elif message.startswith('/跑推'):
                parts = message.split(' ', 4)
                if len(parts) != 5:
                    return "❌ 跑推格式错误！请使用：/跑推 车队队长名称/班号 日期 时间 车队队员名称"
                
                _, leader_or_number, date_range, time_range, member_name = parts
                member_name = member_name.lstrip()
                
                # 尝试解析为车队号
                try:
                    team_number = int(leader_or_number)
                    return await self.join_team_by_number(team_number, date_range, time_range, 
                                                        member_name, group_id, '跑推', user_id)
                except ValueError:
                    return await self.join_team_by_captain(leader_or_number.lstrip(), date_range, time_range, 
                                                         member_name, group_id, '跑推', user_id)
            
            # 共跑指令: /共跑 车队队长名称/班号 日期 时间 车队队员名称
            elif message.startswith('/共跑'):
                parts = message.split(' ', 4)
                if len(parts) != 5:
                    return "❌ 共跑格式错误！请使用：/共跑 车队队长名称/班号 日期 时间 车队队员名称"
                
                _, leader_or_number, date_range, time_range, member_name = parts
                member_name = member_name.lstrip()
                
                # 尝试解析为车队号
                try:
                    team_number = int(leader_or_number)
                    return await self.join_team_by_number(team_number, date_range, time_range, 
                                                        member_name, group_id, '共跑', user_id)
                except ValueError:
                    return await self.join_team_by_captain(leader_or_number.lstrip(), date_range, time_range, 
                                                               member_name, group_id, '共跑', user_id)
            
            # 车队查询指令
            elif message.startswith('/车队查询'):
                parts = message.split(' ', 1)
                if len(parts) == 1:
                    return await self.query_all_teams(group_id)
                else:
                    query_params = parts[1].strip().split()
                    
                    # 如果只有一个参数，尝试按原有逻辑处理（车队号或队长名称）
                    if len(query_params) == 1:
                        query_param = query_params[0]
                        # 尝试解析为车队号
                        try:
                            team_number = int(query_param)
                            return await self.query_team_by_number(team_number, group_id)
                        except ValueError:
                            # 不是车队号，使用多维度查询
                            return await self.query_teams_multi_dimension(query_params, group_id)
                    else:
                        # 多个参数，使用多维度查询
                        return await self.query_teams_multi_dimension(query_params, group_id)
            
            # 车队图片查询指令
            elif message.startswith('/车队图片查询'):
                parts = message.split(' ', 1)
                if len(parts) != 2:
                    return "❌ 车队图片查询格式错误！请使用：/车队图片查询 车队号"
                
                query_param = parts[1].strip()
                try:
                    team_number = int(query_param)
                    return await self.query_team_by_number(team_number, group_id, generate_image=True)
                except ValueError:
                    return "❌ 车队号必须是数字！"
            
            # 可报车队查询指令
            elif message.startswith('/可报车队查询'):
                parts = message.split(' ', 1)
                if len(parts) == 1:
                    return await self.query_available_teams(group_id)
                else:
                    query_params = parts[1].strip().split()
                    # 使用多维度查询，只返回可报名车队
                    return await self.query_teams_multi_dimension(query_params, group_id, available_only=True)
            
            # 撤回班车指令: /撤回班车 车队队长名称/班号 日期 时间
            elif message.startswith('/撤回班车'):
                parts = message.split(' ', 3)
                if len(parts) != 4:
                    return "❌ 撤回班车格式错误！请使用：/撤回班车 车队队长名称/班号 日期 时间"
                
                _, leader_or_number, date_range, time_range = parts
                
                # 尝试解析为车队号
                try:
                    team_number = int(leader_or_number)
                    return await self.cancel_team_by_number(team_number, date_range, time_range, user_id, group_id)
                except ValueError:
                    return await self.cancel_team_by_captain(leader_or_number.lstrip(), date_range, time_range, user_id, group_id)
            
            # 队员撤回指令: /队员撤回 车队队长名称/班号 日期 时间 队员名称
            elif message.startswith('/队员撤回'):
                parts = message.split(' ', 4)
                if len(parts) != 5:
                    return "❌ 队员撤回格式错误！请使用：/队员撤回 车队队长名称/班号 日期 时间 队员名称"
                
                _, leader_or_number, date_range, time_range, member_name = parts
                member_name = member_name.lstrip()
                
                # 尝试解析为车队号
                try:
                    team_number = int(leader_or_number)
                    return await self.remove_member_by_number(team_number, date_range, time_range, 
                                                            member_name, user_id, group_id)
                except ValueError:
                    return await self.remove_member_by_captain(leader_or_number.lstrip(), date_range, time_range, 
                                                             member_name, user_id, group_id)
            
            # 上传车牌指令: /上传车牌 车队号 车牌号
            elif message.startswith('/上传车牌'):
                parts = message.split(' ', 2)
                if len(parts) != 3:
                    return "❌ 上传车牌格式错误！请使用：/上传车牌 车队号 车牌号"
                
                _, team_number_str, license_plate = parts
                
                try:
                    team_number = int(team_number_str)
                    return await self.upload_license_plate(team_number, license_plate, user_id, group_id)
                except ValueError:
                    return "❌ 车队号必须是数字！"

            # 修改车队信息指令: /修改车队 队长名称/班号 日期 时间 [键=值...]
            elif message.startswith('/修改车队'):
                parts = message.split(' ', 4)
                if len(parts) < 4:
                    return "❌ 修改车队格式错误！请使用：/修改车队 队长名称/班号 日期 时间 [日期=... 时间=... 描述=... 歌曲=... 周回=... 倍率=...]"
                
                # 当只有4段时，没有键值对，提示错误
                if len(parts) == 4:
                    return "❌ 请至少提供一个需要修改的键值，例如 日期=8.4 或 时间=13:00-15:00"
                
                _, leader_or_number, date_range, time_range, changes_str = parts
                # 解析键值对，形式为 key=value，用空格分隔
                changes: Dict[str, str] = {}
                for kv in changes_str.strip().split():
                    if '=' in kv:
                        k, v = kv.split('=', 1)
                        changes[k.strip()] = v.strip()
                if not changes:
                    return "❌ 未解析到需要修改的内容，请使用 键=值 的形式"
                
                # 尝试解析为车队号
                try:
                    team_number = int(leader_or_number)
                    return await self.modify_team_info_by_number(team_number, date_range, time_range, user_id, group_id, changes)
                except ValueError:
                    return await self.modify_team_info_by_captain(leader_or_number, date_range, time_range, user_id, group_id, changes)

            # 修改确认指令：/确认修改车队（避免与/修改车队前缀冲突）
            elif message.startswith('/确认修改车队'):
                key = f"{context['group_id']}:{user_id}"
                pending = self.pending_modifications.get(key)
                if not pending:
                    return "❌ 当前没有需要确认的修改或已过期"
                if datetime.now().timestamp() > pending.get('expired_at', 0):
                    # 过期清理
                    del self.pending_modifications[key]
                    return "❌ 修改确认已过期，请重新发起修改"
                group_id = context['group_id']
                self._init_group_data(group_id)
                team_id = pending.get('team_id')
                team = self.teams_data[group_id]['teams'].get(team_id)
                if not team:
                    del self.pending_modifications[key]
                    return "❌ 目标车队不存在或已被撤回"
                # 权限检查：只有队长本人可确认
                if team.get('captain_user_id') != user_id and team.get('captain') != user_id:
                    return "❌ 只有队长本人可以确认并应用修改！"
                # 应用修改
                result = await self._modify_team_info(team, user_id, group_id, pending['changes'])
                # 清理pending
                del self.pending_modifications[key]
                return result if result is not None else "✅ 已应用车队修改并发送提醒"
            
            # 看班指令: /看班 [名称]
            elif message.startswith('/看班'):
                parts = message.split(' ', 1)
                # 优先解析@提及（即使无空格分隔）
                at_qq = _extract_first_at_qq(context)
                if at_qq:
                    # 先尝试通过推时统计的CSV映射QQ->CN，再按CN查询班次；失败则回退按QQ查询
                    try:
                        cn = None
                        if getattr(self, "push_time_service", None):
                            cn = self.push_time_service._get_cn_by_qq(group_id, at_qq)
                        if cn:
                            return await self.view_shifts_by_name(str(cn).lstrip(), group_id)
                    except Exception:
                        pass
                    return await self.view_shifts_by_user_id(at_qq, group_id)
                if len(parts) == 1:
                    # 无参数：根据当前用户QQ映射到CN后按名称查询；若映射失败，则按QQ查询
                    try:
                        cn = None
                        if getattr(self, "push_time_service", None):
                            cn = self.push_time_service._get_cn_by_qq(group_id, user_id)
                        if cn:
                            return await self.view_shifts_by_name(str(cn).lstrip(), group_id)
                    except Exception:
                        pass
                    return await self.view_shifts_by_user_id(user_id, group_id)
                else:
                    # 否则按名称查询
                    name = parts[1].strip()
                    return await self.view_shifts_by_name(name, group_id)
            
            # 替补推车指令: /替补推车 车队队长名称/班号 日期 时间 队员名称 [描述]
            elif message.startswith('/替补推车'):
                parts = message.split(' ', 5)
                if len(parts) < 5:
                    return "❌ 替补推车格式错误！请使用：/替补推车 车队队长名称/班号 日期 时间 队员名称 [描述]"
                
                if len(parts) == 5:
                    _, leader_or_number, date_range, time_range, member_name = parts
                    description = ""
                else:
                    _, leader_or_number, date_range, time_range, member_name, description = parts
                member_name = member_name.lstrip()
                
                # 尝试解析为车队号
                try:
                    team_number = int(leader_or_number)
                    return await self.join_substitute_by_number(team_number, date_range, time_range, 
                                                              member_name, group_id, '推车替补', user_id, description)
                except ValueError:
                    return await self.join_substitute_by_captain(leader_or_number.lstrip(), date_range, time_range, 
                                                               member_name, group_id, '推车替补', user_id, description)
            
            # 替补共跑指令: /替补共跑 车队队长名称/班号 日期 时间 队员名称 [描述]
            elif message.startswith('/替补共跑'):
                parts = message.split(' ', 5)
                if len(parts) < 5:
                    return "❌ 替补共跑格式错误！请使用：/替补共跑 车队队长名称/班号 日期 时间 队员名称 [描述]"
                
                if len(parts) == 5:
                    _, leader_or_number, date_range, time_range, member_name = parts
                    description = ""
                else:
                    _, leader_or_number, date_range, time_range, member_name, description = parts
                member_name = member_name.lstrip()
                
                # 尝试解析为车队号
                try:
                    team_number = int(leader_or_number)
                    return await self.join_substitute_by_number(team_number, date_range, time_range, 
                                                              member_name, group_id, '共跑替补', user_id, description)
                except ValueError:
                    return await self.join_substitute_by_captain(leader_or_number.lstrip(), date_range, time_range, 
                                                               member_name, group_id, '共跑替补', user_id, description)
            
            # 推车时长统计相关指令
            elif self.push_time_service and (
                message.startswith('/推时统计') or 
                message.startswith('/增加推时') or 
                message.startswith('/减少推时') or 
                message.startswith('/增加跑时') or 
                message.startswith('/减少跑时') or 
                message.startswith('/上传cn') or 
                message.startswith('/推时结算') or 
                message.startswith('/底标结算')
            ):
                return await self.push_time_service.handle_message(message, user_id, context)
            
            return None
        
        except Exception as e:
            group_id = context.get('group_id', '')
            self.log_unified("ERROR", f"处理车队指令失败: {e}", group_id=group_id, user_id=user_id)
            return f"❌ 处理指令失败: {str(e)}"
    
    async def join_team_by_number(self, team_number: int, date_range: str, time_range: str, 
                                 member_name: str, group_id: str, join_type: str, user_id: str) -> str:
        """
        按车队号加入车队
        
        Args:
            team_number: 车队号
            date_range: 日期范围
            time_range: 时间范围
            member_name: 队员名称
            group_id: 群组ID
            join_type: 加入类型（推车/共跑）
            user_id: 用户ID
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            # 查找车队
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在！"
            
            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'][team_id]
            
            return await self._join_team(team, date_range, time_range, member_name, join_type, user_id, group_id)
        
        except Exception as e:
            self.logger.error(f"按车队号加入车队失败: {e}")
            return f"❌ 加入车队失败: {str(e)}"
    
    async def join_team_by_captain(self, captain: str, date_range: str, time_range: str, 
                                  member_name: str, group_id: str, join_type: str, user_id: str) -> str:
        """
        按队长名称加入车队
        
        Args:
            captain: 队长名称
            date_range: 日期范围
            time_range: 时间范围
            member_name: 队员名称
            group_id: 群组ID
            join_type: 加入类型（推车/共跑）
            user_id: 用户ID
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            # 解析用户请求的时间范围
            user_date_start, user_date_end = self._parse_date_range(date_range)
            user_time_start, user_time_end = self._parse_time_range(time_range)
            
            if not user_date_start or not user_date_end or user_time_start is None or user_time_end is None:
                return "❌ 日期或时间格式错误！"
            
            # 创建用户请求的时间戳范围
            user_timestamp_ranges = self._create_timestamp_range(
                user_date_start, user_date_end, user_time_start, user_time_end
            )
            
            # 查找匹配的车队 - 使用时间戳重叠检查而不是字符串匹配
            matching_teams = []
            for team in self.teams_data[group_id]['teams'].values():
                if team['captain'] == captain:
                    # 检查时间范围是否有重叠
                    team_timestamp_ranges = [(r[0], r[1]) for r in team['timestamp_ranges']]
                    if self._check_timestamp_overlap(user_timestamp_ranges, team_timestamp_ranges):
                        matching_teams.append(team)
            
            if not matching_teams:
                return f"❌ 未找到队长 {captain} 在 {date_range} {time_range} 的车队！"
            
            if len(matching_teams) > 1:
                return f"❌ 找到多个匹配的车队，请使用车队号进行操作！"
            
            team = matching_teams[0]
            return await self._join_team(team, date_range, time_range, member_name, join_type, user_id, group_id)
        
        except Exception as e:
            self.logger.error(f"按队长名称加入车队失败: {e}")
            return f"❌ 加入车队失败: {str(e)}"
    
    async def _join_team(self, team: Dict[str, Any], date_range: str, time_range: str, 
                        member_name: str, join_type: str, user_id: str, group_id: str) -> str:
        """
        加入车队的核心逻辑
        
        Args:
            team: 车队数据
            date_range: 日期范围
            time_range: 时间范围
            member_name: 队员名称
            join_type: 加入类型
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            操作结果消息
        """
        try:
            self.log_unified("DEBUG", f"开始加入车队流程 - 队员: {member_name}, 车队: {team['team_number']}, 时间: {date_range} {time_range}", group_id, "system")
            
            # 解析用户请求的时间范围
            user_date_start, user_date_end = self._parse_date_range(date_range)
            user_time_start, user_time_end = self._parse_time_range(time_range)
            
            self.log_unified("DEBUG", f"时间解析结果 - 日期: {user_date_start} 到 {user_date_end}, 时间: {user_time_start} 到 {user_time_end}", group_id, "system")
            
            if not user_date_start or not user_date_end or user_time_start is None or user_time_end is None:
                self.log_unified("WARNING", f"时间格式错误 - 日期: {date_range}, 时间: {time_range}", group_id, "system")
                return "❌ 日期或时间格式错误！"
            
            # 检查报名时间是否合法 - 不能报名已过期的时间
            current_time = datetime.now()
            self.log_unified("DEBUG", f"检查时间有效性 - 当前时间: {current_time}", group_id, "system")
            
            # 创建时间戳范围来检查是否过期
            temp_timestamp_ranges = self._create_timestamp_range(
                user_date_start, user_date_end, user_time_start, user_time_end
            )
            
            # 检查报名的时间是否在当前时间之前（已过期）
            if temp_timestamp_ranges:
                # 检查报名时间的开始时间是否已过期
                if current_time.timestamp() > temp_timestamp_ranges[0][0]:
                    self.log_unified("WARNING", f"尝试报名已过期时间段 - 开始时间已过期: {temp_timestamp_ranges[0][0]}", group_id, "system")
                    return "❌ 不能报名已过期的时间段！"
                # 检查报名时间的结束时间是否已过期
                if current_time.timestamp() > temp_timestamp_ranges[-1][1]:
                    self.log_unified("WARNING", f"尝试报名已过期时间段 - 结束时间已过期: {temp_timestamp_ranges[-1][1]}", group_id, "system")
                    return "❌ 不能报名已过期的时间段！"
            
            # 创建用户请求的时间戳范围
            user_timestamp_ranges = self._create_timestamp_range(
                user_date_start, user_date_end, user_time_start, user_time_end
            )
            
            # 检查用户时间是否完全在车队时间段内
            team_timestamp_ranges = [(r[0], r[1]) for r in team['timestamp_ranges']]
            self.log_unified("DEBUG", f"检查时间段匹配 - 用户时间段: {len(user_timestamp_ranges)}个, 车队时间段: {len(team_timestamp_ranges)}个", group_id, "system")
            
            if not self._check_timestamp_within(user_timestamp_ranges, team_timestamp_ranges):
                self.log_unified("WARNING", f"时间段不匹配 - 用户时间不在车队时间段内: {team['date_display']} {team['time_range']}", group_id, "system")
                return f"❌ 用户时间必须完全在车队时间段内！车队时间：{team['date_display']} {team['time_range']}"
            
            # 检查是否是队长本人尝试加入车队
            if member_name == team['captain']:
                self.log_unified("WARNING", f"队长尝试加入自己的车队 - 队长: {member_name}", group_id, "system")
                return f"❌ 队长不能加入自己的车队！队长已经是车队的一员。"
            
            # 检查队员是否已存在
            self.log_unified("DEBUG", f"检查队员重复 - 当前车队成员数: {len(team['members'])}", group_id, "system")
            for member in team['members']:
                if member['name'] == member_name:
                    # 检查是否有时间重叠
                    member_ranges = [(r[0], r[1]) for r in member['timestamp_ranges']]
                    if self._check_timestamp_overlap(user_timestamp_ranges, member_ranges):
                        self.log_unified("WARNING", f"队员时间重叠 - {member_name} 在该时间段已在车队中", group_id, "system")
                        return f"❌ 队员 {member_name} 在该时间段已在车队中！"
            
            # 检查人数限制
            if not self._check_team_capacity(team, user_timestamp_ranges):
                self.log_unified("WARNING", f"车队人数已满 - 最大容量: {self.max_team_size}人", group_id, "system")
                return f"❌ 车队在该时间段已满员（最多{self.max_team_size}人）！"
            
            # 添加队员
            member_data = {
                'name': member_name,
                'user_id': user_id or member_name,
                'join_type': join_type,
                'date_range': date_range,
                'time_range': time_range,
                'timestamp_ranges': user_timestamp_ranges,
                'joined_at': current_time.isoformat()
            }
            
            team['members'].append(member_data)
            self._save_teams()
            
            self.log_unified("INFO", f"队员加入成功 - {member_name} 加入车队 {team['team_number']}, 类型: {join_type}", group_id, "system")
            
            # 自动记录成员加入的推车时长统计
            if self.push_time_service:
                try:
                    await self.push_time_service.auto_record_member_join(
                        member_name,
                        user_id or member_name,
                        join_type, 
                        group_id,
                        user_timestamp_ranges,
                        date_text=date_range,
                        time_text=time_range
                    )
                    self.log_unified("DEBUG", f"推车时长统计记录成功 - {member_name}", group_id, "system")
                except Exception as e:
                    self.log_unified("ERROR", f"自动记录成员加入统计失败: {traceback.format_exc()}", group_id, "system")
                    self.logger.error(f"自动记录成员加入统计失败: {e}")
            
            # 只有队长首次报班时才发送@全体成员通知，后续的推车和共跑不发送
            # 这里不发送通知，因为队长创建车队时已经发送过了
            
            return (f"✅ {join_type}成功！\n"
                   f"🚗 车队号：{team['team_number']}\n"
                   f"👨‍✈️ 队长：{team['captain']}\n"
                   f"👤 队员：{member_name}\n"
                   f"📅 日期：{date_range}\n"
                   f"⏰ 时间：{time_range}")
        
        except Exception as e:
            self.logger.error(f"加入车队核心逻辑失败: {e}")
            return f"❌ 加入车队失败: {str(e)}"
    
    async def _send_team_creation_notification(self, team: Dict[str, Any], group_id: str) -> None:
        """
        发送@全体成员的车队创建通知
        
        Args:
            team: 车队数据
            group_id: 群组ID
        """
        try:
            if not self.message_sender:
                return
            
            # 组装可选信息：综合力、要求周回、要求倍率、描述
            extra_lines = []
            if team.get('comprehensive_power') is not None:
                try:
                    v = float(team['comprehensive_power'])
                    if v >= 10000:
                        s = f"{(v/10000.0):.2f}".rstrip('0').rstrip('.')
                        extra_lines.append(f"💪 车主综合：{s}w")
                    else:
                        extra_lines.append(f"💪 车主综合：{int(v) if v.is_integer() else v}")
                except Exception:
                    extra_lines.append(f"💪 车主综合：{team['comprehensive_power']}")
            if team.get('laps') is not None:
                extra_lines.append(f"🔁 要求周回：{team['laps']}")
            if team.get('multiplier') is not None:
                extra_lines.append(f"✖️ 要求倍率：{team['multiplier']}")
            if team.get('description') and str(team.get('description', '')).strip():
                extra_lines.append(f"📝 描述：{team['description']}")
            extra_info = ("\n" + "\n".join(extra_lines) + "\n") if extra_lines else "\n"

            # 构建@全体成员的车队创建通知消息
            notification_message = {
                "action": "send_group_msg",
                "params": {
                    "group_id": group_id,
                    "message": [
                        {
                            "type": "at",
                            "data": {
                                "qq": "all"
                            }
                        },
                        {
                            "type": "text",
                            "data": {
                                "text": (
                                    f" 📢 车队报班通知\n\n"
                                    f"🚗 车队号：{team['team_number']}\n"
                                    f"👨‍✈️ 队长：{team['captain']}\n"
                                    f"🎵 歌曲：{team['song']}\n"
                                    f"📅 日期：{team['date_display']}\n"
                                    f"⏰ 时间：{team['time_range']}\n"
                                    f"{extra_info}"
                                    f"🎯 开始报班了！有需要的推手或者共跑可以加入车队\n"
                                    f"⚠️ 缺推时的小伙伴记得及时补充推时哦~"
                                )
                            }
                        }
                    ]
                }
            }
            
            # 发送通知消息
            await self.message_sender(notification_message)
            # 使用统一日志格式记录车队创建通知
            self.log_unified("INFO", "已被[车队服务]处理:车队创建通知发送，成功发送响应。", group_id, team['captain_user_id'])
            
        except Exception as e:
            self.logger.error(f"发送车队创建通知失败: {e}")
    
    # 已删除 _send_join_notification 方法
    # 根据需求，只有队长首次报班时才发送@全体成员通知，后续的推车和共跑不发送通知
    
    def _check_team_capacity(self, team: Dict[str, Any], new_timestamp_ranges: List[Tuple[float, float]]) -> bool:
        """
        检查车队容量是否允许新成员加入
        使用与 _has_available_slots 相同的细粒度时间段分析逻辑
        
        Args:
            team: 车队数据
            new_timestamp_ranges: 新成员的时间戳范围
            
        Returns:
            是否可以加入
        """
        try:
            current_time = datetime.now()
            
            # 收集所有时间点（包括队长、成员和新成员的时间点）
            all_time_points = set()
            
            # 添加队长的时间点
            for start_ts, end_ts in team['timestamp_ranges']:
                all_time_points.add(start_ts)
                all_time_points.add(end_ts)
            
            # 添加现有成员的时间点
            for member in team['members']:
                for start_ts, end_ts in member['timestamp_ranges']:
                    all_time_points.add(start_ts)
                    all_time_points.add(end_ts)
            
            # 添加新成员的时间点
            for start_ts, end_ts in new_timestamp_ranges:
                all_time_points.add(start_ts)
                all_time_points.add(end_ts)
            
            # 排序时间点
            sorted_time_points = sorted(all_time_points)
            
            # 检查每个细分时间段
            for i in range(len(sorted_time_points) - 1):
                segment_start = sorted_time_points[i]
                segment_end = sorted_time_points[i + 1]
                
                # 跳过过期的时间段
                if segment_end <= current_time.timestamp():
                    continue
                
                # 统计该时间段的人数
                count = 0
                
                # 检查队长是否在该时间段
                for start_ts, end_ts in team['timestamp_ranges']:
                    if start_ts <= segment_start and segment_end <= end_ts:
                        count += 1
                        break
                
                # 检查现有成员是否在该时间段
                for member in team['members']:
                    for start_ts, end_ts in member['timestamp_ranges']:
                        if start_ts <= segment_start and segment_end <= end_ts:
                            count += 1
                            break
                
                # 检查新成员是否在该时间段
                for start_ts, end_ts in new_timestamp_ranges:
                    if start_ts <= segment_start and segment_end <= end_ts:
                        count += 1
                        break
                
                # 如果该时间段人数超过限制，则不能加入
                if count > self.max_team_size:
                    return False
            
            return True
        except Exception as e:
            self.log_unified("ERROR", f"检查车队容量失败: {e}", group_id="system", user_id="system")
            return False
    
    async def query_all_teams(self, group_id: str) -> str:
        """
        查询所有车队
        
        Args:
            group_id: 群组ID
            
        Returns:
            车队信息
        """
        try:
            # 确保群组ID为字符串类型
            group_id = str(group_id)
            self.log_unified("DEBUG", f"开始查询车队 - 群组ID: '{group_id}' (类型: {type(group_id)})", group_id, "system")
            
            # 检查群组是否存在于teams_data中
            if group_id not in self.teams_data:
                self.log_unified("DEBUG", f"群组 '{group_id}' 不存在于teams_data中，当前群组: {list(self.teams_data.keys())}", group_id, "system")
                self._init_group_data(group_id)
                self.log_unified("DEBUG", f"已为群组 '{group_id}' 初始化空数据结构", group_id, "system")
            else:
                team_count = len(self.teams_data[group_id].get('teams', {}))
                self.log_unified("DEBUG", f"群组 '{group_id}' 已存在，包含 {team_count} 个车队", group_id, "system")
            
            # 清理过期车队
            self._cleanup_expired_teams(group_id)
            
            teams = self.teams_data[group_id]['teams']
            self.log_unified("DEBUG", f"清理过期车队后，群组 '{group_id}' 剩余 {len(teams)} 个车队", group_id, "system")
            
            if not teams:
                self.log_unified("DEBUG", f"群组 '{group_id}' 没有车队信息", group_id, "system")
                return "📋 当前没有车队信息"
            
            # 按车队号排序（过期车队已被清理）
            active_teams = list(teams.values())
            active_teams.sort(key=lambda x: x['team_number'])
            
            # 获取当前时间戳
            current_time = datetime.now()
            timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            self.log_unified("DEBUG", f"准备返回 {len(active_teams)} 个车队的信息", group_id, "system")
            
            result = "🚗 车队信息列表\n\n"
            for team in active_teams:
                member_count = len(team['members'])
                substitute_count = len(team.get('substitutes', []))
                substitute_info = f"，替补：{substitute_count}" if substitute_count > 0 else ""
                license_info = f"\n🚙 车牌：{team['license_plate']}" if team.get('license_plate') else ""
                description_info = f"\n📝 描述：{team['description']}" if team.get('description') and team['description'].strip() else "\n📝 描述：无"
                # 显示文案优化：将“周回/倍率”统一改为“要求周回/要求倍率”，仅影响用户可见文本
                laps_info = f"\n🔁 要求周回：{team['laps']}" if team.get('laps') is not None else "\n🔁 要求周回：无"
                multiplier_info = f"\n✖️ 要求倍率：{team['multiplier']}" if team.get('multiplier') is not None else "\n✖️ 要求倍率：无"
                
                result += (f"🚗 车队号：{team['team_number']}\n"
                          f"👨‍✈️ 队长：{team['captain']}\n"
                          f"📅 日期：{team['date_display']}\n"
                          f"⏰ 时间：{team['time_range']}\n"
                          f"🎵 歌曲：{team['song']}\n"
                          f"👥 队员数：{member_count + 1}{substitute_info}{license_info}{laps_info}{multiplier_info}{description_info}\n\n")
            
            return result.rstrip()
        
        except Exception as e:
            self.log_unified("ERROR", f"查询所有车队失败: {e}", group_id=group_id)
            self.log_unified("ERROR", f"异常详情: {traceback.format_exc()}", group_id, "system")
            return f"❌ 查询车队失败: {str(e)}"
    
    async def query_team_by_number(self, team_number: int, group_id: str, generate_image: bool = True) -> Union[str, Dict[str, Any]]:
        """
        按车队号查询车队
        
        Args:
            team_number: 车队号
            group_id: 群组ID
            generate_image: 是否生成甘特图图片
            
        Returns:
            车队详细信息，包括是否可报名的状态
        """
        try:
            self._init_group_data(group_id)
            
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在！"
            
            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'][team_id]
            
            # 检查是否过期
            current_time = datetime.now()
            if current_time.timestamp() > team['end_timestamp']:
                return f"❌ 车队号 {team_number} 已过期！"
            
            # 尝试生成甘特图图片
            image_path = None
            if generate_image and self.team_query and self.image_generator:
                try:
                    # 保存当前车队数据到临时文件供TeamQuery使用
                    self._save_teams()
                    
                    # 重新加载TeamQuery的数据以获取最新数据
                    self.team_query.teams_data = self.team_query._load_teams_data()
                    
                    # 生成甘特图，传递群组ID
                    image_path = self.team_query.generate_team_gantt_image(str(team_number), group_id)
                    
                    if image_path and os.path.exists(image_path):
                        # 获取当前时间戳
                        current_time = datetime.now()
                        self.log_unified("INFO", f"成功生成车队 {team_number} 的甘特图: {image_path}", group_id, "system")
                        
                        # 获取车队详细信息
                        team_detail = self._format_team_detail(team)
                        can_join = self._has_available_slots(team)
                        join_status = "✅ 可报名" if can_join else "❌ 不可报名（所有时间段已满员）"
                        
                        # 组合文字信息 - 按照用户要求的格式（避免重复标题，规范空行）
                        text_content = f"{team_detail.rstrip()}\n\n📋 报名状态：{join_status}\n\n🚗 车队 {team_number} 甘特图"
                        
                        # 返回包含图片和文字的混合响应
                        return {
                            'mixed_message': True,
                            'content': text_content,
                            'image_path': image_path
                        }
                    else:
                        self.log_unified("WARNING", f"车队 {team_number} 甘特图生成失败，使用文本格式", group_id, "system")
                        
                except Exception as e:
                    self.log_unified("ERROR", f"生成车队 {team_number} 甘特图失败: {e}", group_id=group_id)
                    import traceback
                    self.log_unified("ERROR", f"详细错误信息: {traceback.format_exc()}", group_id, "system")
            
            # 如果图片生成失败或未启用，返回文本格式
            team_detail = self._format_team_detail(team)
            
            # 检查车队是否可报名（根据新逻辑：所有时间段都满人才不可报名）
            can_join = self._has_available_slots(team)
            join_status = "✅ 可报名" if can_join else "❌ 不可报名（所有时间段已满员）"
            
            return f"{team_detail.rstrip()}\n\n📋 报名状态：{join_status}"
        
        except Exception as e:
            self.log_unified("ERROR", f"按车队号查询车队失败: {e}", group_id=group_id)
            return f"❌ 查询车队失败: {str(e)}"
    
    async def query_teams_by_captain(self, captain: str, group_id: str) -> str:
        """
        按队长名称查询车队
        
        Args:
            captain: 队长名称
            group_id: 群组ID
            
        Returns:
            队长的所有车队信息
        """
        try:
            self._init_group_data(group_id)
            
            # 查找队长的所有车队
            captain_teams = []
            current_time = datetime.now()
            
            for team in self.teams_data[group_id]['teams'].values():
                if (team['captain'] == captain and 
                    current_time.timestamp() <= team['end_timestamp']):
                    captain_teams.append(team)
            
            if not captain_teams:
                return f"❌ 未找到队长 {captain} 的有效车队！"
            
            # 按车队号排序
            captain_teams.sort(key=lambda x: x['team_number'])
            
            result = f"🚗 队长 {captain} 的车队信息\n\n"
            for team in captain_teams:
                result += self._format_team_summary(team) + "\n\n"
            
            return result.rstrip()
        
        except Exception as e:
            self.logger.error(f"按队长名称查询车队失败: {e}")
            return f"❌ 查询车队失败: {str(e)}"
    
    async def query_teams_multi_dimension(self, search_params: List[str], group_id: str, available_only: bool = False) -> str:
        """
        多维度查询车队信息
        
        支持按以下维度进行组合搜索：
        1. 队长名称 - 直接匹配队长名称
        2. 日期 - 匹配日期格式如 9.12, 2024.9.12
        3. 时间段 - 匹配时间格式如 14-15, 14:00-15:00
        4. 歌曲名称 - 模糊匹配歌曲名称
        
        Args:
            search_params: 搜索参数列表，不分先后顺序
            group_id: 群组ID
            available_only: 是否只查询可报名车队
            
        Returns:
            匹配的车队信息
        """
        try:
            self._init_group_data(group_id)
            
            # 解析搜索参数
            search_criteria = self._parse_search_params(search_params)
            
            # 获取所有有效车队
            current_time = datetime.now()
            all_teams = []
            
            for team in self.teams_data[group_id]['teams'].values():
                if current_time.timestamp() <= team['end_timestamp']:
                    # 如果只查询可报名车队，需要检查是否有空位
                    if available_only and not self._check_team_availability(team):
                        continue
                    all_teams.append(team)
            
            # 根据搜索条件过滤车队
            matching_teams = self._filter_teams_by_criteria(all_teams, search_criteria)
            
            if not matching_teams:
                criteria_desc = self._format_search_criteria_description(search_criteria)
                return f"❌ 未找到符合条件的车队！\n搜索条件：{criteria_desc}"
            
            # 按车队号排序
            matching_teams.sort(key=lambda x: x['team_number'])
            
            # 格式化结果
            criteria_desc = self._format_search_criteria_description(search_criteria)
            result_type = "可报名车队" if available_only else "车队"
            result = f"🚗 {result_type}查询结果\n搜索条件：{criteria_desc}\n\n"
            
            for team in matching_teams:
                result += self._format_team_summary(team) + "\n\n"
            
            return result.rstrip()
        
        except Exception as e:
            self.logger.error(f"多维度查询车队失败: {e}")
            return f"❌ 查询车队失败: {str(e)}"
    
    def _parse_search_params(self, search_params: List[str]) -> Dict[str, Any]:
        """
        解析搜索参数，识别日期、时间段、队长名称、歌曲名称
        
        Args:
            search_params: 搜索参数列表
            
        Returns:
            解析后的搜索条件字典
        """
        criteria = {
            'captain': None,
            'date': None,
            'time_range': None,
            'song': None
        }
        
        import re
        
        for param in search_params:
            param = param.strip()
            if not param:
                continue
            
            # 检查是否为日期格式 (如 9.12, 2024.9.12, 09.12)
            date_pattern = r'^(\d{4}\.)?(\d{1,2})\.(\d{1,2})$'
            if re.match(date_pattern, param):
                criteria['date'] = param
                continue
            
            # 检查是否为时间段格式 (如 14-15, 14:00-15:00, 14:30-15:30)
            time_pattern = r'^(\d{1,2})(:(\d{2}))?-(\d{1,2})(:(\d{2}))?$'
            if re.match(time_pattern, param):
                criteria['time_range'] = param
                continue
            
            # 如果不是日期或时间格式，则可能是队长名称或歌曲名称
            # 这里我们需要进一步判断，先假设是队长名称，如果没有匹配的队长则作为歌曲名称
            if criteria['captain'] is None:
                criteria['captain'] = param
            elif criteria['song'] is None:
                criteria['song'] = param
        
        return criteria
    
    def _filter_teams_by_criteria(self, teams: List[Dict[str, Any]], criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        根据搜索条件过滤车队
        
        Args:
            teams: 车队列表
            criteria: 搜索条件
            
        Returns:
            符合条件的车队列表
        """
        matching_teams = []
        
        for team in teams:
            # 检查队长名称
            if criteria['captain'] and criteria['captain'] != team['captain']:
                # 如果指定的队长名称不匹配，检查是否应该作为歌曲名称
                if criteria['song'] is None and criteria['captain'] in team['song']:
                    # 将队长条件转为歌曲条件
                    criteria['song'] = criteria['captain']
                    criteria['captain'] = None
                else:
                    continue
            
            # 检查日期
            if criteria['date'] and not self._match_date(team, criteria['date']):
                continue
            
            # 检查时间段
            if criteria['time_range'] and not self._match_time_range(team, criteria['time_range']):
                continue
            
            # 检查歌曲名称（模糊匹配）
            if criteria['song'] and criteria['song'] not in team['song']:
                continue
            
            matching_teams.append(team)
        
        return matching_teams
    
    def _match_date(self, team: Dict[str, Any], date_param: str) -> bool:
        """
        检查车队日期是否匹配搜索条件
        
        Args:
            team: 车队数据
            date_param: 日期参数 (如 9.12, 2024.9.12)
            
        Returns:
            是否匹配
        """
        try:
            import re
            
            # 解析搜索日期
            date_pattern = r'^(\d{4}\.)?(\d{1,2})\.(\d{1,2})$'
            match = re.match(date_pattern, date_param)
            if not match:
                return False
            
            year_part, month_str, day_str = match.groups()
            search_month = int(month_str)
            search_day = int(day_str)
            
            # 如果指定了年份，则需要精确匹配年份
            if year_part:
                search_year = int(year_part.rstrip('.'))
            else:
                search_year = None
            
            # 检查车队的日期范围
            date_display = team['date_display']
            
            # 解析车队日期显示格式 (如 09.12, 09.12-09.13)
            if '-' in date_display:
                # 多天车队
                start_date_str, end_date_str = date_display.split('-')
                start_parts = start_date_str.split('.')
                end_parts = end_date_str.split('.')
                
                start_month, start_day = int(start_parts[0]), int(start_parts[1])
                end_month, end_day = int(end_parts[0]), int(end_parts[1])
                
                # 检查搜索日期是否在范围内
                # 需要考虑跨月的情况
                if start_month == end_month:
                    # 同一个月内的日期范围
                    if search_month == start_month and start_day <= search_day <= end_day:
                        return True
                else:
                    # 跨月的日期范围
                    if (search_month == start_month and search_day >= start_day) or \
                       (search_month == end_month and search_day <= end_day) or \
                       (start_month < search_month < end_month):
                        return True
            else:
                # 单天车队
                parts = date_display.split('.')
                team_month, team_day = int(parts[0]), int(parts[1])
                
                if search_month == team_month and search_day == team_day:
                    return True
            
            return False
        
        except Exception as e:
            self.logger.error(f"日期匹配失败: {e}")
            return False
    
    def _match_time_range(self, team: Dict[str, Any], time_param: str) -> bool:
        """
        检查车队时间段是否与搜索条件有交叉
        
        Args:
            team: 车队数据
            time_param: 时间参数 (如 14-15, 14:00-15:00)
            
        Returns:
            是否有时间交叉
        """
        try:
            import re
            
            # 解析搜索时间段
            time_pattern = r'^(\d{1,2})(:(\d{2}))?-(\d{1,2})(:(\d{2}))?$'
            match = re.match(time_pattern, time_param)
            if not match:
                return False
            
            start_hour = int(match.group(1))
            start_minute = int(match.group(3)) if match.group(3) else 0
            end_hour = int(match.group(4))
            end_minute = int(match.group(6)) if match.group(6) else 0
            
            search_start_minutes = start_hour * 60 + start_minute
            search_end_minutes = end_hour * 60 + end_minute
            
            # 处理跨天情况
            if search_end_minutes <= search_start_minutes:
                search_end_minutes += 24 * 60
            
            # 检查与车队时间段的交叉
            team_start_minutes = team['time_start_minutes']
            team_end_minutes = team['time_end_minutes']
            
            # 处理车队跨天情况
            if team_end_minutes <= team_start_minutes:
                team_end_minutes += 24 * 60
            
            # 检查时间段交叉：搜索时间段被完整包含在车队时间段内，或有交叉
            # 条件：搜索开始时间 < 车队结束时间 AND 搜索结束时间 > 车队开始时间
            has_overlap = (search_start_minutes < team_end_minutes and 
                          search_end_minutes > team_start_minutes)
            
            return has_overlap
        
        except Exception as e:
            self.logger.error(f"时间段匹配失败: {e}")
            return False
    
    def _format_search_criteria_description(self, criteria: Dict[str, Any]) -> str:
        """
        格式化搜索条件描述
        
        Args:
            criteria: 搜索条件
            
        Returns:
            格式化的条件描述
        """
        desc_parts = []
        
        if criteria['captain']:
            desc_parts.append(f"队长：{criteria['captain']}")
        if criteria['date']:
            desc_parts.append(f"日期：{criteria['date']}")
        if criteria['time_range']:
            desc_parts.append(f"时间：{criteria['time_range']}")
        if criteria['song']:
            desc_parts.append(f"歌曲：{criteria['song']}")
        
        return " | ".join(desc_parts) if desc_parts else "无特定条件"
    
    def _format_team_detail(self, team: Dict[str, Any]) -> str:
        """
        格式化车队详细信息
        
        Args:
            team: 车队数据
            
        Returns:
            格式化的车队信息
        """
        try:
            license_info = f"\n🚙 车牌：{team['license_plate']}" if team.get('license_plate') else ""
            
            # 计算当前总人数，确保队长只计算一次
            captain_name = team['captain']
            member_names = [member['name'] for member in team['members']]
            
            # 如果队长在成员列表中，则总人数就是成员数量；否则需要加上队长
            if captain_name in member_names:
                total_members = len(team['members'])
            else:
                total_members = len(team['members']) + 1  # +1 for captain
            people_info = f"👥 总人数：{total_members}"
            
            # 构建基本信息
            result = (f"🚗 车队详细信息\n\n"
                     f"🚗 车队号：{team['team_number']}\n"
                     f"👨‍✈️ 队长：{team['captain']}\n"
                     f"📅 日期：{team['date_display']}\n"
                     f"⏰ 时间：{team['time_range']}\n"
                     f"🎵 歌曲：{team['song']}\n")

            # 如果有要求周回或要求倍率，添加到详细信息中，并计算综合力
            if team.get('laps') is not None:
                result += f"🔁 要求周回：{team['laps']}\n"
            if team.get('multiplier') is not None:
                result += f"✖️ 要求倍率：{team['multiplier']}\n"
            # 计算并展示综合力（优先使用显式字段，否则以 周回×倍率 计算）
            comp = team.get('comprehensive_power')
            if comp is None and team.get('laps') is not None and team.get('multiplier') is not None:
                try:
                    comp = round(float(team['laps']) * float(team['multiplier']), 2)
                except Exception:
                    comp = None
            if comp is not None:
                try:
                    v = float(comp)
                    if v >= 10000:
                        s = f"{(v/10000.0):.2f}".rstrip('0').rstrip('.')
                        result += f"💪 车主综合：{s}w\n"
                    else:
                        result += f"💪 车主综合：{int(v) if v.is_integer() else v}\n"
                except Exception:
                    result += f"💪 车主综合：{comp}\n"
            
            # 如果有描述，添加描述信息
            if team.get('description') and team['description'].strip():
                result += f"📝 描述：{team['description']}\n"
            
            result += f"{people_info}{license_info}\n"
            
            # 添加详细的时间段人数分布
            time_slots_info = self._get_detailed_time_slots_info(team)
            if time_slots_info:
                result += f"\n⏰ 时间段人数分布：\n{time_slots_info}"
            
            if team['members']:
                result += "\n👥 队员列表：\n"
                for i, member in enumerate(team['members'], 1):
                    join_type_prefix = f"[{member['join_type']}]"
                    result += (f"{i}. {join_type_prefix}{member['name']} "
                              f"({member['date_range']} {member['time_range']})\n")
            
            # 添加替补信息
            substitutes = team.get('substitutes', [])
            if substitutes:
                result += "\n🔄 替补列表：\n"
                for i, substitute in enumerate(substitutes, 1):
                    substitute_type_prefix = f"[{substitute['type']}]"
                    substitute_info = (f"{i}. {substitute_type_prefix}{substitute['name']} "
                                     f"({substitute['date_range']} {substitute['time_range']})")
                    if substitute.get('description'):
                        substitute_info += f" - {substitute['description']}"
                    result += substitute_info + "\n"
            
            return result
        except Exception as e:
            self.logger.error(f"格式化车队详细信息失败: {e}")
            return "❌ 格式化车队信息失败"
    
    def _calculate_online_count_range(self, team: Dict[str, Any]) -> Tuple[int, int]:
        """
        计算车队在不同时间段的在线人数范围
        
        Args:
            team: 车队数据
            
        Returns:
            (最小人数, 最大人数) 的元组
        """
        try:
            # 获取车队的所有时间戳范围
            team_ranges = [(r[0], r[1]) for r in team['timestamp_ranges']]
            
            min_count = float('inf')
            max_count = 0
            
            # 为每个车队时间段计算人数
            for team_start, team_end in team_ranges:
                current_count = 1  # 队长算一个人
                
                # 统计在该时间段内的成员数量
                for member in team['members']:
                    member_ranges = [(r[0], r[1]) for r in member['timestamp_ranges']]
                    # 检查成员时间是否与当前车队时间段重叠
                    if self._check_timestamp_overlap([(team_start, team_end)], member_ranges):
                        current_count += 1
                
                min_count = min(min_count, current_count)
                max_count = max(max_count, current_count)
            
            # 如果没有找到任何时间段，返回队长一个人
            if min_count == float('inf'):
                min_count = 1
            
            return min_count, max_count
        except Exception as e:
             self.logger.error(f"计算在线人数范围失败: {e}")
             return 1, 1  # 默认返回队长一个人
    
    def _get_detailed_time_slots_info(self, team: Dict[str, Any]) -> str:
        """
        获取详细的时间段人数分布信息（支持跨天/多日）
        
        设计思路：
        - 逐日遍历 team['timestamp_ranges'] 的每个 (team_start, team_end)；
        - 在该日范围内收集分割点：当天的起止（team_start/team_end）以及所有成员与当天范围交集的起止点；
        - 将分割点排序，构造相邻时间段，统计每个时间段内在线人数（队长 + 与该段有重叠的成员），并格式化输出；
        - 在每一天的区块前添加日期标题（MM.DD），以满足“需要添加日期信息”的展示需求。
        
        Args:
            team: 车队数据
            
        Returns:
            多行字符串。若某天没有形成有效分割（如仅边界点），该天将被跳过。
        """
        try:
            # 获取车队按天的时间戳范围
            team_ranges = [(r[0], r[1]) for r in team['timestamp_ranges']]
            if not team_ranges:
                return ""
            
            lines: List[str] = []
            max_size = self.max_team_size
            
            for day_start, day_end in team_ranges:
                # 收集当天的时间分割点（仅限于当天范围内）
                time_points: Set[float] = set()
                time_points.add(day_start)
                time_points.add(day_end)
                
                # 收集成员与当天范围的交集端点
                for member in team['members']:
                    for member_start, member_end in member['timestamp_ranges']:
                        inter_start = max(member_start, day_start)
                        inter_end = min(member_end, day_end)
                        if inter_start < inter_end:
                            if inter_start > day_start:
                                time_points.add(inter_start)
                            if inter_end < day_end:
                                time_points.add(inter_end)
                
                sorted_points = sorted(time_points)
                if len(sorted_points) < 2:
                    # 无有效区间，跳过该天
                    continue
                
                # 添加日期标题（例如：09.12）
                day_dt = datetime.fromtimestamp(day_start)
                lines.append(day_dt.strftime("%m.%d"))
                
                # 逐段统计人数并输出状态
                for i in range(len(sorted_points) - 1):
                    seg_start = sorted_points[i]
                    seg_end = sorted_points[i + 1]
                    
                    # 双重校验区间严格在当天范围内
                    if seg_start < day_start or seg_end > day_end:
                        continue
                    
                    # 统计当前时间段内在线人数：队长 + 有重叠的成员
                    count = 1  # 队长
                    for member in team['members']:
                        for member_start, member_end in member['timestamp_ranges']:
                            if member_start < seg_end and member_end > seg_start:
                                count += 1
                                break  # 同一成员只计一次
                    
                    start_time = self._timestamp_to_time_str(seg_start)
                    end_time = self._timestamp_to_time_str(seg_end)
                    if count >= max_size:
                        status = "满人"
                    else:
                        status = f"缺{max_size - count}人"
                    
                    lines.append(f"{start_time}-{end_time} {status}")
            
            return "\n".join(lines)
        except Exception as e:
            self.logger.error(f"获取详细时间段信息失败: {e}")

    # 删除了_get_understaffed_time_slots方法（缺人提醒功能已移除）
    
    def _timestamp_to_time_str(self, timestamp: float) -> str:
        """
        将时间戳转换为时间字符串
        
        Args:
            timestamp: 时间戳
            
        Returns:
            格式化的时间字符串 (HH:MM)
        """
        try:
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime('%H:%M')
        except Exception as e:
            self.logger.error(f"时间戳转换失败: {e}")
            return "00:00"
    
    def _format_team_summary(self, team: Dict[str, Any]) -> str:
        """
        格式化车队摘要信息
        
        Args:
            team: 车队数据
            
        Returns:
            格式化的车队摘要
        """
        try:
            member_count = len(team['members'])
            substitute_count = len(team.get('substitutes', []))
            license_info = f" 🚙{team['license_plate']}" if team.get('license_plate') else ""
            description_info = f"\n📝 描述：{team['description']}" if team.get('description') and team['description'].strip() else ""
            laps_info = f"\n🔁 要求周回：{team['laps']}" if team.get('laps') is not None else ""
            multiplier_info = f"\n✖️ 要求倍率：{team['multiplier']}" if team.get('multiplier') is not None else ""
            # 计算综合力（优先使用显式字段，否则以 周回×倍率 计算）
            comp_val = team.get('comprehensive_power')
            if comp_val is None and team.get('laps') is not None and team.get('multiplier') is not None:
                try:
                    comp_val = round(float(team['laps']) * float(team['multiplier']), 2)
                except Exception:
                    comp_val = None
            if comp_val is not None:
                try:
                    v = float(comp_val)
                    if v >= 10000:
                        s = f"{(v/10000.0):.2f}".rstrip('0').rstrip('.')
                        comp_info = f"\n💪 车主综合：{s}w"
                    else:
                        comp_info = f"\n💪 车主综合：{int(v) if v.is_integer() else v}"
                except Exception:
                    comp_info = f"\n💪 车主综合：{comp_val}"
            else:
                comp_info = ""
            substitute_info = f" (替补:{substitute_count})" if substitute_count > 0 else ""
            
            return (f"🚗 车队号：{team['team_number']}\n"
                    f"👤 队长：{team['captain']}\n"
                    f"📅 日期：{team['date_display']}\n"
                    f"⏰ 时间：{team['time_range']}\n"
                    f"🎵 歌曲：{team['song']}\n"
                    f"👥 人数：{member_count + 1}/{self.max_team_size}{substitute_info}{license_info}{laps_info}{multiplier_info}{comp_info}{description_info}")
        except Exception as e:
            self.logger.error(f"格式化车队摘要失败: {e}")
            return "❌ 格式化车队摘要失败"
    
    def _check_team_availability(self, team: Dict[str, Any]) -> bool:
        """
        检查车队是否可报名（是否有空位）
        
        对于跨日期车队，需要检查是否有任何时间段存在空位
        使用与_has_available_slots相同的细粒度时间段分析逻辑
        
        Args:
            team: 车队数据
            
        Returns:
            是否有空位可报名
        """
        try:
            # 使用_has_available_slots方法来检查是否有空位
            # 这个方法已经正确处理了跨日期车队的时间段分析
            return self._has_available_slots(team)
        except Exception as e:
            self.logger.error(f"检查车队可用性失败: {e}")
            return False
    
    async def query_available_teams(self, group_id: str) -> str:
        """
        查询可报名车队
        
        Args:
            group_id: 群组ID
            
        Returns:
            可报名车队信息
        """
        try:
            self._init_group_data(group_id)
            
            # 清理过期车队
            self._cleanup_expired_teams(group_id)
            
            teams = self.teams_data[group_id]['teams']
            if not teams:
                return "📋 当前没有可报名的车队"
            
            # 过滤有空位的车队（过期车队已被清理）
            available_teams = []
            
            for team in teams.values():
                # 检查是否有空位
                if self._has_available_slots(team):
                    available_teams.append(team)
            
            if not available_teams:
                return "📋 当前没有可报名的车队"
            
            # 按车队号排序
            available_teams.sort(key=lambda x: x['team_number'])
            
            result = "🚗 可报名车队列表\n\n"
            for team in available_teams:
                available_slots = self._get_available_time_slots(team)
                license_info = f" 🚙{team['license_plate']}" if team.get('license_plate') else ""
                description_info = f"\n📝 描述：{team['description']}" if team.get('description') and team['description'].strip() else "\n📝 描述：无"
                laps_info = f"\n🔁 要求周回：{team['laps']}" if team.get('laps') is not None else "\n🔁 要求周回：无"
                multiplier_info = f"\n✖️ 要求倍率：{team['multiplier']}" if team.get('multiplier') is not None else "\n✖️ 要求倍率：无"
                
                result += (f"🚗 车队号：{team['team_number']}\n"
                          f"👨‍✈️ 队长：{team['captain']}\n"
                          f"📅 日期：{team['date_display']}\n"
                          f"⏰ 时间：{team['time_range']}\n"
                          f"🎵 歌曲：{team['song']}{license_info}{laps_info}{multiplier_info}{description_info}\n"
                          f"📊 可报名时间段：\n{available_slots}\n\n")
            
            return result.rstrip()
        
        except Exception as e:
            self.log_unified("ERROR", f"查询可报名车队失败: {e}", group_id=group_id)
            return f"❌ 查询可报名车队失败: {str(e)}"
    
    def _has_available_slots(self, team: Dict[str, Any]) -> bool:
        """
        检查车队是否有可用空位
        
        根据新需求：只有当所有时间段都满人时，车队才不可报名
        即：只要有任何一个时间段未满人，车队就可报名
        
        使用与 _get_detailed_time_slots_info 相同的细粒度时间段分析逻辑
        
        Args:
            team: 车队数据
            
        Returns:
            是否有可用空位（是否可报名）
        """
        try:
            # 首先检查车队是否已过期
            current_time = datetime.now()
            if current_time.timestamp() > team['end_timestamp']:
                return False
            
            # 收集所有时间点（开始和结束时间）
            time_points = set()
            
            # 添加车队的时间点
            for start_ts, end_ts in team['timestamp_ranges']:
                time_points.add(start_ts)
                time_points.add(end_ts)
            
            # 添加所有成员的时间点
            for member in team['members']:
                for start_ts, end_ts in member['timestamp_ranges']:
                    time_points.add(start_ts)
                    time_points.add(end_ts)
            
            # 排序时间点
            sorted_time_points = sorted(time_points)
            
            if len(sorted_time_points) < 2:
                return False
            
            has_valid_time_slot = False  # 是否有未过期的时间段
            
            # 为每个时间段计算人数
            for i in range(len(sorted_time_points) - 1):
                start_ts = sorted_time_points[i]
                end_ts = sorted_time_points[i + 1]
                
                # 检查这个时间段是否已过期
                if current_time.timestamp() > end_ts:
                    continue
                
                # 检查这个时间段是否在车队时间范围内
                team_ranges = [(r[0], r[1]) for r in team['timestamp_ranges']]
                if not self._check_timestamp_overlap([(start_ts, end_ts)], team_ranges):
                    continue
                
                has_valid_time_slot = True
                
                # 计算这个时间段的人数（队长+成员）
                count = 1  # 队长
                for member in team['members']:
                    member_ranges = [(r[0], r[1]) for r in member['timestamp_ranges']]
                    if self._check_timestamp_overlap([(start_ts, end_ts)], member_ranges):
                        count += 1
                
                # 如果该时间段未满员，则车队可报名
                if count < self.max_team_size:
                    return True
            
            # 如果没有有效的时间段，则不可报名
            if not has_valid_time_slot:
                return False
            
            # 如果所有有效时间段都满员，则不可报名
            return False
            
        except Exception as e:
            self.logger.error(f"检查车队空位失败: {e}")
            return False
    
    def _get_available_time_slots(self, team: Dict[str, Any]) -> str:
        """
        获取车队的可报名时间段
        
        Args:
            team: 车队数据
            
        Returns:
            可报名时间段描述
        """
        try:
            available_slots = []
            
            # 获取车队的所有时间段范围
            team_ranges = [(r[0], r[1]) for r in team['timestamp_ranges']]
            
            # 为每个时间段范围生成小时级别的时间段
            for team_start, team_end in team_ranges:
                start_dt = datetime.fromtimestamp(team_start)
                end_dt = datetime.fromtimestamp(team_end)
                
                # 按小时分割时间段
                current_time = start_dt.replace(minute=0, second=0, microsecond=0)
                
                while current_time < end_dt:
                    # 计算当前小时段的结束时间
                    next_hour = current_time + timedelta(hours=1)
                    slot_end = min(next_hour, end_dt)
                    
                    # 转换为时间戳
                    slot_start_ts = current_time.timestamp()
                    slot_end_ts = slot_end.timestamp()
                    
                    # 统计这个小时段的人数
                    current_count = 1  # 队长算一个人
                    
                    # 检查队长是否在这个时间段
                    captain_ranges = [(r[0], r[1]) for r in team['timestamp_ranges']]
                    captain_in_slot = self._check_timestamp_overlap(
                        [(slot_start_ts, slot_end_ts)], captain_ranges
                    )
                    
                    if not captain_in_slot:
                        current_count = 0
                    
                    # 统计队员在这个时间段的人数
                    for member in team['members']:
                        member_ranges = [(r[0], r[1]) for r in member['timestamp_ranges']]
                        if self._check_timestamp_overlap([(slot_start_ts, slot_end_ts)], member_ranges):
                            current_count += 1
                    
                    # 如果未满员，添加到可报名列表
                    if current_count < self.max_team_size:
                        available_count = self.max_team_size - current_count
                        
                        # 格式化时间显示
                        if current_time.date() == slot_end.date():
                            time_display = f"{current_time.strftime('%H:%M')}-{slot_end.strftime('%H:%M')}"
                            date_display = current_time.strftime('%m.%d')
                        else:
                            time_display = f"{current_time.strftime('%H:%M')}-{slot_end.strftime('%H:%M')}(次日)"
                            date_display = f"{current_time.strftime('%m.%d')}-{slot_end.strftime('%m.%d')}"
                        
                        available_slots.append(f"  • {date_display} {time_display} (需要{available_count}人)")
                    
                    # 移动到下一个小时
                    current_time = next_hour
            
            return "\n".join(available_slots) if available_slots else "  • 暂无可报名时间段"
        except Exception as e:
            self.log_unified("ERROR", f"获取可报名时间段失败: {e}", group_id="system", user_id="system")
            return "  • 获取时间段信息失败"
    
    async def cancel_team_by_number(self, team_number: int, date_range: str, time_range: str, 
                                   user_id: str, group_id: str) -> str:
        """
        按车队号撤回班车
        
        Args:
            team_number: 车队号
            date_range: 日期范围
            time_range: 时间范围
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在！"
            
            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'][team_id]
            
            return await self._cancel_team(team, date_range, time_range, user_id, group_id)
        
        except Exception as e:
            self.logger.error(f"按车队号撤回班车失败: {e}")
            return f"❌ 撤回班车失败: {str(e)}"
    
    async def cancel_team_by_captain(self, captain: str, date_range: str, time_range: str, 
                                    user_id: str, group_id: str) -> str:
        """
        按队长名称撤回班车
        
        Args:
            captain: 队长名称
            date_range: 日期范围
            time_range: 时间范围
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            # 查找匹配的车队
            matching_teams = []
            for team in self.teams_data[group_id]['teams'].values():
                if (team['captain'] == captain and 
                    team['date_range'] == date_range and 
                    team['time_range'] == time_range):
                    matching_teams.append(team)
            
            if not matching_teams:
                return f"❌ 未找到队长 {captain} 在 {date_range} {time_range} 的车队！"
            
            if len(matching_teams) > 1:
                return f"❌ 找到多个匹配的车队，请使用车队号进行操作！"
            
            team = matching_teams[0]
            return await self._cancel_team(team, date_range, time_range, user_id, group_id)
        
        except Exception as e:
            self.logger.error(f"按队长名称撤回班车失败: {e}")
            return f"❌ 撤回班车失败: {str(e)}"
    
    async def _cancel_team(self, team: Dict[str, Any], date_range: str, time_range: str, 
                          user_id: str, group_id: str) -> str:
        """
        撤回班车的核心逻辑
        
        Args:
            team: 车队数据
            date_range: 日期范围
            time_range: 时间范围
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            操作结果消息
        """
        try:
            self.log_unified("DEBUG", f"开始撤回车队流程 - 车队: {team['team_number']}, 队长: {team['captain']}, 操作者: {user_id}", group_id, "system")
            
            # 权限检查：只有队长可以撤回班车
            if team['captain_user_id'] != user_id and team['captain'] != user_id:
                self.log_unified("WARNING", f"非队长尝试撤回车队 - 队长: {team['captain']}, 操作者: {user_id}", group_id, "system")
                return f"❌ 只有队长 {team['captain']} 可以撤回班车！"
            
            # 检查时间范围是否匹配
            if team['date_range'] != date_range or team['time_range'] != time_range:
                self.log_unified("WARNING", f"撤回车队时间不匹配 - 请求: {date_range} {time_range}, 车队: {team['date_range']} {team['time_range']}", group_id, "system")
                return f"❌ 时间范围不匹配！车队时间：{team['date_display']} {team['time_range']}"
            
            # 自动记录车队撤回的推车时长统计
            if self.push_time_service:
                try:
                    await self.push_time_service.auto_record_team_cancellation(
                        team['captain'],
                        team['captain_user_id'],
                        group_id, 
                        team['timestamp_ranges'],
                        team['members'],
                        date_display=team.get('date_display'),
                        time_range=team.get('time_range')
                    )
                    self.log_unified("DEBUG", f"车队撤回统计记录成功 - 车队: {team['team_number']}", group_id, "system")
                except Exception as e:
                    self.log_unified("ERROR", f"自动记录车队撤回统计失败: {traceback.format_exc()}", group_id, "system")
                    self.logger.error(f"自动记录车队撤回统计失败: {e}")
            
            # 发送at所有成员的提醒消息（在归档车队之前）
            await self._send_team_cancellation_notification(team, group_id)
            
            # 获取车队ID用于清理提醒记录
            team_id = list(self.teams_data[group_id]['teams'].keys())[list(self.teams_data[group_id]['teams'].values()).index(team)]
            
            # 清理车队相关的所有提醒记录，防止撤回后继续发送提醒
            if hasattr(self, 'reminder_service') and self.reminder_service:
                self.reminder_service.clear_team_reminders(group_id, team_id)
                self.log_unified("DEBUG", f"已清理车队 {team['team_number']} 的所有提醒记录", group_id, "system")
            
            # 归档车队（标记为撤回）
            team_number = team['team_number']
            self.log_unified("INFO", f"车队撤回成功 - 车队号: {team_number}, 队长: {team['captain']}, 成员数: {len(team['members'])}", group_id, "system")
            
            self._archive_expired_team(group_id, team_id, team, expire_reason='cancelled')
            
            return (f"✅ 班车撤回成功！\n"
                   f"🚗 车队号：{team_number}\n"
                   f"👨‍✈️ 队长：{team['captain']}\n"
                   f"📅 日期：{team['date_display']}\n"
                   f"⏰ 时间：{team['time_range']}\n"
                   f"🎵 歌曲：{team['song']}")
        
        except Exception as e:
            self.log_unified("ERROR", f"撤回班车核心逻辑失败: {e}", group_id=group_id, user_id=user_id)
            return f"❌ 撤回班车失败: {str(e)}"
    
    async def remove_member_by_number(self, team_number: int, date_range: str, time_range: str, 
                                     member_name: str, user_id: str, group_id: str) -> str:
        """
        按车队号撤回队员
        
        Args:
            team_number: 车队号
            date_range: 日期范围
            time_range: 时间范围
            member_name: 队员名称
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在！"
            
            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'][team_id]
            
            return await self._remove_member(team, date_range, time_range, member_name, user_id, group_id)
        
        except Exception as e:
            self.logger.error(f"按车队号撤回队员失败: {e}")
            return f"❌ 撤回队员失败: {str(e)}"
    
    async def remove_member_by_captain(self, captain: str, date_range: str, time_range: str, 
                                      member_name: str, user_id: str, group_id: str) -> str:
        """
        按队长名称撤回队员（包括普通成员和替补成员）
        
        Args:
            captain: 队长名称
            date_range: 日期范围
            time_range: 时间范围
            member_name: 队员名称
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            # 查找匹配的车队
            matching_teams = []
            for team in self.teams_data[group_id]['teams'].values():
                if team['captain'] == captain:
                    # 检查是否有匹配的普通队员
                    found_member = False
                    for member in team['members']:
                        if (member['name'] == member_name and 
                            member['date_range'] == date_range and 
                            member['time_range'] == time_range):
                            matching_teams.append(team)
                            found_member = True
                            break
                    
                    # 如果在普通成员中没找到，检查替补成员
                    if not found_member and 'substitutes' in team:
                        for substitute in team['substitutes']:
                            if (substitute['name'] == member_name and 
                                substitute['date_range'] == date_range and 
                                substitute['time_range'] == time_range):
                                matching_teams.append(team)
                                break
            
            if not matching_teams:
                return f"❌ 未找到队长 {captain} 车队中的队员 {member_name} 在 {date_range} {time_range}！"
            
            if len(matching_teams) > 1:
                return f"❌ 找到多个匹配的车队，请使用车队号进行操作！"
            
            team = matching_teams[0]
            return await self._remove_member(team, date_range, time_range, member_name, user_id, group_id)
        
        except Exception as e:
            self.log_unified("ERROR", f"按队长名称撤回队员失败: {e}", group_id=group_id, user_id=user_id)
            return f"❌ 撤回队员失败: {str(e)}"
    
    async def _remove_member(self, team: Dict[str, Any], date_range: str, time_range: str, 
                            member_name: str, user_id: str, group_id: str,
                            notify_captain: bool = True, notify_member: bool = False) -> str:
        """
        撤回队员的核心逻辑（包括普通成员和替补成员）
        
        Args:
            team: 车队数据
            date_range: 日期范围
            time_range: 时间范围
            member_name: 队员名称
            user_id: 用户ID
            
        Returns:
            操作结果消息
        """
        try:
            # 查找要撤回的队员（先在普通成员中查找）
            target_member = None
            member_index = -1
            is_substitute = False
            
            for i, member in enumerate(team['members']):
                if (member['name'] == member_name and 
                    member['date_range'] == date_range and 
                    member['time_range'] == time_range):
                    target_member = member
                    member_index = i
                    break
            
            # 如果在普通成员中没找到，在替补成员中查找
            if not target_member and 'substitutes' in team:
                for i, substitute in enumerate(team['substitutes']):
                    if (substitute['name'] == member_name and 
                        substitute['date_range'] == date_range and 
                        substitute['time_range'] == time_range):
                        target_member = substitute
                        member_index = i
                        is_substitute = True
                        break
            
            if not target_member:
                return f"❌ 未找到队员 {member_name} 在 {date_range} {time_range} 的记录！"
            
            # 权限检查：只有队员本人或队长可以撤回
            if (target_member['user_id'] != user_id and 
                target_member['name'] != user_id and 
                team['captain_user_id'] != user_id and 
                team['captain'] != user_id):
                return f"❌ 只有队员本人或队长可以撤回队员！"
            
            # 移除队员（根据是否为替补选择不同的列表）
            if is_substitute:
                removed_member = team['substitutes'].pop(member_index)
                member_type = "替补成员"
            else:
                removed_member = team['members'].pop(member_index)
                member_type = "队员"

            self._save_teams()

            # 撤回后立即清理该成员的提醒记录，避免继续向其发送提醒
            try:
                if hasattr(self, 'reminder_service') and self.reminder_service:
                    # 获取车队ID
                    team_id = None
                    team_number = team.get('team_number')
                    for tid, t in self.teams_data.get(group_id, {}).get('teams', {}).items():
                        if t.get('team_number') == team_number:
                            team_id = tid
                            break
                    if team_id:
                        self.reminder_service.clear_member_reminders(group_id, team_id, removed_member.get('name', member_name))
                        self.log_unified("DEBUG", f"已清理成员提醒记录: {removed_member.get('name', member_name)}", group_id, user_id)
            except Exception as e:
                self.log_unified("ERROR", f"清理成员提醒记录失败: {e}", group_id, user_id)
            
            # 自动记录成员撤回的推车时长统计（仅对普通成员）
            if not is_substitute and self.push_time_service:
                try:
                    await self.push_time_service.auto_record_member_removal(
                        member_name,
                        removed_member['user_id'],
                        removed_member['join_type'],
                        group_id,
                        removed_member['timestamp_ranges'],
                        date_text=date_range,
                        time_text=time_range
                    )
                except Exception as e:
                    self.log_unified("ERROR", f"自动记录成员撤回统计失败: {e}", group_id, user_id)
            
            # 发送提醒消息（可选：队长或队员）
            if notify_captain:
                await self._send_member_removal_notification(team, member_name, date_range, time_range, removed_member, group_id)
            if notify_member:
                await self._send_member_auto_removal_notification_to_member(team, member_name, date_range, time_range, removed_member, group_id)
            
            return (f"✅ {member_type}撤回成功！\n"
                   f"🚗 车队号：{team['team_number']}\n"
                   f"👨‍✈️ 队长：{team['captain']}\n"
                   f"👤 {member_type}：{member_name}\n"
                   f"📅 日期：{date_range}\n"
                   f"⏰ 时间：{time_range}\n"
                   f"🚌 类型：{removed_member.get('substitute_type', removed_member.get('join_type', '未知'))}")
        
        except Exception as e:
            self.log_unified("ERROR", f"撤回队员核心逻辑失败: {e}", group_id, user_id)
            return f"❌ 撤回队员失败: {str(e)}"
    
    async def upload_license_plate(self, team_number: int, license_plate: str, 
                                  user_id: str, group_id: str) -> str:
        """
        上传车牌
        
        Args:
            team_number: 车队号
            license_plate: 车牌号
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在！"
            
            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'][team_id]
            
            # 权限检查：队长和成员都可以上传车牌
            is_captain = team['captain_user_id'] == user_id or team['captain'] == user_id
            is_member = False
            
            # 检查是否是车队成员
            for member in team['members']:
                if member['user_id'] == user_id or member['name'] == user_id:
                    is_member = True
                    break
            
            if not is_captain and not is_member:
                return f"❌ 只有队长 {team['captain']} 或车队成员可以上传车牌！"
            
            # 检查是否过期
            current_time = datetime.now()
            if current_time.timestamp() > team['end_timestamp']:
                return f"❌ 车队号 {team_number} 已过期，无法上传车牌！"
            
            # 更新车牌
            old_plate = team.get('license_plate')
            team['license_plate'] = license_plate
            self._save_teams()
            
            if old_plate:
                return (f"✅ 车牌更新成功！\n"
                       f"🚗 车队号：{team_number}\n"
                       f"👨‍✈️ 队长：{team['captain']}\n"
                       f"🚙 原车牌：{old_plate}\n"
                       f"🚙 新车牌：{license_plate}")
            else:
                return (f"✅ 车牌上传成功！\n"
                       f"🚗 车队号：{team_number}\n"
                       f"👨‍✈️ 队长：{team['captain']}\n"
                       f"🚙 车牌：{license_plate}")
        
        except Exception as e:
            self.logger.error(f"上传车牌失败: {e}")
            return f"❌ 上传车牌失败: {str(e)}"
    
    async def view_shifts_by_user_id(self, user_id: str, group_id: str) -> str:
        """
        根据用户QQ号查看班次信息
        
        Args:
            user_id: 用户QQ号
            group_id: 群组ID
            
        Returns:
            格式化的班次信息
        """
        try:
            self._init_group_data(group_id)
            
            # 获取当前时间
            current_time = datetime.now()
            current_timestamp = current_time.timestamp()
            
            # 收集匹配用户ID的班次信息
            shift_info = []
            
            for team_id, team in self.teams_data[group_id]['teams'].items():
                # 检查车队是否过期
                if team['end_timestamp'] <= current_timestamp:
                    continue
                
                team_number = team['team_number']
                
                # 检查队长是否匹配用户ID
                if team.get('captain_user_id') == user_id:
                    # 格式化队长的实际时间信息（队长的时间就是车队的时间）
                    date_str = team['date_range']
                    time_str = team['time_range']
                    # 获取车牌号信息
                    license_plate = team.get('license_plate', '')
                    shift_info.append({
                            'type': '队长',
                            'team_number': team_number,
                            'captain': team['captain'],
                            'song': team.get('song', ''),
                            'date_time': f"{date_str} {time_str}",
                            'license_plate': license_plate,
                            'sort_time': team['created_at']
                        })
                
                # 检查队员是否匹配用户ID
                for member in team['members']:
                    if member.get('user_id') == user_id:
                        # 格式化队员的实际时间信息（使用队员自己的时间）
                        date_str = member.get('date_range', team['date_range'])
                        time_str = member.get('time_range', team['time_range'])
                        # 获取车牌号信息
                        license_plate = team.get('license_plate', '')
                        shift_info.append({
                            'type': member['join_type'],
                            'team_number': team_number,
                            'captain': team['captain'],
                            'song': team.get('song', ''),
                            'date_time': f"{date_str} {time_str}",
                            'license_plate': license_plate,
                            'sort_time': member.get('joined_at', team.get('created_at', 0))
                        })
                
                # 检查替补成员是否匹配用户ID
                for substitute in team.get('substitutes', []):
                    if substitute.get('user_id') == user_id:
                        # 格式化替补成员的实际时间信息
                        date_str = substitute.get('date_range', team['date_range'])
                        time_str = substitute.get('time_range', team['time_range'])
                        # 获取车牌号信息
                        license_plate = team.get('license_plate', '')
                        shift_info.append({
                            'type': f"{substitute['type']}",  # 替补类型：推车替补/共跑替补
                            'team_number': team_number,
                            'captain': team['captain'],
                            'song': team.get('song', ''),
                            'date_time': f"{date_str} {time_str}",
                            'license_plate': license_plate,
                            'sort_time': substitute.get('joined_at', team.get('created_at', 0))
                        })
            
            if not shift_info:
                return "📋 你的班次信息：\n\n暂无班次信息"
            
            # 按班次类型分组
            shifts_by_type = {}
            for info in shift_info:
                shift_type = info['type']
                if shift_type not in shifts_by_type:
                    shifts_by_type[shift_type] = []
                shifts_by_type[shift_type].append(info)
            
            # 格式化输出
            result_lines = ["📋 你的班次信息："]
            
            # 修复：将替补类型与存储值保持一致以保证显示
            # 存储的替补类型为“推车替补/共跑替补”，因此这里的顺序列表也采用该字符串
            type_order = ['队长', '推车', '共跑', '跑推', '推车替补', '共跑替补']
            for shift_type in type_order:
                if shift_type in shifts_by_type:
                    result_lines.append(f"\n🔸 {shift_type}：")
                    
                    # 按时间排序
                    shifts_by_type[shift_type].sort(key=lambda x: x['sort_time'])
                    
                    for info in shifts_by_type[shift_type]:
                        # 构建基本信息
                        base_info = f"  \u8f66\u961f{info['team_number']}- {info['captain']} - {info.get('song', '')} - {info['date_time']}"
                        # 如果有车牌号，添加车牌号信息
                        if info.get('license_plate'):
                            base_info += f" - 车牌:{info['license_plate']}"
                        result_lines.append(base_info)

            # 兜底逻辑：显示未在预设顺序中的其他类型，避免遗漏
            other_types = [t for t in shifts_by_type.keys() if t not in type_order]
            for shift_type in sorted(other_types):
                result_lines.append(f"\n🔸 {shift_type}：")
                shifts_by_type[shift_type].sort(key=lambda x: x['sort_time'])
                for info in shifts_by_type[shift_type]:
                    base_info = f"  \u8f66\u961f{info['team_number']}- {info['captain']} - {info.get('song', '')} - {info['date_time']}"
                    if info.get('license_plate'):
                        base_info += f" - 车牌:{info['license_plate']}"
                    result_lines.append(base_info)
            
            return "\n".join(result_lines)
            
        except Exception as e:
            self.logger.error(f"根据用户ID查看班次失败: {e}")
            return f"❌ 查看班次失败: {str(e)}"
    
    async def view_shifts_by_name(self, name: str, group_id: str) -> str:
        """
        根据名称查看班次信息
        
        Args:
            name: 要查找的名称
            group_id: 群组ID
            
        Returns:
            格式化的班次信息
        """
        try:
            self._init_group_data(group_id)
            
            # 获取当前时间
            current_time = datetime.now()
            current_timestamp = current_time.timestamp()
            
            # 收集匹配名称的班次信息
            shift_info = []
            
            for team_id, team in self.teams_data[group_id]['teams'].items():
                # 检查车队是否过期
                if team['end_timestamp'] <= current_timestamp:
                    continue
                
                team_number = team['team_number']
                
                # 检查队长是否匹配
                if team['captain'] == name:
                    # 格式化队长的实际时间信息（队长的时间就是车队的时间）
                    date_str = team['date_range']
                    time_str = team['time_range']
                    # 获取车牌号信息
                    license_plate = team.get('license_plate', '')
                    shift_info.append({
                        'type': '队长',
                        'team_number': team_number,
                        'captain': team['captain'],
                        'song': team.get('song', ''),
                        'date_time': f"{date_str} {time_str}",
                        'license_plate': license_plate,
                        'sort_time': team['created_at']
                    })
                
                # 检查队员是否匹配
                for member in team['members']:
                    if member['name'] == name:
                        # 格式化队员的实际时间信息（使用队员自己的时间）
                        date_str = member.get('date_range', team['date_range'])
                        time_str = member.get('time_range', team['time_range'])
                        # 获取车牌号信息
                        license_plate = team.get('license_plate', '')
                        shift_info.append({
                            'type': member['join_type'],
                            'team_number': team_number,
                            'captain': team['captain'],
                            'song': team.get('song', ''),
                            'date_time': f"{date_str} {time_str}",
                            'license_plate': license_plate,
                            'sort_time': member.get('joined_at', team.get('created_at', 0))
                        })
                
                # 检查替补成员是否匹配
                for substitute in team.get('substitutes', []):
                    if substitute['name'] == name:
                        # 格式化替补成员的实际时间信息
                        date_str = substitute.get('date_range', team['date_range'])
                        time_str = substitute.get('time_range', team['time_range'])
                        # 获取车牌号信息
                        license_plate = team.get('license_plate', '')
                        shift_info.append({
                            'type': f"{substitute['type']}",  # 替补类型：推车替补/共跑替补
                            'team_number': team_number,
                            'captain': team['captain'],
                            'song': team.get('song', ''),
                            'date_time': f"{date_str} {time_str}",
                            'license_plate': license_plate,
                            'sort_time': substitute.get('joined_at', team.get('created_at', 0))
                        })
            
            if not shift_info:
                return f"📋 未找到 {name} 的班次信息"
            
            # 按班次类型分组
            shifts_by_type = {}
            for info in shift_info:
                shift_type = info['type']
                if shift_type not in shifts_by_type:
                    shifts_by_type[shift_type] = []
                shifts_by_type[shift_type].append(info)
            
            # 格式化输出
            result_lines = [f"📋 {name} 的班次信息："]
            
            # 修复：将替补类型与存储值保持一致以保证显示
            type_order = ['队长', '推车', '共跑', '跑推', '推车替补', '共跑替补']
            for shift_type in type_order:
                if shift_type in shifts_by_type:
                    result_lines.append(f"\n🔸 {shift_type}：")
                    
                    # 按时间排序
                    shifts_by_type[shift_type].sort(key=lambda x: x['sort_time'])
                    
                    for info in shifts_by_type[shift_type]:
                        # 构建基本信息
                        base_info = f"  \u8f66\u961f{info['team_number']}- {info['captain']} - {info.get('song', '')} - {info['date_time']}"
                        # 如果有车牌号，添加车牌号信息
                        if info.get('license_plate'):
                            base_info += f" - 车牌:{info['license_plate']}"
                        result_lines.append(base_info)

            # 兜底逻辑：显示未在预设顺序中的其他类型，避免遗漏
            other_types = [t for t in shifts_by_type.keys() if t not in type_order]
            for shift_type in sorted(other_types):
                result_lines.append(f"\n🔸 {shift_type}：")
                shifts_by_type[shift_type].sort(key=lambda x: x['sort_time'])
                for info in shifts_by_type[shift_type]:
                    base_info = f"  \u8f66\u961f{info['team_number']}- {info['captain']} - {info.get('song', '')} - {info['date_time']}"
                    if info.get('license_plate'):
                        base_info += f" - 车牌:{info['license_plate']}"
                    result_lines.append(base_info)
            
            return "\n".join(result_lines)
            
        except Exception as e:
            self.logger.error(f"根据名称查看班次失败: {e}")
            return f"❌ 查看班次失败: {str(e)}"
    
    async def _send_member_removal_notification(self, team: Dict[str, Any], member_name: str, 
                                              date_range: str, time_range: str, 
                                              removed_member: Dict[str, Any], group_id: str):
        """
        发送队员撤回的at队长提醒消息
        
        Args:
            team: 车队数据
            member_name: 撤回的队员名称
            date_range: 日期范围
            time_range: 时间范围
            removed_member: 被撤回的队员信息
            group_id: 群组ID
        """
        try:
            # 构建@队长的消息
            message_segments = [
                {
                    "type": "at",
                    "data": {
                        "qq": str(team['captain_user_id'])
                    }
                },
                {
                    "type": "text",
                    "data": {
                        "text": (f" 队员撤回提醒\n"
                                f"🚗 车队号：{team['team_number']}\n"
                                f"👤 撤回队员：{member_name}\n"
                                f"📅 日期：{date_range}\n"
                                f"⏰ 时间：{time_range}\n"
                                f"🚌 类型：{removed_member['join_type']}")
                    }
                }
            ]
            
            # 构建napcat API请求
            response_data = {
                "action": "send_group_msg",
                "params": {
                    "group_id": group_id,
                    "message": message_segments
                }
            }
            
            # 发送消息
            if self.message_sender:
                success = await self.message_sender(response_data)
                if success:
                    self.log_unified("INFO", f"队员撤回提醒发送成功: 群{group_id}, 车队{team['team_number']}, 队长{team['captain_user_id']}", group_id=group_id, user_id=team['captain_user_id'])
                else:
                    self.log_unified("ERROR", f"队员撤回提醒发送失败: 群{group_id}, 车队{team['team_number']}", group_id=group_id, user_id="system")
            else:
                self.log_unified("WARNING", f"消息发送器未设置，无法发送队员撤回提醒", group_id="system", user_id="system")
                
        except Exception as e:
            self.log_unified("ERROR", f"发送队员撤回提醒失败: {e}", group_id=group_id, user_id="system")

    async def _send_member_auto_removal_notification_to_member(self, team: Dict[str, Any], member_name: str,
                                                               date_range: str, time_range: str,
                                                               removed_member: Dict[str, Any], group_id: str):
        """
        发送因车队时间/日期修改导致的自动撤回提醒（@被撤回的队员，不再@队长）
        """
        try:
            # 使用队员的用户ID进行@
            member_user_id = str(removed_member.get('user_id', ''))
            if not member_user_id:
                # 若缺少用户ID，仍发送文本提示
                self.log_unified("WARNING", f"缺少队员user_id，无法@，仅发送文本通知", group_id=group_id, user_id="system")
            
            member_type = removed_member.get('substitute_type', removed_member.get('join_type', '未知'))
            message_segments = []
            if member_user_id:
                message_segments.append({
                    "type": "at",
                    "data": {"qq": member_user_id}
                })
                message_segments.append({
                    "type": "text",
                    "data": {"text": " "}
                })
            message_segments.append({
                "type": "text",
                "data": {
                    "text": (f"队员撤回提醒\n"
                             f"🚗 车队号：{team['team_number']}\n"
                             f"👤 撤回队员：{member_name}\n"
                             f"📅 日期：{date_range}\n"
                             f"⏰ 时间：{time_range}\n"
                             f"🚌 类型：{member_type}")
                }
            })

            response_data = {
                "action": "send_group_msg",
                "params": {
                    "group_id": group_id,
                    "message": message_segments
                }
            }

            if self.message_sender:
                success = await self.message_sender(response_data)
                if success:
                    self.log_unified("INFO", f"自动撤回@队员提醒发送成功: 群{group_id}, 车队{team['team_number']}, 队员{member_user_id}", group_id=group_id, user_id=member_user_id)
                else:
                    self.log_unified("ERROR", f"自动撤回@队员提醒发送失败: 群{group_id}, 车队{team['team_number']}", group_id=group_id, user_id="system")
            else:
                self.log_unified("WARNING", f"消息发送器未设置，无法发送自动撤回@队员提醒", group_id="system", user_id="system")

        except Exception as e:
            self.log_unified("ERROR", f"发送自动撤回@队员提醒失败: {e}", group_id=group_id, user_id="system")
    
    async def _send_team_cancellation_notification(self, team: Dict[str, Any], group_id: str):
        """
        发送车队撤回的at所有成员提醒消息
        
        Args:
            team: 车队数据
            group_id: 群组ID
        """
        try:
            # 构建消息段，先添加文本说明
            message_segments = [
                {
                    "type": "text",
                    "data": {
                        "text": f"车队撤回通知\n🚗 车队号：{team['team_number']}\n👨‍✈️ 队长：{team['captain']}\n\n📢 以下队员请注意：\n"
                    }
                }
            ]
            
            # 为每个队员添加@消息
            for i, member in enumerate(team['members']):
                # 添加@消息
                message_segments.append({
                    "type": "at",
                    "data": {
                        "qq": str(member['user_id'])
                    }
                })
                
                # 在@消息后添加空格或换行
                if i < len(team['members']) - 1:
                    message_segments.append({
                        "type": "text",
                        "data": {
                            "text": " "
                        }
                    })
            
            # 添加结尾文本
            message_segments.append({
                "type": "text",
                "data": {
                    "text": "\n\n您所在的车队已被撤回!"
                }
            })
            
            # 构建napcat API请求
            response_data = {
                "action": "send_group_msg",
                "params": {
                    "group_id": group_id,
                    "message": message_segments
                }
            }
            
            # 发送消息
            if self.message_sender:
                success = await self.message_sender(response_data)
                if success:
                    member_count = len(team['members'])
                    self.log_unified("INFO", f"车队撤回提醒发送成功: 群{group_id}, 车队{team['team_number']}, 通知{member_count}名队员", group_id=group_id, user_id=team['captain_user_id'])
                else:
                    self.log_unified("ERROR", f"车队撤回提醒发送失败: 群{group_id}, 车队{team['team_number']}", group_id=group_id, user_id="system")
            else:
                self.log_unified("WARNING", f"消息发送器未设置，无法发送车队撤回提醒", group_id="system", user_id="system")
                
        except Exception as e:
            self.log_unified("ERROR", f"发送车队撤回提醒失败: {e}", group_id=group_id, user_id="system")
    
    # ==================== 替补功能相关方法 ====================
    
    async def join_substitute_by_number(self, team_number: int, date_range: str, time_range: str, 
                                       member_name: str, group_id: str, substitute_type: str, 
                                       user_id: str, description: str = "") -> str:
        """
        按车队号加入替补
        
        Args:
            team_number: 车队号
            date_range: 日期范围
            time_range: 时间范围
            member_name: 替补队员名称
            group_id: 群组ID
            substitute_type: 替补类型（推车替补/共跑替补）
            user_id: 用户ID
            description: 描述信息
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            # 查找车队
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在！"
            
            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'][team_id]
            
            return await self._join_substitute(team, date_range, time_range, member_name, 
                                             substitute_type, user_id, group_id, description)
        
        except Exception as e:
            self.logger.error(f"按车队号加入替补失败: {e}")
            return f"❌ 加入替补失败: {str(e)}"
    
    async def join_substitute_by_captain(self, captain: str, date_range: str, time_range: str, 
                                        member_name: str, group_id: str, substitute_type: str, 
                                        user_id: str, description: str = "") -> str:
        """
        按队长名称加入替补
        
        Args:
            captain: 队长名称
            date_range: 日期范围
            time_range: 时间范围
            member_name: 替补队员名称
            group_id: 群组ID
            substitute_type: 替补类型（推车替补/共跑替补）
            user_id: 用户ID
            description: 描述信息
            
        Returns:
            操作结果消息
        """
        try:
            self._init_group_data(group_id)
            
            # 查找队长的车队
            matching_teams = []
            for team_id, team in self.teams_data[group_id]['teams'].items():
                if team['captain'] == captain:
                    matching_teams.append(team)
            
            if not matching_teams:
                return f"❌ 找不到队长 {captain} 的车队！"
            
            if len(matching_teams) > 1:
                team_numbers = [str(team['team_number']) for team in matching_teams]
                return f"❌ 队长 {captain} 有多个车队（{', '.join(team_numbers)}），请使用车队号指定！"
            
            team = matching_teams[0]
            return await self._join_substitute(team, date_range, time_range, member_name, 
                                             substitute_type, user_id, group_id, description)
        
        except Exception as e:
            self.logger.error(f"按队长名称加入替补失败: {e}")
            return f"❌ 加入替补失败: {str(e)}"
    
    async def _join_substitute(self, team: Dict[str, Any], date_range: str, time_range: str, 
                              member_name: str, substitute_type: str, user_id: str, 
                              group_id: str, description: str = "") -> str:
        """
        替补加入车队的核心逻辑
        
        Args:
            team: 车队信息
            date_range: 日期范围
            time_range: 时间范围
            member_name: 替补队员名称
            substitute_type: 替补类型（推车替补/共跑替补）
            user_id: 用户ID
            group_id: 群组ID
            description: 描述信息
            
        Returns:
            操作结果消息
        """
        try:
            # 解析用户输入的日期和时间
            user_date_start, user_date_end = self._parse_date_range(date_range)
            if not user_date_start or not user_date_end:
                return f"❌ 日期格式错误！请使用格式：YYYY-MM-DD 或 YYYY-MM-DD~YYYY-MM-DD"
            
            user_time_start, user_time_end = self._parse_time_range(time_range)
            if user_time_start is None or user_time_end is None:
                return f"❌ 时间格式错误！请使用格式：HH:MM~HH:MM"
            
            # 创建用户时间戳范围
            user_timestamp_ranges = self._create_timestamp_range(
                user_date_start, user_date_end, user_time_start, user_time_end
            )
            
            # 创建车队时间戳范围
            team_date_start, team_date_end = self._parse_date_range(team['date_range'])
            team_time_start, team_time_end = self._parse_time_range(team['time_range'])
            team_timestamp_ranges = self._create_timestamp_range(
                team_date_start, team_date_end, team_time_start, team_time_end
            )
            
            # 检查替补时间是否在车队时间范围内
            if not self._check_timestamp_within(user_timestamp_ranges, team_timestamp_ranges):
                return f"❌ 替补时间必须在车队时间范围内！\n车队时间：{team['date_range']} {team['time_range']}"
            
            # 检查是否已经存在同名替补
            for substitute in team.get('substitutes', []):
                if substitute['name'] == member_name:
                    return f"❌ 替补 {member_name} 已存在！"
            
            # 创建替补数据
            substitute_data = {
                'name': member_name,
                'type': substitute_type,
                'date_range': date_range,
                'time_range': time_range,
                'timestamp_ranges': user_timestamp_ranges,
                'user_id': user_id,
                'joined_at': datetime.now().timestamp(),  # 使用时间戳格式，与正式成员保持一致
                'description': description
            }
            
            # 添加替补到车队
            if 'substitutes' not in team:
                team['substitutes'] = []
            team['substitutes'].append(substitute_data)
            
            # 保存数据
            self._save_teams()
            
            # 记录推车时长统计（如果有推车时长服务）
            if self.push_time_service and substitute_type == '推车替补':
                # 获取操作员信息（从用户ID获取CN，这里简化处理）
                operator_cn = member_name  # 在实际应用中，可能需要从用户系统获取操作员CN
                operator_qq = user_id
                
                await self.push_time_service.record_push_time_operation(
                    user_id, member_name, 'join_substitute', group_id, 
                    f"替补推车加入车队{team['team_number']}: {date_range} {time_range}",
                    operator_cn=operator_cn, operator_qq=operator_qq
                )
            
            # 构建成功消息
            success_msg = f"✅ 替补 {member_name} 成功加入车队 {team['team_number']}！\n"
            success_msg += f"📅 时间：{date_range} {time_range}\n"
            success_msg += f"🏷️ 类型：{substitute_type}\n"
            if description:
                success_msg += f"📝 描述：{description}\n"
            success_msg += f"⚠️ 注意：替补时间结束后请手动调整推时/跑时"
            
            self.log_unified("INFO", f"替补加入成功: {member_name} -> 车队{team['team_number']} ({substitute_type})", 
                           group_id=group_id, user_id=user_id)
            
            return success_msg
            
        except Exception as e:
            self.logger.error(f"替补加入车队失败: {e}")
            return f"❌ 替补加入失败: {str(e)}"

    async def delete_team_by_captain(self, captain: str, date_range: str, time_range: str, 
                                   user_id: str, group_id: str) -> str:
        """
        车队队长删除车队功能
        
        Args:
            captain: 队长名称
            date_range: 日期范围
            time_range: 时间范围
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            删除结果消息
        """
        try:
            # 初始化群组数据
            self._init_group_data(group_id)
            
            # 查找匹配的车队
            matching_teams = []
            for team_id, team in self.teams_data[group_id]['teams'].items():
                if (team['captain'] == captain and 
                    self._match_date(team, date_range) and 
                    self._match_time_range(team, time_range)):
                    matching_teams.append((team_id, team))
            
            if not matching_teams:
                return f"❌ 未找到队长 {captain} 在指定时间段的车队"
            
            if len(matching_teams) > 1:
                return f"❌ 找到多个匹配的车队，请提供更精确的时间范围"
            
            team_id, team = matching_teams[0]
            
            # 权限检查：只有队长本人可以删除车队
            if team['captain_user_id'] != user_id and team['captain'] != user_id:
                return f"❌ 只有队长本人可以删除车队！"
            
            # 执行删除操作
            return await self._delete_team(team, date_range, time_range, user_id, group_id)
            
        except Exception as e:
            self.log_unified("ERROR", f"删除车队失败: {str(e)}", group_id, user_id)
            return f"❌ 删除车队失败: {str(e)}"

    async def delete_team_by_number(self, team_number: int, date_range: str, time_range: str, 
                                  user_id: str, group_id: str) -> str:
        """
        通过车队号删除车队功能
        
        Args:
            team_number: 车队号
            date_range: 日期范围
            time_range: 时间范围
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            删除结果消息
        """
        try:
            # 初始化群组数据
            self._init_group_data(group_id)
            
            # 通过车队号查找车队
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在"
            
            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'].get(team_id)
            
            if not team:
                return f"❌ 车队号 {team_number} 对应的车队不存在"
            
            # 检查时间范围是否匹配
            if not (self._match_date(team, date_range) and self._match_time_range(team, time_range)):
                return f"❌ 车队号 {team_number} 的时间范围与指定时间不匹配"
            
            # 权限检查：只有队长本人可以删除车队
            if team['captain_user_id'] != user_id and team['captain'] != user_id:
                return f"❌ 只有队长本人可以删除车队！"
            
            # 执行删除操作
            return await self._delete_team(team, date_range, time_range, user_id, group_id)
            
        except Exception as e:
            self.log_unified("ERROR", f"删除车队失败: {str(e)}", group_id, user_id)
            return f"❌ 删除车队失败: {str(e)}"

    async def _delete_team(self, team: Dict[str, Any], date_range: str, time_range: str, 
                         user_id: str, group_id: str) -> str:
        """
        删除车队的核心逻辑
        
        Args:
            team: 车队数据
            date_range: 日期范围
            time_range: 时间范围
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            删除结果消息
        """
        try:
            team_number = team['team_number']
            captain = team['captain']
            song = team['song']
            
            # 获取车队ID
            team_id = None
            for tid, t in self.teams_data[group_id]['teams'].items():
                if t['team_number'] == team_number:
                    team_id = tid
                    break
            
            if not team_id:
                return f"❌ 无法找到车队ID"

            # 清理该车队的所有提醒记录，确保删除后不再发送任何提醒
            try:
                if hasattr(self, 'reminder_service') and self.reminder_service:
                    self.reminder_service.clear_team_reminders(group_id, team_id)
                    self.log_unified("DEBUG", f"已清理车队提醒记录: {team_id}", group_id, user_id)
            except Exception as e:
                self.log_unified("ERROR", f"清理车队提醒记录失败: {e}", group_id, user_id)

            # 归档车队（使用与撤回班车相同的逻辑）
            self._archive_expired_team(group_id, team_id, team, 'captain_deleted')
            
            # 从活跃车队中移除
            del self.teams_data[group_id]['teams'][team_id]
            
            # 释放车队号 - 确保正确处理类型转换
            team_number_str = str(team_number)
            if team_number_str in self.teams_data[group_id]['team_numbers']:
                del self.teams_data[group_id]['team_numbers'][team_number_str]
                self.log_unified("INFO", f"释放车队号: {team_number_str}", group_id, user_id)
            else:
                self.log_unified("WARNING", f"车队号 {team_number_str} 不在team_numbers中", group_id, user_id)
            
            # 保存数据
            self._save_teams()
            
            # 记录删除日志
            self.log_unified("INFO", f"车队删除成功 - 车队号:{team_number}, 队长:{captain}, 歌曲:{song}", group_id, user_id)
            
            # 发送删除通知
            await self._send_team_deletion_notification(team, group_id)
            
            return (f"✅ 车队删除成功！\n"
                   f"🚗 车队号：{team_number}\n"
                   f"👨‍✈️ 队长：{captain}\n"
                   f"📅 日期：{team['date_display']}\n"
                   f"⏰ 时间：{team['time_range']}\n"
                   f"🎵 歌曲：{song}")
            
        except Exception as e:
            self.log_unified("ERROR", f"删除车队核心逻辑失败: {str(e)}", group_id, user_id)
            return f"❌ 删除车队失败: {str(e)}"

    async def _send_team_deletion_notification(self, team: Dict[str, Any], group_id: str):
        """
        发送车队删除通知
        
        Args:
            team: 车队数据
            group_id: 群组ID
        """
        try:
            if self.message_sender:
                notification_data = {
                    "action": "send_message",
                    "data": {
                        "group_id": group_id,
                        "message": {
                            "type": "at_all",
                            "content": f"🚨 车队删除通知\n\n"
                                     f"🚗 车队号：{team['team_number']}\n"
                                     f"👨‍✈️ 队长：{team['captain']}\n"
                                     f"📅 日期：{team['date_display']}\n"
                                     f"⏰ 时间：{team['time_range']}\n"
                                     f"🎵 歌曲：{team['song']}\n\n"
                                     f"该车队已被队长删除，请相关成员注意！"
                        }
                    }
                }
                await self.message_sender.send_message(notification_data)
        except Exception as e:
            self.log_unified("ERROR", f"发送车队删除通知失败: {str(e)}", group_id)

    async def edit_team_description_by_captain(self, captain: str, date_range: str, time_range: str, 
                                             new_description: str, user_id: str, group_id: str) -> str:
        """
        车队队长修改车队描述功能
        
        Args:
            captain: 队长名称
            date_range: 日期范围
            time_range: 时间范围
            new_description: 新的描述内容
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            修改结果消息
        """
        try:
            # 初始化群组数据
            self._init_group_data(group_id)
            
            # 查找匹配的车队
            matching_teams = []
            for team_id, team in self.teams_data[group_id]['teams'].items():
                if (team['captain'] == captain and 
                    self._match_date(team, date_range) and 
                    self._match_time_range(team, time_range)):
                    matching_teams.append((team_id, team))
            
            if not matching_teams:
                return f"❌ 未找到队长 {captain} 在指定时间段的车队"
            
            if len(matching_teams) > 1:
                return f"❌ 找到多个匹配的车队，请提供更精确的时间范围"
            
            team_id, team = matching_teams[0]
            
            # 权限检查：只有队长本人可以修改车队描述
            if team['captain_user_id'] != user_id and team['captain'] != user_id:
                return f"❌ 只有队长本人可以修改车队描述！"
            
            # 保存旧描述用于日志
            old_description = team.get('description', '')
            
            # 更新描述
            team['description'] = new_description
            
            # 保存数据
            self._save_teams()
            
            # 记录修改日志
            self.log_unified("INFO", f"车队描述修改成功 - 车队号:{team['team_number']}, 队长:{captain}, 旧描述:'{old_description}', 新描述:'{new_description}'", group_id, user_id)
            
            # 构建返回消息
            result_msg = (f"✅ 车队描述修改成功！\n"
                         f"🚗 车队号：{team['team_number']}\n"
                         f"👨‍✈️ 队长：{captain}\n"
                         f"📅 日期：{team['date_display']}\n"
                         f"⏰ 时间：{team['time_range']}\n"
                         f"🎵 歌曲：{team['song']}\n"
                         f"📝 新描述：{new_description if new_description.strip() else '(无描述)'}")
            
            return result_msg
            
        except Exception as e:
            self.log_unified("ERROR", f"修改车队描述失败: {str(e)}", group_id, user_id)
            return f"❌ 修改车队描述失败: {str(e)}"

    async def modify_team_info_by_number(self, team_number: int, date_range: str, time_range: str,
                                         user_id: str, group_id: str, changes: Dict[str, str]) -> str:
        """
        通过车队号修改车队信息（日期、时间、描述、歌曲、周回、倍率）
        - 仅队长可操作
        - 修改日期/时间需进行成员冲突处理并同步推时变化
        """
        try:
            # 初始化群组数据
            self._init_group_data(group_id)

            # 查找车队
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在"

            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'].get(team_id)
            if not team:
                return f"❌ 车队号 {team_number} 对应的车队不存在"

            # 权限检查：只有队长本人可以修改车队信息
            if team['captain_user_id'] != user_id and team['captain'] != user_id:
                return f"❌ 只有队长本人可以修改车队信息！"

            # 先执行预览与确认流程（如需）
            return await self._prepare_team_modification(team_id, team, user_id, group_id, changes)
        except Exception as e:
            self.log_unified("ERROR", f"按车队号修改车队失败: {str(e)}", group_id, user_id)
            return f"❌ 修改车队信息失败: {str(e)}"

    async def modify_team_info_by_captain(self, captain: str, date_range: str, time_range: str,
                                          user_id: str, group_id: str, changes: Dict[str, str]) -> str:
        """
        通过队长名称+时间段定位车队并修改信息（日期、时间、描述、歌曲、周回、倍率）
        - 仅队长可操作
        - 修改日期/时间需进行成员冲突处理并同步推时变化
        """
        try:
            # 初始化群组数据
            self._init_group_data(group_id)

            # 查找匹配的车队
            matching_teams: List[Tuple[str, Dict[str, Any]]] = []
            for tid, t in self.teams_data[group_id]['teams'].items():
                if (t['captain'] == captain and 
                    self._match_date(t, date_range) and 
                    self._match_time_range(t, time_range)):
                    matching_teams.append((tid, t))

            if not matching_teams:
                return f"❌ 未找到队长 {captain} 在指定时间段的车队"
            if len(matching_teams) > 1:
                return f"❌ 找到多个匹配的车队，请提供更精确的时间范围"

            tid, team = matching_teams[0]

            # 权限检查：只有队长本人可以修改车队信息
            if team['captain_user_id'] != user_id and team['captain'] != user_id:
                return f"❌ 只有队长本人可以修改车队信息！"

            # 先执行预览与确认流程（如需）
            return await self._prepare_team_modification(tid, team, user_id, group_id, changes)
        except Exception as e:
            self.log_unified("ERROR", f"按队长名称修改车队失败: {str(e)}", group_id, user_id)
            return f"❌ 修改车队信息失败: {str(e)}"

    async def _prepare_team_modification(self, team_id: str, team: Dict[str, Any], user_id: str, group_id: str,
                                         changes: Dict[str, str]) -> str:
        """
        预览并缓存可能导致队员撤回的修改：
        - 若修改包含日期/时间且会导致队员撤回，则生成预览并要求 3 分钟内确认
        - 否则直接应用修改
        """
        try:
            allowed_keys = {'日期', '时间', '描述', '歌曲', '周回', '倍率'}
            invalid = [k for k in changes.keys() if k not in allowed_keys]
            if invalid:
                return f"❌ 不支持的修改键：{'、'.join(invalid)}，可用键：日期、时间、描述、歌曲、周回、倍率"

            # 周回/倍率输入校验（与正式修改一致），但不立即应用
            if '周回' in changes:
                raw = changes['周回'].strip()
                try:
                    val = float(raw)
                    if not (10.0 <= val <= 40.0):
                        return "❌ 周回范围应为10-40（支持整数或小数）"
                except Exception:
                    return "❌ 周回格式错误，请输入数字，例如 31 或 31.5"
            if '倍率' in changes:
                raw = changes['倍率'].strip()
                try:
                    val = float(raw)
                    if not (2.0 <= val <= 4.0):
                        return "❌ 倍率范围应为2-4（支持小数）"
                except Exception:
                    return "❌ 倍率格式错误，请输入数字，例如 3 或 3.45"

            sensitive_changed = ('日期' in changes) or ('时间' in changes)
            if not sensitive_changed:
                # 无敏感变更，直接应用
                return await self._modify_team_info(team, user_id, group_id, changes)

            # 解析新的日期/时间
            new_date_range = changes.get('日期', team.get('date_range', '')) or team.get('date_range', '')
            new_time_range = changes.get('时间', team.get('time_range', '')) or team.get('time_range', '')

            date_start, date_end = self._parse_date_range(new_date_range)
            time_start, time_end = self._parse_time_range(new_time_range)
            if not date_start or not date_end or time_start is None or time_end is None:
                return "❌ 新的日期或时间格式错误！"

            # 创建新的时间戳范围
            new_ranges = self._create_timestamp_range(date_start, date_end, time_start, time_end)
            if not new_ranges:
                return "❌ 新时间范围解析失败！"

            # 计算冲突成员（预览，不执行撤回）
            conflict_members = []
            for member in list(team.get('members', [])):
                member_ranges = [(r[0], r[1]) for r in member.get('timestamp_ranges', [])]
                if not self._check_timestamp_within(member_ranges, new_ranges):
                    conflict_members.append(member)

            if not conflict_members:
                # 无撤回风险，直接应用修改
                return await self._modify_team_info(team, user_id, group_id, changes)

            # 生成待确认缓存（3分钟有效）
            key = f"{group_id}:{user_id}"
            self.pending_modifications[key] = {
                'team_id': team_id,
                'team_number': team.get('team_number'),
                'changes': changes,
                'expired_at': datetime.now().timestamp() + 180,
                'conflicts_preview': [
                    {
                        'name': m['name'],
                        'date': m.get('date_range', team.get('date_range')),
                        'time': m.get('time_range', team.get('time_range')),
                        'user_id': m.get('user_id')
                    } for m in conflict_members
                ]
            }

            # 构建预览提示
            lines = [
                "⚠️ 检测到敏感时间变更：" + f"{new_date_range} {new_time_range}",
                "以下队员将因时间冲突被撤回（预览）："
            ]
            for info in self.pending_modifications[key]['conflicts_preview']:
                lines.append(f"- {info['name']}（{info['date']} {info['time']}）")
            lines.append("请在3分钟内发送 /确认修改车队 才会应用修改；超时将自动取消。")

            # 附加拟修改的非时间字段摘要
            extra = []
            if '描述' in changes:
                extra.append("描述")
            if '歌曲' in changes:
                extra.append("歌曲")
            if '周回' in changes:
                extra.append("周回")
            if '倍率' in changes:
                extra.append("倍率")
            if extra:
                lines.append("拟同时修改：" + "、".join(extra))

            return "\n".join(lines)
        except Exception as e:
            self.log_unified("ERROR", f"预览并缓存车队修改失败: {str(e)}", group_id, user_id)
            return f"❌ 修改预览失败: {str(e)}"

    async def _modify_team_info(self, team: Dict[str, Any], user_id: str, group_id: str, changes: Dict[str, str]) -> str:
        """
        核心修改逻辑：
        - 支持键：'日期'、'时间'、'描述'、'歌曲'、'周回'、'倍率'
        - 日期/时间变更：更新车队时间字段，移除冲突队员，同步队长跑时变化
        """
        try:
            allowed_keys = {'日期', '时间', '描述', '歌曲', '周回', '倍率'}
            invalid = [k for k in changes.keys() if k not in allowed_keys]
            if invalid:
                return f"❌ 不支持的修改键：{'、'.join(invalid)}，可用键：日期、时间、描述、歌曲、周回、倍率"

            changed_fields: List[str] = []

            # 记录原时间信息用于推时调整
            old_ranges = [(r[0], r[1]) for r in team.get('timestamp_ranges', [])]
            old_duration_hours = sum((end - start) for start, end in old_ranges) / 3600 if old_ranges else 0.0

            # 非敏感字段（不引发冲突检查）
            # 队长名称禁止修改
            if '描述' in changes:
                team['description'] = changes['描述'].strip()
                changed_fields.append("描述")
            if '歌曲' in changes:
                team['song'] = changes['歌曲'].strip()
                changed_fields.append("歌曲")

            # 周回与倍率（非敏感字段）
            if '周回' in changes:
                raw = changes['周回'].strip()
                try:
                    new_laps = float(raw)
                    if not (10.0 <= new_laps <= 40.0):
                        return "❌ 周回范围应为10-40（支持整数或小数）"
                    old_laps = team.get('laps')
                    team['laps'] = new_laps
                    changed_fields.append(f"周回→{old_laps if old_laps is not None else '(未设)'}→{new_laps}")
                except Exception:
                    return "❌ 周回格式错误，请输入数字，例如 31 或 31.5"
            if '倍率' in changes:
                raw = changes['倍率'].strip()
                try:
                    new_mul = float(raw)
                    if not (2.0 <= new_mul <= 4.0):
                        return "❌ 倍率范围应为2-4（支持小数）"
                    old_mul = team.get('multiplier')
                    team['multiplier'] = new_mul
                    changed_fields.append(f"倍率→{old_mul if old_mul is not None else '(未设)'}→{new_mul}")
                except Exception:
                    return "❌ 倍率格式错误，请输入数字，例如 3 或 3.45"

            # 敏感字段：日期/时间
            sensitive_changed = ('日期' in changes) or ('时间' in changes)
            # 记录被自动撤回的成员，用于合并消息中进行@提示
            removed_members_info: List[Dict[str, Any]] = []

            if sensitive_changed:
                # 解析新的日期/时间
                new_date_range = changes.get('日期', team.get('date_range', '')) or team.get('date_range', '')
                new_time_range = changes.get('时间', team.get('time_range', '')) or team.get('time_range', '')

                date_start, date_end = self._parse_date_range(new_date_range)
                time_start, time_end = self._parse_time_range(new_time_range)
                if not date_start or not date_end or time_start is None or time_end is None:
                    return "❌ 新的日期或时间格式错误！"

                # 创建新的时间戳范围
                new_ranges = self._create_timestamp_range(date_start, date_end, time_start, time_end)
                if not new_ranges:
                    return "❌ 新时间范围解析失败！"

                # 更新日期显示（考虑跨日/跨天）
                if date_start == date_end and time_start < time_end:
                    date_display = date_start.strftime('%m.%d')
                elif date_start == date_end and time_start >= time_end:
                    next_day = date_start + timedelta(days=1)
                    date_display = f"{date_start.strftime('%m.%d')}-{next_day.strftime('%m.%d')}"
                else:
                    date_display = f"{date_start.strftime('%m.%d')}-{date_end.strftime('%m.%d')}"

                # 应用更新到车队
                team['date_range'] = new_date_range
                team['date_display'] = date_display
                team['time_range'] = new_time_range
                team['time_start_minutes'] = time_start
                team['time_end_minutes'] = time_end
                team['timestamp_ranges'] = new_ranges
                team['start_timestamp'] = new_ranges[0][0]
                team['end_timestamp'] = new_ranges[-1][1]
                changed_fields.append(f"时间→{new_date_range} {new_time_range}")

                # 找出与新时间不相容的队员并撤回（视为自动撤回）
                conflict_members = []
                for member in list(team.get('members', [])):
                    member_ranges = [(r[0], r[1]) for r in member.get('timestamp_ranges', [])]
                    if not self._check_timestamp_within(member_ranges, new_ranges):
                        conflict_members.append(member)

                for m in conflict_members:
                    try:
                        member_date = m.get('date_range', team['date_range'])
                        member_time = m.get('time_range', team['time_range'])
                        member_user_id = m.get('user_id')
                        await self._remove_member(
                            team,
                            member_date,
                            member_time,
                            m['name'],
                            user_id,
                            group_id,
                            notify_captain=False,
                            notify_member=False
                        )
                        removed_members_info.append({
                            'name': m['name'],
                            'date': member_date,
                            'time': member_time,
                            'user_id': member_user_id
                        })
                    except Exception as e:
                        self.log_unified("ERROR", f"自动撤回冲突队员失败: {e}", group_id, user_id)

                # 同步队长跑时变化（基于总时长差）
                new_duration_hours = sum((end - start) for start, end in new_ranges) / 3600
                delta = round(new_duration_hours - old_duration_hours, 3)
                if self.push_time_service and abs(delta) > 0:
                    try:
                        captain_name = team.get('captain')
                        # 获取操作员信息（通常是队长本人）
                        operator_cn = captain_name  # 使用队长名称作为操作员名称
                        operator_qq = user_id  # 使用user_id作为操作员QQ
                        
                        if delta > 0:
                            self.push_time_service.add_pushed_time(captain_name, delta, group_id, operator_cn, operator_qq)
                        else:
                            self.push_time_service.reduce_pushed_time(captain_name, -delta, group_id, operator_cn, operator_qq)
                    except Exception as e:
                        self.log_unified("ERROR", f"同步队长跑时变化失败: {e}", group_id, user_id)

            # 保存修改
            self._save_teams()

            # 构建返回或发送的消息
            result_lines = ["✅ 车队信息修改成功！"]
            if changed_fields:
                result_lines.append("已修改：" + "，".join(changed_fields))
            if removed_members_info:
                # 合并为一条消息，并在每个队员后@其QQ
                # 若有消息发送器，构建消息段发送；否则退化为纯文本
                if self.message_sender:
                    segments = []
                    segments.append({
                        "type": "text",
                        "data": {"text": "\n".join(result_lines) + "\n以下队员因时间冲突已自动撤回：\n"}
                    })
                    for info in removed_members_info:
                        # 文本部分
                        segments.append({
                            "type": "text",
                            "data": {"text": f"- {info['name']}（{info['date']} {info['time']}）"}
                        })
                        # @队员
                        if info.get('user_id'):
                            segments.append({
                                "type": "at",
                                "data": {"qq": str(info['user_id'])}
                            })
                        # 换行
                        segments.append({
                            "type": "text",
                            "data": {"text": "\n"}
                        })
                    response_data = {
                        "action": "send_group_msg",
                        "params": {
                            "group_id": group_id,
                            "message": segments
                        }
                    }
                    try:
                        success = await self.message_sender(response_data)
                        if success:
                            self.log_unified("INFO", f"车队修改与自动撤回合并提醒发送成功: 群{group_id}, 车队{team['team_number']}", group_id=group_id, user_id=team.get('captain_user_id', 'system'))
                        else:
                            self.log_unified("ERROR", f"车队修改与自动撤回合并提醒发送失败: 群{group_id}, 车队{team['team_number']}", group_id=group_id, user_id="system")
                    except Exception as e:
                        self.log_unified("ERROR", f"发送合并提醒失败: {e}", group_id=group_id, user_id="system")
                    # 已发送群消息，这里不再返回文本，避免重复消息
                    return None
                else:
                    # 无消息发送器，退化为纯文本
                    result_lines.append("以下队员因时间冲突已自动撤回：")
                    for info in removed_members_info:
                        result_lines.append(f"- {info['name']}（{info['date']} {info['time']}）")
            return "\n".join(result_lines)

        except Exception as e:
            self.log_unified("ERROR", f"修改车队信息失败: {str(e)}\n{traceback.format_exc()}", group_id, user_id)
            return f"❌ 修改车队信息失败: {str(e)}"

    async def edit_team_description_by_number(self, team_number: int, date_range: str, time_range: str, 
                                            new_description: str, user_id: str, group_id: str) -> str:
        """
        通过车队号修改车队描述功能
        
        Args:
            team_number: 车队号
            date_range: 日期范围
            time_range: 时间范围
            new_description: 新的描述内容
            user_id: 用户ID
            group_id: 群组ID
            
        Returns:
            修改结果消息
        """
        try:
            # 初始化群组数据
            self._init_group_data(group_id)
            
            # 通过车队号查找车队
            if team_number not in self.teams_data[group_id]['team_numbers']:
                return f"❌ 车队号 {team_number} 不存在"
            
            team_id = self.teams_data[group_id]['team_numbers'][team_number]
            team = self.teams_data[group_id]['teams'].get(team_id)
            
            if not team:
                return f"❌ 车队号 {team_number} 对应的车队不存在"
            
            # 检查时间范围是否匹配
            if not (self._match_date(team, date_range) and self._match_time_range(team, time_range)):
                return f"❌ 车队号 {team_number} 的时间范围与指定时间不匹配"
            
            # 权限检查：只有队长本人可以修改车队描述
            if team['captain_user_id'] != user_id and team['captain'] != user_id:
                return f"❌ 只有队长本人可以修改车队描述！"
            
            # 保存旧描述用于日志
            old_description = team.get('description', '')
            
            # 更新描述
            team['description'] = new_description
            
            # 保存数据
            self._save_teams()
            
            # 记录修改日志
            self.log_unified("INFO", f"车队描述修改成功 - 车队号:{team_number}, 队长:{team['captain']}, 旧描述:'{old_description}', 新描述:'{new_description}'", group_id, user_id)
            
            # 构建返回消息
            result_msg = (f"✅ 车队描述修改成功！\n"
                         f"🚗 车队号：{team_number}\n"
                         f"👨‍✈️ 队长：{team['captain']}\n"
                         f"📅 日期：{team['date_display']}\n"
                         f"⏰ 时间：{team['time_range']}\n"
                         f"🎵 歌曲：{team['song']}\n"
                         f"📝 新描述：{new_description if new_description.strip() else '(无描述)'}")
            
            return result_msg
            
        except Exception as e:
            self.log_unified("ERROR", f"修改车队描述失败: {str(e)}", group_id, user_id)
            return f"❌ 修改车队描述失败: {str(e)}"
    """
    TeamService 车队服务模块

    文件说明：
    - 负责创建/查询/删除车队，成员加入/撤回，以及替补加入等功能。
    - 变更说明：修复“/看班”与“/看班 名称”无法显示替补成员的问题。
      原因是替补成员类型字符串与输出顺序列表不一致：存储为“推车替补/共跑替补”，
      输出列表使用“替补推车/替补共跑”，导致分组不匹配而不显示。
    - 修复方式：统一类型字符串并增加兜底显示逻辑，确保未来新增类型也能显示。
    """
