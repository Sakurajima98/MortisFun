#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目 - 车队查询模块

本模块负责车队信息的查询和处理，包括：
- 根据车队号查询车队详细信息
- 解析车队成员和时间数据
- 为甘特图生成准备数据
- 智能分段处理teams数据

作者: Trae Builder
创建时间: 2024
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from .image_generator import ImageGenerator

class TeamQuery:
    """
    车队查询类
    
    负责车队信息的查询、数据处理和甘特图生成。
    支持智能分段和时间序列分析。
    """
    
    def __init__(self, data_dir: str = None):
        """
        初始化车队查询器
        
        Args:
            data_dir: 数据目录路径
        """
        self.logger = logging.getLogger(__name__)
        
        # 设置数据目录
        if data_dir:
            self.data_dir = data_dir
        else:
            self.data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        
        # 初始化图片生成器
        self.image_generator = ImageGenerator()
        
        # 加载车队数据
        self.teams_data = self._load_teams_data()
    
    def _log_unified(self, level: str, message: str, group_id: str = "system", user_id: str = "system", log_file_path: str = None):
        """
        统一日志记录方法
        
        Args:
            level: 日志级别 (INFO, WARNING, ERROR, DEBUG)
            message: 日志消息
            group_id: 群组ID
            user_id: 用户ID
            log_file_path: 日志文件路径
        """
        try:
            current_time = datetime.now()
            timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            log_msg = f"[{timestamp}][{level}][G:{group_id}][U:{user_id}]: {message}"
            
            # 打印到控制台
            print(log_msg)
            
            # 写入日志文件
            if log_file_path:
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(log_msg + "\n")
            
            # 使用logging模块记录
            if level == "INFO":
                self.logger.info(message)
            elif level == "WARNING":
                self.logger.warning(message)
            elif level == "ERROR":
                self.logger.error(message)
            elif level == "DEBUG":
                self.logger.debug(message)
        except Exception:
            pass
    
    def _load_teams_data(self) -> Dict[str, Any]:
        """
        加载车队数据
        
        Returns:
            车队数据字典
        """
        teams_file = os.path.join(self.data_dir, 'teams.json')
        
        if not os.path.exists(teams_file):
            # 记录文件不存在警告
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("WARNING", f"车队数据文件不存在: {teams_file}", "system", "system", log_file_path)
            except Exception:
                pass
            return {}
        
        try:
            with open(teams_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 记录数据加载成功日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    self._log_unified("INFO", f"成功加载车队数据，共 {len(data)} 个车队", log_file_path=log_file_path)
                except Exception:
                    pass
                return data
        except Exception as e:
            # 记录数据加载失败日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("ERROR", f"加载车队数据失败: {e}", "system", "system", log_file_path)
            except Exception:
                pass
            return {}
    

    
    def query_team_by_number(self, team_number: str, group_id: str = None) -> Optional[Dict[str, Any]]:
        """
        根据车队号查询车队信息
        
        Args:
            team_number: 车队号
            group_id: 群组ID，如果不提供则在所有群组中搜索
            
        Returns:
            车队信息字典，如果未找到返回None
        """
        try:
            # 标准化车队号（去除空格，转换为整数）
            team_number_int = int(str(team_number).strip())
            
            # 如果指定了群组ID，直接在该群组中查找
            if group_id and group_id in self.teams_data:
                return self._find_team_in_group(team_number_int, group_id)
            
            # 否则在所有群组中搜索
            for group_id, group_data in self.teams_data.items():
                team_info = self._find_team_in_group(team_number_int, group_id)
                if team_info:
                    return team_info
            
            # 记录未找到车队警告
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("WARNING", f"未找到车队号: {team_number}", log_file_path=log_file_path)
            except Exception:
                pass
            return None
                
        except Exception as e:
            # 记录查询失败错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("ERROR", f"查询车队失败: {e}", log_file_path=log_file_path)
            except Exception:
                pass
            return None
    
    def _find_team_in_group(self, team_number: int, group_id: str) -> Optional[Dict[str, Any]]:
        """
        在指定群组中查找车队
        
        Args:
            team_number: 车队号
            group_id: 群组ID
            
        Returns:
            车队信息字典，如果未找到返回None
        """
        try:
            group_data = self.teams_data.get(group_id, {})
            team_numbers = group_data.get('team_numbers', {})
            teams = group_data.get('teams', {})
            
            # 查找车队号对应的team_id
            team_id = team_numbers.get(str(team_number))
            if not team_id:
                return None
            
            # 获取车队信息
            team_info = teams.get(team_id)
            if not team_info:
                return None
            
            # 复制并添加必要信息
            processed_team = team_info.copy()
            processed_team['team_number'] = team_number
            processed_team['group_id'] = group_id
            
            # 处理和验证数据
            processed_team = self._process_team_data(processed_team)
            
            # 记录找到车队信息日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("INFO", f"在群组 {group_id} 中找到车队 {team_number}", log_file_path=log_file_path)
            except Exception:
                pass
            return processed_team
            
        except Exception as e:
            # 记录群组查找失败错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("ERROR", f"在群组 {group_id} 中查找车队 {team_number} 失败: {e}", log_file_path=log_file_path)
            except Exception:
                pass
            return None
    
    def _process_team_data(self, team_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理车队数据
        
        Args:
            team_info: 原始车队信息
            
        Returns:
            处理后的车队信息
        """
        processed = team_info.copy()
        
        try:
            # 处理时间戳范围
            if 'timestamp_ranges' in processed:
                processed['timestamp_ranges'] = self._validate_timestamp_ranges(
                    processed['timestamp_ranges']
                )
            
            # 处理成员数据
            if 'members' in processed:
                processed['members'] = self._process_members_data(processed['members'])
            
            # 生成显示用的日期
            if 'timestamp_ranges' in processed and processed['timestamp_ranges']:
                first_timestamp = processed['timestamp_ranges'][0][0]
                processed['date_display'] = datetime.fromtimestamp(first_timestamp).strftime('%Y-%m-%d')
            else:
                processed['date_display'] = datetime.now().strftime('%Y-%m-%d')
            
            # 确保必要字段存在
            processed.setdefault('song', '未知歌曲')
            processed.setdefault('captain', '未知队长')
            processed.setdefault('members', [])
            
            return processed
            
        except Exception as e:
            # 记录错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("ERROR", f"处理车队数据失败: {e}", log_file_path=log_file_path)
            except Exception:
                pass
            return processed
    
    def _validate_timestamp_ranges(self, timestamp_ranges: List[List[int]]) -> List[List[int]]:
        """
        验证和修正时间戳范围
        
        Args:
            timestamp_ranges: 时间戳范围列表
            
        Returns:
            验证后的时间戳范围
        """
        validated_ranges = []
        
        for range_data in timestamp_ranges:
            if len(range_data) >= 2:
                start_ts, end_ts = range_data[0], range_data[1]
                
                # 确保开始时间小于结束时间
                if start_ts < end_ts:
                    validated_ranges.append([start_ts, end_ts])
                else:
                    # 获取当前时间戳
                    try:
                        log_dir = "logs"
                        if not os.path.exists(log_dir):
                            os.makedirs(log_dir)
                        log_file_path = os.path.join(log_dir, "unified.log")
                        current_time = datetime.now()
                        timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                        log_msg = f"[{timestamp}][WARNING][G:system][U:system]: 无效的时间范围: {start_ts} >= {end_ts}"
                        print(log_msg)
                        with open(log_file_path, "a", encoding="utf-8") as f:
                            f.write(log_msg + "\n")
                    except Exception:
                        pass
        
        return validated_ranges
    
    def _process_members_data(self, members: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        处理成员数据
        
        Args:
            members: 成员列表
            
        Returns:
            处理后的成员列表
        """
        processed_members = []
        
        for member in members:
            processed_member = member.copy()
            
            # 确保必要字段存在
            processed_member.setdefault('name', '未知成员')
            processed_member.setdefault('join_type', '')
            
            # 处理时间戳范围
            if 'timestamp_ranges' in processed_member:
                processed_member['timestamp_ranges'] = self._validate_timestamp_ranges(
                    processed_member['timestamp_ranges']
                )
            else:
                processed_member['timestamp_ranges'] = []
            
            processed_members.append(processed_member)
        
        return processed_members
    
    def generate_team_gantt_image(self, team_number: str, group_id: str = None) -> Optional[str]:
        """
        生成车队甘特图图片
        
        Args:
            team_number: 车队号
            group_id: 群组ID，如果不提供则在所有群组中搜索
            
        Returns:
            生成的图片文件路径，失败返回None
        """
        try:
            # 查询车队信息
            team_data = self.query_team_by_number(team_number, group_id)
            if not team_data:
                # 记录无法找到车队错误日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    self._log_unified("ERROR", f"无法找到车队 {team_number}", log_file_path=log_file_path)
                except Exception:
                    pass
                return None
            
            # 验证数据完整性
            if not self._validate_team_data_for_gantt(team_data):
                # 记录数据不完整错误日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    self._log_unified("ERROR", f"车队 {team_number} 数据不完整，无法生成甘特图", log_file_path=log_file_path)
                except Exception:
                    pass
                return None
            
            # 应用智能分段
            segmented_data = self._apply_intelligent_segmentation(team_data)
            
            # 生成甘特图
            image_path = self.image_generator.generate_team_gantt_chart(segmented_data)
            
            # 记录成功生成甘特图信息日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("INFO", f"成功生成车队 {team_number} 甘特图: {image_path}", log_file_path=log_file_path)
            except Exception:
                pass
            return image_path
            
        except Exception as e:
            # 记录生成甘特图失败错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                log_msg = f"[{timestamp}][ERROR][G:system][U:system]: 生成车队甘特图失败: {e}"
                print(log_msg)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(log_msg + "\n")
            except Exception:
                pass
            return None
    
    def _validate_team_data_for_gantt(self, team_data: Dict[str, Any]) -> bool:
        """
        验证车队数据是否适合生成甘特图
        
        Args:
            team_data: 车队数据
            
        Returns:
            是否有效
        """
        # 检查必要字段
        required_fields = ['team_number', 'captain', 'timestamp_ranges']
        for field in required_fields:
            if field not in team_data:
                # 记录缺少必要字段警告日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    self._log_unified("WARNING", f"缺少必要字段: {field}", log_file_path=log_file_path)
                except Exception:
                    pass
                return False
        
        # 检查是否有时间数据
        has_time_data = False
        
        # 检查队长时间数据
        if team_data.get('timestamp_ranges'):
            has_time_data = True
        
        # 检查成员时间数据
        for member in team_data.get('members', []):
            if member.get('timestamp_ranges'):
                has_time_data = True
                break
        
        if not has_time_data:
            # 记录没有时间数据警告日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("WARNING", "没有找到任何时间数据", log_file_path=log_file_path)
            except Exception:
                pass
            return False
        
        return True
    
    def _apply_intelligent_segmentation(self, team_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        应用智能分段算法
        
        根据时间间隔和成员活动模式，智能分割时间段以优化甘特图显示效果。
        
        Args:
            team_data: 车队数据
            
        Returns:
            分段后的车队数据
        """
        segmented_data = team_data.copy()
        
        try:
            # 收集所有时间点
            all_timestamps = []
            
            # 添加队长时间点
            for start_ts, end_ts in team_data.get('timestamp_ranges', []):
                all_timestamps.extend([start_ts, end_ts])
            
            # 添加成员时间点
            for member in team_data.get('members', []):
                for start_ts, end_ts in member.get('timestamp_ranges', []):
                    all_timestamps.extend([start_ts, end_ts])
            
            if not all_timestamps:
                return segmented_data
            
            # 排序并去重
            all_timestamps = sorted(set(all_timestamps))
            
            # 分析时间间隔
            time_gaps = []
            for i in range(1, len(all_timestamps)):
                gap = all_timestamps[i] - all_timestamps[i-1]
                time_gaps.append(gap)
            
            if not time_gaps:
                return segmented_data
            
            # 计算平均间隔和标准差
            avg_gap = sum(time_gaps) / len(time_gaps)
            
            # 识别显著的时间间隔（超过平均值2倍的间隔）
            significant_gaps = []
            for i, gap in enumerate(time_gaps):
                if gap > avg_gap * 2 and gap > 300:  # 至少5分钟的间隔
                    significant_gaps.append({
                        'index': i,
                        'gap': gap,
                        'before': all_timestamps[i],
                        'after': all_timestamps[i+1]
                    })
            
            # 应用分段逻辑
            if significant_gaps:
                segmented_data = self._segment_by_gaps(segmented_data, significant_gaps)
            
            # 优化显示顺序
            segmented_data = self._optimize_display_order(segmented_data)
            
            return segmented_data
            
        except Exception as e:
            # 获取当前时间戳
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                log_msg = f"[{timestamp}][ERROR][G:system][U:system]: 智能分段失败: {e}"
                print(log_msg)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(log_msg + "\n")
            except Exception:
                pass
            return segmented_data
    
    def _segment_by_gaps(self, team_data: Dict[str, Any], gaps: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        根据时间间隔进行分段
        
        Args:
            team_data: 车队数据
            gaps: 显著时间间隔列表
            
        Returns:
            分段后的数据
        """
        # 这里可以实现更复杂的分段逻辑
        # 目前保持原始数据结构，但可以添加分段标记
        segmented_data = team_data.copy()
        
        # 为数据添加分段信息
        segmented_data['segments'] = []
        
        if gaps:
            # 根据间隔创建分段
            segment_boundaries = [gap['before'] for gap in gaps] + [gap['after'] for gap in gaps]
            segment_boundaries = sorted(set(segment_boundaries))
            
            for i in range(len(segment_boundaries) - 1):
                segmented_data['segments'].append({
                    'start': segment_boundaries[i],
                    'end': segment_boundaries[i + 1],
                    'index': i
                })
        
        return segmented_data
    
    def _optimize_display_order(self, team_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        优化显示顺序
        
        将队长放在最前面，然后按照加入时间或活动时间排序成员。
        
        Args:
            team_data: 车队数据
            
        Returns:
            优化后的数据
        """
        optimized_data = team_data.copy()
        
        try:
            # 对成员按照首次活动时间排序
            members = optimized_data.get('members', [])
            
            def get_first_activity_time(member):
                timestamp_ranges = member.get('timestamp_ranges', [])
                if timestamp_ranges:
                    return min(ts_range[0] for ts_range in timestamp_ranges)
                return float('inf')  # 没有活动时间的成员排在最后
            
            members.sort(key=get_first_activity_time)
            optimized_data['members'] = members
            
            return optimized_data
            
        except Exception as e:
            # 获取当前时间戳
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("ERROR", f"优化显示顺序失败: {e}", log_file_path=log_file_path)
            except Exception:
                pass
            return optimized_data
    
    def get_team_summary(self, team_number: str) -> Optional[Dict[str, Any]]:
        """
        获取车队摘要信息
        
        Args:
            team_number: 车队号
            
        Returns:
            车队摘要信息
        """
        try:
            team_data = self.query_team_by_number(team_number)
            if not team_data:
                return None
            
            # 计算统计信息
            total_members = len(team_data.get('members', [])) + 1  # +1 for captain
            
            # 计算总活动时间
            total_duration = 0
            
            # 队长活动时间
            for start_ts, end_ts in team_data.get('timestamp_ranges', []):
                total_duration += end_ts - start_ts
            
            # 成员活动时间
            for member in team_data.get('members', []):
                for start_ts, end_ts in member.get('timestamp_ranges', []):
                    total_duration += end_ts - start_ts
            
            # 转换为小时
            total_hours = total_duration / 3600
            
            summary = {
                'team_number': team_number,
                'song': team_data.get('song', '未知歌曲'),
                'captain': team_data.get('captain', '未知队长'),
                'total_members': total_members,
                'total_activity_hours': round(total_hours, 2),
                'date': team_data.get('date_display', '未知日期')
            }
            
            return summary
            
        except Exception as e:
            # 记录错误日志
            try:
                log_dir = os.path.dirname(os.path.abspath(__file__))
                log_dir = os.path.join(log_dir, '..', 'logs')
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                self._log_unified("ERROR", f"获取车队摘要失败: {e}", log_file_path=log_file_path)
            except Exception:
                pass
            return None
    
    def list_all_teams(self) -> List[str]:
        """
        列出所有车队号
        
        Returns:
            车队号列表
        """
        return list(self.teams_data.keys())
    
    def search_teams_by_captain(self, captain_name: str) -> List[str]:
        """
        根据队长名称搜索车队
        
        Args:
            captain_name: 队长名称
            
        Returns:
            匹配的车队号列表
        """
        matching_teams = []
        
        for team_number, team_data in self.teams_data.items():
            if captain_name.lower() in team_data.get('captain', '').lower():
                matching_teams.append(team_number)
        
        return matching_teams
    
    def search_teams_by_song(self, song_name: str) -> List[str]:
        """
        根据歌曲名称搜索车队
        
        Args:
            song_name: 歌曲名称
            
        Returns:
            匹配的车队号列表
        """
        matching_teams = []
        
        for team_number, team_data in self.teams_data.items():
            if song_name.lower() in team_data.get('song', '').lower():
                matching_teams.append(team_number)
        
        return matching_teams