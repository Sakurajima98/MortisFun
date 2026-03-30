import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from .base_service import BaseService


class TeamReminderService(BaseService):
    """
    车队定时提醒服务类
    
    主要功能：
    - 定时扫描即将开始的班车
    - 发送@提醒消息给队长和乘客
    - 检测车牌上传状态
    - 管理提醒任务的生命周期
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, message_sender: Callable = None, server=None):
        """
        初始化车队定时提醒服务
        
        Args:
            config: 配置信息
            data_manager: 数据管理器
            text_formatter: 文本格式化器
            message_sender: 消息发送回调函数
            server: 服务器实例
        """
        super().__init__(config, data_manager, text_formatter)
        self.logger = logging.getLogger(__name__)
        self.message_sender = message_sender
        self.server = server
        
        # 车队数据文件路径
        self.teams_file = os.path.join(self.data_manager.base_path, 'team', 'teams.json')
        
        # 确保车队数据目录存在
        try:
            team_dir = os.path.dirname(self.teams_file)
            if not os.path.exists(team_dir):
                os.makedirs(team_dir, exist_ok=True)
                self.log_unified("INFO", f"创建车队数据目录: {team_dir}", "system", "system")
            else:
                self.log_unified("DEBUG", f"车队数据目录已存在: {team_dir}", "system", "system")
        except Exception as e:
            self.log_unified("ERROR", f"创建车队数据目录失败: {e}", "system", "system")
        
        # 改进的提醒状态跟踪 - 使用时间戳作为key，确保精确的防重复
        self.sent_reminders = {}  # 统一的已发送提醒记录 {reminder_key: timestamp}
        
        # 定时任务控制
        self.is_running = False
        self.reminder_task = None
        
        self.log_unified("INFO", "车队定时提醒服务初始化完成", group_id="system", user_id="system")
    
    def _generate_reminder_key(self, reminder_type: str, group_id: str, team_id: str, 
                             target_timestamp: float, extra_info: str = "") -> str:
        """
        生成提醒键值，用于防重复发送
        
        Args:
            reminder_type: 提醒类型 (captain/member/understaffed)
            group_id: 群组ID
            team_id: 车队ID
            target_timestamp: 目标时间戳（精确到分钟）
            extra_info: 额外信息（如成员名称）
            
        Returns:
            唯一的提醒键值
        """
        # 将时间戳精确到分钟级别，避免秒级差异导致的重复发送
        minute_timestamp = int(target_timestamp // 60) * 60
        
        # 为了更好的防重复，添加日期信息
        date_str = datetime.fromtimestamp(target_timestamp).strftime('%Y%m%d')
        
        if extra_info:
            return f"{reminder_type}:{group_id}:{team_id}:{date_str}:{minute_timestamp}:{extra_info}"
        else:
            return f"{reminder_type}:{group_id}:{team_id}:{date_str}:{minute_timestamp}"
    
    def _is_reminder_sent(self, reminder_key: str) -> bool:
        """
        检查提醒是否已发送
        
        Args:
            reminder_key: 提醒键值
            
        Returns:
            是否已发送
        """
        return reminder_key in self.sent_reminders
    
    def _mark_reminder_sent(self, reminder_key: str) -> None:
        """
        标记提醒已发送
        
        Args:
            reminder_key: 提醒键值
        """
        self.sent_reminders[reminder_key] = datetime.now().timestamp()
    
    def _clean_expired_reminders(self) -> None:
        """
        清理过期的提醒记录（超过24小时的记录）
        """
        current_time = datetime.now().timestamp()
        expired_keys = []
        
        for key, timestamp in self.sent_reminders.items():
            if current_time - timestamp > 24 * 3600:  # 24小时
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.sent_reminders[key]
        
        if expired_keys:
            self.log_unified("DEBUG", f"清理了 {len(expired_keys)} 条过期提醒记录", "system", "system")

    def _load_teams_data(self) -> Dict[str, Any]:
        """
        加载车队数据
        
        Returns:
            车队数据字典
        """
        if not os.path.exists(self.teams_file):
            self.log_unified("WARNING", f"车队数据文件不存在: {self.teams_file}", "system", "system")
            return {}
        
        try:
            with open(self.teams_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 移除调试日志输出，减少日志噪音
                return data
        except Exception as e:
            self.log_unified("ERROR", f"加载车队数据失败: {e}", "system", "system")
            return {}

    def _get_teams_starting_soon(self, teams_data: Dict[str, Any], minutes_ahead: int) -> List[Dict[str, Any]]:
        """
        获取即将开始的车队列表
        
        Args:
            teams_data: 车队数据
            minutes_ahead: 提前多少分钟
            
        Returns:
            即将开始的车队列表
        """
        try:
            current_time = datetime.now()
            target_time = current_time + timedelta(minutes=minutes_ahead)
            
            starting_teams = []
            
            for group_id, group_data in teams_data.items():
                if 'teams' not in group_data:
                    continue
                    
                for team_id, team in group_data['teams'].items():
                    # 使用时间戳范围进行准确的日期时间比较
                    timestamp_ranges = team.get('timestamp_ranges', [])
                    if not timestamp_ranges:
                        continue
                    
                    # 检查每个时间段是否有即将开始的
                    for start_ts, end_ts in timestamp_ranges:
                        team_start_time = datetime.fromtimestamp(start_ts)
                        
                        # 计算时间差（分钟）
                        time_diff_minutes = (team_start_time - target_time).total_seconds() / 60
                        
                        # 检查是否在目标时间前后1分钟内
                        if abs(time_diff_minutes) <= 1:
                            # 添加必要的字段
                            team_copy = team.copy()
                            team_copy['group_id'] = group_id
                            team_copy['team_id'] = team_id
                            team_copy['start_timestamp'] = start_ts
                            team_copy['start_time'] = team_start_time
                            starting_teams.append(team_copy)
                            break  # 找到一个匹配的时间段就足够了
            
            return starting_teams
            
        except Exception as e:
            self.log_unified("ERROR", f"获取即将开始的车队时出错: {e}", group_id="system", user_id="system")
            return []

    def _get_members_starting_soon(self, teams_data: Dict[str, Any], minutes_ahead: int) -> List[Dict[str, Any]]:
        """
        获取即将开始时间段的成员列表
        
        Args:
            teams_data: 车队数据
            minutes_ahead: 提前多少分钟
            
        Returns:
            即将开始时间段的成员列表，每个元素包含成员信息和所属车队信息
        """
        try:
            current_time = datetime.now()
            target_time = current_time + timedelta(minutes=minutes_ahead)
            
            starting_members = []
            
            for group_id, group_data in teams_data.items():
                if 'teams' not in group_data:
                    continue
                    
                for team_id, team in group_data['teams'].items():
                    # 检查每个成员的时间段
                    for member in team.get('members', []):
                        # 使用成员的时间戳范围进行准确的日期时间比较
                        member_timestamp_ranges = member.get('timestamp_ranges', [])
                        if not member_timestamp_ranges:
                            continue
                        
                        # 检查每个时间段是否有即将开始的
                        for start_ts, end_ts in member_timestamp_ranges:
                            member_start_time = datetime.fromtimestamp(start_ts)
                            
                            # 计算时间差（分钟）
                            time_diff_minutes = (member_start_time - target_time).total_seconds() / 60
                            
                            # 检查是否在目标时间前后1分钟内
                            if abs(time_diff_minutes) <= 1:
                                # 从时间戳转换回时间字符串用于显示
                                start_dt = datetime.fromtimestamp(start_ts)
                                end_dt = datetime.fromtimestamp(end_ts)
                                start_time_str = start_dt.strftime('%H:%M')
                                end_time_str = end_dt.strftime('%H:%M')
                                
                                # 创建时间段信息
                                time_slot = {
                                    'start': start_time_str,
                                    'end': end_time_str
                                }
                                
                                # 创建成员提醒信息
                                member_info = {
                                    'group_id': group_id,
                                    'team_id': team_id,
                                    'team_number': team.get('team_number', ''),
                                    'license_plate': team.get('license_plate'),
                                    'member': member,
                                    'time_slot': time_slot,
                                    'start_minutes': start_dt.hour * 60 + start_dt.minute,
                                    'start_timestamp': start_ts
                                }
                                starting_members.append(member_info)
                                break  # 找到一个匹配的时间段就足够了
            
            return starting_members
            
        except Exception as e:
            self.logger.error(f"获取即将开始时间段的成员时出错: {e}")
            return []

    def _check_timestamp_overlap(self, range1: List[tuple], range2: List[tuple]) -> bool:
        """
        检查两个时间戳范围是否有重叠
        
        Args:
            range1: 第一个时间戳范围列表
            range2: 第二个时间戳范围列表
            
        Returns:
            是否有重叠
        """
        for start1, end1 in range1:
            for start2, end2 in range2:
                if start1 < end2 and start2 < end1:
                    return True
        return False
    
    def _get_understaffed_time_slots(self, teams_data: Dict[str, Any], minutes_ahead: int) -> List[Dict[str, Any]]:
        """
        获取即将开始且缺人的时间段列表（提前指定分钟数）
        修复跨天提醒问题：正确处理跨日期的车队时间段
        
        Args:
            teams_data: 车队数据
            minutes_ahead: 提前多少分钟检查
            
        Returns:
            缺人时间段信息列表
        """
        understaffed_slots = []
        current_time = datetime.now()
        target_time = current_time + timedelta(minutes=minutes_ahead)
        
        try:
            for group_id, group_data in teams_data.items():
                if 'teams' not in group_data:
                    continue
                    
                for team_id, team in group_data['teams'].items():
                    # 跳过已过期的车队
                    if current_time.timestamp() > team.get('end_timestamp', 0):
                        continue
                    
                    # 获取车队的详细时间段分布（修复后的版本）
                    time_slots_info = self._get_team_time_slots_distribution_fixed(team)
                    
                    for slot_info in time_slots_info:
                        # 检查是否缺人
                        if slot_info['missing_count'] > 0:
                            # 使用时间戳进行准确的时间比较
                            slot_start_timestamp = slot_info['start_timestamp']
                            slot_datetime = datetime.fromtimestamp(slot_start_timestamp)
                            
                            # 计算时间差（分钟）
                            time_diff_minutes = (slot_datetime - target_time).total_seconds() / 60
                            
                            # 检查时间段是否在目标时间前后1分钟内
                            if abs(time_diff_minutes) <= 1:
                                understaffed_slots.append({
                                    'group_id': group_id,
                                    'team_id': team_id,
                                    'team_number': team['team_number'],
                                    'captain': team['captain'],
                                    'song': team['song'],
                                    'time_slot': slot_info['time_slot'],
                                    'missing_count': slot_info['missing_count'],
                                    'start_minutes': slot_info['start_minutes'],
                                    'slot_datetime': slot_datetime,
                                    'start_timestamp': slot_start_timestamp
                                })
                                
        except Exception as e:
            self.logger.error(f"获取缺人时间段失败: {e}")
            
        return understaffed_slots
    
    def _get_team_time_slots_distribution_fixed(self, team: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        获取车队的时间段人数分布信息（修复跨天问题）
        
        Args:
            team: 车队数据
            
        Returns:
            时间段分布信息列表
        """
        max_team_size = 5  # 最大车队人数
        slots_info = []
        
        try:
            # 获取车队的时间戳范围
            team_ranges = [(r[0], r[1]) for r in team['timestamp_ranges']]
            if not team_ranges:
                return slots_info
            
            # 不再按日期分组，而是直接处理所有时间段
            # 收集所有相关的时间点
            time_points = set()
            
            # 添加车队的时间点
            for start_ts, end_ts in team_ranges:
                time_points.add(start_ts)
                time_points.add(end_ts)
            
            # 添加成员的时间点（只要与车队时间范围有重叠）
            for member in team['members']:
                for start_ts, end_ts in member['timestamp_ranges']:
                    # 检查成员时间段是否与车队时间范围重叠
                    if self._check_timestamp_overlap([(start_ts, end_ts)], team_ranges):
                        # 计算重叠部分的时间点
                        for team_start, team_end in team_ranges:
                            overlap_start = max(start_ts, team_start)
                            overlap_end = min(end_ts, team_end)
                            if overlap_start < overlap_end:
                                time_points.add(overlap_start)
                                time_points.add(overlap_end)
            
            # 排序时间点
            sorted_time_points = sorted(time_points)
            
            if len(sorted_time_points) < 2:
                return slots_info
            
            # 为每个时间段计算人数
            for i in range(len(sorted_time_points) - 1):
                start_ts = sorted_time_points[i]
                end_ts = sorted_time_points[i + 1]
                
                # 确保这个时间段在车队时间范围内
                if not self._check_timestamp_overlap([(start_ts, end_ts)], team_ranges):
                    continue
                
                # 计算这个时间段的人数（队长+成员）
                count = 1  # 队长
                for member in team['members']:
                    member_ranges = [(r[0], r[1]) for r in member['timestamp_ranges']]
                    if self._check_timestamp_overlap([(start_ts, end_ts)], member_ranges):
                        count += 1
                
                # 格式化时间显示
                start_time = self._timestamp_to_time_str(start_ts)
                end_time = self._timestamp_to_time_str(end_ts)
                
                # 计算开始和结束的分钟数
                start_dt = datetime.fromtimestamp(start_ts)
                end_dt = datetime.fromtimestamp(end_ts)
                start_minutes = start_dt.hour * 60 + start_dt.minute
                end_minutes = end_dt.hour * 60 + end_dt.minute
                
                # 计算缺人数量
                missing_count = max(0, max_team_size - count)
                
                slots_info.append({
                    'time_slot': f"{start_time}-{end_time}",
                    'current_count': count,
                    'missing_count': missing_count,
                    'start_minutes': start_minutes,
                    'end_minutes': end_minutes,
                    'start_timestamp': start_ts,
                    'end_timestamp': end_ts
                })
                
        except Exception as e:
            self.logger.error(f"获取车队时间段分布失败: {e}")
            
        return slots_info

    async def _send_understaffed_reminder(self, slot_info: Dict[str, Any]) -> bool:
        """
        发送缺人提醒消息（不带@）
        
        Args:
            slot_info: 缺人时间段信息
            
        Returns:
            是否发送成功
        """
        try:
            if not self.message_sender:
                self.log_unified("WARNING", "消息发送器未设置，无法发送缺人提醒", "system", "system")
                return False
            
            # 构建缺人提醒消息
            reminder_text = (
                f"🚨 缺人提醒\n"
                f"车队号{slot_info['team_number']}，队长{slot_info['captain']}的车队在{slot_info['time_slot']}"
                f"缺{slot_info['missing_count']}人打歌曲{slot_info['song']}，"
                f"请有需要的推手或者共跑加入车队，推时不够的小伙伴注意咯~"
            )
            
            # 发送普通文本消息（不带@）
            message_data = {
                "action": "send_group_msg",
                "params": {
                    "group_id": slot_info['group_id'],
                    "message": reminder_text
                }
            }
            
            # 发送消息
            await self.message_sender(message_data)
            
            self.log_unified(
                "INFO", 
                f"发送缺人提醒成功 - 车队号: {slot_info['team_number']}, "
                f"时间段: {slot_info['time_slot']}, 缺人数: {slot_info['missing_count']}", 
                slot_info['group_id'], 
                "system"
            )
            
            return True
            
        except Exception as e:
            self.log_unified("ERROR", f"发送缺人提醒失败: {e}", slot_info.get('group_id', 'system'), "system")
            return False
    


    async def _send_captain_reminder(self, team: Dict[str, Any]) -> bool:
        """
        发送队长提醒消息
        
        Args:
            team: 车队信息
            
        Returns:
            是否发送成功
        """
        try:
            group_id = team['group_id']
            captain_user_id = team.get('captain_user_id', team['captain'])
            team_number = team['team_number']
            # 使用start_time字段而不是time_start_minutes
            start_time_dt = team.get('start_time')
            if start_time_dt:
                start_time = start_time_dt.strftime('%H:%M')
            else:
                # 如果没有start_time，尝试从timestamp_ranges获取
                timestamp_ranges = team.get('timestamp_ranges', [])
                if timestamp_ranges:
                    start_timestamp = timestamp_ranges[0][0]
                    start_time_dt = datetime.fromtimestamp(start_timestamp)
                    start_time = start_time_dt.strftime('%H:%M')
                else:
                    start_time = "未知时间"
            
            # 构建@消息
            message_segments = [
                {
                    "type": "at",
                    "data": {
                        "qq": str(captain_user_id)
                    }
                },
                {
                    "type": "text",
                    "data": {
                        "text": f" 你有一个{start_time}的{team_number}号车等待发车上传车牌。"
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
                    # 获取当前时间戳
                    self.log_unified("INFO", f"队长提醒发送成功: 群{group_id}, 车队{team_number}, 队长{captain_user_id}", group_id=group_id, user_id=captain_user_id)
                    return True
                else:
                    self.log_unified("ERROR", f"队长提醒发送失败: 群{group_id}, 车队{team_number}", group_id=group_id, user_id="system")
            else:
                self.log_unified("WARNING", f"消息发送器未设置，无法发送队长提醒", group_id="system", user_id="system")
            
            return False
        except Exception as e:
            self.log_unified("ERROR", f"发送队长提醒时出错: {e}", group_id="system", user_id="system")
            return False
    
    # 删除了_send_understaffed_reminder方法
    
    def _timestamp_to_time_str(self, timestamp: float) -> str:
        """
        将时间戳转换为时间字符串
        
        Args:
            timestamp: 时间戳
            
        Returns:
            时间字符串，格式为HH:MM
        """
        try:
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime("%H:%M")
        except Exception:
            return "00:00"
            
        except Exception as e:
            self.logger.error(f"发送队长提醒时出错: {e}")
            return False
    
    async def _send_member_time_slot_reminder(self, member_info: Dict[str, Any]) -> bool:
        """
        发送基于成员时间段的提醒消息
        
        Args:
            member_info: 成员信息，包含成员、车队和时间段信息
            
        Returns:
            是否发送成功
        """
        try:
            group_id = member_info['group_id']
            team_number = member_info['team_number']
            license_plate = member_info.get('license_plate')
            member = member_info['member']
            time_slot = member_info['time_slot']
            
            # 获取成员信息
            member_user_id = member.get('user_id', member['name'])
            join_type = member.get('role', '推车')
            start_time = time_slot.get('start', '')
            end_time = time_slot.get('end', '')
            
            # 检查是否有车牌
            if license_plate:
                plate_info = f"车牌号为:{team_number}号车车牌:[{license_plate}]"
            else:
                plate_info = "队长还没有上传车牌，请耐心等待并留意群内消息。"
            
            # 构建时间段信息
            time_info = f"{start_time}-{end_time}" if end_time else start_time
            
            # 构建@消息
            message_segments = [
                {
                    "type": "at",
                    "data": {
                        "qq": str(member_user_id)
                    }
                },
                {
                    "type": "text",
                    "data": {
                        "text": f"你有一个{time_info}的{team_number}号车等待{join_type}，{plate_info}"
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
                    self.logger.info(f"成员时间段提醒发送成功: 群{group_id}, 车队{team_number}, 成员{member_user_id}, 时间段{time_info}")
                    return True
                else:
                    # 获取当前时间戳
                    current_time = datetime.now()
                    self.log_unified("ERROR", f"成员时间段提醒发送失败: 车队{team_number}, 成员{member_user_id}, 时间段{time_info}", group_id=group_id, user_id="system")
                    return False
            else:
                # 获取当前时间戳
                current_time = datetime.now()
                self.log_unified("WARNING", "消息发送器未设置，无法发送成员时间段提醒", group_id="system", user_id="system")
                return False
            
        except Exception as e:
            self.log_unified("ERROR", f"发送成员时间段提醒时出错: {e}", group_id="system", user_id="system")
            return False

    async def _send_member_time_slot_reminder_batch(self, group_id: str, team_id: str, team_number: str,
                                                    license_plate: Optional[str], members: List[Dict[str, Any]]) -> bool:
        """
        合并发送同一车队的成员时间段提醒（一条消息包含多名成员）

        Args:
            group_id: 群组ID
            team_id: 车队ID
            team_number: 车队号
            license_plate: 车牌号（可能为空）
            members: 成员信息列表（包含member、time_slot、start_timestamp）

        Returns:
            是否发送成功
        """
        try:
            header_lines = [f"🚗 {team_number}号车成员时间提醒"]
            plate_info = f"车牌:[{license_plate}]" if license_plate else "队长还没有上传车牌，请耐心等待并留意群内消息。"

            segments: List[Dict[str, Any]] = []
            segments.append({"type": "text", "data": {"text": "\n".join(header_lines) + "\n"}})

            for info in members:
                member = info.get('member', {})
                time_slot = info.get('time_slot', {})
                member_user_id = str(member.get('user_id', member.get('name', '')))
                join_type = member.get('role', '推车')
                start_time = time_slot.get('start', '')
                end_time = time_slot.get('end', '')
                time_info = f"{start_time}-{end_time}" if end_time else start_time

                segments.append({"type": "text", "data": {"text": f"- {member.get('name', '')}：{time_info} 等待{join_type} "}})
                if member_user_id:
                    segments.append({"type": "at", "data": {"qq": member_user_id}})
                segments.append({"type": "text", "data": {"text": "\n"}})

            segments.append({"type": "text", "data": {"text": plate_info}})

            response_data = {
                "action": "send_group_msg",
                "params": {"group_id": group_id, "message": segments}
            }

            if self.message_sender:
                success = await self.message_sender(response_data)
                if success:
                    self.log_unified("INFO", f"成员合并提醒发送成功: 群{group_id}, 车队{team_number}, 成员数{len(members)}",
                                     group_id=group_id, user_id="system")
                    return True
                else:
                    self.log_unified("ERROR", f"成员合并提醒发送失败: 车队{team_number}", group_id=group_id, user_id="system")
                    return False
            else:
                self.log_unified("WARNING", "消息发送器未设置，无法发送成员合并提醒", group_id="system", user_id="system")
                return False
        except Exception as e:
            self.log_unified("ERROR", f"发送成员合并提醒时出错: {e}", group_id="system", user_id="system")
            return False
    
    async def _check_and_send_reminders(self):
        """
        检查并发送提醒消息（使用改进的防重复机制）
        """
        try:
            teams_data = self._load_teams_data()
            if not teams_data:
                return
            
            # 清理过期的提醒记录
            self._clean_expired_reminders()
            
            # 检查队长提醒（开始前5分钟）
            captain_teams = self._get_teams_starting_soon(teams_data, 5)
            for team in captain_teams:
                # 生成队长提醒的唯一标识符
                reminder_key = self._generate_reminder_key(
                    'captain', team['group_id'], team['team_id'], 
                    team['start_timestamp']
                )
                
                # 如果还没有发送过这个提醒，则发送
                if not self._is_reminder_sent(reminder_key):
                    self.log_unified("DEBUG", f"准备发送队长提醒 - 键值: {reminder_key}", team['group_id'], "system")
                    success = await self._send_captain_reminder(team)
                    if success:
                        self._mark_reminder_sent(reminder_key)
                        self.log_unified("INFO", f"队长提醒发送成功并标记 - 键值: {reminder_key}", team['group_id'], "system")
                else:
                    self.log_unified("DEBUG", f"队长提醒已发送过，跳过 - 键值: {reminder_key}", team['group_id'], "system")
            
            # 检查成员时间段提醒（开始前2分钟），并对同队成员进行合并发送
            starting_members = self._get_members_starting_soon(teams_data, 2)
            grouped: Dict[tuple, Dict[str, Any]] = {}
            for info in starting_members:
                key = (info['group_id'], info['team_id'])
                if key not in grouped:
                    grouped[key] = {
                        'group_id': info['group_id'],
                        'team_id': info['team_id'],
                        'team_number': info['team_number'],
                        'license_plate': info.get('license_plate'),
                        'members': []
                    }
                grouped[key]['members'].append(info)

            for (group_id, team_id), bundle in grouped.items():
                min_ts = min(m['start_timestamp'] for m in bundle['members'])
                names = ",".join(sorted(m['member'].get('name', '') for m in bundle['members']))
                reminder_key = self._generate_reminder_key('member_batch', group_id, team_id, min_ts, names)

                if not self._is_reminder_sent(reminder_key):
                    self.log_unified("DEBUG", f"准备发送成员合并提醒 - 键值: {reminder_key}", group_id, "system")
                    success = await self._send_member_time_slot_reminder_batch(
                        group_id,
                        team_id,
                        str(bundle['team_number']),
                        bundle.get('license_plate'),
                        bundle['members']
                    )
                    if success:
                        self._mark_reminder_sent(reminder_key)
                        self.log_unified("INFO", f"成员合并提醒发送成功并标记 - 键值: {reminder_key}", group_id, "system")
                        await asyncio.sleep(0.5)
                else:
                    self.log_unified("DEBUG", f"成员合并提醒已发送过，跳过 - 键值: {reminder_key}", group_id, "system")
            
            # 检查缺人时段提醒（开始前30分钟）
            understaffed_slots = self._get_understaffed_time_slots(teams_data, 30)
            for slot_info in understaffed_slots:
                # 生成缺人提醒的唯一标识符
                reminder_key = self._generate_reminder_key(
                    'understaffed', slot_info['group_id'], slot_info['team_id'],
                    slot_info['start_timestamp'], slot_info['time_slot']
                )
                
                if not self._is_reminder_sent(reminder_key):
                    self.log_unified("DEBUG", f"准备发送缺人提醒 - 键值: {reminder_key}", slot_info['group_id'], "system")
                    success = await self._send_understaffed_reminder(slot_info)
                    if success:
                        self._mark_reminder_sent(reminder_key)
                        self.log_unified("INFO", f"缺人提醒发送成功并标记 - 键值: {reminder_key}", slot_info['group_id'], "system")
                        # 避免发送过快，间隔0.5秒
                        await asyncio.sleep(0.5)
                else:
                    self.log_unified("DEBUG", f"缺人提醒已发送过，跳过 - 键值: {reminder_key}", slot_info['group_id'], "system")
                
        except Exception as e:
            self.log_unified("ERROR", f"检查和发送提醒时出错: {e}", group_id="system", user_id="system")

    async def _reminder_loop(self):
        """
        提醒循环任务
        """
        self.log_unified("INFO", f"车队定时提醒服务开始运行", group_id="system", user_id="system")
        
        while self.is_running:
            try:
                await self._check_and_send_reminders()
                # 每30秒检查一次
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.log_unified("INFO", f"提醒循环任务被取消", group_id="system", user_id="system")
                break
            except Exception as e:
                self.log_unified("ERROR", f"提醒循环出错: {e}", group_id="system", user_id="system")
                # 出错后等待1分钟再继续
                await asyncio.sleep(60)
        
        self.log_unified("INFO", f"车队定时提醒服务已停止", group_id="system", user_id="system")
    
    async def start_reminder_service(self):
        """
        启动定时提醒服务
        """
        if self.is_running:
            self.log_unified("WARNING", f"定时提醒服务已在运行中", group_id="system", user_id="system")
            return
        
        self.is_running = True
        self.reminder_task = asyncio.create_task(self._reminder_loop())
        self.log_unified("INFO", f"车队定时提醒服务已启动", group_id="system", user_id="system")
    
    async def stop_reminder_service(self):
        """
        停止定时提醒服务
        """
        if not self.is_running:
            self.log_unified("WARNING", f"定时提醒服务未在运行", group_id="system", user_id="system")
            return
        
        self.is_running = False
        
        if self.reminder_task:
            self.reminder_task.cancel()
            try:
                await self.reminder_task
            except asyncio.CancelledError:
                pass
            self.reminder_task = None
        
        self.log_unified("INFO", f"车队定时提醒服务已停止", group_id="system", user_id="system")
    
    def set_message_sender(self, message_sender: Callable):
        """
        设置消息发送器
        
        Args:
            message_sender: 消息发送回调函数
        """
        self.message_sender = message_sender
        self.log_unified("INFO", "消息发送器已设置", group_id="system", user_id="system")
    
    def get_reminder_status(self) -> Dict[str, Any]:
        """
        获取提醒服务状态
        
        Returns:
            服务状态信息
        """
        return {
            "is_running": self.is_running,
            "captain_reminders_count": len(self.captain_reminders_sent),
            "passenger_reminders_count": len(self.passenger_reminders_sent),
            "understaffed_reminders_count": len(self.understaffed_reminders_sent),
            "has_message_sender": self.message_sender is not None
        }
    
    def clear_team_reminders(self, group_id: str, team_id: str) -> None:
        """
        清除指定车队的提醒记录（兼容单项和合并成员提醒键）
        
        Args:
            group_id: 群组ID
            team_id: 车队ID
        """
        try:
            # 统一处理所有提醒类型的前缀
            prefixes = [
                f"captain:{group_id}:{team_id}:",
                f"member:{group_id}:{team_id}:",
                f"member_batch:{group_id}:{team_id}:",
                f"understaffed:{group_id}:{team_id}:"
            ]

            keys_to_remove = []
            for key in list(self.sent_reminders.keys()):
                if any(key.startswith(pfx) for pfx in prefixes):
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                del self.sent_reminders[key]

            if keys_to_remove:
                self.log_unified("INFO", f"清除了车队 {team_id} 的 {len(keys_to_remove)} 个提醒记录", 
                               group_id=group_id, user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"清除车队提醒记录时出错: {e}", 
                            group_id=group_id, user_id="system")

    def clear_member_reminders(self, group_id: str, team_id: str, member_name: str) -> None:
        """
        清除指定队员的提醒记录（用于撤回后立即停止向该成员发送提醒）

        Args:
            group_id: 群组ID
            team_id: 车队ID
            member_name: 成员名称
        """
        try:
            keys_to_remove = []
            prefix_single = f"member:{group_id}:{team_id}:"
            prefix_batch = f"member_batch:{group_id}:{team_id}:"
            for key in list(self.sent_reminders.keys()):
                # 单成员提醒键：member:{group_id}:{team_id}:{date}:{minute}:{member_name}
                if key.startswith(prefix_single) and key.endswith(f":{member_name}"):
                    keys_to_remove.append(key)
                    continue
                # 合并成员提醒键：member_batch:{group_id}:{team_id}:{date}:{minute}:{names(逗号分隔)}
                if key.startswith(prefix_batch):
                    last_token = key.split(":")[-1]
                    names = last_token.split(",")
                    if member_name in names:
                        keys_to_remove.append(key)

            for key in keys_to_remove:
                del self.sent_reminders[key]

            if keys_to_remove:
                self.log_unified("INFO", f"清除了成员 {member_name} 的 {len(keys_to_remove)} 个提醒记录", 
                                 group_id=group_id, user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"清除成员提醒记录时出错: {e}", group_id=group_id, user_id="system")

    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理消息（提醒服务不处理用户消息）
        
        Args:
            message: 消息内容
            user_id: 用户ID
            **kwargs: 其他参数
            
        Returns:
            None（提醒服务不响应用户消息）
        """
        return None
    
    def get_help_text(self) -> str:
        """
        获取帮助文本
        
        Returns:
            帮助文本
        """
        return "🔔 车队定时提醒服务\n\n自动在班车开始前发送提醒消息：\n- 开始前5分钟提醒队长上传车牌\n- 开始前2分钟根据队员的具体时间段提醒准备上车\n- 缺人时段开始前30分钟提醒队长注意招募"