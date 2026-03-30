#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日车队统计服务

功能说明：
- 每天 21:00 统计指定群聊中“昨日21:00到今日21:00结束”的所有车队（自然过期 + 撤回）
- 将统计结果发送到对应的目标群聊（逐条分发避免截断）
- 对每个车队标注结束类型：自然过期或撤回
- 支持多个群聊的1对1映射发送

作者：Assistant
创建时间：2025-01-11
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Optional
from .base_service import BaseService
from utils.image_generator import ImageGenerator


class DailyTeamStatisticsService(BaseService):
    """
    每日车队统计服务类
    
    负责定时统计车队信息并发送到指定群聊
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, message_sender=None, server=None):
        """
        初始化每日车队统计服务
        
        Args:
            config: 配置信息
            data_manager: 数据管理器
            text_formatter: 文本格式化器
            message_sender: 消息发送器
            server: 服务器实例
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.message_sender = message_sender
        
        # 获取服务配置
        self.service_config = config.get('services', {}).get('daily_team_statistics', {})
        self.enabled = self.service_config.get('enabled', False)
        self.schedule_time = self.service_config.get('schedule_time', '21:00')
        self.group_mappings = self.service_config.get('group_mappings', {})
        self.timezone = self.service_config.get('timezone', 'Asia/Shanghai')
        
        # 车队数据文件路径
        self.teams_record_file = os.path.join(str(self.data_manager.base_path), 'team', 'teams_record.json')
        self.teams_file = os.path.join(str(self.data_manager.base_path), 'team', 'teams.json')
        
        # 初始化甘特图生成器
        self.image_generator = ImageGenerator()
        
        # 定时任务状态
        self._scheduler_task = None
        self._is_running = False
        
        # 注意：不在__init__中启动调度器，因为此时可能没有事件循环
        # 调度器将在服务器启动后通过start_service()方法启动
    
    def start_service(self):
        """
        启动服务（在事件循环运行后调用）
        """
        if self.enabled and not self._is_running:
            self._start_scheduler()
    
    def _start_scheduler(self):
        """
        启动定时任务调度器
        """
        if not self._is_running:
            self._is_running = True
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())
            print(f"[INFO] 每日车队统计服务已启动，计划执行时间：{self.schedule_time}")
    
    async def _scheduler_loop(self):
        """
        定时任务循环
        """
        while self._is_running:
            try:
                # 计算下次执行时间
                now = datetime.now()
                target_time = self._get_next_execution_time(now)
                
                # 等待到执行时间
                wait_seconds = (target_time - now).total_seconds()
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                
                # 执行统计任务
                await self._execute_daily_statistics()
                
                # 等待1分钟，避免重复执行
                await asyncio.sleep(60)
                
            except Exception as e:
                print(f"[ERROR] 定时任务执行失败: {e}")
                await asyncio.sleep(300)  # 出错后等待5分钟再重试
    
    def _get_next_execution_time(self, current_time: datetime) -> datetime:
        """
        获取下次执行时间
        
        Args:
            current_time: 当前时间
            
        Returns:
            下次执行的时间
        """
        # 解析执行时间
        hour, minute = map(int, self.schedule_time.split(':'))
        
        # 计算今天的执行时间
        today_execution = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # 如果今天的执行时间已过，则计算明天的执行时间
        if current_time >= today_execution:
            return today_execution + timedelta(days=1)
        else:
            return today_execution
    
    async def _execute_daily_statistics(self):
        """
        执行每日统计任务

        变更说明：
        - 由“仅统计自然过期车队”改为“统计昨日结束的所有车队（自然过期 + 撤回）”；
        - 统计时对每个车队标注类型（📌 自然过期 / 📌 撤回）。
        """
        try:
            print(f"[INFO] 开始执行每日车队统计任务")
            
            tz = ZoneInfo(self.timezone)
            now_local = datetime.now(tz)
            hour, minute = map(int, self.schedule_time.split(':'))
            today_boundary = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
            window_end = today_boundary if now_local >= today_boundary else (today_boundary - timedelta(days=1))
            window_start = window_end - timedelta(days=1)
            range_text = f"{window_start.strftime('%m.%d %H:%M')} - {window_end.strftime('%m.%d %H:%M')}"
            
            # 遍历所有群聊映射
            for source_group, target_group in self.group_mappings.items():
                try:
                    try:
                        await self._archive_expired_teams_until(source_group, window_end)
                    except Exception as e:
                        print(f"[ERROR] 群聊 {source_group} 归档过期车队失败: {e}")

                    expired_teams = await self._get_ended_teams_in_window(source_group, window_start, window_end)
                    
                    if expired_teams:
                        # 格式化消息并生成甘特图
                        message_segments = await self._format_statistics_message_with_gantt(expired_teams, source_group, range_text)
                        
                        # 发送混合消息到目标群聊
                        await self._send_statistics_message_with_gantt(target_group, message_segments)
                        
                        print(f"[INFO] 已发送群聊 {source_group} 的21点窗口结束车队统计(含甘特图)到群聊 {target_group}，共 {len(expired_teams)} 个车队")
                    else:
                        print(f"[INFO] 群聊 {source_group} 在统计窗口 {range_text} 内无结束车队")
                        
                except Exception as e:
                    print(f"[ERROR] 处理群聊 {source_group} 的统计任务失败: {e}")
            
            print(f"[INFO] 每日车队统计任务执行完成")
            
        except Exception as e:
            print(f"[ERROR] 执行每日统计任务失败: {e}")

    async def _archive_expired_teams_until(self, group_id: str, cutoff_local: datetime) -> int:
        cutoff_ts = cutoff_local.timestamp()

        team_service = None
        if self.server is not None and hasattr(self.server, 'services'):
            try:
                team_service = self.server.services.get('team')
            except Exception:
                team_service = None

        if team_service is not None and hasattr(team_service, 'archive_expired_teams_until'):
            return int(team_service.archive_expired_teams_until(str(group_id), float(cutoff_ts)))

        return int(self._archive_expired_teams_until_by_files(str(group_id), float(cutoff_ts)))

    def _archive_expired_teams_until_by_files(self, group_id: str, cutoff_timestamp: float) -> int:
        try:
            if not os.path.exists(self.teams_file):
                return 0

            os.makedirs(os.path.dirname(self.teams_record_file), exist_ok=True)

            with open(self.teams_file, 'r', encoding='utf-8') as f:
                teams_data = json.load(f) or {}

            if os.path.exists(self.teams_record_file):
                with open(self.teams_record_file, 'r', encoding='utf-8') as f:
                    teams_record = json.load(f) or {}
            else:
                teams_record = {}

            group_data = teams_data.get(group_id) or {}
            teams = group_data.get('teams') or {}
            team_numbers = group_data.get('team_numbers') or {}

            if group_id not in teams_record:
                teams_record[group_id] = {'archived_teams': []}
            if 'archived_teams' not in teams_record[group_id]:
                teams_record[group_id]['archived_teams'] = []

            archived_list = teams_record[group_id]['archived_teams']
            archived_team_ids = set()
            for t in archived_list:
                tid = t.get('team_id')
                if tid:
                    archived_team_ids.add(str(tid))

            tz = ZoneInfo(self.timezone)
            archived_at = datetime.now(tz).isoformat()

            moved = 0
            for team_id, team in list(teams.items()):
                end_ts = team.get('end_timestamp', 0)
                if not end_ts:
                    continue
                try:
                    if float(end_ts) > float(cutoff_timestamp):
                        continue
                except Exception:
                    continue

                if str(team_id) in archived_team_ids:
                    del teams[team_id]
                    moved += 1
                    continue

                team_copy = dict(team)
                team_copy['archived_at'] = archived_at
                team_copy['team_id'] = str(team_id)
                team_copy['expire_reason'] = team_copy.get('expire_reason') or 'natural'
                archived_list.append(team_copy)

                del teams[team_id]

                team_number = team_copy.get('team_number')
                if team_number is not None:
                    team_numbers.pop(str(team_number), None)
                    try:
                        team_numbers.pop(int(team_number), None)
                    except Exception:
                        pass

                moved += 1

            group_data['teams'] = teams
            group_data['team_numbers'] = team_numbers
            teams_data[group_id] = group_data
            teams_record[group_id]['archived_teams'] = archived_list

            with open(self.teams_file, 'w', encoding='utf-8') as f:
                json.dump(teams_data, f, ensure_ascii=False, indent=2)

            with open(self.teams_record_file, 'w', encoding='utf-8') as f:
                json.dump(teams_record, f, ensure_ascii=False, indent=2)

            return moved
        except Exception as e:
            print(f"[ERROR] 文件归档过期车队失败: {e}")
            return 0

    async def _get_ended_teams_in_window(self, group_id: str, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
        try:
            if not os.path.exists(self.teams_record_file):
                return []

            with open(self.teams_record_file, 'r', encoding='utf-8') as f:
                teams_record = json.load(f) or {}

            group_data = teams_record.get(str(group_id)) or {}
            archived_teams = group_data.get('archived_teams', []) or []

            tz = ZoneInfo(self.timezone)

            def _parse_archived_at(value: str) -> Optional[datetime]:
                if not value:
                    return None
                try:
                    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=tz)
                    return dt.astimezone(tz)
                except Exception:
                    return None

            def _get_effective_end_time(team: Dict[str, Any]) -> Optional[datetime]:
                reason = team.get('expire_reason')
                archived_at = _parse_archived_at(team.get('archived_at', ''))
                end_ts = team.get('end_timestamp', 0)

                if reason in ('cancelled', 'captain_deleted', 'captain_deleted_by_admin'):
                    return archived_at

                if end_ts:
                    try:
                        return datetime.fromtimestamp(float(end_ts), tz)
                    except Exception:
                        return archived_at

                return archived_at

            results: List[Dict[str, Any]] = []
            enriched: List[Dict[str, Any]] = []
            for team in archived_teams:
                end_time = _get_effective_end_time(team)
                if end_time is None:
                    continue

                if end_time > window_start and end_time <= window_end:
                    team_copy = dict(team)
                    team_copy['_ended_at'] = end_time.timestamp()
                    enriched.append(team_copy)

            enriched.sort(key=lambda x: x.get('_ended_at', 0), reverse=True)
            for t in enriched:
                t.pop('_ended_at', None)
                results.append(t)

            return results

        except Exception as e:
            print(f"[ERROR] 获取窗口结束车队失败: {e}")
            return []
    
    async def _get_expired_teams(self, group_id: str, target_date: datetime) -> List[Dict[str, Any]]:
        """
        获取指定群聊在指定日期“结束”的车队信息（包含自然过期与撤回）。

        核心规则（统一以归档时间为准）：
        - 归档时间 `archived_at` 即视为车队“结束”的实际发生日（无论是自然过期还是撤回），
          因为车队从活跃集合迁移到归档集合发生在归档时刻，统计上的“昨日结束”应以此为依据；
        - 当 `archived_at` 缺失时，才回退到自然结束时间 `end_timestamp`；若 `end_timestamp` 恰好为 00:00:00，
          仍沿用既有约定：视为前一日结束（跨天边界归并）。

        这样可以覆盖如下场景：
        - 自然过期的车队由于系统归档任务延迟，在次日才进入归档集合，此时统计“昨日结束”应按归档日计入；
        - 撤回（cancelled）车队本就以归档时刻定义结束，保持与自然过期一致的口径，避免漏计。

        Args:
            group_id: 群聊ID
            target_date: 目标日期（date对象或datetime），按本地时区计算

        Returns:
            指定群聊在目标日期归档（或视同结束）的车队列表（含自然过期与撤回）。
        """
        try:
            # 读取车队记录文件
            if not os.path.exists(self.teams_record_file):
                return []
            
            with open(self.teams_record_file, 'r', encoding='utf-8') as f:
                teams_record = json.load(f)
            
            expired_teams = []
            tz = ZoneInfo(self.timezone)
            
            # 目标日期（date对象），用于比较
            if hasattr(target_date, 'date'):
                # 如果传入的是datetime，转换为date
                target_date = target_date.date()
            
            # 检查是否有该群聊的记录
            if group_id not in teams_record:
                return []
            
            group_data = teams_record[group_id]
            archived_teams = group_data.get('archived_teams', [])
            
            # 遍历该群聊的归档车队
            for team in archived_teams:
                archived_at = team.get('archived_at', '')
                expire_reason = team.get('expire_reason')
                end_timestamp = team.get('end_timestamp', 0)
                
                # 计算结束时间（本地时区）
                end_local: Optional[datetime] = None
                archived_local: Optional[datetime] = None
                
                if end_timestamp and end_timestamp > 0:
                    try:
                        end_local = datetime.fromtimestamp(end_timestamp, tz)
                    except Exception:
                        end_local = None
                
                if archived_at:
                    try:
                        archived_dt = datetime.fromisoformat(archived_at.replace('Z', '+00:00'))
                        archived_local = archived_dt.astimezone(tz)
                    except Exception:
                        archived_local = None
                
                # 计算“结束日期”（统一以归档时间为主，缺失时回退到结束时间）
                effective_date: Optional[datetime.date] = None

                if archived_local is not None:
                    # 主规则：归档日期即为“结束日期”
                    effective_date = archived_local.date()
                else:
                    # 回退规则：使用自然结束时间（保留00:00归并到前一日的处理）
                    if end_local is None:
                        # 两者都缺失，无法判断，跳过
                        continue
                    effective_date = end_local.date()
                    if end_local.hour == 0 and end_local.minute == 0 and end_local.second == 0:
                        effective_date = effective_date - timedelta(days=1)

                # 仅统计目标日期的车队
                if effective_date != target_date:
                    continue

                # 满足“昨日结束”的车队加入列表（保留原始字段用于后续类型标注）
                expired_teams.append(team)
            
            return expired_teams
            
        except Exception as e:
            print(f"[ERROR] 读取车队记录失败: {e}")
            import traceback
            print(f"[ERROR] 详细错误信息: {traceback.format_exc()}")
            return []
    
    def _format_statistics_message(self, teams: List[Dict[str, Any]], group_id: str, date_str: str) -> str:
        """
        格式化统计消息
        兼容多种数据格式：teams_record.json格式和测试数据格式
        
        Args:
            teams: 车队列表
            group_id: 群聊ID
            date_str: 日期字符串
            
        Returns:
            格式化后的消息
        """
        # 消息头部
        message_lines = [f"{date_str} 群聊[{group_id}]中的结束车队（自然过期/撤回）如下："]
        
        # 遍历每个车队
        for team in teams:
            # 兼容不同的数据格式
            # 优先使用teams_record.json格式，回退到测试数据格式
            captain = team.get('captain', team.get('creator_name', '未知'))
            date_display = team.get('date_display', team.get('date_range', '未知'))
            time_range = team.get('time_range', '未知')
            song = team.get('song', team.get('team_name', '未知'))
            team_number = team.get('team_number', team.get('team_id', ''))
            members = team.get('members', [])
            
            # 添加车队编号（如果有）
            team_title = f"🚗 车队{team_number}" if team_number else "🚗 车队"
            
            # 标注车队结束类型
            expire_reason = team.get('expire_reason')
            reason_text = '自然过期' if expire_reason in (None, 'natural') else '撤回'

            message_lines.extend([
                team_title,
                f"👨‍✈️ 队长：{captain}",
                f"📅 日期：{date_display}",
                f"⏰ 时间：{time_range}",
                f"🎵 歌曲：{song}",
                f"📌 类型：{reason_text}"
            ])

            # 如果有要求周回或要求倍率，追加展示
            laps = team.get('laps')
            multiplier = team.get('multiplier')
            if laps is not None:
                message_lines.append(f"🔁 要求周回：{laps}")
            if multiplier is not None:
                message_lines.append(f"✖️ 要求倍率：{multiplier}")
            
            # 添加队员列表（如果有）
            if members:
                message_lines.append("👥 队员列表：")
                for i, member in enumerate(members, 1):
                    # 兼容不同的成员数据格式
                    member_name = member.get('name', member.get('user_name', '未知'))
                    member_type = member.get('join_type', '推车')
                    member_time = member.get('time_range', time_range)
                    member_date = member.get('date_range', date_display)
                    
                    # 格式化成员信息，包含日期
                    message_lines.append(f"  {i}. [{member_type}]{member_name} ({member_date} {member_time})")
            
            # 添加替补列表（如果有）
            substitutes = team.get('substitutes', [])
            if substitutes:
                message_lines.append("🔄 替补列表：")
                for j, sub in enumerate(substitutes, 1):
                    sub_name = sub.get('name', '未知')
                    sub_type = sub.get('type', '替补')
                    sub_date = sub.get('date_range', date_display)
                    sub_time = sub.get('time_range', time_range)
                    line = f"  {j}. [{sub_type}]{sub_name} ({sub_date} {sub_time})"
                    description = sub.get('description')
                    if description:
                        line += f" - {description}"
                    message_lines.append(line)
            
            # 添加空行分隔
            message_lines.append("")
        
        return "\n".join(message_lines).strip()
    
    async def _format_statistics_message_with_gantt(self, teams: List[Dict[str, Any]], group_id: str, date_str: str) -> List[Dict[str, Any]]:
        """
        格式化统计消息并生成甘特图，返回混合消息格式
        
        Args:
            teams: 车队列表
            group_id: 群聊ID
            date_str: 日期字符串
            
        Returns:
            混合消息格式的消息列表
        """
        message_segments = []
        
        # 添加消息头部（强调昨日结束的车队并标注类型）
        header_text = f"{date_str} 群聊[{group_id}]中的结束车队（自然过期/撤回）如下："
        message_segments.append({
            "type": "text",
            "data": {
                "text": header_text
            }
        })
        
        # 遍历每个车队
        for i, team in enumerate(teams):
            # 兼容不同的数据格式
            captain = team.get('captain', team.get('creator_name', '未知'))
            date_display = team.get('date_display', team.get('date_range', '未知'))
            time_range = team.get('time_range', '未知')
            song = team.get('song', team.get('team_name', '未知'))
            team_number = team.get('team_number', team.get('team_id', ''))
            members = team.get('members', [])
            
            # 构建车队信息文本
            team_title = f"🚗 车队{team_number}" if team_number else "🚗 车队"
            # 标注车队结束类型
            expire_reason = team.get('expire_reason')
            reason_text = '自然过期' if expire_reason in (None, 'natural') else '撤回'

            team_info_lines = [
                f"\n{team_title}",
                f"👨‍✈️ 队长：{captain}",
                f"📅 日期：{date_display}",
                f"⏰ 时间：{time_range}",
                f"🎵 歌曲：{song}",
                f"📌 类型：{reason_text}"
            ]

            # 如果有要求周回或要求倍率，追加展示
            laps = team.get('laps')
            multiplier = team.get('multiplier')
            if laps is not None:
                team_info_lines.append(f"🔁 要求周回：{laps}")
            if multiplier is not None:
                team_info_lines.append(f"✖️ 要求倍率：{multiplier}")
            
            # 添加队员列表（如果有）
            if members:
                team_info_lines.append("👥 队员列表：")
                for j, member in enumerate(members, 1):
                    member_name = member.get('name', member.get('user_name', '未知'))
                    member_type = member.get('join_type', '推车')
                    member_time = member.get('time_range', time_range)
                    member_date = member.get('date_range', date_display)
                    
                    # 格式化成员信息，包含日期
                    team_info_lines.append(f"  {j}. [{member_type}]{member_name} ({member_date} {member_time})")
            
            # 添加替补列表（如果有）
            substitutes = team.get('substitutes', [])
            if substitutes:
                team_info_lines.append("🔄 替补列表：")
                for k, sub in enumerate(substitutes, 1):
                    sub_name = sub.get('name', '未知')
                    sub_type = sub.get('type', '替补')
                    sub_date = sub.get('date_range', date_display)
                    sub_time = sub.get('time_range', time_range)
                    line = f"  {k}. [{sub_type}]{sub_name} ({sub_date} {sub_time})"
                    description = sub.get('description')
                    if description:
                        line += f" - {description}"
                    team_info_lines.append(line)
            
            # 添加车队信息文本
            message_segments.append({
                "type": "text",
                "data": {
                    "text": "\n".join(team_info_lines)
                }
            })
            
            # 生成并添加甘特图
            try:
                gantt_path = self._generate_team_gantt_chart(team)
                if gantt_path and os.path.exists(gantt_path):
                    # 添加甘特图
                    message_segments.append({
                        "type": "image",
                        "data": {
                            "file": f"file:///{gantt_path.replace(os.sep, '/')}"
                        }
                    })
                else:
                    # 如果甘特图生成失败，添加提示文本
                    message_segments.append({
                        "type": "text",
                        "data": {
                            "text": "📊 甘特图生成失败"
                        }
                    })
            except Exception as e:
                print(f"[ERROR] 为车队{team_number}生成甘特图时出错: {e}")
                message_segments.append({
                    "type": "text",
                    "data": {
                        "text": "📊 甘特图生成出错"
                    }
                })
        
        return message_segments
    
    def _convert_team_data_for_gantt(self, team_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将teams_record.json格式的车队数据转换为甘特图生成所需的格式
        
        Args:
            team_data: teams_record.json格式的车队数据
            
        Returns:
            甘特图生成器所需的数据格式
        """
        try:
            # 构建甘特图所需的数据格式
            gantt_data = {
                'team_number': team_data.get('team_id', team_data.get('team_number', '')),
                'captain': team_data.get('captain', '未知'),
                'song': team_data.get('song', team_data.get('team_name', '未知')),
                'date_display': team_data.get('date_display', team_data.get('date_range', '')),
                'laps': team_data.get('laps'),
                'multiplier': team_data.get('multiplier'),
                'timestamp_ranges': [],
                'members': []
            }
            
            # 处理车队整体时间范围
            start_ts = team_data.get('start_timestamp')
            end_ts = team_data.get('end_timestamp')
            if start_ts and end_ts:
                gantt_data['timestamp_ranges'] = [[start_ts, end_ts]]
            else:
                # 处理timestamp_ranges格式（可能是字典列表或简单列表）
                timestamp_ranges = team_data.get('timestamp_ranges', [])
                converted_ranges = []
                
                for time_range in timestamp_ranges:
                    if isinstance(time_range, dict):
                        # 如果是字典格式，提取start_timestamp和end_timestamp
                        start_ts = time_range.get('start_timestamp')
                        end_ts = time_range.get('end_timestamp')
                        if start_ts and end_ts:
                            converted_ranges.append([start_ts, end_ts])
                    elif isinstance(time_range, (list, tuple)) and len(time_range) == 2:
                        # 如果已经是简单列表格式，直接使用
                        converted_ranges.append(time_range)
                
                gantt_data['timestamp_ranges'] = converted_ranges
            
            # 转换成员数据格式
            for member in team_data.get('members', []):
                gantt_member = {
                    'name': member.get('name', '未知'),
                    'timestamp_ranges': [],
                    'join_type': member.get('join_type', '推车')
                }
                
                # 处理成员时间范围
                member_start = member.get('start_timestamp')
                member_end = member.get('end_timestamp')
                if member_start and member_end:
                    gantt_member['timestamp_ranges'] = [[member_start, member_end]]
                else:
                    # 处理成员的timestamp_ranges格式
                    member_ranges = member.get('timestamp_ranges', [])
                    converted_member_ranges = []
                    
                    for time_range in member_ranges:
                        if isinstance(time_range, dict):
                            # 如果是字典格式，提取start_timestamp和end_timestamp
                            start_ts = time_range.get('start_timestamp')
                            end_ts = time_range.get('end_timestamp')
                            if start_ts and end_ts:
                                converted_member_ranges.append([start_ts, end_ts])
                        elif isinstance(time_range, (list, tuple)) and len(time_range) == 2:
                            # 如果已经是简单列表格式，直接使用
                            converted_member_ranges.append(time_range)
                    
                    gantt_member['timestamp_ranges'] = converted_member_ranges
                
                gantt_data['members'].append(gantt_member)
            
            return gantt_data
            
        except Exception as e:
            print(f"[ERROR] 转换车队数据格式失败: {e}")
            return None
    
    def _generate_team_gantt_chart(self, team_data: Dict[str, Any]) -> Optional[str]:
        """
        为单个车队生成甘特图
        
        Args:
            team_data: 车队数据
            
        Returns:
            生成的甘特图文件路径，失败时返回None
        """
        try:
            # 转换数据格式
            gantt_data = self._convert_team_data_for_gantt(team_data)
            if not gantt_data:
                return None
            
            # 生成甘特图
            image_path = self.image_generator.generate_team_gantt_chart(gantt_data)
            return image_path
            
        except Exception as e:
            print(f"[ERROR] 生成车队甘特图失败: {e}")
            return None
    
    async def _send_statistics_message(self, group_id: str, message: str):
        """
        发送统计消息到指定群聊
        使用与team服务和APP统一的消息格式，通过napcat发送
        
        Args:
            group_id: 目标群聊ID
            message: 要发送的消息
        """
        try:
            if self.server and hasattr(self.server, 'send_response_to_napcat'):
                # 构建napcat API格式的群聊消息，与team服务保持一致
                response_data = {
                    "action": "send_group_msg",
                    "params": {
                        "group_id": group_id,
                        "message": [{
                            "type": "text",
                            "data": {
                                "text": message
                            }
                        }]
                    }
                }
                
                # 通过服务器的napcat函数发送消息
                success = await self.server.send_response_to_napcat(response_data)
                if success:
                    # 使用统一日志格式记录消息发送
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                    log_msg = f"[{timestamp}][INFO][G:{group_id}][U:system]:已被[每日车队统计服务]处理:每日车队统计消息发送，成功发送响应。"
                    print(log_msg)
                else:
                    # 使用统一日志格式记录发送失败
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                    log_msg = f"[{timestamp}][ERROR][G:{group_id}][U:system]:已被[每日车队统计服务]处理:发送消息失败。"
                    print(log_msg)
            else:
                # 使用统一日志格式记录配置问题
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
                log_msg = f"[{timestamp}][WARNING][G:{group_id}][U:system]:已被[每日车队统计服务]处理:服务器未配置或不支持消息发送。"
                print(log_msg)
                print(f"[DEBUG] 消息内容：\n{message}")
                
        except Exception as e:
            # 使用统一日志格式记录异常
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            log_msg = f"[{timestamp}][ERROR][G:{group_id}][U:system]:已被[每日车队统计服务]处理:发送消息异常: {e}"
            print(log_msg)
    
    async def _send_statistics_message_with_gantt(self, group_id: str, message_segments: List[Dict[str, Any]]):
        """
        发送包含甘特图的混合消息到指定群聊（逐条分发版本）

        设计动机：
        - 避免一次性发送过长的聚合消息在 Napcat/QQ 端被截断，导致“只展示昨天一个车队”的问题；
        - 改为分条消息发送：先发一个头部说明，然后按“每个车队一条文本+一张图片”的方式逐条发送，提升稳定性。

        Args:
            group_id: 目标群聊ID
            message_segments: 混合消息段列表（第一个元素为头部文本，之后每个车队包含文本段与图片段）
        """
        try:
            # 1) 发送消息头部（如果存在）
            header_sent = False
            if message_segments and message_segments[0].get('type') == 'text':
                header_payload = {
                    "action": "send_group_msg",
                    "params": {
                        "group_id": group_id,
                        "message": [message_segments[0]]
                    }
                }
                if self.message_sender:
                    await self.message_sender(header_payload)
                elif self.server and hasattr(self.server, 'send_response_to_napcat'):
                    await self.server.send_response_to_napcat(header_payload)
                else:
                    print(f"[INFO] 车队统计消息（群聊 {group_id}）:")
                    print(message_segments[0]['data']['text'])
                header_sent = True

            # 2) 逐条发送每个车队的文本+图片组合
            #    从第二个段开始扫描，将相邻的文本段和图片段组成一个消息包，逐一发送
            i = 1
            total_chunks = 0
            while i < len(message_segments):
                chunk: List[dict] = []
                # 文本段（必选）
                if message_segments[i].get('type') == 'text':
                    chunk.append(message_segments[i])
                    i += 1
                # 图片段（可选）
                if i < len(message_segments) and message_segments[i].get('type') == 'image':
                    chunk.append(message_segments[i])
                    i += 1

                if not chunk:
                    # 如果出现意外的段类型，向前推进避免死循环
                    i += 1
                    continue

                payload = {
                    "action": "send_group_msg",
                    "params": {
                        "group_id": group_id,
                        "message": chunk
                    }
                }

                # 实际发送：优先使用 message_sender；若未配置则回退到 server.send_response_to_napcat；再回退到控制台输出
                sent_ok = False
                if self.message_sender:
                    sent_ok = await self.message_sender(payload)
                elif self.server and hasattr(self.server, 'send_response_to_napcat'):
                    sent_ok = await self.server.send_response_to_napcat(payload)
                else:
                    # 控制台回退输出
                    for seg in chunk:
                        if seg['type'] == 'text':
                            print(seg['data']['text'])
                        elif seg['type'] == 'image':
                            print(f"[图片] {seg['data']['file']}")
                    sent_ok = True

                total_chunks += 1
                if sent_ok:
                    print(f"[INFO] 车队统计消息分条发送成功: 群聊 {group_id}，第 {total_chunks} 条")
                else:
                    print(f"[ERROR] 车队统计消息分条发送失败: 群聊 {group_id}，第 {total_chunks} 条")

                # 适当等待，降低风控风险（QQ端对频繁消息有风控）
                await asyncio.sleep(0.4)

            if header_sent:
                print(f"[INFO] 车队统计消息发送完成: 群聊 {group_id}，共 {total_chunks} 条车队信息")
            else:
                print(f"[INFO] 车队统计消息发送完成（无头部）: 群聊 {group_id}，共 {total_chunks} 条车队信息")

        except Exception as e:
            print(f"[ERROR] 发送统计消息失败: {e}")
            import traceback
            print(f"[ERROR] 详细错误信息: {traceback.format_exc()}")
    
    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理消息
        
        Args:
            message: 消息内容
            user_id: 用户ID
            **kwargs: 其他参数
            
        Returns:
            处理结果
        """
        # 检查是否是手动触发统计命令
        if message.strip() == '/手动统计车队':
            # 从kwargs中获取group_id
            context = kwargs.get('context', {})
            raw_group_id = context.get('group_id')
            group_id_str = str(raw_group_id) if raw_group_id is not None else None
            
            if group_id_str:
                # 1) 在源群聊触发：直接执行该源群聊的统计
                if group_id_str in self.group_mappings:
                    await self._execute_manual_statistics(group_id_str)
                    return None
                
                # 2) 在目标群聊触发：查找所有映射到该目标群聊的源群聊，并逐一统计
                source_groups = [src for src, tgt in self.group_mappings.items() if str(tgt) == group_id_str]
                if source_groups:
                    for src in source_groups:
                        await self._execute_manual_statistics(src)
                    return None
                
                # 未找到对应映射，提示并忽略
                print(f"[WARNING] 群聊 {group_id_str} 未在群聊映射中找到对应源或目标群聊")
                return None
        
        return None
    
    async def _execute_manual_statistics(self, source_group: str):
        """
        执行手动统计任务
        
        Args:
            source_group: 源群聊ID
        """
        try:
            target_group = self.group_mappings.get(source_group)
            if not target_group:
                print(f"[WARNING] 群聊 {source_group} 未配置目标群聊")
                return
            
            # 使用配置时区计算昨天日期
            tz = ZoneInfo(self.timezone)
            now_local = datetime.now(tz)
            yesterday_local = now_local - timedelta(days=1)
            yesterday_date = yesterday_local.date()
            yesterday_str = yesterday_local.strftime('%m.%d')
            
            # 获取昨日结束的车队信息（自然过期 + 撤回）
            expired_teams = await self._get_expired_teams(source_group, yesterday_date)
            
            if expired_teams:
                # 格式化消息并生成甘特图
                message_segments = await self._format_statistics_message_with_gantt(expired_teams, source_group, yesterday_str)
                
                # 发送混合消息
                await self._send_statistics_message_with_gantt(target_group, message_segments)
                
                print(f"[INFO] 手动统计完成：群聊 {source_group} -> {target_group}，昨日结束车队共 {len(expired_teams)} 个(含甘特图)")
            else:
                print(f"[INFO] 群聊 {source_group} 昨日无结束车队")
                
        except Exception as e:
            print(f"[ERROR] 执行手动统计失败: {e}")
            import traceback
            print(f"[ERROR] 详细错误信息: {traceback.format_exc()}")
        
    def stop_scheduler(self):
        """
        停止定时任务调度器
        """
        self._is_running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            print(f"[INFO] 每日车队统计服务已停止")
        
    def get_help_text(self) -> str:
        """
        获取帮助文本
        
        Returns:
            帮助文本
        """
        return """
📊 每日车队统计服务

🔧 功能说明：
• 每天 00:01 自动统计“昨日结束”的所有车队（自然过期 + 撤回）
• 为每个车队标注结束类型（📌 自然过期 / 📌 撤回）
• 支持在统计消息中生成对应的甘特图并逐条发送

⚙️ 配置信息：
• 执行时间：{}
• 群聊映射：{}

🎯 手动命令：
• /手动统计车队 - 手动执行统计任务（源群聊或目标群聊均可触发）
""".format(
            self.schedule_time,
            ', '.join([f"{k}->{v}" for k, v in self.group_mappings.items()])
        )
