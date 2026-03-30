#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推车时长每日统计服务

功能说明：
- 每天凌晨0:01统计昨日所有手动增减推时和跑时的操作记录
- 从fun_bot.log中搜索并提取相关日志信息
- 将统计结果发送到指定群聊
- 支持多个群聊的统计和发送

作者：Assistant
创建时间：2025-01-11
"""

import asyncio
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from .base_service import BaseService


class DailyPushTimeStatisticsService(BaseService):
    """
    推车时长每日统计服务类
    
    负责定时统计手动推时操作记录并发送到指定群聊
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, message_sender=None, server=None):
        """
        初始化推车时长每日统计服务
        
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
        self.service_config = config.get('services', {}).get('daily_push_time_statistics', {})
        self.enabled = self.service_config.get('enabled', True)
        self.schedule_time = self.service_config.get('schedule_time', '00:01')
        self.group_mappings = self.service_config.get('group_mappings', {})
        self.timezone = self.service_config.get('timezone', 'Asia/Shanghai')
        
        # 日志文件路径
        self.log_file_path = self.service_config.get('log_file_path', 'fun_bot.log')
        
        # 新增配置项
        self.auto_save_log = self.service_config.get('auto_save_log', True)
        self.log_retention_days = self.service_config.get('log_retention_days', 30)
        self.enable_manual_trigger = self.service_config.get('enable_manual_trigger', True)
        self.notification_enabled = self.service_config.get('notification_enabled', False)
        self.statistics_format = self.service_config.get('statistics_format', 'detailed')
        
        # 定时任务状态
        self._scheduler_task = None
        self._is_running = False
        
        # 手动推时操作的正则表达式模式
        # 匹配实际日志格式：[timestamp][INFO][G:group_id][U:user_id]:/操作类型 用户名 时长
        # 或者：[timestamp][INFO][U:user_id]:/操作类型 用户名 时长 (无群组信息)
        self.push_time_patterns = [
            # 带群组信息的格式
            r'.*?\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\[INFO\]\[G:(\d+)\]\[U:(\d+)\]:/增加推时\s+(\S+)\s+([\d.-]+)',
            r'.*?\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\[INFO\]\[G:(\d+)\]\[U:(\d+)\]:/减少推时\s+(\S+)\s+([\d.-]+)',
            r'.*?\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\[INFO\]\[G:(\d+)\]\[U:(\d+)\]:/增加跑时\s+(\S+)\s+([\d.-]+)',
            r'.*?\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\[INFO\]\[G:(\d+)\]\[U:(\d+)\]:/减少跑时\s+(\S+)\s+([\d.-]+)',
            # 无群组信息的格式
            r'.*?\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\[INFO\]\[U:(\d+)\]:/增加推时\s+(\S+)\s+([\d.-]+)',
            r'.*?\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\[INFO\]\[U:(\d+)\]:/减少推时\s+(\S+)\s+([\d.-]+)',
            r'.*?\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\[INFO\]\[U:(\d+)\]:/增加跑时\s+(\S+)\s+([\d.-]+)',
            r'.*?\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\[INFO\]\[U:(\d+)\]:/减少跑时\s+(\S+)\s+([\d.-]+)'
        ]
        
        # 操作类型映射
        self.operation_types = {
            '增加推时': 'add_push_time',
            '减少推时': 'reduce_push_time', 
            '增加跑时': 'add_pushed_time',
            '减少跑时': 'reduce_pushed_time'
        }
        
        print(f"[INFO] 推车时长每日统计服务初始化完成，启用状态：{self.enabled}")
    
    def start_service(self):
        """
        启动服务（在事件循环运行后调用）
        """
        if self.enabled and not self._is_running:
            self._start_scheduler()
            # 启动时清理过期日志
            if self.auto_save_log and self.log_retention_days > 0:
                self._cleanup_old_logs()
    
    def _cleanup_old_logs(self):
        """
        清理过期的统计日志文件
        """
        try:
            log_dir = os.path.join("data", "log", "push_time")
            if not os.path.exists(log_dir):
                return
            
            cutoff_date = datetime.now() - timedelta(days=self.log_retention_days)
            
            for filename in os.listdir(log_dir):
                if filename.startswith("push_time_statistics_") and filename.endswith(".log"):
                    file_path = os.path.join(log_dir, filename)
                    try:
                        # 获取文件修改时间
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_mtime < cutoff_date:
                            os.remove(file_path)
                            print(f"[INFO] 已删除过期日志文件: {filename}")
                    except Exception as e:
                        print(f"[ERROR] 删除日志文件 {filename} 失败: {e}")
            
        except Exception as e:
            print(f"[ERROR] 清理过期日志失败: {e}")
    
    def get_service_status(self) -> Dict[str, Any]:
        """
        获取服务状态信息
        
        Returns:
            Dict[str, Any]: 服务状态信息
        """
        return {
            'service_name': '推车时长每日统计服务',
            'enabled': self.enabled,
            'running': self._is_running,
            'schedule_time': self.schedule_time,
            'log_file_path': self.log_file_path,
            'auto_save_log': self.auto_save_log,
            'log_retention_days': self.log_retention_days,
            'enable_manual_trigger': self.enable_manual_trigger,
            'notification_enabled': self.notification_enabled,
            'statistics_format': self.statistics_format,
            'group_mappings': self.group_mappings,
            'timezone': self.timezone
        }
    
    def _start_scheduler(self):
        """
        启动定时任务调度器
        """
        if not self._is_running:
            self._is_running = True
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())
            print(f"[INFO] 推车时长每日统计服务调度器已启动，计划执行时间：{self.schedule_time}")
    
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
                print(f"[ERROR] 推车时长每日统计调度器异常: {e}")
                await asyncio.sleep(60)  # 出错后等待1分钟再重试
    
    def _get_next_execution_time(self, current_time: datetime) -> datetime:
        """
        计算下次执行时间
        
        Args:
            current_time: 当前时间
            
        Returns:
            下次执行时间
        """
        try:
            # 解析执行时间
            hour, minute = map(int, self.schedule_time.split(':'))
            
            # 计算今天的执行时间
            today_execution = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # 如果今天的执行时间已过，则计算明天的执行时间
            if current_time >= today_execution:
                next_execution = today_execution + timedelta(days=1)
            else:
                next_execution = today_execution
                
            return next_execution
            
        except Exception as e:
            print(f"[ERROR] 计算下次执行时间失败: {e}")
            # 默认返回明天的0:01
            tomorrow = current_time + timedelta(days=1)
            return tomorrow.replace(hour=0, minute=1, second=0, microsecond=0)
    
    async def _execute_daily_statistics(self):
        """
        执行每日统计任务
        """
        try:
            print(f"[INFO] 开始执行推车时长每日统计任务")
            
            # 获取昨日日期
            yesterday = datetime.now() - timedelta(days=1)
            date_str = yesterday.strftime('%Y-%m-%d')
            
            # 解析日志文件，获取昨日的手动推时操作记录
            operations = await self._parse_log_file(date_str)
            
            if not operations:
                print(f"[INFO] {date_str} 没有找到手动推时操作记录")
                return
            
            # 按群组分组统计
            grouped_operations = self._group_operations_by_group(operations)
            
            # 为每个群组保存日志文件
            for group_id, group_operations in grouped_operations.items():
                try:
                    # 保存统计日志到文件
                    await self._save_statistics_log(group_id, group_operations, date_str)
                    
                    print(f"[INFO] 已保存群组 {group_id} 的推车时长统计日志，包含 {len(group_operations)} 条操作记录")
                    
                except Exception as e:
                    print(f"[ERROR] 保存群组 {group_id} 的统计日志失败: {e}")
            
            print(f"[INFO] 推车时长每日统计任务完成，共处理 {len(operations)} 条操作记录")
            
        except Exception as e:
            print(f"[ERROR] 执行推车时长每日统计任务失败: {e}")
    
    async def _parse_log_file(self, target_date: str) -> List[Dict[str, Any]]:
        """
        解析日志文件，提取指定日期的手动推时操作记录
        
        Args:
            target_date: 目标日期 (YYYY-MM-DD格式)
            
        Returns:
            操作记录列表
        """
        operations = []
        
        try:
            if not os.path.exists(self.log_file_path):
                print(f"[WARNING] 日志文件不存在: {self.log_file_path}")
                return operations
            
            print(f"[INFO] 开始解析日志文件: {self.log_file_path}，目标日期: {target_date}")
            
            with open(self.log_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    # 更精确的日期检查：确保只匹配指定日期的记录
                    # 检查日志行是否包含目标日期，并且是在时间戳位置
                    if target_date not in line:
                        continue
                    
                    # 尝试匹配各种手动推时操作模式
                    for i, pattern in enumerate(self.push_time_patterns):
                        match = re.search(pattern, line)
                        if match:
                            groups = match.groups()
                            
                            # 提取时间戳并验证日期
                            timestamp_str = groups[0]
                            operation_date = timestamp_str.split(' ')[0]  # 提取日期部分 YYYY-MM-DD
                            
                            # 严格验证：只处理目标日期的记录
                            if operation_date != target_date:
                                continue
                            
                            # 根据模式索引确定操作类型
                            operation_types = ['add_push_time', 'reduce_push_time', 'add_pushed_time', 'reduce_pushed_time',
                                             'add_push_time', 'reduce_push_time', 'add_pushed_time', 'reduce_pushed_time']
                            operation_type = operation_types[i % 4]  # 因为有8个模式，但只有4种操作类型
                            
                            # 处理不同格式的匹配结果
                            if len(groups) == 5:  # 带群组信息的格式
                                timestamp_str, group_id, user_id, cn, hours = groups
                            elif len(groups) == 4:  # 无群组信息的格式
                                timestamp_str, user_id, cn, hours = groups
                                group_id = None
                            else:
                                print(f"[WARNING] 未知的匹配格式，跳过行 {line_num}: {line}")
                                continue
                            
                            operation = {
                                'timestamp': timestamp_str,
                                'group_id': group_id,
                                'user_id': user_id,
                                'cn': cn,
                                'hours': float(hours),
                                'operation_type': operation_type,
                                'line_number': line_num,
                                'raw_line': line,
                                'original_log': line,
                                'date': operation_date  # 添加日期字段便于验证
                            }
                            operations.append(operation)
                            print(f"[DEBUG] 匹配到{target_date}的操作记录: {operation_type} - {cn} - {hours}小时")
                            break
            
            print(f"[INFO] 日志解析完成，找到 {target_date} 的 {len(operations)} 条手动推时操作记录")
            return operations
            
        except Exception as e:
            print(f"[ERROR] 解析日志文件失败: {e}")
            return operations
    
    def _group_operations_by_group(self, operations: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        按群组ID分组操作记录
        
        Args:
            operations: 操作记录列表
            
        Returns:
            按群组分组的操作记录
        """
        grouped = {}
        
        for operation in operations:
            group_id = operation['group_id']
            if group_id not in grouped:
                grouped[group_id] = []
            grouped[group_id].append(operation)
        
        # 按时间戳排序每个群组的操作记录
        for group_id in grouped:
            grouped[group_id].sort(key=lambda x: x['timestamp'])
        
        return grouped
    
    def _format_statistics_message(self, operations: List[Dict[str, Any]], group_id: str, date_str: str) -> str:
        """
        格式化统计消息
        
        Args:
            operations: 操作记录列表
            group_id: 群组ID
            date_str: 日期字符串
            
        Returns:
            格式化后的消息
        """
        try:
            # 消息头部
            message = f"📊 推车时长操作统计\n"
            message += f"📅 日期：{date_str}\n"
            message += f"👥 群组：{group_id}\n"
            message += f"📝 操作记录：{len(operations)} 条\n\n"
            
            # 按操作类型分类统计
            type_stats = {}
            for operation in operations:
                op_type = operation['operation_type']
                if op_type not in type_stats:
                    type_stats[op_type] = []
                type_stats[op_type].append(operation)
            
            # 操作类型中文名称映射
            type_names = {
                'add_push_time': '➕ 增加推时',
                'reduce_push_time': '➖ 减少推时',
                'add_pushed_time': '➕ 增加跑时',
                'reduce_pushed_time': '➖ 减少跑时'
            }
            
            # 按类型显示操作记录
            for op_type, type_operations in type_stats.items():
                type_name = type_names.get(op_type, op_type)
                message += f"{type_name} ({len(type_operations)} 条):\n"
                
                for operation in type_operations:
                    time_part = operation['timestamp'].split(' ')[1].split(',')[0]  # 提取时间部分
                    message += f"  🕐 {time_part} | {operation['cn']} | {operation['hours']}小时\n"
                
                message += "\n"
            
            # 统计汇总
            message += "📈 操作汇总:\n"
            total_add_push = sum(op['hours'] for op in operations if op['operation_type'] == 'add_push_time')
            total_reduce_push = sum(op['hours'] for op in operations if op['operation_type'] == 'reduce_push_time')
            total_add_pushed = sum(op['hours'] for op in operations if op['operation_type'] == 'add_pushed_time')
            total_reduce_pushed = sum(op['hours'] for op in operations if op['operation_type'] == 'reduce_pushed_time')
            
            if total_add_push > 0:
                message += f"  ➕ 总增加推时：{total_add_push:.1f}小时\n"
            if total_reduce_push > 0:
                message += f"  ➖ 总减少推时：{total_reduce_push:.1f}小时\n"
            if total_add_pushed > 0:
                message += f"  ➕ 总增加跑时：{total_add_pushed:.1f}小时\n"
            if total_reduce_pushed > 0:
                message += f"  ➖ 总减少跑时：{total_reduce_pushed:.1f}小时\n"
            
            net_push_change = total_add_push - total_reduce_push
            net_pushed_change = total_add_pushed - total_reduce_pushed
            
            message += f"\n💡 净变化:\n"
            message += f"  推时净变化：{net_push_change:+.1f}小时\n"
            message += f"  跑时净变化：{net_pushed_change:+.1f}小时\n"
            
            return message
            
        except Exception as e:
            print(f"[ERROR] 格式化统计消息失败: {e}")
            return f"❌ 格式化统计消息失败: {str(e)}"
    
    async def _save_statistics_log(self, group_id: str, operations: List[Dict[str, Any]], date_str: str):
        """
        保存统计日志到文件
        
        Args:
            group_id: 群组ID
            operations: 操作记录列表
            date_str: 日期字符串
        """
        try:
            # 检查是否启用自动保存日志
            if not self.auto_save_log:
                print(f"[INFO] 自动保存日志已禁用，跳过保存群组 {group_id} 的统计日志")
                return
            
            # 确保日志目录存在
            log_dir = os.path.join("data", "log", "push_time")
            os.makedirs(log_dir, exist_ok=True)
            
            # 构造日志文件名
            log_filename = f"push_time_statistics_{group_id}_{date_str}.log"
            log_filepath = os.path.join(log_dir, log_filename)
            
            # 写入日志文件
            with open(log_filepath, 'w', encoding='utf-8') as f:
                f.write(f"# 推车时长操作统计日志\n")
                f.write(f"# 日期: {date_str}\n")
                f.write(f"# 群组: {group_id}\n")
                f.write(f"# 记录数量: {len(operations)}\n")
                f.write(f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# 统计格式: {self.statistics_format}\n\n")
                
                # 根据统计格式输出不同详细程度的信息
                if self.statistics_format == 'detailed':
                    for operation in operations:
                        # 详细格式：直接复制原始日志内容
                        f.write(f"{operation['original_log']}\n")
                elif self.statistics_format == 'summary':
                    # 摘要格式：只输出关键信息
                    for operation in operations:
                        f.write(f"{operation['timestamp']} | {operation['cn']} | {operation['operation_type']} | {operation['hours']}小时\n")
                else:
                    # 默认格式
                    for operation in operations:
                        f.write(f"{operation['original_log']}\n")
            
            print(f"[INFO] 已保存统计日志到文件: {log_filepath}")
            
        except Exception as e:
            print(f"[ERROR] 保存统计日志失败: {e}")
            raise
    
    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理消息
        
        Args:
            message: 消息内容
            user_id: 用户ID
            **kwargs: 其他参数，包含context等
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果
        """
        # 检查是否启用手动触发功能
        if not self.enable_manual_trigger:
            return None
        
        # 从kwargs中获取group_id
        group_id = kwargs.get('group_id')
        if not group_id:
            # 尝试从context中获取
            context = kwargs.get('context', {})
            group_id = context.get('group_id')
            if not group_id:
                return None
            
        # 手动触发统计
        if message.strip() == '/推时统计':
            await self._execute_manual_statistics(group_id)
            return {
                'type': 'text',
                'content': '✅ 已手动触发推车时长统计任务'
            }
        
        # 查看服务状态
        elif message.strip() == '/推时统计状态':
            status = self.get_service_status()
            status_text = f"""📊 推车时长每日统计服务状态

🔧 服务配置：
• 服务状态：{'🟢 已启用' if status['enabled'] else '🔴 已禁用'}
• 运行状态：{'🟢 运行中' if status['running'] else '🔴 未运行'}
• 执行时间：{status['schedule_time']}
• 时区：{status['timezone']}

📁 日志配置：
• 日志文件：{status['log_file_path']}
• 自动保存：{'🟢 已启用' if status['auto_save_log'] else '🔴 已禁用'}
• 保留天数：{status['log_retention_days']} 天
• 统计格式：{status['statistics_format']}

🎛️ 功能开关：
• 手动触发：{'🟢 已启用' if status['enable_manual_trigger'] else '🔴 已禁用'}
• 消息通知：{'🟢 已启用' if status['notification_enabled'] else '🔴 已禁用'}

📋 群组映射：{len(status['group_mappings'])} 个群组"""
            
            return {
                'type': 'text',
                'content': status_text
            }
        
        return None
    
    async def _execute_manual_statistics(self, source_group: str):
        """
        执行手动统计任务
        
        Args:
            source_group: 触发统计的群组ID
        """
        try:
            print(f"[INFO] 手动触发推车时长统计，来源群组: {source_group}")
            
            # 获取昨日日期
            yesterday = datetime.now() - timedelta(days=1)
            date_str = yesterday.strftime('%Y-%m-%d')
            
            # 解析日志文件
            operations = await self._parse_log_file(date_str)
            
            if not operations:
                print(f"[INFO] {date_str} 暂无手动推时操作记录")
                return
            
            # 按群组分组统计
            grouped_operations = self._group_operations_by_group(operations)
            
            # 为每个群组保存日志文件
            for group_id, group_operations in grouped_operations.items():
                try:
                    await self._save_statistics_log(group_id, group_operations, date_str)
                    print(f"[INFO] 手动统计已保存群组 {group_id} 的日志，包含 {len(group_operations)} 条记录")
                except Exception as e:
                    print(f"[ERROR] 手动统计保存群组 {group_id} 日志失败: {e}")
            
        except Exception as e:
            print(f"[ERROR] 执行手动推车时长统计失败: {e}")
    
    def stop_scheduler(self):
        """
        停止定时任务调度器
        """
        if self._is_running:
            self._is_running = False
            if self._scheduler_task:
                self._scheduler_task.cancel()
            print(f"[INFO] 推车时长每日统计服务调度器已停止")
    
    def get_help_text(self) -> str:
        """
        获取帮助文本
        
        Returns:
            str: 帮助文本
        """
        help_text = """📊 推车时长每日统计服务

🔧 功能说明：
• 每日自动统计昨日的手动推时操作记录
• 从日志文件中提取增减推车时长和跑时的操作
• 支持多群组统计和日志保存
• 可配置的统计格式和日志保留策略

📋 支持的指令："""
        
        if self.enable_manual_trigger:
            help_text += """
• /推时统计 - 手动触发昨日推时统计
• /推时统计状态 - 查看服务配置和运行状态"""
        else:
            help_text += """
• 手动触发功能已禁用"""
        
        help_text += f"""

⏰ 自动执行：
• 执行时间：每日 {self.schedule_time}
• 时区：{self.timezone}
• 服务状态：{'🟢 已启用' if self.enabled else '🔴 已禁用'}

📁 日志配置：
• 自动保存：{'🟢 已启用' if self.auto_save_log else '🔴 已禁用'}
• 保留天数：{self.log_retention_days} 天
• 统计格式：{self.statistics_format}

📝 统计内容：
• 增加推车时长操作
• 减少推车时长操作  
• 增加被推时长操作
• 减少被推时长操作

💡 提示：统计结果会自动保存到 data/log/push_time/ 目录下"""
        
        return help_text