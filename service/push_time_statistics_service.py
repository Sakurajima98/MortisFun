#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推车时长统计服务
 
该服务负责和管理统计用户的推车时长、被推时长（跑者时长）以及净推车时长。
主要功能包括：
1. 自动记录车队活动中的推车和跑者时长
2. 手动增减时长管理
3. 时长统计查询
4. CN（用户名）管理和查重
5. 管理员权限控制的结算功能
6. 变更日志记录和管理

作者: Assistant
创建时间: 2025
"""

import os
import csv
import json
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from .base_service import BaseService
from utils.image_generator import ImageGenerator

# 尝试导入推时计算器管理器
try:
    from ..utils.push_time_calculator import PushTimeCalculatorManager
except ImportError:
    # 如果相对导入失败，尝试绝对导入
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from utils.push_time_calculator import PushTimeCalculatorManager


class PushTimeStatisticsService(BaseService):
    """
    推车时长统计服务类
     
    管理用户的推车时长、被推时长和净推车时长统计，
    支持自动记录、手动管理、查询和结算功能，
    以及完整的变更日志记录系统。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, message_sender=None, server=None):
        """
        初始化推车时长统计服务
        
        Args:
            config: 配置字典
            data_manager: 数据管理器
            text_formatter: 文本格式化器
            message_sender: 消息发送器（可选）
            server: 服务器实例（可选）
        """
        super().__init__(config, data_manager, text_formatter)
        self.message_sender = message_sender
        self.server = server
        
        # 从配置文件读取管理员用户ID列表和服务配置
        push_time_config = config.get('services', {}).get('push_time_statistics', {})
        self.admin_list = push_time_config.get('admin_users', [])
        
        # 推车时长统计服务配置
        self.data_directory = push_time_config.get('data_directory', 'data/tuiche')
        self.auto_backup = push_time_config.get('auto_backup', True)
        self.backup_interval_hours = push_time_config.get('backup_interval_hours', 24)
        
        # 变更日志配置
        self.change_log_directory = push_time_config.get('change_log_directory', 'data/push_time/web_push_time_log')
        self.max_log_file_size = push_time_config.get('max_log_file_size', 50 * 1024 * 1024)  # 50MB
        self.max_retry_attempts = push_time_config.get('max_retry_attempts', 3)
        self.retry_delay = push_time_config.get('retry_delay', 0.1)  # 100ms
        
        # 线程锁用于并发控制
        self._change_log_locks = {}
        self._lock_creation_lock = threading.Lock()
        
        # 初始化推时计算器管理器
        self.calculator_manager = PushTimeCalculatorManager()
        
        # 图片生成器（用于推时记录图片输出）
        try:
            self.image_generator = ImageGenerator()
        except Exception:
            self.image_generator = None
        
        # 确保数据目录存在
        import os
        try:
            if not os.path.exists(self.data_directory):
                os.makedirs(self.data_directory)
                self.log_unified("INFO", f"创建推车时长统计数据目录: {self.data_directory}", group_id="system", user_id="system")
            else:
                self.log_unified("DEBUG", f"推车时长统计数据目录已存在: {self.data_directory}", group_id="system", user_id="system")
                
            # 确保变更日志目录存在
            if not os.path.exists(self.change_log_directory):
                os.makedirs(self.change_log_directory)
                self.log_unified("INFO", f"创建变更日志目录: {self.change_log_directory}", group_id="system", user_id="system")
                
        except Exception as e:
            self.log_unified("ERROR", f"创建目录失败: {e}", group_id="system", user_id="system")
        
        self.log_unified("INFO", "推车时长统计服务初始化完成 - 使用CSV表格直接存储模式，支持变更日志记录", group_id="system", user_id="system")
    
    def _get_csv_file_path(self, group_id: str) -> str:
        """
        获取指定群组的CSV文件路径
         
        Args:
            group_id: 群组ID
             
        Returns:
            CSV文件的完整路径
        """
        filename = f"push_time_statistics_{group_id}.csv"
        return os.path.join(self.data_directory, filename)
    
    def _read_csv_data(self, group_id: str) -> Dict[str, Dict[str, Any]]:
        """
        从CSV文件读取推车时长统计数据
         
        Args:
            group_id: 群组ID
             
        Returns:
            统计数据字典，格式为：
            {
                "cn_name": {
                    "qq_number": str,        # QQ号
                    "push_time": float,      # 推车时长（小时）
                    "pushed_time": float,    # 被推时长/跑者时长（小时）
                    "net_push_time": float   # 净推车时长（小时）
                }
            }
        """
        csv_file = self._get_csv_file_path(group_id)
        data = {}
        
        try:
            # 检查原始文件是否存在，如果不存在则尝试查找测试文件
            if not os.path.exists(csv_file):
                test_csv_path = os.path.join(self.data_directory, "push_time_statistics_test_csv_group.csv")
                if os.path.exists(test_csv_path):
                    csv_file = test_csv_path
                    # 获取当前时间戳
                    self.log_unified("DEBUG", f"使用测试CSV文件: {test_csv_path}", group_id, "system")
            
            if os.path.exists(csv_file):
                with open(csv_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        cn = row.get('CN名称', '').strip()
                        if cn:
                            # 向后兼容：检查是否有QQ号列
                            qq_number = row.get('QQ号', '').strip()
                            if not qq_number:
                                qq_number = "未记录"
                            
                            data[cn] = {
                                'qq_number': qq_number,
                                'push_time': float(row.get('推车时长(小时)', 0)),
                                'pushed_time': float(row.get('被推时长(小时)', 0)),
                                'net_push_time': float(row.get('净推车时长(小时)', 0))
                            }
                self.log_unified("DEBUG", f"从CSV文件读取群组 {group_id} 的数据，包含 {len(data)} 条记录", group_id, "system")
            else:
                self.log_unified("DEBUG", f"群组 {group_id} 的CSV文件不存在，返回空数据", group_id, "system")
        except Exception as e:
            self.log_unified("ERROR", f"读取群组 {group_id} 的CSV数据失败: {e}", group_id, "system")
        
        return data
    
    def _write_csv_data(self, group_id: str, data: Dict[str, Dict[str, Any]]) -> None:
        """
        将推车时长统计数据写入CSV文件
        
        Args:
            group_id: 群组ID
            data: 要写入的数据字典
        """
        csv_file = self._get_csv_file_path(group_id)
        
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(csv_file), exist_ok=True)
            
            with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                
                # 写入表头（包含QQ号字段）
                writer.writerow(['CN名称', 'QQ号', '推车时长(小时)', '被推时长(小时)', '净推车时长(小时)'])
                
                # 按净推车时长降序排序并写入数据
                sorted_data = sorted(data.items(), 
                                   key=lambda x: x[1].get('net_push_time', 0), 
                                   reverse=True)
                
                for cn, user_data in sorted_data:
                    writer.writerow([
                        cn,
                        user_data.get('qq_number', '未记录'),
                        f"{user_data.get('push_time', 0):.3f}",
                        f"{user_data.get('pushed_time', 0):.3f}",
                        f"{user_data.get('net_push_time', 0):.3f}"
                    ])
            
            self.log_unified("DEBUG", f"成功写入群组 {group_id} 的CSV数据，包含 {len(data)} 条记录", group_id=group_id, user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"写入群组 {group_id} 的CSV数据失败: {e}", group_id=group_id, user_id="system")
    
    def _get_change_log_lock(self, group_id: str) -> threading.Lock:
        """
        获取指定群组的变更日志文件锁
        
        Args:
            group_id: 群组ID
            
        Returns:
            线程锁对象
        """
        with self._lock_creation_lock:
            if group_id not in self._change_log_locks:
                self._change_log_locks[group_id] = threading.Lock()
            return self._change_log_locks[group_id]
    
    def _get_change_log_file_path(self, group_id: str) -> str:
        """
        获取指定群组的变更日志文件路径
        
        Args:
            group_id: 群组ID
            
        Returns:
            变更日志文件的完整路径
        """
        filename = f"push_time_{group_id}_change_logs.json"
        return os.path.join(self.change_log_directory, filename)
    
    def _generate_change_id(self) -> str:
        """
        生成唯一的变更ID
         
        Returns:
            格式为 "chg_" + 时间戳 + 随机数的变更ID
        """
        import random
        timestamp = hex(int(time.time()))[2:]  # 去掉0x前缀
        random_part = f"{random.random():.8f}"[2:]  # 去掉0.前缀
        return f"chg_{timestamp}{random_part}"
    
    def _create_change_log_entry(self, group_id: str, change_type: str, operator_cn: str, 
                                operator_qq: str, influence_cn: List[str], influence_qq: List[str],
                                content: Dict[str, Any], reason: str, 
                                event_description: str = None) -> Dict[str, Any]:
        """
        创建变更日志条目
         
        Args:
            group_id: 群组ID
            change_type: 变更类型（如"推时变动"）
            operator_cn: 操作人员CN
            operator_qq: 操作人员QQ
            influence_cn: 受影响用户CN列表
            influence_qq: 受影响用户QQ列表
            content: 变更内容详情
            reason: 变更原因
            event_description: 事件描述（可选）
            
        Returns:
            变更日志条目字典
        """
        change_entry = {
            "id": self._generate_change_id(),
            "change_type": change_type,
            "change_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operator_cn": operator_cn,
            "operator_qq": operator_qq,
            "group_id": group_id,
            "influence_cn": influence_cn,
            "influence_qq": influence_qq,
            "content": content,
            "reason": reason
        }
         
        if event_description:
            change_entry["event_description"] = event_description
            
        return change_entry
    
    def _read_change_logs(self, group_id: str) -> List[Dict[str, Any]]:
        """
        读取变更日志文件
         
        Args:
            group_id: 群组ID
             
        Returns:
            变更日志列表
        """
        log_file = self._get_change_log_file_path(group_id)
         
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return []
        except Exception as e:
            self.log_unified("ERROR", f"读取变更日志失败: {e}", group_id, "system")
            return []
    
    def _write_change_logs(self, group_id: str, logs: List[Dict[str, Any]]) -> bool:
        """
        写入变更日志文件（原子性操作）
         
        Args:
            group_id: 群组ID
            logs: 变更日志列表
             
        Returns:
            是否写入成功
        """
        log_file = self._get_change_log_file_path(group_id)
        temp_file = f"{log_file}.tmp"
         
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
             
            # 写入临时文件
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, ensure_ascii=False, indent=4)
             
            # 原子性替换
            if os.name == 'nt':  # Windows
                if os.path.exists(log_file):
                    os.remove(log_file)
                os.rename(temp_file, log_file)
            else:  # Unix/Linux
                os.rename(temp_file, log_file)
             
            return True
            
        except Exception as e:
            self.log_unified("ERROR", f"写入变更日志失败: {e}", group_id, "system")
            # 清理临时文件
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except:
                pass
            return False
    
    def _check_and_rotate_log_file(self, group_id: str) -> None:
        """
        检查并轮转日志文件（如果文件过大）
         
        Args:
            group_id: 群组ID
        """
        log_file = self._get_change_log_file_path(group_id)
         
        try:
            if os.path.exists(log_file) and os.path.getsize(log_file) > self.max_log_file_size:
                # 创建备份文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_file = f"{log_file}.{timestamp}.bak"
                 
                # 移动当前文件到备份
                os.rename(log_file, backup_file)
                 
                self.log_unified("INFO", f"日志文件轮转: {log_file} -> {backup_file}", group_id, "system")
                 
        except Exception as e:
            self.log_unified("ERROR", f"日志文件轮转失败: {e}", group_id, "system")
    
    def _record_change_log_with_retry(self, group_id: str, change_entry: Dict[str, Any]) -> bool:
        """
        带重试机制的变更日志记录
         
        Args:
            group_id: 群组ID
            change_entry: 变更日志条目
             
        Returns:
            是否记录成功
        """
        lock = self._get_change_log_lock(group_id)
         
        for attempt in range(self.max_retry_attempts):
            try:
                with lock:
                    # 检查并轮转日志文件
                    self._check_and_rotate_log_file(group_id)
                     
                    # 读取现有日志
                    logs = self._read_change_logs(group_id)
                     
                    # 添加新的变更记录到开头（保持时间倒序）
                    logs.insert(0, change_entry)
                     
                    # 写入日志文件
                    if self._write_change_logs(group_id, logs):
                        self.log_unified("DEBUG", f"变更日志记录成功: {change_entry['id']}", group_id, "system")
                        return True
                    else:
                        raise Exception("写入日志文件失败")
                         
            except Exception as e:
                self.log_unified("WARNING", f"变更日志记录失败 (尝试 {attempt + 1}/{self.max_retry_attempts}): {e}", group_id, "system")
                 
                if attempt < self.max_retry_attempts - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))  # 指数退避
                else:
                    self.log_unified("ERROR", f"变更日志记录最终失败: {e}", group_id, "system")
                    return False
         
        return False
    
    def _record_push_time_change(self, group_id: str, cn: str, qq_number: str, 
                                operation_type: str, before_data: Dict[str, float], 
                                after_data: Dict[str, float], operator_cn: str = "系统", 
                                operator_qq: str = "system", reason: str = "手动调整") -> bool:
        """
        记录推时变动日志
         
        Args:
            group_id: 群组ID
            cn: 用户CN
            qq_number: 用户QQ号
            operation_type: 操作类型（如"推车操作"、"共跑操作"等）
            before_data: 变更前数据
            after_data: 变更后数据
            operator_cn: 操作人员CN
            operator_qq: 操作人员QQ
            reason: 变更原因
            
        Returns:
            是否记录成功
        """
        try:
            # 计算变化量
            delta = {
                "push": after_data["push_time"] - before_data["push_time"],
                "pushed": after_data["pushed_time"] - before_data["pushed_time"],
                "net": after_data["net_push_time"] - before_data["net_push_time"]
            }
            
            # 创建变更内容
            content = {
                "operation_label": operation_type,
                "before": {
                    "push_time": before_data["push_time"],
                    "pushed_time": before_data["pushed_time"],
                    "net_push_time": before_data["net_push_time"]
                },
                "after": {
                    "push_time": after_data["push_time"],
                    "pushed_time": after_data["pushed_time"],
                    "net_push_time": after_data["net_push_time"]
                },
                "delta": delta
            }
            
            # 生成事件描述
            event_description = f"{operator_cn}({operator_qq})执行了{operation_type}，影响对象为{cn}"
            
            # 创建变更日志条目
            change_entry = self._create_change_log_entry(
                group_id=group_id,
                change_type="推时变动",
                operator_cn=operator_cn,
                operator_qq=operator_qq,
                influence_cn=[cn],
                influence_qq=[qq_number],
                content=content,
                reason=reason,
                event_description=event_description
            )
            
            # 记录变更日志
            return self._record_change_log_with_retry(group_id, change_entry)
            
        except Exception as e:
            self.log_unified("ERROR", f"记录推时变动日志失败: {e}", group_id, "system")
            return False
    
    def get_recent_change_logs(self, group_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取最近的变更记录
         
        Args:
            group_id: 群组ID
            limit: 返回记录数量限制
             
        Returns:
            最近的变更记录列表
        """
        try:
            logs = self._read_change_logs(group_id)
            return logs[:limit]  # 由于日志是按时间倒序存储的，直接取前N条
        except Exception as e:
            self.log_unified("ERROR", f"获取最近变更记录失败: {e}", group_id, "system")
            return []
    
    def _get_cn_by_qq(self, group_id: str, qq_number: str) -> Optional[str]:
        """
        根据QQ号反查CN（基于CSV数据）
        """
        try:
            data = self._read_csv_data(group_id)
            for cn, ud in data.items():
                if str(ud.get("qq_number", "")).strip() == str(qq_number).strip():
                    return cn
            return None
        except Exception:
            return None
    
    def _extract_first_at_qq(self, context: Dict[str, Any]) -> Optional[str]:
        """
        从原始OneBot事件中提取首个@的QQ号
        """
        try:
            msg = context.get('message')
            if isinstance(msg, list):
                for seg in msg:
                    if seg.get('type') == 'at':
                        qq = str(seg.get('data', {}).get('qq') or '').strip()
                        if qq and qq != 'all':
                            return qq
        except Exception:
            pass
        return None
    
    def _read_user_push_log(self, group_id: str, cn: str) -> List[Dict[str, Any]]:
        """
        读取用户推时行为日志（JSON数组）
        """
        try:
            filepath = self._push_log_file_path(group_id, cn)
            if not os.path.exists(filepath):
                return []
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f) or []
        except Exception:
            return []
    
    def _beautify_push_log_lines(self, records: List[Dict[str, Any]], limit: Optional[int] = 10) -> List[str]:
        """
        将推时行为记录格式化为美观的文本行
        """
        if limit is not None and limit > 0:
            records = records[:limit]
        lines: List[str] = []
        for r in records:
            time = r.get("time", "")
            typ = r.get("type", "")
            target = r.get("target", "")
            detail = r.get("detail", "")
            reason = r.get("reason")
            base = f"{time} - {typ} - {target} - {detail}"
            if reason and str(reason).strip():
                base = f"{base} - {str(reason).strip()}"
            lines.append(base)
        return lines if lines else ["暂无记录"]
    
    def _generate_push_log_image(self, group_id: str, cn: str, title: str, lines: List[str]) -> Optional[str]:
        """
        生成推时记录图片（自适应大小）
        """
        try:
            if not self.image_generator:
                return None
            from PIL import Image, ImageDraw
            margin = 40
            header_h = 80
            row_h = 42
            max_len = max([len(x) for x in lines] + [len(title)])
            est_width = max(600, margin * 2 + int(max_len * 14))
            est_height = margin * 2 + header_h + row_h * len(lines)
            img = Image.new('RGB', (est_width, est_height), '#FFFFFF')
            draw = ImageDraw.Draw(img)
            draw.rectangle((0, 0, est_width, header_h), fill='#4A90E2')
            draw.text((margin, header_h/2 - 10), title, fill='#FFFFFF', font=self.image_generator.fonts.get('title'))
            y = header_h + margin/2
            for line in lines:
                draw.text((margin, y), line, fill='#333333', font=self.image_generator.fonts.get('content'))
                y += row_h
            out_dir = os.path.join('data', 'push_log', str(group_id), 'images')
            os.makedirs(out_dir, exist_ok=True)
            filename = f"{cn}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            out_path = os.path.join(out_dir, filename)
            img.save(out_path, format='PNG')
            return out_path
        except Exception as e:
            self.log_unified("ERROR", f"生成推时记录图片失败: {e}", group_id, "system")
            return None
    
    # ------------------------------
    # 推时变更用户/群组级别行为日志（JSON）
    # 存储路径：data/push_log/{group_id}/{cn}.json
    # ------------------------------
    def _normalize_cn(self, cn: str) -> str:
        """
        归一化CN（用户名）文本
        
        规则：
        - 删除前导空格（保留尾部空格与内部格式）
        """
        try:
            return (cn or "").lstrip()
        except Exception:
            return cn
    
    def _ensure_push_log_dir(self, group_id: str) -> str:
        """
        确保指定群组的推时行为日志目录存在
        
        Returns:
            该群组日志目录绝对或相对路径
        """
        base_dir = os.path.join("data", "push_log", str(group_id))
        try:
            os.makedirs(base_dir, exist_ok=True)
        except Exception as e:
            self.log_unified("ERROR", f"创建推时日志目录失败: {e}", group_id, "system")
        return base_dir
    
    def _push_log_file_path(self, group_id: str, cn: str) -> str:
        """
        获取用户/群组推时日志文件路径
        """
        safe_cn = self._normalize_cn(cn)
        dir_path = self._ensure_push_log_dir(group_id)
        return os.path.join(dir_path, f"{safe_cn}.json")
    
    def _format_time_range_display(self, time_text: Optional[str]) -> str:
        """
        规范化时间段显示：将 '16-19' 转为 '16:00-19:00'
        若已为 'HH:MM-HH:MM' 则保持不变
        """
        s = (time_text or "").strip()
        if not s:
            return ""
        try:
            if ":" in s:
                # 已包含分钟信息，直接返回
                return s
            # 形如 '16-19'
            parts = s.split('-', 1)
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                return f"{int(parts[0]):02d}:00-{int(parts[1]):02d}:00"
            return s
        except Exception:
            return s
    
    def _format_oper_target(self, date_text: Optional[str], time_text: Optional[str],
                            timestamp_ranges: Optional[List[Tuple[float, float]]] = None) -> str:
        """
        生成操作对象时间展示文本（示例：[2025-12-27]{16:00-19:00}）
        优先使用传入的date_text/time_text；若缺失则根据时间戳范围推断第一个时间段的日期与时间。
        """
        date_s = (date_text or "").strip()
        time_s = self._format_time_range_display(time_text)
        if not date_s and timestamp_ranges:
            try:
                # 使用第一个时间段推断日期与时间
                start_ts, end_ts = timestamp_ranges[0]
                dt_start = datetime.fromtimestamp(start_ts)
                dt_end = datetime.fromtimestamp(end_ts)
                date_s = dt_start.strftime("%Y-%m-%d")
                time_s = f"{dt_start.strftime('%H:%M')}-{dt_end.strftime('%H:%M')}"
            except Exception:
                pass
        if not date_s and not time_s:
            return "[—]"
        if date_s and time_s:
            return f"[{date_s}]{{{time_s}}}"
        if date_s:
            return f"[{date_s}]"
        return f"{{{time_s}}}"
    
    def _append_user_push_log(self, group_id: str, cn: str, operation_type: str,
                              target_text: str, detail_text: str, reason: Optional[str] = None) -> None:
        """
        追加一条用户推时行为日志到 data/push_log/{group}/{CN}.json
        
        存储格式（JSON数组，每条元素结构如下）：
        {
            "time": "YYYY-MM-DD HH:MM:SS,mmm",
            "type": "操作类型",
            "target": "[操作对象时间]",
            "detail": "详细变更信息",
            "reason": "变更原因"  // 可选
        }
        """
        try:
            filepath = self._push_log_file_path(group_id, cn)
            # 读取现有日志
            entries: List[Dict[str, Any]] = []
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    try:
                        entries = json.load(f) or []
                    except Exception:
                        entries = []
            # 构造新条目
            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            entry = {
                "time": now_text,
                "type": operation_type,
                "target": target_text,
                "detail": detail_text
            }
            if reason is not None and str(reason).strip():
                entry["reason"] = str(reason).strip()
            # 添加到头部（时间倒序）
            entries.insert(0, entry)
            # 原子写入
            temp_file = f"{filepath}.tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(entries, f, ensure_ascii=False, indent=2)
            if os.path.exists(filepath) and os.name == "nt":
                os.remove(filepath)
            os.rename(temp_file, filepath)
        except Exception as e:
            self.log_unified("ERROR", f"写入推时行为日志失败: {e}", group_id, "system")
    
    # ------------------------------
    # 业务方法：用户存在校验与CN上传
    # ------------------------------
    def _ensure_user_exists(self, group_id: str, cn: str, qq_number: str = "未记录") -> None:
        """
        确保用户在CSV文件中存在，如果不存在则创建，如果存在但QQ号为"未记录"则更新QQ号
         
        Args:
            group_id: 群组ID
            cn: 用户CN（用户名）
            qq_number: 用户QQ号，默认为"未记录"
        """
        data = self._read_csv_data(group_id)
        data_changed = False
        
        if cn not in data:
            # 用户不存在，创建新用户
            data[cn] = {
                "qq_number": qq_number,
                "push_time": 0.0,
                "pushed_time": 0.0,
                "net_push_time": 0.0
            }
            data_changed = True
            self.log_unified("DEBUG", f"在群组 {group_id} 中创建用户 {cn} 的初始数据，QQ号: {qq_number}", group_id, "system")
        elif data[cn].get("qq_number", "未记录") == "未记录" and qq_number != "未记录":
            # 用户存在但QQ号为"未记录"，且提供了有效的QQ号，则更新QQ号
            data[cn]["qq_number"] = qq_number
            data_changed = True
            self.log_unified("DEBUG", f"在群组 {group_id} 中更新用户 {cn} 的QQ号: {qq_number}", group_id, "system")
        
        if data_changed:
            self._write_csv_data(group_id, data)
    
    def upload_cn(self, cn: str, group_id: str, qq_number: str = None, operator_cn: str = "系统", operator_qq: str = "system") -> str:
        """
        上传CN（用户名）到统计系统
         
        Args:
            cn: 用户CN（用户名）
            group_id: 群组ID
            qq_number: 用户QQ号（可选）
            operator_cn: 操作人员CN
            operator_qq: 操作人员QQ
            
        Returns:
            操作结果消息
        """
        try:
            # 从CSV文件读取当前数据
            data = self._read_csv_data(group_id)
            
            # 检查CN是否已存在（查重）
            if cn in data:
                return f"❌ CN '{cn}' 已存在，请更换一个CN"
            
            # 如果没有提供QQ号，使用默认值
            if qq_number is None:
                qq_number = "未记录"
            
            # 添加新的CN
            data[cn] = {
                "qq_number": qq_number,
                "push_time": 0.0,
                "pushed_time": 0.0,
                "net_push_time": 0.0
            }
            
            # 写入CSV文件
            self._write_csv_data(group_id, data)
            
            # 记录变更日志
            self._record_push_time_change(
                group_id=group_id,
                cn=cn,
                qq_number=qq_number,
                operation_type="新增用户",
                before_data={
                    "push_time": 0.0,
                    "pushed_time": 0.0,
                    "net_push_time": 0.0
                },
                after_data={
                    "push_time": 0.0,
                    "pushed_time": 0.0,
                    "net_push_time": 0.0
                },
                operator_cn=operator_cn,
                operator_qq=operator_qq,
                reason=f"新增用户{cn}，QQ号：{qq_number}"
            )
            
            self.log_unified("INFO", f"成功上传CN: {cn} 到群组 {group_id}，QQ号: {qq_number}", group_id, "system")
            return f"✅ 成功上传CN '{cn}'，QQ号: {qq_number}，初始推车时长、被推时长、净推车时长均为0小时"
            
        except Exception as e:
            self.log_unified("ERROR", f"上传CN失败: {e}", group_id, "system")
            return f"❌ 上传CN失败: {str(e)}"
    
    def add_push_time(self, cn: str, hours: float, group_id: str, operator_cn: str = "系统", operator_qq: str = "system", reason: str = "") -> str:
        """
        增加推车时长
         
        Args:
            cn: 用户CN
            hours: 要增加的小时数
            group_id: 群组ID
            operator_cn: 操作人员CN
            operator_qq: 操作人员QQ
            reason: 变更原因（可选）
            
        Returns:
            操作结果消息
        """
        try:
            self.log_unified("DEBUG", f"开始为用户 {cn} 增加推车时长 {hours}小时", group_id, "system")
            
            # 从CSV文件读取当前数据
            data = self._read_csv_data(group_id)
            
            # 确保用户存在
            if cn not in data:
                self.log_unified("DEBUG", f"用户 {cn} 不存在，创建新用户记录", group_id, "system")
                data[cn] = {
                    "qq_number": "未记录",
                    "push_time": 0.0,
                    "pushed_time": 0.0,
                    "net_push_time": 0.0
                }
            
            # 记录变更前的数据
            before_data = {
                "push_time": data[cn]["push_time"],
                "pushed_time": data[cn]["pushed_time"],
                "net_push_time": data[cn]["net_push_time"]
            }
            
            old_time = data[cn]["push_time"]
            data[cn]["push_time"] += hours
            new_time = data[cn]["push_time"]
            
            self.log_unified("DEBUG", f"用户 {cn} 推车时长变化: {old_time:.3f}h -> {new_time:.3f}h", group_id, "system")
            
            # 重新计算净推车时长 - 使用推时计算器管理器的统一方法
            old_net_time = data[cn]["net_push_time"]
            data[cn]["net_push_time"] = self.calculator_manager.calculate_net_push_time(
                group_id, data[cn]["push_time"], data[cn]["pushed_time"]
            )
            reduction = self._calculate_reduction(data[cn]["pushed_time"], group_id)
            
            self.log_unified("DEBUG", f"用户 {cn} 净推车时长重新计算: {old_net_time:.3f}h -> {data[cn]['net_push_time']:.3f}h (减免: {reduction:.3f}h)", group_id, "system")
            
            # 记录变更后的数据
            after_data = {
                "push_time": data[cn]["push_time"],
                "pushed_time": data[cn]["pushed_time"],
                "net_push_time": data[cn]["net_push_time"]
            }
            
            # 写入CSV文件
            self._write_csv_data(group_id, data)
            
            # 记录变更日志
            self._record_push_time_change(
                group_id=group_id,
                cn=cn,
                qq_number=data[cn].get("qq_number", "未记录"),
                operation_type="增加推车时长",
                before_data=before_data,
                after_data=after_data,
                operator_cn=operator_cn,
                operator_qq=operator_qq,
                reason=reason or f"手动增加推车时长{hours}小时"
            )
            # 行为日志（用户/群组）
            target = "[—]"
            detail = f"手动增加推车时长{hours:.3f}小时，净推车时长更新为{data[cn]['net_push_time']:.3f}小时。"
            self._append_user_push_log(group_id, cn, "手动增加推时", target, detail, reason)
            
            self.log_unified("INFO", f"为用户 {cn} 增加推车时长 {hours}小时", group_id, "system")
            return f"✅ 成功为 '{cn}' 增加推车时长 {hours}小时\n" \
                   f"推车时长: {old_time:.3f}小时 → {new_time:.3f}小时\n" \
                   f"净推车时长: {data[cn]['net_push_time']:.3f}小时"
            
        except Exception as e:
            self.log_unified("ERROR", f"增加推车时长失败: {e}", group_id, "system")
            return f"❌ 增加推车时长失败: {str(e)}"
    
    def reduce_push_time(self, cn: str, hours: float, group_id: str, operator_cn: str = "系统", operator_qq: str = "system", reason: str = "") -> str:
        """
        减少推车时长
         
        Args:
            cn: 用户CN
            hours: 要减少的小时数
            group_id: 群组ID
            operator_cn: 操作人员CN
            operator_qq: 操作人员QQ
            reason: 变更原因（可选）
            
        Returns:
            操作结果消息
        """
        try:
            self.log_unified("DEBUG", f"开始为用户 {cn} 减少推车时长 {hours}小时", group_id, "system")
            
            # 从CSV文件读取当前数据
            data = self._read_csv_data(group_id)
            
            # 确保用户存在
            if cn not in data:
                self.log_unified("DEBUG", f"用户 {cn} 不存在，创建新用户记录", group_id, "system")
                data[cn] = {
                    "qq_number": "未记录",
                    "push_time": 0.0,
                    "pushed_time": 0.0,
                    "net_push_time": 0.0
                }
            
            # 记录变更前的数据
            before_data = {
                "push_time": data[cn]["push_time"],
                "pushed_time": data[cn]["pushed_time"],
                "net_push_time": data[cn]["net_push_time"]
            }
            
            old_time = data[cn]["push_time"]
            data[cn]["push_time"] = old_time - hours
            new_time = data[cn]["push_time"]
            
            self.log_unified("DEBUG", f"用户 {cn} 推车时长变化: {old_time:.3f}h -> {new_time:.3f}h", group_id, "system")
            
            # 重新计算净推车时长 - 使用推时计算器管理器的统一方法
            old_net_time = data[cn]["net_push_time"]
            data[cn]["net_push_time"] = self.calculator_manager.calculate_net_push_time(
                group_id, data[cn]["push_time"], data[cn]["pushed_time"]
            )
            reduction = self._calculate_reduction(data[cn]["pushed_time"], group_id)
            
            self.log_unified("DEBUG", f"用户 {cn} 净推车时长重新计算: {old_net_time:.3f}h -> {data[cn]['net_push_time']:.3f}h (减免: {reduction:.3f}h)", group_id, "system")
            
            # 记录变更后的数据
            after_data = {
                "push_time": data[cn]["push_time"],
                "pushed_time": data[cn]["pushed_time"],
                "net_push_time": data[cn]["net_push_time"]
            }
            
            # 写入CSV文件
            self._write_csv_data(group_id, data)
            
            # 记录变更日志
            self._record_push_time_change(
                group_id=group_id,
                cn=cn,
                qq_number=data[cn].get("qq_number", "未记录"),
                operation_type="减少推车时长",
                before_data=before_data,
                after_data=after_data,
                operator_cn=operator_cn,
                operator_qq=operator_qq,
                reason=reason or f"手动减少推车时长{hours}小时"
            )
            # 行为日志
            target = "[—]"
            detail = f"手动减少推车时长{hours:.3f}小时，净推车时长更新为{data[cn]['net_push_time']:.3f}小时。"
            self._append_user_push_log(group_id, cn, "手动减少推时", target, detail, reason)
            
            self.log_unified("INFO", f"为用户 {cn} 减少推车时长 {hours}小时", group_id, "system")
            return f"✅ 成功为 '{cn}' 减少推车时长 {hours}小时\n" \
                   f"推车时长: {old_time:.3f}小时 → {new_time:.3f}小时\n" \
                   f"净推车时长: {data[cn]['net_push_time']:.3f}小时"
            
        except Exception as e:
            self.log_unified("ERROR", f"减少推车时长失败: {e}", group_id, "system")
            return f"❌ 减少推车时长失败: {str(e)}"
    
    def add_pushed_time(self, cn: str, hours: float, group_id: str, operator_cn: str = "系统", operator_qq: str = "system", reason: str = "") -> str:
        """
        增加被推时长（跑者时长）
         
        Args:
            cn: 用户CN
            hours: 要增加的小时数
            group_id: 群组ID
            operator_cn: 操作人员CN
            operator_qq: 操作人员QQ
            reason: 变更原因（可选）
            
        Returns:
            操作结果消息
        """
        try:
            self.log_unified("DEBUG", f"开始为用户 {cn} 增加被推时长 {hours}小时", group_id, "system")
            
            # 从CSV文件读取当前数据
            data = self._read_csv_data(group_id)
            
            # 确保用户存在
            if cn not in data:
                self.log_unified("DEBUG", f"用户 {cn} 不存在，创建新用户记录", group_id, "system")
                data[cn] = {
                    "qq_number": "未记录",
                    "push_time": 0.0,
                    "pushed_time": 0.0,
                    "net_push_time": 0.0
                }
            
            # 记录变更前的数据
            before_data = {
                "push_time": data[cn]["push_time"],
                "pushed_time": data[cn]["pushed_time"],
                "net_push_time": data[cn]["net_push_time"]
            }
            
            old_time = data[cn]["pushed_time"]
            data[cn]["pushed_time"] += hours
            new_time = data[cn]["pushed_time"]
            
            self.log_unified("DEBUG", f"用户 {cn} 被推时长变化: {old_time:.3f}h -> {new_time:.3f}h", group_id, "system")
            
            # 重新计算净推车时长 - 使用推时计算器管理器的统一方法
            old_reduction = self._calculate_reduction(old_time, group_id)
            reduction = self._calculate_reduction(data[cn]["pushed_time"], group_id)
            old_net_time = data[cn]["net_push_time"]
            data[cn]["net_push_time"] = self.calculator_manager.calculate_net_push_time(
                group_id, data[cn]["push_time"], data[cn]["pushed_time"]
            )
            
            self.log_unified("DEBUG", f"用户 {cn} 减免时长变化: {old_reduction:.3f}h -> {reduction:.3f}h, 净推车时长: {old_net_time:.3f}h -> {data[cn]['net_push_time']:.3f}h", group_id, "system")
            
            # 记录变更后的数据
            after_data = {
                "push_time": data[cn]["push_time"],
                "pushed_time": data[cn]["pushed_time"],
                "net_push_time": data[cn]["net_push_time"]
            }
            
            # 写入CSV文件
            self._write_csv_data(group_id, data)
            
            # 记录变更日志
            self._record_push_time_change(
                group_id=group_id,
                cn=cn,
                qq_number=data[cn].get("qq_number", "未记录"),
                operation_type="增加被推时长",
                before_data=before_data,
                after_data=after_data,
                operator_cn=operator_cn,
                operator_qq=operator_qq,
                reason=reason or f"手动增加被推时长{hours}小时"
            )
            # 行为日志
            target = "[—]"
            detail = f"手动增加跑者时长{hours:.3f}小时，净推车时长更新为{data[cn]['net_push_time']:.3f}小时。"
            self._append_user_push_log(group_id, cn, "手动增加跑时", target, detail, reason)
            
            self.log_unified("INFO", f"为用户 {cn} 增加被推时长 {hours}小时", group_id, "system")
            return f"✅ 成功为 '{cn}' 增加被推时长 {hours}小时\n" \
                   f"被推时长: {old_time:.3f}小时 → {new_time:.3f}小时\n" \
                   f"净推车时长: {data[cn]['net_push_time']:.3f}小时"
            
        except Exception as e:
            self.log_unified("ERROR", f"增加被推时长失败: {e}", group_id, "system")
            return f"❌ 增加被推时长失败: {str(e)}"
    
    def reduce_pushed_time(self, cn: str, hours: float, group_id: str, operator_cn: str = "系统", operator_qq: str = "system", reason: str = "") -> str:
        """
        减少被推时长（跑者时长）
         
        Args:
            cn: 用户CN
            hours: 要减少的小时数
            group_id: 群组ID
            operator_cn: 操作人员CN
            operator_qq: 操作人员QQ
            reason: 变更原因（可选）
            
        Returns:
            操作结果消息
        """
        try:
            self.log_unified("DEBUG", f"开始为用户 {cn} 减少被推时长 {hours}小时", group_id, "system")
            
            # 从CSV文件读取当前数据
            data = self._read_csv_data(group_id)
            
            # 确保用户存在
            if cn not in data:
                self.log_unified("DEBUG", f"用户 {cn} 不存在，创建新用户记录", group_id, "system")
                data[cn] = {
                    "qq_number": "未记录",
                    "push_time": 0.0,
                    "pushed_time": 0.0,
                    "net_push_time": 0.0
                }
            
            # 记录变更前的数据
            before_data = {
                "push_time": data[cn]["push_time"],
                "pushed_time": data[cn]["pushed_time"],
                "net_push_time": data[cn]["net_push_time"]
            }
            
            old_time = data[cn]["pushed_time"]
            data[cn]["pushed_time"] = old_time - hours
            new_time = data[cn]["pushed_time"]
            
            self.log_unified("DEBUG", f"用户 {cn} 被推时长变化: {old_time:.3f}h -> {new_time:.3f}h", group_id, "system")
            
            # 重新计算净推车时长 - 使用推时计算器管理器的统一方法
            old_reduction = self._calculate_reduction(old_time, group_id)
            reduction = self._calculate_reduction(data[cn]["pushed_time"], group_id)
            old_net_time = data[cn]["net_push_time"]
            data[cn]["net_push_time"] = self.calculator_manager.calculate_net_push_time(
                group_id, data[cn]["push_time"], data[cn]["pushed_time"]
            )
            
            self.log_unified("DEBUG", f"用户 {cn} 减免时长变化: {old_reduction:.3f}h -> {reduction:.3f}h, 净推车时长: {old_net_time:.3f}h -> {data[cn]['net_push_time']:.3f}h", group_id, "system")
            
            # 记录变更后的数据
            after_data = {
                "push_time": data[cn]["push_time"],
                "pushed_time": data[cn]["pushed_time"],
                "net_push_time": data[cn]["net_push_time"]
            }
            
            # 写入CSV文件
            self._write_csv_data(group_id, data)
            
            # 记录变更日志
            self._record_push_time_change(
                group_id=group_id,
                cn=cn,
                qq_number=data[cn].get("qq_number", "未记录"),
                operation_type="减少被推时长",
                before_data=before_data,
                after_data=after_data,
                operator_cn=operator_cn,
                operator_qq=operator_qq,
                reason=reason or f"手动减少被推时长{hours}小时"
            )
            # 行为日志
            target = "[—]"
            detail = f"手动减少跑者时长{hours:.3f}小时，净推车时长更新为{data[cn]['net_push_time']:.3f}小时。"
            self._append_user_push_log(group_id, cn, "手动减少跑时", target, detail, reason)
            
            self.log_unified("INFO", f"为用户 {cn} 减少被推时长 {hours}小时", group_id, "system")
            return f"✅ 成功为 '{cn}' 减少被推时长 {hours}小时\n" \
                   f"被推时长: {old_time:.3f}小时 → {new_time:.3f}小时\n" \
                   f"净推车时长: {data[cn]['net_push_time']:.3f}小时"
            
        except Exception as e:
            self.log_unified("ERROR", f"减少被推时长失败: {e}", group_id, "system")
            return f"❌ 减少被推时长失败: {str(e)}"
    
    def query_statistics(self, cn: str, group_id: str) -> str:
        """
        查询用户的推车时长统计
        
        Args:
            cn: 用户CN
            group_id: 群组ID
            
        Returns:
            统计信息
        """
        try:
            # 从CSV文件读取当前数据
            data = self._read_csv_data(group_id)
            
            # 如果用户不存在，创建初始数据
            if cn not in data:
                data[cn] = {
                    "push_time": 0.0,
                    "pushed_time": 0.0,
                    "net_push_time": 0.0
                }
                # 写入CSV文件以保存新用户
                self._write_csv_data(group_id, data)
            
            user_data = data[cn]
            push_time = user_data["push_time"]
            pushed_time = user_data["pushed_time"]
            net_push_time = user_data["net_push_time"]
            
            return f"📊 '{cn}' 的推车时长统计\n" \
                   f"🚗 推车时长: {push_time:.3f}小时\n" \
                   f"🏃 被推时长: {pushed_time:.3f}小时\n" \
                   f"💎 净推车时长: {net_push_time:.3f}小时"
            
        except Exception as e:
            self.log_unified("ERROR", f"查询推车时长统计失败: {e}", group_id, "system")
            return f"❌ 查询推车时长统计失败: {str(e)}"
    
    def _calculate_reduction(self, pushed_time: float, group_id: str) -> float:
        """
        根据被推时长和群组ID计算需要减少的推车时长
         
        Args:
            pushed_time: 被推时长（小时）
            group_id: 群组ID，用于选择计算规则
             
        Returns:
            需要减少的推车时长（小时）
        """
        # 使用推时计算器管理器获取对应群组的计算器
        calculator = self.calculator_manager.get_calculator(group_id)
        return calculator.calculate_reduction(pushed_time)
    
    def recalculate_net_push_time(self, group_id: str) -> str:
        """
        重新计算指定群组所有用户的净推车时长
         
        使用推时计算器管理器根据群组ID获取对应的计算器，
        重新计算所有用户的净推车时长并更新到CSV文件
         
        Args:
            group_id: 群组ID
             
        Returns:
            操作结果消息
        """
        try:
            self.log_unified("INFO", f"开始重新计算群组 {group_id} 的净推车时长", group_id, "system")
            
            # 详细调试：显示计算器管理器的配置
            self.log_unified("DEBUG", f"计算器管理器配置: {self.calculator_manager._calculators}", group_id, "system")
            
            # 调试：显示工厂类的配置映射
            from utils.push_time_calculator import PushTimeCalculatorFactory
            self.log_unified("DEBUG", f"工厂类配置映射: {PushTimeCalculatorFactory.GROUP_CALCULATOR_MAPPING}", group_id, "system")
            
            # 调试：检查群组ID是否在映射中
            self.log_unified("DEBUG", f"群组 {group_id} 是否在映射中: {group_id in PushTimeCalculatorFactory.GROUP_CALCULATOR_MAPPING}", group_id, "system")
            if group_id in PushTimeCalculatorFactory.GROUP_CALCULATOR_MAPPING:
                self.log_unified("DEBUG", f"映射中的计算器类: {PushTimeCalculatorFactory.GROUP_CALCULATOR_MAPPING[group_id]}", group_id, "system")
            
            # 调试：清空计算器管理器缓存并重新获取
            self.log_unified("DEBUG", "清空计算器管理器缓存", group_id, "system")
            self.calculator_manager.clear_cache(group_id)
            
            # 调试：重新获取计算器
            calculator_after_clear = self.calculator_manager.get_calculator(group_id)
            self.log_unified("DEBUG", f"清空缓存后的计算器类型: {type(calculator_after_clear).__name__}", group_id, "system")
            
            # 调试：工厂直接创建计算器的测试
            factory_calculator = PushTimeCalculatorFactory.create_calculator(group_id)
            self.log_unified("DEBUG", f"工厂直接创建的计算器类型: {type(factory_calculator).__name__}", group_id, "system")
            
            # 从CSV文件读取当前数据
            data = self._read_csv_data(group_id)
            
            if not data:
                return f"❌ 群组 {group_id} 暂无推时统计数据"
            
            # 获取对应群组的推时计算器
            calculator = self.calculator_manager.get_calculator(group_id)
            calculator_name = calculator.__class__.__name__
            
            self.log_unified("INFO", f"群组 {group_id} 使用计算器: {calculator_name}", group_id, "system")
            self.log_unified("DEBUG", f"计算器详细信息: {calculator}", group_id, "system")
            self.log_unified("DEBUG", f"计算器类型: {type(calculator)}", group_id, "system")
            
            # 测试计算器工厂直接创建
            direct_calculator = PushTimeCalculatorFactory.create_calculator(group_id)
            self.log_unified("DEBUG", f"工厂直接创建的计算器类型: {type(direct_calculator).__name__}", group_id, "system")
            
            # 统计变化
            processed_users = 0
            changed_users = 0
            total_old_net_time = 0.0
            total_new_net_time = 0.0
            
            # 遍历所有用户重新计算净推车时长
            for cn, user_data in data.items():
                processed_users += 1
                old_net_time = user_data.get("net_push_time", 0.0)
                push_time = user_data.get("push_time", 0.0)
                pushed_time = user_data.get("pushed_time", 0.0)
                
                # 详细调试：显示每个用户的计算过程
                self.log_unified("DEBUG", f"用户 {cn} 原始数据 - 推时: {push_time:.3f}h, 跑时: {pushed_time:.3f}h, 原净推时: {old_net_time:.3f}h", group_id, "system")
                
                # 使用计算器重新计算净推车时长
                new_net_time = calculator.calculate_net_push_time(push_time, pushed_time)
                
                # 详细调试：显示计算结果
                self.log_unified("DEBUG", f"用户 {cn} 重新计算结果 - 新净推时: {new_net_time:.3f}h", group_id, "system")
                
                # 更新数据
                user_data["net_push_time"] = new_net_time
                
                # 统计变化
                total_old_net_time += old_net_time
                total_new_net_time += new_net_time
                
                if abs(old_net_time - new_net_time) > 0.001:  # 有显著变化
                    changed_users += 1
                    self.log_unified("INFO", f"用户 {cn} 净推车时长已更新: {old_net_time:.3f}h -> {new_net_time:.3f}h (推时: {push_time:.3f}h, 跑时: {pushed_time:.3f}h)", group_id, "system")
                else:
                    self.log_unified("DEBUG", f"用户 {cn} 净推车时长无变化: {old_net_time:.3f}h", group_id, "system")
            
            # 写入更新后的数据
            self._write_csv_data(group_id, data)
            
            # 记录完成信息
            self.log_unified("INFO", f"群组 {group_id} 净推车时长重新计算完成 - 使用计算器: {calculator_name}, 处理用户: {processed_users}, 发生变化: {changed_users}, 总净推时变化: {total_old_net_time:.3f}h -> {total_new_net_time:.3f}h", group_id, "system")
            
            return f"✅ 净推车时长重新计算完成\n" \
                   f"📊 使用计算器: {calculator_name}\n" \
                   f"👥 处理用户: {processed_users}\n" \
                   f"🔄 发生变化: {changed_users}\n" \
                   f"📈 总净推时变化: {total_old_net_time:.3f}h → {total_new_net_time:.3f}h"
            
        except Exception as e:
            self.log_unified("ERROR", f"重新计算净推车时长失败: {e}", group_id, "system")
            import traceback
            self.log_unified("ERROR", f"错误堆栈: {traceback.format_exc()}", group_id, "system")
            return f"❌ 重新计算净推车时长失败: {str(e)}"
    
    def export_statistics_table(self, group_id: str, export_format: str = 'csv') -> str:
        """
        导出推车时长统计表格（返回主文件路径）
         
        Args:
            group_id: 群组ID
            export_format: 导出格式，支持 'csv'
             
        Returns:
            导出结果消息
        """
        try:
            # 从CSV文件读取群组数据
            group_data = self._read_csv_data(group_id)
            if not group_data:
                return "❌ 该群组暂无推车时长统计数据"
            
            # 获取主文件路径
            filepath = self._get_csv_file_path(group_id)
            
            # 检查文件是否存在
            if not os.path.exists(filepath):
                return f"❌ 群组数据文件不存在: {filepath}"
            
            # 导出CSV格式（实际上就是返回主文件信息）
            if export_format.lower() == 'csv':
                self.log_unified("INFO", f"推车时长统计表格路径: {filepath}", group_id, "system")
                return f"✅ 推车时长统计表格\n📁 文件路径: {filepath}\n📊 包含 {len(group_data)} 条记录"
            
            else:
                return f"❌ 不支持的导出格式: {export_format}"
                
        except Exception as e:
            self.log_unified("ERROR", f"获取推车时长统计表格失败: {e}", group_id, "system")
            return f"❌ 获取失败: {str(e)}"
    
    def query_all_push_time(self, user_id: str, group_id: str) -> Dict[str, Any]:
        """
        查询对应群聊的所有推时信息并生成文件（仅管理员可用）
        
        Args:
            user_id: 执行操作的用户ID
            group_id: 群组ID
            
        Returns:
            包含文件发送信息的字典或错误消息
        """
        try:
            # 检查管理员权限
            if user_id not in self.admin_list:
                return {
                    'type': 'text',
                    'content': "❌ 权限不足，只有管理员可以执行推时查询"
                }
            
            # 从CSV文件读取数据
            data = self._read_csv_data(group_id)
            
            if not data:
                return {
                    'type': 'text',
                    'content': "❌ 该群组暂无推车时长统计数据"
                }
            
            # 获取原始CSV文件路径
            original_csv_path = self._get_csv_file_path(group_id)
            
            # 检查原始文件是否存在，如果不存在则尝试查找测试文件
            if not os.path.exists(original_csv_path):
                # 尝试查找测试CSV文件
                test_csv_path = os.path.join(self.data_directory, "push_time_statistics_test_csv_group.csv")
                if os.path.exists(test_csv_path):
                    original_csv_path = test_csv_path
                    self.log_unified("INFO", f"使用测试CSV文件: {test_csv_path}", group_id="system", user_id="system")
                else:
                    return {
                        'type': 'text',
                        'content': "❌ 推车时长统计数据文件不存在"
                    }
            
            # 获取当前时间戳
            self.log_unified("INFO", "管理员查询所有推时信息", group_id, user_id)
            
            # 直接返回原始CSV文件
            file_name = f"push_time_all_{group_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            return {
                'type': 'file',
                'file_path': original_csv_path,
                'file_name': file_name,
                'description': f"📊 群组 {group_id} 的所有推时信息\n📁 包含 {len(data)} 条记录\n⏰ 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }
            
        except Exception as e:
            self.log_unified("ERROR", f"查询所有推时信息失败: {e}", group_id, user_id)
            return {
                'type': 'text',
                'content': f"❌ 查询所有推时信息失败: {str(e)}"
            }
    
    def settlement(self, user_id: str, group_id: str) -> str:
        """
        执行推时结算（仅管理员可用）
         
        Args:
            user_id: 执行操作的用户ID
            group_id: 群组ID
             
        Returns:
            结算结果消息
        """
        try:
            self.log_unified("INFO", f"管理员 {user_id} 开始执行推时结算", group_id, "system")
            
            # 检查管理员权限
            if user_id not in self.admin_list:
                self.log_unified("WARNING", f"用户 {user_id} 尝试执行推时结算但权限不足", group_id, "system")
                return "❌ 权限不足，只有管理员可以执行推时结算"
            
            # 从CSV文件读取数据
            data = self._read_csv_data(group_id)
            
            if not data:
                self.log_unified("WARNING", f"群组 {group_id} 暂无推车时长统计数据，结算取消", group_id, "system")
                return "❌ 该群组暂无推车时长统计数据"
            
            self.log_unified("DEBUG", f"开始处理 {len(data)} 个用户的结算数据", group_id, "system")
            settlement_results = []
            
            for cn, user_data in data.items():
                old_push_time = user_data["push_time"]
                pushed_time = user_data["pushed_time"]
                old_net_push_time = user_data["net_push_time"]
                
                # 计算需要减少的推车时长（折算跑者时长）
                reduction = self._calculate_reduction(pushed_time, group_id)
                
                # 计算新的净推车时长 - 使用推时计算器管理器的统一方法
                new_net_push_time = self.calculator_manager.calculate_net_push_time(
                    group_id, old_push_time, pushed_time
                )
                
                self.log_unified("DEBUG", f"用户 {cn} 结算前: 推车{old_push_time:.3f}h, 被推{pushed_time:.3f}h, 净推{old_net_push_time:.3f}h, 减免: {reduction:.3f}h", group_id, "system")
                
                # 更新数据：
                # 1. 跑者时长清零
                # 2. 净推车时长 = 推车时长 - 折算跑者时长
                # 3. 推车时长替换为净推车时长
                user_data["pushed_time"] = 0.0
                user_data["net_push_time"] = new_net_push_time
                user_data["push_time"] = new_net_push_time
                
                self.log_unified("DEBUG", f"用户 {cn} 结算后: 推车{new_net_push_time:.3f}h, 被推0.0h, 净推{new_net_push_time:.3f}h", group_id, "system")
                
                settlement_results.append(
                    f"{cn}: {old_push_time:.3f}h → {new_net_push_time:.3f}h (减少{reduction:.3f}h)"
                )
            
            # 写入CSV文件
            self._write_csv_data(group_id, data)
            
            # 记录结算完成
            self.log_unified("INFO", f"推时结算完成，共处理 {len(data)} 个用户，管理员: {user_id}", group_id, "system")
            self.log_unified("INFO", "管理员执行了推时结算", group_id, user_id)
            
            return "✅ 推时结算完成，请各位使用/推时统计 cn查看自己的推时信息。"
            
        except Exception as e:
            self.log_unified("ERROR", f"推时结算失败: {e}", group_id, user_id)
            return f"❌ 推时结算失败: {str(e)}"
    
    def base_settlement(self, user_id: str, group_id: str) -> str:
        """
        执行底标结算（仅管理员可用）
         
        Args:
            user_id: 执行操作的用户ID
            group_id: 群组ID
             
        Returns:
            结算结果消息
        """
        try:
            self.log_unified("INFO", f"管理员 {user_id} 开始执行底标结算", group_id, "system")
            
            # 检查管理员权限
            if user_id not in self.admin_list:
                self.log_unified("WARNING", f"用户 {user_id} 尝试执行底标结算但权限不足", group_id, "system")
                return "❌ 权限不足，只有管理员可以执行底标结算"
            
            # 从CSV文件读取数据
            data = self._read_csv_data(group_id)
            
            if not data:
                return "❌ 该群组暂无推车时长统计数据"
            
            settlement_results = []
            
            for cn, user_data in data.items():
                # 记录变更前数据
                old_push_time = user_data.get("push_time", 0.0)
                old_pushed_time = user_data.get("pushed_time", 0.0)
                old_net_push_time = user_data.get("net_push_time", 0.0)
                
                # 底标结算：净推车时长减3小时（允许负数）
                new_net_push_time = old_net_push_time - 3.0
                
                # 更新数据
                user_data["push_time"] = new_net_push_time
                user_data["net_push_time"] = new_net_push_time
                
                # 记录变更后数据
                after_data = {
                    "push_time": user_data["push_time"],
                    "pushed_time": old_pushed_time,
                    "net_push_time": user_data["net_push_time"]
                }
                before_data = {
                    "push_time": old_push_time,
                    "pushed_time": old_pushed_time,
                    "net_push_time": old_net_push_time
                }
                
                # 写入变更日志（类型：底标减少）
                try:
                    qq_number = str(user_data.get("qq_number", "未记录"))
                    self._record_push_time_change(
                        group_id=group_id,
                        cn=cn,
                        qq_number=qq_number,
                        operation_type="底标减少",
                        before_data=before_data,
                        after_data=after_data,
                        operator_cn="管理员",
                        operator_qq=str(user_id),
                        reason="底标结算减少3小时"
                    )
                except Exception:
                    pass
                
                # 写入用户行为日志（类型：底标减少）
                try:
                    detail = (
                        f"底标结算：净推车时长减少3.000小时，"
                        f"更新为{new_net_push_time:.3f}小时。"
                    )
                    self._append_user_push_log(
                        group_id=group_id,
                        cn=cn,
                        operation_type="底标减少",
                        target_text="[—]",
                        detail_text=detail,
                        reason="底标结算减少3小时"
                    )
                except Exception:
                    pass
                
                settlement_results.append(
                    f"{cn}: {old_net_push_time:.3f}h → {new_net_push_time:.3f}h"
                )
            
            # 写入CSV文件
            self._write_csv_data(group_id, data)
            
            self.log_unified("INFO", f"管理员 {user_id} 执行了底标结算 (群组: {group_id})", group_id, user_id)
            
            return "✅ 底标结算完成，请各位使用/推时统计 cn查看自己的推时信息。"
            
        except Exception as e:
            self.log_unified("ERROR", f"底标结算失败: {e}", group_id, user_id)
            return f"❌ 底标结算失败: {str(e)}"
    
    def query_negative_push_time(self, user_id: str, group_id: str) -> Dict[str, Any]:
        """
        查询所有推时为负数的CN并生成表格文件（仅管理员可用）
        
        Args:
            user_id: 执行操作的用户ID
            group_id: 群组ID
            
        Returns:
            包含文件发送信息的字典或错误消息
        """
        try:
            # 检查管理员权限
            if user_id not in self.admin_list:
                return {
                    'type': 'text',
                    'content': "❌ 权限不足，只有管理员可以执行推时负数查询"
                }
            
            # 从CSV文件读取数据
            data = self._read_csv_data(group_id)
            
            if not data:
                return {
                    'type': 'text',
                    'content': "❌ 该群组暂无推车时长统计数据"
                }
            
            # 筛选出推时为负数的用户数据
            negative_users = []
            for cn, user_data in data.items():
                net_push_time = user_data["net_push_time"]
                if net_push_time < 0:
                    negative_users.append({
                        'CN': cn,
                        '推车时长(h)': user_data["push_time"],
                        '跑者时长(h)': user_data["pushed_time"],
                        '净推车时长(h)': net_push_time
                    })
            
            if not negative_users:
                return {
                    'type': 'text',
                    'content': "✅ 当前没有推时为负数的用户"
                }
            
            # 生成CSV文件
            import pandas as pd
            from datetime import datetime
            
            # 创建DataFrame
            df = pd.DataFrame(negative_users)
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"negative_push_time_{group_id}_{timestamp}.csv"
            
            # 确保目录存在
            tuiche_dir = Path(self.data_directory) / "tuiche"
            tuiche_dir.mkdir(parents=True, exist_ok=True)
            
            # 保存文件
            file_path = tuiche_dir / filename
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            self.log_unified("INFO", f"管理员 {user_id} 执行了推时负数查询，生成文件: {file_path} (群组: {group_id})", group_id, user_id)
            
            # 返回文件发送格式
            return {
                'type': 'file',
                'file_path': str(file_path),
                'file_name': filename,
                'description': f"📊 推时负数查询结果（共{len(negative_users)}人）"
            }
            
        except Exception as e:
            self.log_unified("ERROR", f"推时负数查询失败: {e}", group_id, "system")
            return {
                'type': 'text',
                'content': f"❌ 推时负数查询失败: {str(e)}"
            }
    
    def _calculate_time_duration(self, timestamp_ranges: List[Tuple[float, float]]) -> float:
        """
        计算时间段的总时长（小时）
         
        Args:
            timestamp_ranges: 时间戳范围列表
             
        Returns:
            总时长（小时），支持分钟级别精度
        """
        total_seconds = 0
        for start_ts, end_ts in timestamp_ranges:
            total_seconds += (end_ts - start_ts)
        # 转换为小时并保留3位小数以支持分钟级别精度（1分钟 = 0.017小时）
        return round(total_seconds / 3600, 3)
    
    async def auto_record_team_creation(self, captain: str, captain_user_id: str, group_id: str, timestamp_ranges: List[Tuple[float, float]],
                                        date_display: Optional[str] = None, time_range: Optional[str] = None) -> None:
        """
        自动记录车队创建时的时长
         
        Args:
            captain: 队长名称
            captain_user_id: 队长用户ID
            group_id: 群组ID
            timestamp_ranges: 时间戳范围列表
            date_display: 用于展示的日期文本（如 12.27 或 08.03-08.07）
            time_range: 用于展示的时间文本（如 16-19 或 13:00-15:00）
        """
        try:
            if not captain or not timestamp_ranges:
                return
            
            # 队长的跑者时长 = 整个班车的时长
            duration = self._calculate_time_duration(timestamp_ranges)
            
            if duration > 0:
                # 确保用户存在（传入QQ号）
                self._ensure_user_exists(group_id, captain, captain_user_id)
                
                # 从CSV读取数据
                data = self._read_csv_data(group_id)
                
                # 记录变更前的数据
                before_data = {
                    "push_time": data[captain]["push_time"],
                    "pushed_time": data[captain]["pushed_time"],
                    "net_push_time": data[captain]["net_push_time"]
                }
                
                # 更新数据 - 使用推时计算器管理器的统一方法
                data[captain]["pushed_time"] += duration
                data[captain]["net_push_time"] = self.calculator_manager.calculate_net_push_time(
                    group_id, data[captain]["push_time"], data[captain]["pushed_time"]
                )
                
                # 记录变更后的数据
                after_data = {
                    "push_time": data[captain]["push_time"],
                    "pushed_time": data[captain]["pushed_time"],
                    "net_push_time": data[captain]["net_push_time"]
                }
                
                # 写入CSV文件
                self._write_csv_data(group_id, data)
                
                # 记录变更日志
                change_entry = self._create_change_log_entry(
                    group_id=group_id,
                    change_type="auto_team_creation",
                    operator_cn="系统",
                    operator_qq="system",
                    influence_cn=[captain],
                    influence_qq=[captain_user_id],
                    content={
                        "operation": "车队创建自动增加跑者时长",
                        "duration": duration,
                        "before": before_data,
                        "after": after_data
                    },
                    reason="车队创建自动记录",
                    event_description=f"车队创建：队长 {captain} 自动增加跑者时长 {duration} 小时"
                )
                self._record_change_log_with_retry(group_id, change_entry)
                
                # 使用统一日志格式记录
                current_pushed_time = data[captain]["pushed_time"]
                current_push_time = data[captain]["push_time"]
                self.log_unified("INFO", f"已被[推时统计服务]处理:队长报班自动增加跑者时长{duration}小时，当前 {captain}的跑者时长为:{current_pushed_time}小时，推车时长为:{current_push_time}小时，已成功发送响应。", group_id, captain_user_id)
                
                # 用户/群组行为日志
                target = self._format_oper_target(date_display, time_range, timestamp_ranges)
                detail = f"自动增加跑时{duration:.3f}小时，净推车时长更新为{after_data['net_push_time']:.3f}小时。"
                self._append_user_push_log(group_id, captain, "报班", target, detail)
        
        except Exception as e:
            self.log_unified("ERROR", f"自动记录车队创建失败: {e}", group_id, captain_user_id)
    
    async def auto_record_member_join(self, member_name: str, member_user_id: str, join_type: str, group_id: str, timestamp_ranges: List[Tuple[float, float]],
                                      date_text: Optional[str] = None, time_text: Optional[str] = None) -> None:
        """
        自动记录成员加入时的时长
         
        Args:
            member_name: 成员名称
            member_user_id: 成员用户ID
            join_type: 加入类型（'推车' 或 '共跑'）
            group_id: 群组ID
            timestamp_ranges: 时间戳范围列表
            date_text: 操作日期文本（用户输入）
            time_text: 操作时间文本（用户输入）
        """
        try:
            if not member_name or not timestamp_ranges:
                return
            
            duration = self._calculate_time_duration(timestamp_ranges)
            
            if duration > 0:
                # 确保用户存在（传入QQ号）
                self._ensure_user_exists(group_id, member_name, member_user_id)
                
                # 从CSV读取数据
                data = self._read_csv_data(group_id)
                
                # 记录变更前的数据
                before_data = {
                    "push_time": data[member_name]["push_time"],
                    "pushed_time": data[member_name]["pushed_time"],
                    "net_push_time": data[member_name]["net_push_time"]
                }
                
                if join_type == '推车':
                    data[member_name]["push_time"] += duration
                    # 使用统一日志格式记录推车
                    current_push_time = data[member_name]["push_time"]
                    current_pushed_time = data[member_name]["pushed_time"]
                    self.log_unified("INFO", f"已被[推时统计服务]处理:成员加入推车自动增加推车时长{duration}小时，当前 {member_name}的推车时长为:{current_push_time}小时，已成功发送响应。", group_id, member_user_id)
                elif join_type == '共跑':
                    data[member_name]["pushed_time"] += duration
                    # 使用统一日志格式记录共跑
                    current_pushed_time = data[member_name]["pushed_time"]
                    current_push_time = data[member_name]["push_time"]
                    self.log_unified("INFO", f"已被[推时统计服务]处理:成员加入共跑自动增加跑者时长{duration}小时，当前 {member_name}的跑者时长为:{current_pushed_time}小时，已成功发送响应。", group_id, member_user_id)
                elif join_type == '跑推':
                    adjusted_duration = round(duration * 0.2, 3)
                    data[member_name]["push_time"] += adjusted_duration
                    current_push_time = data[member_name]["push_time"]
                    current_pushed_time = data[member_name]["pushed_time"]
                    self.log_unified("INFO", f"已被[推时统计服务]处理:成员加入跑推自动增加推车时长{adjusted_duration}小时，当前 {member_name}的推车时长为:{current_push_time}小时，已成功发送响应。", group_id, member_user_id)
                
                # 重新计算净推车时长 - 使用推时计算器管理器的统一方法
                data[member_name]["net_push_time"] = self.calculator_manager.calculate_net_push_time(
                    group_id, data[member_name]["push_time"], data[member_name]["pushed_time"]
                )
                
                # 记录变更后的数据
                after_data = {
                    "push_time": data[member_name]["push_time"],
                    "pushed_time": data[member_name]["pushed_time"],
                    "net_push_time": data[member_name]["net_push_time"]
                }
                
                # 写入CSV文件
                self._write_csv_data(group_id, data)
                
                # 记录变更日志
                change_entry = self._create_change_log_entry(
                    group_id=group_id,
                    change_type="auto_member_join",
                    operator_cn="系统",
                    operator_qq="system",
                    influence_cn=[member_name],
                    influence_qq=[member_user_id],
                    content={
                        "operation": f"成员加入{join_type}自动增加时长",
                        "join_type": join_type,
                        "duration": duration if join_type != '跑推' else adjusted_duration,
                        "before": before_data,
                        "after": after_data
                    },
                    reason="成员加入自动记录",
                    event_description=f"成员加入：{member_name} 加入{join_type}，自动增加时长 {duration if join_type != '跑推' else adjusted_duration} 小时"
                )
                self._record_change_log_with_retry(group_id, change_entry)
                
                # 用户/群组行为日志
                target = self._format_oper_target(date_text, time_text, timestamp_ranges)
                if join_type == '推车':
                    detail = f"自动增加推时{duration:.3f}小时，净推车时长更新为{after_data['net_push_time']:.3f}小时。"
                    self._append_user_push_log(group_id, member_name, "推车", target, detail)
                elif join_type == '共跑':
                    detail = f"自动增加跑时{duration:.3f}小时，净推车时长更新为{after_data['net_push_time']:.3f}小时。"
                    self._append_user_push_log(group_id, member_name, "共跑", target, detail)
                elif join_type == '跑推':
                    detail = f"自动增加推时{adjusted_duration:.3f}小时（按跑推0.2倍），净推车时长更新为{after_data['net_push_time']:.3f}小时。"
                    self._append_user_push_log(group_id, member_name, "跑推", target, detail)
        
        except Exception as e:
            self.log_unified("ERROR", f"自动记录成员加入失败: {e}", group_id, member_user_id)
    
    async def auto_record_member_removal(self, member_name: str, member_user_id: str, join_type: str, group_id: str, timestamp_ranges: List[Tuple[float, float]],
                                         date_text: Optional[str] = None, time_text: Optional[str] = None) -> None:
        """
        自动记录成员回收时的时长减少
         
        Args:
            member_name: 成员名称
            member_user_id: 成员用户ID
            join_type: 加入类型（'推车' 或 '共跑'）
            group_id: 群组ID
            timestamp_ranges: 时间戳范围列表
            date_text: 操作日期文本（用户输入）
            time_text: 操作时间文本（用户输入）
        """
        try:
            if not member_name or not timestamp_ranges:
                return
            
            duration = self._calculate_time_duration(timestamp_ranges)
            
            if duration > 0:
                # 确保用户存在（传入QQ号）
                self._ensure_user_exists(group_id, member_name, member_user_id)
                
                # 从CSV读取数据
                data = self._read_csv_data(group_id)
                
                # 记录变更前的数据
                before_data = {
                    "push_time": data[member_name]["push_time"],
                    "pushed_time": data[member_name]["pushed_time"],
                    "net_push_time": data[member_name]["net_push_time"]
                }
                
                if join_type == '推车':
                    old_time = data[member_name]["push_time"]
                    data[member_name]["push_time"] = old_time - duration
                    # 使用统一日志格式记录推车回收
                    current_push_time = data[member_name]["push_time"]
                    current_pushed_time = data[member_name]["pushed_time"]
                    self.log_unified("INFO", f"已被[推时统计服务]处理:推车成员回收自动减少推车时长{duration}小时，当前 {member_name}的推车时长为:{current_push_time}小时，已成功发送响应。", group_id, member_user_id)
                elif join_type == '共跑':
                    old_time = data[member_name]["pushed_time"]
                    data[member_name]["pushed_time"] = old_time - duration
                    # 使用统一日志格式记录共跑回收
                    current_pushed_time = data[member_name]["pushed_time"]
                    current_push_time = data[member_name]["push_time"]
                    self.log_unified("INFO", f"已被[推时统计服务]处理:共跑成员回收自动减少跑者时长{duration}小时，当前 {member_name}的跑者时长为:{current_pushed_time}小时，已成功发送响应。", group_id, member_user_id)
                
                # 重新计算净推车时长 - 使用推时计算器管理器的统一方法
                data[member_name]["net_push_time"] = self.calculator_manager.calculate_net_push_time(
                    group_id, data[member_name]["push_time"], data[member_name]["pushed_time"]
                )
                
                # 记录变更后的数据
                after_data = {
                    "push_time": data[member_name]["push_time"],
                    "pushed_time": data[member_name]["pushed_time"],
                    "net_push_time": data[member_name]["net_push_time"]
                }
                
                # 写入CSV文件
                self._write_csv_data(group_id, data)
                
                # 记录变更日志
                change_entry = self._create_change_log_entry(
                    group_id=group_id,
                    change_type="auto_member_removal",
                    operator_cn="系统",
                    operator_qq="system",
                    influence_cn=[member_name],
                    influence_qq=[member_user_id],
                    content={
                        "operation": f"成员回收{join_type}自动减少时长",
                        "join_type": join_type,
                        "duration": duration,
                        "before": before_data,
                        "after": after_data
                    },
                    reason="成员回收自动记录",
                    event_description=f"成员回收：{member_name} 回收{join_type}，自动减少时长 {duration} 小时"
                )
                self._record_change_log_with_retry(group_id, change_entry)
                
                # 用户/群组行为日志
                target = self._format_oper_target(date_text, time_text, timestamp_ranges)
                if join_type == '推车':
                    detail = f"自动减少推时{duration:.3f}小时，净推车时长更新为{after_data['net_push_time']:.3f}小时。"
                    self._append_user_push_log(group_id, member_name, "撤回推车", target, detail)
                elif join_type == '共跑':
                    detail = f"自动减少跑时{duration:.3f}小时，净推车时长更新为{after_data['net_push_time']:.3f}小时。"
                    self._append_user_push_log(group_id, member_name, "撤回共跑", target, detail)
        
        except Exception as e:
            self.log_unified("ERROR", f"自动记录成员回收失败: {e}", group_id, member_user_id)
    
    async def auto_record_team_cancellation(self, captain: str, captain_user_id: str, group_id: str, timestamp_ranges: List[Tuple[float, float]], members: List[Dict[str, Any]],
                                            date_display: Optional[str] = None, time_range: Optional[str] = None) -> None:
        """
        自动记录车队回收时的时长减少
         
        Args:
            captain: 队长名称
            captain_user_id: 队长用户ID
            group_id: 群组ID
            timestamp_ranges: 时间戳范围列表
            members: 成员列表
            date_display: 用于展示的日期文本
            time_range: 用于展示的时间文本
        """
        try:
            # 减少队长的跑者时长
            if captain and timestamp_ranges:
                duration = self._calculate_time_duration(timestamp_ranges)
                
                if duration > 0:
                    # 确保用户存在（传入QQ号）
                    self._ensure_user_exists(group_id, captain, captain_user_id)
                    
                    # 从CSV读取数据
                    data = self._read_csv_data(group_id)
                    
                    # 记录变更前数据
                    before_data = {
                        "pushed_time": data[captain]["pushed_time"],
                        "net_push_time": data[captain]["net_push_time"]
                    }
                    
                    # 更新数据
                    old_time = data[captain]["pushed_time"]
                    data[captain]["pushed_time"] = old_time - duration
                    
                    # 重新计算净推车时长 - 使用推时计算器管理器的统一方法
                data[captain]["net_push_time"] = self.calculator_manager.calculate_net_push_time(
                    group_id, data[captain]["push_time"], data[captain]["pushed_time"]
                )
                
                # 记录变更后数据
                after_data = {
                    "pushed_time": data[captain]["pushed_time"],
                    "net_push_time": data[captain]["net_push_time"]
                }
                
                # 获取更新后的当前跑者时长
                current_pushed_time = data[captain]["pushed_time"]
                
                # 写入CSV文件
                self._write_csv_data(group_id, data)
                
                # 记录变更日志
                change_entry = self._create_change_log_entry(
                    group_id=group_id,
                    change_type="auto_team_cancellation",
                    operator_cn="系统",
                    operator_qq="system",
                    influence_cn=[captain],
                    influence_qq=[captain_user_id],
                    content={
                        "before": before_data,
                        "after": after_data,
                        "duration": duration,
                        "member_count": len(members)
                    },
                    reason="自动车队回收",
                    event_description=f"自动记录车队回收：队长 {captain} 减少跑者时长 {duration}小时"
                )
                self._record_change_log_with_retry(group_id, change_entry)
                
                self.log_unified("INFO", f"自动记录回收班车: 队长 {captain} 减少跑者时长 {duration}小时，当前 {captain}的跑者时长为：{current_pushed_time}小时", group_id, captain_user_id)
                
                # 用户/群组行为日志（队长）
                target = self._format_oper_target(date_display, time_range, timestamp_ranges)
                detail = f"自动减少跑时{duration:.3f}小时，净推车时长更新为{after_data['net_push_time']:.3f}小时。"
                self._append_user_push_log(group_id, captain, "撤回班车", target, detail)
            
            # 减少所有队员的时长
            for member in members:
                member_name = member.get('name', '')
                member_user_id = member.get('user_id', '')
                join_type = member.get('join_type', '')
                member_timestamp_ranges = member.get('timestamp_ranges', [])
                
                if member_name and join_type and member_timestamp_ranges:
                    await self.auto_record_member_removal(member_name, member_user_id, join_type, group_id, member_timestamp_ranges)
        
        except Exception as e:
            self.log_unified("ERROR", f"自动记录车队回收失败: {e}", group_id, captain_user_id)

    async def record_push_time_operation(self, user_id: str, member_name: str, operation: str, group_id: str, 
                                        description: str = "", operator_cn: str = "系统", operator_qq: str = "system") -> None:
        """
        记录推车时长相关操作（用于替补等特殊情况）
        
        Args:
            user_id: 用户ID
            member_name: 成员名称
            operation: 操作类型（如 'join_substitute', 'leave_substitute'）
            group_id: 群组ID
            description: 操作描述
            operator_cn: 操作人员CN
            operator_qq: 操作人员QQ
        """
        try:
            # 记录操作日志
            self.log_unified("INFO", f"推车时长操作记录 - 用户: {member_name}, 操作: {operation}, 描述: {description}", 
                           group_id=group_id, user_id=user_id)
            
            # 确保用户存在于统计数据中
            self._ensure_user_exists(group_id, member_name, user_id)
            
            # 获取用户当前数据用于变更日志
            data = self._read_csv_data(group_id)
            user_data = data.get(member_name, {
                "qq_number": "未记录",
                "push_time": 0.0,
                "pushed_time": 0.0,
                "net_push_time": 0.0
            })
            
            # 记录变更日志
            operation_labels = {
                'join_substitute': '替补加入',
                'leave_substitute': '替补离开'
            }
            operation_label = operation_labels.get(operation, f'特殊操作({operation})')
            
            # 创建变更内容
            content = {
                "operation_label": operation_label,
                "description": description,
                "current_data": {
                    "push_time": user_data["push_time"],
                    "pushed_time": user_data["pushed_time"],
                    "net_push_time": user_data["net_push_time"]
                }
            }
            
            # 生成事件描述
            event_description = f"{operator_cn}({operator_qq})记录了{operation_label}操作，影响对象为{member_name}"
            
            # 创建变更日志条目
            change_entry = self._create_change_log_entry(
                group_id=group_id,
                change_type="推时操作记录",
                operator_cn=operator_cn,
                operator_qq=operator_qq,
                influence_cn=[member_name],
                influence_qq=[user_data["qq_number"]],
                content=content,
                reason=f"{operation_label}: {description}",
                event_description=event_description
            )
            
            # 记录变更日志
            self._record_change_log_with_retry(group_id, change_entry)
            
            # 根据操作类型进行相应处理
            if operation == 'join_substitute':
                # 替补加入时的处理（暂时只记录日志，不自动增减时长）
                self.log_unified("INFO", f"替补加入记录: {member_name} - {description}", 
                               group_id=group_id, user_id=user_id)
            elif operation == 'leave_substitute':
                # 替补离开时的处理
                self.log_unified("INFO", f"替补离开记录: {member_name} - {description}", 
                               group_id=group_id, user_id=user_id)
            else:
                # 其他操作类型
                self.log_unified("INFO", f"其他推车时长操作: {member_name} - {operation} - {description}", 
                               group_id=group_id, user_id=user_id)
                
        except Exception as e:
            self.log_unified("ERROR", f"记录推车时长操作失败: {str(e)}", group_id=group_id, user_id=user_id)
    
    def get_help_text(self) -> str:
        """
        获取帮助文本
         
        Returns:
            帮助文本
        """
        return """
📊 推车时长统计服务帮助

🔧 手动管理指令：
/增加推时 <CN> <小时数> - 增加推车时长
/减少推时 <CN> <小时数> - 减少推车时长
/增加跑时 <CN> <小时数> - 增加被推时长
/减少跑时 <CN> <小时数> - 减少被推时长

📋 查询指令：
/推时统计 <CN> - 查询推车时长统计

👤 用户管理：
/上传cn <CN> - 上传新的CN到系统

🔒 管理员指令：
/推时查询 - 查询当前群聊所有推时信息文件
/推时负数查询 - 查询当前群聊推时负数用户文件
/推时结算 - 执行推时结算
/底标结算 - 执行底标结算
/导出推时表格 - 导出推车时长统计表格
/净推时重新计算 - 根据群组推时计算规则重新计算所有用户的净推时

💡 说明：
- CN为用户名称，时长单位为小时
- 系统会自动记录车队活动中的推车和跑者时长
- 跑推计入推车时长的0.2倍（包含替补跑推）
- 结算功能仅管理员可用
- 查询功能返回对应群聊的数据文件
- 导出的表格保存在data/push_time目录下
- 净推时重新计算会根据群组ID使用对应的推时计算规则
"""
    
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
        group_id = kwargs.get('group_id', 'default')
        
        # 手动增减时长指令
        if message.startswith('/增加推时'):
            parts = message.split()
            if len(parts) >= 3:
                cn = self._normalize_cn(parts[1])
                try:
                    hours = float(parts[2])
                    reason = " ".join(parts[3:]) if len(parts) > 3 else ""
                    response = self.add_push_time(cn, hours, group_id, reason=reason)
                    return {
                        'type': 'text',
                        'content': response
                    }
                except ValueError:
                    return {
                        'type': 'text',
                        'content': '❌ 时长必须是数字'
                    }
            else:
                return {
                    'type': 'text',
                    'content': '❌ 格式错误，请使用：/增加推时 <CN> <小时数>'
                }
        
        elif message.startswith('/减少推时'):
            parts = message.split()
            if len(parts) >= 3:
                cn = self._normalize_cn(parts[1])
                try:
                    hours = float(parts[2])
                    reason = " ".join(parts[3:]) if len(parts) > 3 else ""
                    response = self.reduce_push_time(cn, hours, group_id, reason=reason)
                    return {
                        'type': 'text',
                        'content': response
                    }
                except ValueError:
                    return {
                        'type': 'text',
                        'content': '❌ 时长必须是数字'
                    }
            else:
                return {
                    'type': 'text',
                    'content': '❌ 格式错误，请使用：/减少推时 <CN> <小时数>'
                }
        
        elif message.startswith('/增加跑时'):
            parts = message.split()
            if len(parts) >= 3:
                cn = self._normalize_cn(parts[1])
                try:
                    hours = float(parts[2])
                    reason = " ".join(parts[3:]) if len(parts) > 3 else ""
                    response = self.add_pushed_time(cn, hours, group_id, reason=reason)
                    return {
                        'type': 'text',
                        'content': response
                    }
                except ValueError:
                    return {
                        'type': 'text',
                        'content': '❌ 时长必须是数字'
                    }
            else:
                return {
                    'type': 'text',
                    'content': '❌ 格式错误，请使用：/增加跑时 <CN> <小时数>'
                }
        
        elif message.startswith('/减少跑时'):
            parts = message.split()
            if len(parts) >= 3:
                cn = self._normalize_cn(parts[1])
                try:
                    hours = float(parts[2])
                    reason = " ".join(parts[3:]) if len(parts) > 3 else ""
                    response = self.reduce_pushed_time(cn, hours, group_id, reason=reason)
                    return {
                        'type': 'text',
                        'content': response
                    }
                except ValueError:
                    return {
                        'type': 'text',
                        'content': '❌ 时长必须是数字'
                    }
            else:
                return {
                    'type': 'text',
                    'content': '❌ 格式错误，请使用：/减少跑时 <CN> <小时数>'
                }
        
        # 查询指令 - 支持默认QQ映射与@提及
        elif message.startswith('/推时统计'):
            parts = message.split()
            context = kwargs.get('context', {}) or {}
            # 优先解析@提及（即使无空格分隔）
            at_qq = self._extract_first_at_qq(context)
            if at_qq:
                cn = self._get_cn_by_qq(group_id, at_qq)
                if not cn:
                    return {
                        'type': 'text',
                        'content': '❌ 未找到被@用户的CN绑定信息'
                    }
                response = self.query_statistics(cn, group_id)
                return {
                    'type': 'text',
                    'content': response
                }
            # 无@时，判断是否提供了CN；否则使用当前QQ映射
            if len(parts) >= 2:
                cn = self._normalize_cn(parts[1])
            else:
                cn = self._get_cn_by_qq(group_id, user_id)
                if not cn:
                    return {
                        'type': 'text',
                        'content': '❌ 未找到您的CN，请先使用/上传cn <CN> 进行绑定'
                    }
            response = self.query_statistics(cn, group_id)
            return {
                'type': 'text',
                'content': response
            }
        
        # 上传CN指令
        elif message.startswith('/上传cn'):
            parts = message.split()
            if len(parts) >= 2:
                cn = parts[1]
                # 传递用户QQ号
                response = self.upload_cn(cn, group_id, user_id)
                return {
                    'type': 'text',
                    'content': response
                }
            else:
                return {
                    'type': 'text',
                    'content': '❌ 格式错误，请使用：/上传cn <CN>'
                }
        
        # 管理员结算指令
        elif message == '/推时结算':
            response = self.settlement(user_id, group_id)
            return {
                'type': 'text',
                'content': response
            }
        
        elif message == '/底标结算':
            response = self.base_settlement(user_id, group_id)
            return {
                'type': 'text',
                'content': response
            }
        
        # 导出表格指令
        elif message == '/导出推时表格':
            response = self.export_statistics_table(group_id)
            return {
                'type': 'text',
                'content': response
            }
        
        # 查询所有推时信息指令
        elif message == '/推时查询':
            response = self.query_all_push_time(user_id, group_id)
            
            # 如果是文件类型，返回简单格式让app.py处理文件路径转换
            if response.get('type') == 'file':
                file_path = response['file_path']
                description = response['description']
                
                # 返回简单格式，类似塔罗牌服务的格式
                return {
                    'content': description,
                    'file_path': file_path  # 使用相对路径，让app.py自动转换
                }
            else:
                # 文本类型直接返回
                return response
        
        # 推时记录（默认最近10条，支持CN或@用户；/全推时记录 返回全部，图片形式）
        elif message.startswith('/推时记录') or message.startswith('/全推时记录'):
            parts = message.split()
            context = kwargs.get('context', {}) or {}
            show_all = message.startswith('/全推时记录')
            limit = None if show_all else 10
            target_cn: Optional[str] = None
            at_qq = self._extract_first_at_qq(context)
            if at_qq:
                target_cn = self._get_cn_by_qq(group_id, at_qq)
            elif len(parts) >= 2:
                target_cn = self._normalize_cn(parts[1])
            else:
                target_cn = self._get_cn_by_qq(group_id, user_id)
            if not target_cn:
                return {
                    'type': 'text',
                    'content': '❌ 未找到对应CN，请先使用/上传cn <CN> 进行绑定'
                }
            records = self._read_user_push_log(group_id, target_cn)
            lines = self._beautify_push_log_lines(records, limit=limit)
            title = f"{target_cn} 的推时记录（{'全部' if show_all else f'最近{min(10, len(records))}条'}）"
            img_path = self._generate_push_log_image(group_id, target_cn, title, lines)
            if img_path:
                return {
                    'mixed_message': True,
                    'content': title,
                    'image_path': img_path
                }
            else:
                return {
                    'type': 'text',
                    'content': f"{title}\n" + "\n".join(lines)
                }
        
        # 查询负数推时指令
        elif message == '/推时负数查询':
            response = self.query_negative_push_time(user_id, group_id)
            
            # 如果是文件类型，返回简单格式让app.py处理文件路径转换
            if response.get('type') == 'file':
                file_path = response['file_path']
                description = response['description']
                
                # 返回简单格式，类似塔罗牌服务的格式
                return {
                    'content': description,
                    'file_path': file_path  # 使用相对路径，让app.py自动转换
                }
            else:
                # 文本类型直接返回
                return response
        
        # 净推时重新计算指令
        elif message == '/净推时重新计算':
            response = self.recalculate_net_push_time(group_id)
            return {
                'type': 'text',
                'content': response
            }
        
        return None










