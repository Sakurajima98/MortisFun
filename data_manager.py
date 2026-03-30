#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目数据管理器
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
import threading
from contextlib import contextmanager


class DataManager:
    """
    数据管理器类
    
    负责处理项目中所有数据的存储、读取、管理和维护。
    提供统一的数据访问接口，确保数据的一致性和完整性。
    支持线程安全的文件操作和自动备份功能。
    """
    
    def __init__(self, base_path: str = "data"):
        """
        初始化数据管理器
        
        Args:
            base_path (str): 数据存储的基础路径
        """
        # 确保使用绝对路径，避免工作目录变化导致的路径错误
        if not os.path.isabs(base_path):
            # 如果是相对路径，基于当前脚本所在目录计算绝对路径
            script_dir = os.path.dirname(os.path.abspath(__file__))
            base_path = os.path.join(script_dir, base_path)
        
        self.base_path = Path(base_path)
        self.users_path = self.base_path / "users"
        self.chat_path = self.base_path / "chat"
        self.fortune_path = self.base_path / "fortune"
        self.tarot_path = self.base_path / "tarot"
        self.usage_path = self.base_path / "usage"
        self.chat_stats_path = self.base_path / "chat_stats"
        self.chat_logs_path = self.base_path / "chat_logs"
        
        # 线程锁，确保文件操作的线程安全
        self._file_locks = {}
        self._global_lock = threading.RLock()
        
        # 日志记录器
        self.logger = logging.getLogger(__name__)
        
        # 确保所有必要的目录存在
        self.ensure_directories()
        
        # 数据文件编码
        self.encoding = 'utf-8'
        
        self.logger.info(f"数据管理器初始化完成，基础路径: {self.base_path}")
    
    def ensure_directories(self):
        """
        确保所有必要的数据目录存在
        创建缺失的目录并设置适当的权限（Linux环境）
        """
        try:
            directories = [
                self.base_path,
                self.users_path,
                self.chat_path,
                self.fortune_path,
                self.tarot_path,
                self.usage_path,
                self.chat_stats_path,
                self.chat_logs_path
            ]
            
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
                # 在Linux环境下设置目录权限
                if os.name == 'posix':  # Unix/Linux系统
                    os.chmod(directory, 0o755)
                    
            self.logger.info("所有数据目录创建完成")
            
        except Exception as e:
            self.logger.error(f"创建数据目录失败: {e}")
            raise
    
    @contextmanager
    def _get_file_lock(self, file_path: str):
        """
        获取文件锁，确保文件操作的线程安全
        
        Args:
            file_path (str): 文件路径
            
        Yields:
            threading.Lock: 文件锁对象
        """
        with self._global_lock:
            if file_path not in self._file_locks:
                self._file_locks[file_path] = threading.Lock()
            lock = self._file_locks[file_path]
        
        with lock:
            yield lock
    
    def _safe_write_json(self, file_path: Path, data: Any) -> bool:
        """
        安全地写入JSON数据到文件
        
        Args:
            file_path (Path): 文件路径
            data (Any): 要写入的数据
            
        Returns:
            bool: 写入是否成功
        """
        try:
            with self._get_file_lock(str(file_path)):
                # 确保父目录存在
                file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # 先写入临时文件，然后原子性地移动到目标文件
                temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
                
                with open(temp_path, 'w', encoding=self.encoding) as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                # 原子性地移动文件
                temp_path.replace(file_path)
                
                # 在Linux环境下设置文件权限
                if os.name == 'posix':
                    os.chmod(file_path, 0o644)
                    
                return True
                
        except Exception as e:
            self.logger.error(f"写入文件失败 {file_path}: {e}")
            # 清理临时文件
            temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
            if temp_path.exists():
                temp_path.unlink()
            return False
    
    def _safe_read_json(self, file_path: Path) -> Optional[Any]:
        """
        安全地从文件读取JSON数据
        
        Args:
            file_path (Path): 文件路径
            
        Returns:
            Optional[Any]: 读取的数据，如果失败则返回None
        """
        try:
            with self._get_file_lock(str(file_path)):
                if not file_path.exists():
                    return None
                    
                with open(file_path, 'r', encoding=self.encoding) as f:
                    return json.load(f)
                    
        except Exception as e:
            self.logger.error(f"读取文件失败 {file_path}: {e}")
            return None
    
    def save_user_data(self, user_id: str, data: Dict[str, Any]) -> bool:
        """
        保存用户数据
        
        Args:
            user_id (str): 用户ID
            data (Dict[str, Any]): 用户数据
            
        Returns:
            bool: 保存是否成功
        """
        try:
            # 添加元数据
            user_data = {
                'user_id': user_id,
                'created_at': data.get('created_at', datetime.now().isoformat()),
                'updated_at': datetime.now().isoformat(),
                'data': data
            }
            
            file_path = self.users_path / f"user_{user_id}.json"
            success = self._safe_write_json(file_path, user_data)
            
            if success:
                self.logger.info(f"用户数据保存成功: {user_id}")
            else:
                self.logger.error(f"用户数据保存失败: {user_id}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"保存用户数据异常 {user_id}: {e}")
            return False
    
    def load_user_data(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        加载用户数据
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Optional[Dict[str, Any]]: 用户数据，如果不存在则返回None
        """
        try:
            file_path = self.users_path / f"user_{user_id}.json"
            user_data = self._safe_read_json(file_path)
            
            if user_data:
                self.logger.debug(f"用户数据加载成功: {user_id}")
                return user_data.get('data', {})
            else:
                self.logger.debug(f"用户数据不存在: {user_id}")
                return None
                
        except Exception as e:
            self.logger.error(f"加载用户数据异常 {user_id}: {e}")
            return None
    
    def save_fortune_data(self, user_id: str, fortune_type: str, data: Dict[str, Any]) -> bool:
        """
        保存运势数据
        
        Args:
            user_id (str): 用户ID
            fortune_type (str): 运势类型（daily/zodiac）
            data (Dict[str, Any]): 运势数据
            
        Returns:
            bool: 保存是否成功
        """
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 构建运势数据结构
            fortune_data = {
                'user_id': user_id,
                'fortune_type': fortune_type,
                'date': today,
                'created_at': datetime.now().isoformat(),
                'data': data
            }
            
            # 文件路径：fortune/fortune_type/YYYY-MM/user_id_date.json
            year_month = datetime.now().strftime('%Y-%m')
            type_path = self.fortune_path / fortune_type / year_month
            file_path = type_path / f"{user_id}_{today}.json"
            
            success = self._safe_write_json(file_path, fortune_data)
            
            if success:
                self.logger.info(f"运势数据保存成功: {user_id}, {fortune_type}, {today}")
            else:
                self.logger.error(f"运势数据保存失败: {user_id}, {fortune_type}, {today}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"保存运势数据异常 {user_id}, {fortune_type}: {e}")
            return False
    
    def load_fortune_data(self, user_id: str, fortune_type: str, date: str = None) -> Optional[Dict[str, Any]]:
        """
        加载运势数据
        
        Args:
            user_id (str): 用户ID
            fortune_type (str): 运势类型（daily/zodiac）
            date (str, optional): 指定日期，默认为今天
            
        Returns:
            Optional[Dict[str, Any]]: 运势数据，如果不存在则返回None
        """
        try:
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')
            
            # 解析日期以获取年月
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            year_month = date_obj.strftime('%Y-%m')
            
            # 构建文件路径
            type_path = self.fortune_path / fortune_type / year_month
            file_path = type_path / f"{user_id}_{date}.json"
            
            fortune_data = self._safe_read_json(file_path)
            
            if fortune_data:
                self.logger.debug(f"运势数据加载成功: {user_id}, {fortune_type}, {date}")
                return fortune_data.get('data', {})
            else:
                self.logger.debug(f"运势数据不存在: {user_id}, {fortune_type}, {date}")
                return None
                
        except Exception as e:
            self.logger.error(f"加载运势数据异常 {user_id}, {fortune_type}, {date}: {e}")
            return None
    
    def get_fortune_history(self, user_id: str, fortune_type: str, days: int = 7) -> List[Dict[str, Any]]:
        """
        获取用户运势历史记录
        
        Args:
            user_id (str): 用户ID
            fortune_type (str): 运势类型
            days (int): 获取最近几天的记录
            
        Returns:
            List[Dict[str, Any]]: 运势历史记录列表
        """
        history = []
        
        try:
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                fortune_data = self.load_fortune_data(user_id, fortune_type, date)
                
                if fortune_data:
                    history.append({
                        'date': date,
                        'data': fortune_data
                    })
            
            self.logger.debug(f"运势历史加载成功: {user_id}, {fortune_type}, {len(history)}条记录")
            return history
            
        except Exception as e:
            self.logger.error(f"获取运势历史异常 {user_id}, {fortune_type}: {e}")
            return []
    
    def save_chat_history(self, user_id: str, messages: List[Dict[str, Any]]) -> bool:
        """
        保存对话历史
        
        Args:
            user_id (str): 用户ID
            messages (List[Dict[str, Any]]): 对话消息列表
            
        Returns:
            bool: 保存是否成功
        """
        try:
            chat_data = {
                'user_id': user_id,
                'updated_at': datetime.now().isoformat(),
                'message_count': len(messages),
                'messages': messages
            }
            
            file_path = self.chat_path / f"chat_{user_id}.json"
            success = self._safe_write_json(file_path, chat_data)
            
            if success:
                self.logger.info(f"对话历史保存成功: {user_id}, {len(messages)}条消息")
            else:
                self.logger.error(f"对话历史保存失败: {user_id}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"保存对话历史异常 {user_id}: {e}")
            return False
    
    def load_chat_history(self, user_id: str) -> List[Dict[str, Any]]:
        """
        加载对话历史
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            List[Dict[str, Any]]: 对话历史列表
        """
        try:
            file_path = self.chat_path / f"chat_{user_id}.json"
            chat_data = self._safe_read_json(file_path)
            
            if chat_data:
                messages = chat_data.get('messages', [])
                self.logger.debug(f"对话历史加载成功: {user_id}, {len(messages)}条消息")
                return messages
            else:
                self.logger.debug(f"对话历史不存在: {user_id}")
                return []
                
        except Exception as e:
            self.logger.error(f"加载对话历史异常 {user_id}: {e}")
            return []
    
    def clear_chat_history(self, user_id: str) -> bool:
        """
        清除用户对话历史
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            bool: 清除是否成功
        """
        try:
            file_path = self.chat_path / f"chat_{user_id}.json"
            
            if file_path.exists():
                file_path.unlink()
                self.logger.info(f"对话历史清除成功: {user_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"清除对话历史异常 {user_id}: {e}")
            return False
    
    def save_tarot_record(self, user_id: str, record: Dict[str, Any]) -> bool:
        """
        保存塔罗牌记录
        
        Args:
            user_id (str): 用户ID
            record (Dict[str, Any]): 塔罗牌记录
            
        Returns:
            bool: 保存是否成功
        """
        try:
            tarot_data = {
                'user_id': user_id,
                'created_at': datetime.now().isoformat(),
                'record': record
            }
            
            file_path = self.tarot_path / f"tarot_{user_id}.json"
            success = self._safe_write_json(file_path, tarot_data)
            
            if success:
                self.logger.info(f"塔罗牌记录保存成功: {user_id}")
            else:
                self.logger.error(f"塔罗牌记录保存失败: {user_id}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"保存塔罗牌记录异常 {user_id}: {e}")
            return False
    
    def load_tarot_record(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        加载塔罗牌记录
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Optional[Dict[str, Any]]: 塔罗牌记录，如果不存在则返回None
        """
        try:
            file_path = self.tarot_path / f"tarot_{user_id}.json"
            tarot_data = self._safe_read_json(file_path)
            
            if tarot_data:
                self.logger.debug(f"塔罗牌记录加载成功: {user_id}")
                return tarot_data.get('record', {})
            else:
                self.logger.debug(f"塔罗牌记录不存在: {user_id}")
                return None
                
        except Exception as e:
            self.logger.error(f"加载塔罗牌记录异常 {user_id}: {e}")
            return None
    
    def clear_tarot_record(self, user_id: str) -> bool:
        """
        清除用户塔罗牌记录
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            bool: 清除是否成功
        """
        try:
            file_path = self.tarot_path / f"tarot_{user_id}.json"
            
            if file_path.exists():
                file_path.unlink()
                self.logger.info(f"塔罗牌记录清除成功: {user_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"清除塔罗牌记录异常 {user_id}: {e}")
            return False
    
    def cleanup_old_data(self, days: int = 30):
        """
        清理过期数据
        
        Args:
            days (int): 保留天数，超过此天数的数据将被清理
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            cleaned_count = 0
            
            # 清理运势数据
            for fortune_type_path in self.fortune_path.iterdir():
                if fortune_type_path.is_dir():
                    for year_month_path in fortune_type_path.iterdir():
                        if year_month_path.is_dir():
                            for file_path in year_month_path.glob("*.json"):
                                try:
                                    # 从文件名提取日期
                                    filename = file_path.stem
                                    date_str = filename.split('_')[-1]
                                    file_date = datetime.strptime(date_str, '%Y-%m-%d')
                                    
                                    if file_date < cutoff_date:
                                        file_path.unlink()
                                        cleaned_count += 1
                                        
                                except (ValueError, IndexError):
                                    # 忽略无法解析日期的文件
                                    continue
            
            # 清理空目录
            self._cleanup_empty_directories(self.fortune_path)
            
            self.logger.info(f"数据清理完成，清理了 {cleaned_count} 个过期文件")
            
        except Exception as e:
            self.logger.error(f"数据清理异常: {e}")
    
    def _cleanup_empty_directories(self, path: Path):
        """
        递归清理空目录
        
        Args:
            path (Path): 要清理的路径
        """
        try:
            for item in path.iterdir():
                if item.is_dir():
                    self._cleanup_empty_directories(item)
                    # 如果目录为空，删除它
                    try:
                        item.rmdir()
                    except OSError:
                        # 目录不为空，忽略
                        pass
                        
        except Exception as e:
            self.logger.debug(f"清理空目录时出错 {path}: {e}")
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """
        获取存储统计信息
        
        Returns:
            Dict[str, Any]: 存储统计信息
        """
        try:
            stats = {
                'users_count': len(list(self.users_path.glob("*.json"))),
                'chat_files_count': len(list(self.chat_path.glob("*.json"))),
                'tarot_files_count': len(list(self.tarot_path.glob("*.json"))),
                'fortune_files_count': 0,
                'total_size_mb': 0
            }
            
            # 统计运势文件数量
            for fortune_type_path in self.fortune_path.iterdir():
                if fortune_type_path.is_dir():
                    stats['fortune_files_count'] += len(list(fortune_type_path.rglob("*.json")))
            
            # 计算总大小
            total_size = 0
            for file_path in self.base_path.rglob("*.json"):
                total_size += file_path.stat().st_size
            
            stats['total_size_mb'] = round(total_size / (1024 * 1024), 2)
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取存储统计信息异常: {e}")
            return {}
    
    def save_data(self, file_path: str, data: Any) -> bool:
        """
        通用数据保存方法
        
        Args:
            file_path (str): 文件路径（相对于base_path）
            data (Any): 要保存的数据
            
        Returns:
            bool: 保存是否成功
        """
        try:
            full_path = self.base_path / file_path
            return self._safe_write_json(full_path, data)
            
        except Exception as e:
            self.logger.error(f"保存数据失败 {file_path}: {e}")
            return False
    
    def load_data(self, file_path: str) -> Optional[Any]:
        """
        通用数据加载方法
        
        Args:
            file_path (str): 文件路径（相对于base_path）
            
        Returns:
            Optional[Any]: 加载的数据，如果失败则返回None
        """
        try:
            full_path = self.base_path / file_path
            return self._safe_read_json(full_path)
            
        except Exception as e:
            self.logger.error(f"加载数据失败 {file_path}: {e}")
            return None
    
    def file_exists(self, file_path: str) -> bool:
        """
        检查文件是否存在
        
        Args:
            file_path (str): 文件路径（相对于base_path）
            
        Returns:
            bool: 文件是否存在
        """
        try:
            full_path = self.base_path / file_path
            return full_path.exists()
            
        except Exception as e:
            self.logger.error(f"检查文件存在性失败 {file_path}: {e}")
            return False
    
    def delete_file(self, file_path: str) -> bool:
        """
        删除文件
        
        Args:
            file_path (str): 文件路径（相对于base_path）
            
        Returns:
            bool: 删除是否成功
        """
        try:
            full_path = self.base_path / file_path
            if full_path.exists():
                full_path.unlink()
                self.logger.info(f"文件删除成功: {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"删除文件失败 {file_path}: {e}")
            return False

    def backup_data(self, backup_path: str) -> bool:
        """
        备份数据到指定路径
        
        Args:
            backup_path (str): 备份路径
            
        Returns:
            bool: 备份是否成功
        """
        try:
            import shutil
            
            backup_dir = Path(backup_path)
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # 创建时间戳目录
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_target = backup_dir / f"mortisfun_backup_{timestamp}"
            
            # 复制整个数据目录
            shutil.copytree(self.base_path, backup_target)
            
            self.logger.info(f"数据备份成功: {backup_target}")
            return True
            
        except Exception as e:
            self.logger.error(f"数据备份失败: {e}")
            return False
    
    def get_daily_usage_count(self, user_id: str, service_name: str, action: str = 'default') -> int:
        """
        获取用户今日某服务的使用次数
        
        Args:
            user_id (str): 用户ID
            service_name (str): 服务名称
            action (str): 操作类型
            
        Returns:
            int: 今日使用次数
        """
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            file_path = self.usage_path / f"usage_{user_id}_{today}.json"
            
            usage_data = self._safe_read_json(file_path)
            if not usage_data:
                return 0
                
            service_usage = usage_data.get('services', {}).get(service_name, {})
            return service_usage.get(action, 0)
            
        except Exception as e:
            self.logger.error(f"获取每日使用次数失败 {user_id}, {service_name}, {action}: {e}")
            return 0
    
    def increment_daily_usage_count(self, user_id: str, service_name: str, action: str = 'default') -> bool:
        """
        增加用户今日某服务的使用次数
        
        Args:
            user_id (str): 用户ID
            service_name (str): 服务名称
            action (str): 操作类型
            
        Returns:
            bool: 操作是否成功
        """
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            file_path = self.usage_path / f"usage_{user_id}_{today}.json"
            
            # 加载现有数据
            usage_data = self._safe_read_json(file_path) or {
                'user_id': user_id,
                'date': today,
                'created_at': datetime.now().isoformat(),
                'services': {}
            }
            
            # 更新使用次数
            if 'services' not in usage_data:
                usage_data['services'] = {}
            if service_name not in usage_data['services']:
                usage_data['services'][service_name] = {}
            
            current_count = usage_data['services'][service_name].get(action, 0)
            usage_data['services'][service_name][action] = current_count + 1
            usage_data['updated_at'] = datetime.now().isoformat()
            
            # 保存数据
            success = self._safe_write_json(file_path, usage_data)
            
            if success:
                self.logger.debug(f"使用次数更新成功: {user_id}, {service_name}, {action}")
            else:
                self.logger.error(f"使用次数更新失败: {user_id}, {service_name}, {action}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"增加每日使用次数失败 {user_id}, {service_name}, {action}: {e}")
            return False
    
    def get_user_usage_stats(self, user_id: str, days: int = 7) -> Dict[str, Any]:
        """
        获取用户最近几天的使用统计
        
        Args:
            user_id (str): 用户ID
            days (int): 统计天数
            
        Returns:
            Dict[str, Any]: 使用统计数据
        """
        try:
            stats = {
                'user_id': user_id,
                'period_days': days,
                'daily_stats': [],
                'service_totals': {},
                'total_usage': 0
            }
            
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                file_path = self.usage_path / f"usage_{user_id}_{date}.json"
                
                usage_data = self._safe_read_json(file_path)
                if usage_data:
                    daily_total = 0
                    services = usage_data.get('services', {})
                    
                    # 统计当日各服务使用情况
                    for service_name, actions in services.items():
                        service_total = sum(actions.values())
                        daily_total += service_total
                        
                        # 累计服务总计
                        if service_name not in stats['service_totals']:
                            stats['service_totals'][service_name] = 0
                        stats['service_totals'][service_name] += service_total
                    
                    stats['daily_stats'].append({
                        'date': date,
                        'total': daily_total,
                        'services': services
                    })
                    
                    stats['total_usage'] += daily_total
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取用户使用统计失败 {user_id}: {e}")
            return {}
    
    def increment_group_chat_counter(self, group_id: str, user_id: str, timestamp: Optional[datetime] = None) -> bool:
        """
        增加群聊中某个用户的发言计数
        
        Args:
            group_id (str): 群聊ID
            user_id (str): 用户ID
            timestamp (Optional[datetime]): 消息时间，默认为当前时间
            
        Returns:
            bool: 更新是否成功
        """
        try:
            if not group_id or not user_id:
                return False
            
            ts = timestamp or datetime.now()
            date_str = ts.strftime('%Y-%m-%d')
            
            file_path = self.chat_stats_path / f"chat_{group_id}_{date_str}.json"
            
            stats_data = self._safe_read_json(file_path) or {
                'group_id': str(group_id),
                'date': date_str,
                'created_at': datetime.now().isoformat(),
                'total_messages': 0,
                'users': {}
            }
            
            if 'users' not in stats_data:
                stats_data['users'] = {}
            stats_data['total_messages'] = int(stats_data.get('total_messages', 0)) + 1
            stats_data['users'][str(user_id)] = int(stats_data['users'].get(str(user_id), 0)) + 1
            stats_data['updated_at'] = datetime.now().isoformat()
            
            success = self._safe_write_json(file_path, stats_data)
            if success:
                self.logger.debug(f"群聊消息计数更新成功: G:{group_id}, U:{user_id}, D:{date_str}")
            else:
                self.logger.error(f"群聊消息计数更新失败: G:{group_id}, U:{user_id}, D:{date_str}")
            return success
        except Exception as e:
            self.logger.error(f"增加群聊消息计数失败 G:{group_id}, U:{user_id}: {e}")
            return False
    
    def get_group_chat_stats(self, group_id: str, days: int = 7) -> Dict[str, Any]:
        """
        获取群聊最近几天的聊天统计
        
        Args:
            group_id (str): 群聊ID
            days (int): 统计天数
            
        Returns:
            Dict[str, Any]: 聊天统计数据
        """
        try:
            stats: Dict[str, Any] = {
                'group_id': str(group_id),
                'period_days': days,
                'daily_stats': [],
                'user_totals': {},
                'total_messages': 0
            }
            
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                file_path = self.chat_stats_path / f"chat_{group_id}_{date}.json"
                
                day_data = self._safe_read_json(file_path)
                if not day_data:
                    continue
                
                total_messages = int(day_data.get('total_messages', 0))
                users: Dict[str, int] = {
                    str(uid): int(count) for uid, count in (day_data.get('users', {}) or {}).items()
                }
                
                stats['daily_stats'].append({
                    'date': date,
                    'total_messages': total_messages,
                    'users': users
                })
                
                stats['total_messages'] += total_messages
                
                for uid, count in users.items():
                    stats['user_totals'][uid] = stats['user_totals'].get(uid, 0) + count
            
            return stats
        except Exception as e:
            self.logger.error(f"获取群聊统计失败 G:{group_id}: {e}")
            return {}
    
    def append_group_chat_message(self, group_id: str, user_id: str, content: str, timestamp: Optional[datetime] = None, sender_name: Optional[str] = None) -> bool:
        """
        追加一条群聊消息到聊天日志
        
        Args:
            group_id (str): 群聊ID
            user_id (str): 用户ID
            content (str): 消息内容
            timestamp (Optional[datetime]): 消息时间，默认为当前时间
            
        Returns:
            bool: 是否追加成功
        """
        try:
            if not group_id or not user_id:
                return False
            
            text = (content or "").strip()
            if not text:
                return False
            
            ts = timestamp or datetime.now()
            date_str = ts.strftime('%Y-%m-%d')
            
            file_path = self.chat_logs_path / f"chat_{group_id}_{date_str}.json"
            
            log_data = self._safe_read_json(file_path) or {
                "group_id": str(group_id),
                "date": date_str,
                "created_at": datetime.now().isoformat(),
                "messages": []
            }
            
            if "messages" not in log_data or not isinstance(log_data["messages"], list):
                log_data["messages"] = []
            
            log_data["messages"].append({
                "time": ts.isoformat(),
                "user_id": str(user_id),
                "content": text,
                "display_name": str(sender_name) if sender_name else str(user_id)
            })
            log_data["updated_at"] = datetime.now().isoformat()
            
            return self._safe_write_json(file_path, log_data)
        except Exception as e:
            self.logger.error(f"追加群聊消息失败 G:{group_id}, U:{user_id}: {e}")
            return False
    
    def get_group_chat_messages(self, group_id: str, date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        获取指定日期的群聊消息列表
        
        Args:
            group_id (str): 群聊ID
            date (Optional[datetime]): 目标日期，默认当天
            
        Returns:
            List[Dict[str, Any]]: 消息列表
        """
        try:
            if not group_id:
                return []
            
            base_dt = date or datetime.now()
            date_str = base_dt.strftime('%Y-%m-%d')
            file_path = self.chat_logs_path / f"chat_{group_id}_{date_str}.json"
            
            log_data = self._safe_read_json(file_path)
            if not log_data:
                return []
            
            messages = log_data.get("messages") or []
            if not isinstance(messages, list):
                return []
            
            return messages
        except Exception as e:
            self.logger.error(f"获取群聊消息失败 G:{group_id}: {e}")
            return []
    
    def cleanup_old_usage_data(self, days: int = 30):
        """
        清理过期的使用统计数据
        
        Args:
            days (int): 保留天数
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            cleaned_count = 0
            
            for file_path in self.usage_path.glob("usage_*.json"):
                try:
                    # 从文件名提取日期
                    filename = file_path.stem
                    date_str = filename.split('_')[-1]
                    file_date = datetime.strptime(date_str, '%Y-%m-%d')
                    
                    if file_date < cutoff_date:
                        file_path.unlink()
                        cleaned_count += 1
                        
                except (ValueError, IndexError):
                    # 忽略无法解析日期的文件
                    continue
            
            self.logger.info(f"使用统计数据清理完成，清理了 {cleaned_count} 个过期文件")
            
        except Exception as e:
            self.logger.error(f"清理使用统计数据异常: {e}")
    
    def get_teams_by_group(self, group_id: str) -> List[Dict[str, Any]]:
        """
        根据群组ID获取车队数据
        
        Args:
            group_id (str): 群组ID
            
        Returns:
            List[Dict[str, Any]]: 车队数据列表
        """
        try:
            # 车队记录文件路径
            teams_record_file = self.base_path / 'team' / 'teams_record.json'
            
            if not teams_record_file.exists():
                self.logger.warning(f"车队记录文件不存在: {teams_record_file}")
                return []
            
            # 读取车队记录数据
            teams_data = self._safe_read_json(teams_record_file)
            if not teams_data:
                self.logger.warning("车队记录数据为空")
                return []
            
            # 获取指定群组的车队数据
            group_teams = teams_data.get(group_id, [])
            
            self.logger.info(f"获取群组 {group_id} 的车队数据，共 {len(group_teams)} 条")
            return group_teams
            
        except Exception as e:
            self.logger.error(f"获取群组车队数据失败: {e}")
            return []
