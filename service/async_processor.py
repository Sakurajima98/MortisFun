#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
异步处理模块
提供异步任务处理、队列管理和后台作业功能
"""

import threading
import uuid
import logging
from typing import Dict, Any, Optional, Callable
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, Future
from queue import Queue, Empty


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskPriority(Enum):
    """任务优先级枚举"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class Task:
    """任务数据类"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    func: Optional[Callable] = None
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    timeout: Optional[float] = None
    callback: Optional[Callable] = None
    
    def __lt__(self, other):
        """用于优先级队列排序"""
        return self.priority.value > other.priority.value


class AsyncTaskProcessor:
    """
    异步任务处理器
    支持任务队列、优先级调度、重试机制等
    """
    
    def __init__(self, max_workers: int = 4, queue_size: int = 1000):
        """
        初始化异步任务处理器
        
        Args:
            max_workers: 最大工作线程数
            queue_size: 队列最大大小
        """
        self.max_workers = max_workers
        self.queue_size = queue_size
        self.logger = logging.getLogger(__name__)
        
        # 任务存储
        self.tasks: Dict[str, Task] = {}
        self.task_queue = Queue(maxsize=queue_size)
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.futures: Dict[str, Future] = {}
        
        # 控制标志
        self.running = False
        self.worker_thread = None
        
        # 统计信息
        self.stats = {
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "cancelled_tasks": 0
        }
    
    def start(self) -> bool:
        """
        启动任务处理器
        
        Returns:
            bool: 是否启动成功
        """
        try:
            if self.running:
                self.logger.warning("任务处理器已在运行")
                return True
            
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.worker_thread.start()
            
            self.logger.info(f"异步任务处理器已启动，工作线程数: {self.max_workers}")
            return True
            
        except Exception as e:
            self.logger.error(f"启动任务处理器失败: {e}")
            return False
    
    def stop(self, timeout: float = 30.0) -> bool:
        """
        停止任务处理器
        
        Args:
            timeout: 停止超时时间
            
        Returns:
            bool: 是否停止成功
        """
        try:
            if not self.running:
                return True
            
            self.logger.info("正在停止任务处理器...")
            self.running = False
            
            # 等待工作线程结束
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=timeout)
            
            # 关闭线程池
            self.executor.shutdown(wait=True, timeout=timeout)
            
            # 取消未完成的任务
            for task_id, future in self.futures.items():
                if not future.done():
                    future.cancel()
                    if task_id in self.tasks:
                        self.tasks[task_id].status = TaskStatus.CANCELLED
                        self.stats["cancelled_tasks"] += 1
            
            self.logger.info("任务处理器已停止")
            return True
            
        except Exception as e:
            self.logger.error(f"停止任务处理器失败: {e}")
            return False
    
    def submit_task(self, 
                   func: Callable, 
                   *args, 
                   name: str = "",
                   priority: TaskPriority = TaskPriority.NORMAL,
                   max_retries: int = 3,
                   timeout: Optional[float] = None,
                   callback: Optional[Callable] = None,
                   **kwargs) -> Optional[str]:
        """
        提交任务
        
        Args:
            func: 要执行的函数
            *args: 函数参数
            name: 任务名称
            priority: 任务优先级
            max_retries: 最大重试次数
            timeout: 超时时间
            callback: 完成回调函数
            **kwargs: 函数关键字参数
            
        Returns:
            Optional[str]: 任务ID，如果提交失败则返回None
        """
        try:
            if not self.running:
                self.logger.error("任务处理器未运行")
                return None
            
            # 创建任务
            task = Task(
                name=name or func.__name__,
                func=func,
                args=args,
                kwargs=kwargs,
                priority=priority,
                max_retries=max_retries,
                timeout=timeout,
                callback=callback
            )
            
            # 存储任务
            self.tasks[task.id] = task
            
            # 添加到队列
            self.task_queue.put(task, timeout=5.0)
            
            self.stats["total_tasks"] += 1
            self.logger.debug(f"任务已提交: {task.id} - {task.name}")
            
            return task.id
            
        except Exception as e:
            self.logger.error(f"提交任务失败: {e}")
            return None
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            Optional[Dict[str, Any]]: 任务状态信息
        """
        if task_id not in self.tasks:
            return None
        
        task = self.tasks[task_id]
        
        # 计算运行时间
        runtime = None
        if task.started_at:
            end_time = task.completed_at or datetime.now()
            runtime = (end_time - task.started_at).total_seconds()
        
        return {
            "id": task.id,
            "name": task.name,
            "status": task.status.value,
            "priority": task.priority.value,
            "created_at": task.created_at.isoformat(),
            "started_at": task.started_at.isoformat() if task.started_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "runtime": runtime,
            "retry_count": task.retry_count,
            "max_retries": task.max_retries,
            "error": task.error,
            "has_result": task.result is not None
        }
    
    def get_task_result(self, task_id: str) -> Any:
        """
        获取任务结果
        
        Args:
            task_id: 任务ID
            
        Returns:
            Any: 任务结果
        """
        if task_id not in self.tasks:
            return None
        
        task = self.tasks[task_id]
        if task.status == TaskStatus.COMPLETED:
            return task.result
        elif task.status == TaskStatus.FAILED:
            raise Exception(f"任务执行失败: {task.error}")
        else:
            raise Exception(f"任务尚未完成，当前状态: {task.status.value}")
    
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 是否取消成功
        """
        try:
            if task_id not in self.tasks:
                return False
            
            task = self.tasks[task_id]
            
            # 如果任务正在运行，尝试取消Future
            if task_id in self.futures:
                future = self.futures[task_id]
                if future.cancel():
                    task.status = TaskStatus.CANCELLED
                    self.stats["cancelled_tasks"] += 1
                    self.logger.info(f"任务已取消: {task_id}")
                    return True
            
            # 如果任务还在队列中，标记为取消
            if task.status == TaskStatus.PENDING:
                task.status = TaskStatus.CANCELLED
                self.stats["cancelled_tasks"] += 1
                self.logger.info(f"任务已取消: {task_id}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"取消任务失败: {e}")
            return False
    
    def get_queue_info(self) -> Dict[str, Any]:
        """
        获取队列信息
        
        Returns:
            Dict[str, Any]: 队列信息
        """
        return {
            "queue_size": self.task_queue.qsize(),
            "max_queue_size": self.queue_size,
            "active_workers": len([f for f in self.futures.values() if not f.done()]),
            "max_workers": self.max_workers,
            "running": self.running,
            "stats": self.stats.copy()
        }
    
    def cleanup_completed_tasks(self, max_age_hours: int = 24) -> int:
        """
        清理已完成的任务
        
        Args:
            max_age_hours: 最大保留时间（小时）
            
        Returns:
            int: 清理的任务数量
        """
        try:
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            cleaned_count = 0
            
            # 收集要清理的任务ID
            tasks_to_remove = []
            for task_id, task in self.tasks.items():
                if (task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED] and
                    task.completed_at and task.completed_at < cutoff_time):
                    tasks_to_remove.append(task_id)
            
            # 清理任务
            for task_id in tasks_to_remove:
                del self.tasks[task_id]
                if task_id in self.futures:
                    del self.futures[task_id]
                cleaned_count += 1
            
            if cleaned_count > 0:
                self.logger.info(f"已清理 {cleaned_count} 个过期任务")
            
            return cleaned_count
            
        except Exception as e:
            self.logger.error(f"清理任务失败: {e}")
            return 0
    
    def _worker_loop(self):
        """
        工作线程主循环
        """
        self.logger.info("任务处理工作线程已启动")
        
        while self.running:
            try:
                # 从队列获取任务
                try:
                    task = self.task_queue.get(timeout=1.0)
                except Empty:
                    continue
                
                # 检查任务是否已被取消
                if task.status == TaskStatus.CANCELLED:
                    self.task_queue.task_done()
                    continue
                
                # 提交任务到线程池
                future = self.executor.submit(self._execute_task, task)
                self.futures[task.id] = future
                
                # 标记队列任务完成
                self.task_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"工作线程异常: {e}")
        
        self.logger.info("任务处理工作线程已停止")
    
    def _execute_task(self, task: Task) -> Any:
        """
        执行任务
        
        Args:
            task: 要执行的任务
            
        Returns:
            Any: 任务结果
        """
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        
        try:
            self.logger.debug(f"开始执行任务: {task.id} - {task.name}")
            
            # 执行任务函数
            if task.timeout:
                # 带超时的执行
                import signal
                
                def timeout_handler(signum, frame):
                    raise TimeoutError(f"任务执行超时: {task.timeout}秒")
                
                old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(int(task.timeout))
                
                try:
                    result = task.func(*task.args, **task.kwargs)
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
            else:
                # 正常执行
                result = task.func(*task.args, **task.kwargs)
            
            # 任务成功完成
            task.result = result
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            
            self.stats["completed_tasks"] += 1
            self.logger.debug(f"任务执行成功: {task.id} - {task.name}")
            
            # 执行回调
            if task.callback:
                try:
                    task.callback(task.id, result, None)
                except Exception as e:
                    self.logger.error(f"任务回调执行失败: {e}")
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            task.error = error_msg
            task.retry_count += 1
            
            # 检查是否需要重试
            if task.retry_count <= task.max_retries:
                self.logger.warning(f"任务执行失败，准备重试 ({task.retry_count}/{task.max_retries}): {task.id} - {error_msg}")
                
                # 重新提交任务
                task.status = TaskStatus.PENDING
                task.started_at = None
                
                try:
                    self.task_queue.put(task, timeout=5.0)
                except Exception as queue_error:
                    self.logger.error(f"重试任务提交失败: {queue_error}")
                    task.status = TaskStatus.FAILED
                    task.completed_at = datetime.now()
                    self.stats["failed_tasks"] += 1
            else:
                # 重试次数用尽，标记为失败
                task.status = TaskStatus.FAILED
                task.completed_at = datetime.now()
                self.stats["failed_tasks"] += 1
                
                self.logger.error(f"任务执行失败，重试次数用尽: {task.id} - {error_msg}")
                
                # 执行回调
                if task.callback:
                    try:
                        task.callback(task.id, None, error_msg)
                    except Exception as callback_error:
                        self.logger.error(f"任务回调执行失败: {callback_error}")
            
            raise
        
        finally:
            # 清理Future引用
            if task.id in self.futures:
                del self.futures[task.id]


# 创建全局异步处理器实例
async_processor = AsyncTaskProcessor(max_workers=4, queue_size=1000)