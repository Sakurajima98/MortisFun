#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目监控和日志工具

本文件实现API调用监控、性能统计和结构化日志功能。

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict, deque


class APIMonitor:
    """
    API调用监控类
    
    负责监控API调用的性能、成功率、错误统计等。
    """
    
    def __init__(self, max_history: int = 1000):
        """
        初始化API监控器
        
        Args:
            max_history: 保留的历史记录数量
        """
        self.max_history = max_history
        self.call_history = deque(maxlen=max_history)
        self.stats = {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'total_response_time': 0.0,
            'error_counts': defaultdict(int),
            'hourly_stats': defaultdict(lambda: {'calls': 0, 'errors': 0, 'total_time': 0.0})
        }
    
    def record_call(self, 
                   endpoint: str, 
                   success: bool, 
                   response_time: float, 
                   error_type: Optional[str] = None,
                   usage: Optional[Dict[str, Any]] = None) -> None:
        """
        记录API调用
        
        Args:
            endpoint: API端点
            success: 是否成功
            response_time: 响应时间（秒）
            error_type: 错误类型
            usage: 使用统计（如token数量）
        """
        timestamp = datetime.now()
        hour_key = timestamp.strftime('%Y-%m-%d %H')
        
        # 记录调用历史
        call_record = {
            'timestamp': timestamp.isoformat(),
            'endpoint': endpoint,
            'success': success,
            'response_time': response_time,
            'error_type': error_type,
            'usage': usage or {}
        }
        self.call_history.append(call_record)
        
        # 更新统计信息
        self.stats['total_calls'] += 1
        self.stats['total_response_time'] += response_time
        
        if success:
            self.stats['successful_calls'] += 1
        else:
            self.stats['failed_calls'] += 1
            if error_type:
                self.stats['error_counts'][error_type] += 1
        
        # 更新小时统计
        hourly = self.stats['hourly_stats'][hour_key]
        hourly['calls'] += 1
        hourly['total_time'] += response_time
        if not success:
            hourly['errors'] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        total_calls = self.stats['total_calls']
        if total_calls == 0:
            return {
                'total_calls': 0,
                'success_rate': 0.0,
                'average_response_time': 0.0,
                'error_distribution': {},
                'recent_performance': []
            }
        
        success_rate = (self.stats['successful_calls'] / total_calls) * 100
        avg_response_time = self.stats['total_response_time'] / total_calls
        
        # 获取最近24小时的性能数据
        now = datetime.now()
        recent_hours = []
        for i in range(24):
            hour = now - timedelta(hours=i)
            hour_key = hour.strftime('%Y-%m-%d %H')
            hourly_data = self.stats['hourly_stats'].get(hour_key, {'calls': 0, 'errors': 0, 'total_time': 0.0})
            
            avg_time = hourly_data['total_time'] / hourly_data['calls'] if hourly_data['calls'] > 0 else 0
            error_rate = (hourly_data['errors'] / hourly_data['calls'] * 100) if hourly_data['calls'] > 0 else 0
            
            recent_hours.append({
                'hour': hour_key,
                'calls': hourly_data['calls'],
                'error_rate': error_rate,
                'avg_response_time': avg_time
            })
        
        return {
            'total_calls': total_calls,
            'success_rate': success_rate,
            'average_response_time': avg_response_time,
            'error_distribution': dict(self.stats['error_counts']),
            'recent_performance': recent_hours[::-1]  # 最新的在前
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        获取健康状态
        
        Returns:
            Dict[str, Any]: 健康状态信息
        """
        stats = self.get_stats()
        
        # 计算健康评分
        health_score = 100
        status = 'healthy'
        issues = []
        
        # 检查成功率
        if stats['success_rate'] < 95:
            health_score -= 20
            issues.append(f"成功率较低: {stats['success_rate']:.1f}%")
            if stats['success_rate'] < 80:
                status = 'critical'
            elif status == 'healthy':
                status = 'warning'
        
        # 检查响应时间
        if stats['average_response_time'] > 10:
            health_score -= 15
            issues.append(f"响应时间较慢: {stats['average_response_time']:.2f}s")
            if stats['average_response_time'] > 30:
                status = 'critical'
            elif status == 'healthy':
                status = 'warning'
        
        # 检查错误分布
        error_types = len(stats['error_distribution'])
        if error_types > 3:
            health_score -= 10
            issues.append(f"错误类型较多: {error_types}种")
            if status == 'healthy':
                status = 'warning'
        
        return {
            'status': status,
            'health_score': max(0, health_score),
            'issues': issues,
            'last_updated': datetime.now().isoformat()
        }


class StructuredLogger:
    """
    结构化日志记录器
    
    提供结构化的日志记录功能，便于日志分析和监控。
    """
    
    def __init__(self, name: str, level: int = logging.INFO):
        """
        初始化结构化日志记录器
        
        Args:
            name: 日志记录器名称
            level: 日志级别
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # 如果没有处理器，添加一个
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def log_api_call(self, 
                    endpoint: str, 
                    method: str = 'POST',
                    status: str = 'success',
                    response_time: float = 0.0,
                    error: Optional[str] = None,
                    usage: Optional[Dict[str, Any]] = None,
                    **kwargs) -> None:
        """
        记录API调用日志
        
        Args:
            endpoint: API端点
            method: HTTP方法
            status: 调用状态
            response_time: 响应时间
            error: 错误信息
            usage: 使用统计
            **kwargs: 其他参数
        """
        log_data = {
            'event_type': 'api_call',
            'endpoint': endpoint,
            'method': method,
            'status': status,
            'response_time': response_time,
            'timestamp': datetime.now().isoformat()
        }
        
        if error:
            log_data['error'] = error
        
        if usage:
            log_data['usage'] = usage
        
        log_data.update(kwargs)
        
        if status == 'success':
            self.logger.info(json.dumps(log_data, ensure_ascii=False))
        else:
            self.logger.error(json.dumps(log_data, ensure_ascii=False))
    
    def log_service_event(self, 
                         service: str,
                         event: str,
                         level: str = 'info',
                         **kwargs) -> None:
        """
        记录服务事件日志
        
        Args:
            service: 服务名称
            event: 事件描述
            level: 日志级别
            **kwargs: 其他参数
        """
        log_data = {
            'event_type': 'service_event',
            'service': service,
            'event': event,
            'timestamp': datetime.now().isoformat()
        }
        
        log_data.update(kwargs)
        
        log_message = json.dumps(log_data, ensure_ascii=False)
        
        if level == 'debug':
            self.logger.debug(log_message)
        elif level == 'info':
            self.logger.info(log_message)
        elif level == 'warning':
            self.logger.warning(log_message)
        elif level == 'error':
            self.logger.error(log_message)
        elif level == 'critical':
            self.logger.critical(log_message)
    
    def log_user_action(self, 
                       user_id: str,
                       action: str,
                       service: str,
                       **kwargs) -> None:
        """
        记录用户行为日志
        
        Args:
            user_id: 用户ID
            action: 用户行为
            service: 相关服务
            **kwargs: 其他参数
        """
        log_data = {
            'event_type': 'user_action',
            'user_id': user_id,
            'action': action,
            'service': service,
            'timestamp': datetime.now().isoformat()
        }
        
        log_data.update(kwargs)
        
        self.logger.info(json.dumps(log_data, ensure_ascii=False))


# 全局监控实例
api_monitor = APIMonitor()
structured_logger = StructuredLogger('mortisfun')