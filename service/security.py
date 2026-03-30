#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
安全管理模块
提供API密钥管理、请求限流和安全验证功能
"""

import os
import time
import hashlib
import logging
from typing import Dict, Any, Optional, List
from collections import defaultdict, deque
from threading import Lock
from datetime import datetime


class APIKeyManager:
    """
    API密钥管理器
    提供安全的API密钥存储和访问
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._keys = {}
        self._lock = Lock()
    
    def set_key(self, service_name: str, api_key: str, encrypt: bool = True) -> bool:
        """
        设置API密钥
        
        Args:
            service_name: 服务名称
            api_key: API密钥
            encrypt: 是否加密存储
            
        Returns:
            bool: 是否设置成功
        """
        try:
            with self._lock:
                if encrypt:
                    # 简单的混淆处理（实际应用中应使用更强的加密）
                    encrypted_key = self._obfuscate_key(api_key)
                    self._keys[service_name] = {
                        'key': encrypted_key,
                        'encrypted': True,
                        'created_at': datetime.now()
                    }
                else:
                    self._keys[service_name] = {
                        'key': api_key,
                        'encrypted': False,
                        'created_at': datetime.now()
                    }
                
                self.logger.info(f"API密钥已设置: {service_name}")
                return True
                
        except Exception as e:
            self.logger.error(f"设置API密钥失败: {e}")
            return False
    
    def get_key(self, service_name: str) -> Optional[str]:
        """
        获取API密钥
        
        Args:
            service_name: 服务名称
            
        Returns:
            Optional[str]: API密钥，如果不存在则返回None
        """
        try:
            with self._lock:
                if service_name not in self._keys:
                    # 尝试从环境变量获取
                    env_key = f"{service_name.upper()}_API_KEY"
                    api_key = os.getenv(env_key)
                    if api_key:
                        self.set_key(service_name, api_key, encrypt=False)
                        return api_key
                    return None
                
                key_info = self._keys[service_name]
                if key_info['encrypted']:
                    return self._deobfuscate_key(key_info['key'])
                else:
                    return key_info['key']
                    
        except Exception as e:
            self.logger.error(f"获取API密钥失败: {e}")
            return None
    
    def remove_key(self, service_name: str) -> bool:
        """
        移除API密钥
        
        Args:
            service_name: 服务名称
            
        Returns:
            bool: 是否移除成功
        """
        try:
            with self._lock:
                if service_name in self._keys:
                    del self._keys[service_name]
                    self.logger.info(f"API密钥已移除: {service_name}")
                    return True
                return False
                
        except Exception as e:
            self.logger.error(f"移除API密钥失败: {e}")
            return False
    
    def list_services(self) -> List[str]:
        """
        列出所有已配置的服务
        
        Returns:
            List[str]: 服务名称列表
        """
        with self._lock:
            return list(self._keys.keys())
    
    def _obfuscate_key(self, key: str) -> str:
        """
        混淆API密钥（简单实现）
        
        Args:
            key: 原始密钥
            
        Returns:
            str: 混淆后的密钥
        """
        # 简单的Base64编码 + 反转（实际应用中应使用更强的加密）
        import base64
        encoded = base64.b64encode(key.encode()).decode()
        return encoded[::-1]  # 反转字符串
    
    def _deobfuscate_key(self, obfuscated_key: str) -> str:
        """
        解混淆API密钥
        
        Args:
            obfuscated_key: 混淆的密钥
            
        Returns:
            str: 原始密钥
        """
        import base64
        reversed_key = obfuscated_key[::-1]  # 反转回来
        return base64.b64decode(reversed_key.encode()).decode()


class RateLimiter:
    """
    请求限流器
    基于滑动窗口算法实现请求频率限制
    """
    
    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        """
        初始化限流器
        
        Args:
            max_requests: 窗口期内最大请求数
            window_seconds: 窗口期长度（秒）
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = defaultdict(deque)  # 每个客户端的请求时间队列
        self._lock = Lock()
        self.logger = logging.getLogger(__name__)
    
    def is_allowed(self, client_id: str) -> bool:
        """
        检查是否允许请求
        
        Args:
            client_id: 客户端标识
            
        Returns:
            bool: 是否允许请求
        """
        current_time = time.time()
        
        with self._lock:
            # 获取客户端的请求队列
            client_requests = self.requests[client_id]
            
            # 清理过期的请求记录
            cutoff_time = current_time - self.window_seconds
            while client_requests and client_requests[0] < cutoff_time:
                client_requests.popleft()
            
            # 检查是否超过限制
            if len(client_requests) >= self.max_requests:
                self.logger.warning(f"客户端 {client_id} 请求频率超限")
                return False
            
            # 记录当前请求
            client_requests.append(current_time)
            return True
    
    def get_remaining_requests(self, client_id: str) -> int:
        """
        获取剩余请求数
        
        Args:
            client_id: 客户端标识
            
        Returns:
            int: 剩余请求数
        """
        current_time = time.time()
        
        with self._lock:
            client_requests = self.requests[client_id]
            
            # 清理过期的请求记录
            cutoff_time = current_time - self.window_seconds
            while client_requests and client_requests[0] < cutoff_time:
                client_requests.popleft()
            
            return max(0, self.max_requests - len(client_requests))
    
    def get_reset_time(self, client_id: str) -> Optional[datetime]:
        """
        获取限制重置时间
        
        Args:
            client_id: 客户端标识
            
        Returns:
            Optional[datetime]: 重置时间，如果没有限制则返回None
        """
        with self._lock:
            client_requests = self.requests[client_id]
            
            if not client_requests:
                return None
            
            # 最早的请求时间 + 窗口期 = 重置时间
            earliest_request = client_requests[0]
            reset_timestamp = earliest_request + self.window_seconds
            return datetime.fromtimestamp(reset_timestamp)
    
    def clear_client(self, client_id: str) -> bool:
        """
        清除客户端的请求记录
        
        Args:
            client_id: 客户端标识
            
        Returns:
            bool: 是否清除成功
        """
        with self._lock:
            if client_id in self.requests:
                del self.requests[client_id]
                self.logger.info(f"已清除客户端 {client_id} 的请求记录")
                return True
            return False


class SecurityValidator:
    """
    安全验证器
    提供请求安全验证功能
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.blocked_ips = set()
        self.suspicious_patterns = [
            # SQL注入模式
            r"(?i)(union|select|insert|update|delete|drop|create|alter)\s+",
            # XSS模式
            r"(?i)<script[^>]*>.*?</script>",
            # 路径遍历模式
            r"\.\./",
            # 命令注入模式
            r"(?i)(;|\||&|`|\$\(|\${)"
        ]
    
    def validate_request_content(self, content: str) -> Dict[str, Any]:
        """
        验证请求内容安全性
        
        Args:
            content: 请求内容
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        import re
        
        try:
            # 检查内容长度
            if len(content) > 10000:  # 10KB限制
                return {
                    "valid": False,
                    "reason": "content_too_large",
                    "message": "请求内容过大"
                }
            
            # 检查可疑模式
            for pattern in self.suspicious_patterns:
                if re.search(pattern, content):
                    self.logger.warning(f"检测到可疑内容模式: {pattern}")
                    return {
                        "valid": False,
                        "reason": "suspicious_pattern",
                        "message": "检测到可疑内容模式"
                    }
            
            return {
                "valid": True,
                "message": "内容验证通过"
            }
            
        except Exception as e:
            self.logger.error(f"内容验证异常: {e}")
            return {
                "valid": False,
                "reason": "validation_error",
                "message": f"验证异常: {e}"
            }
    
    def validate_api_key_format(self, api_key: str, service: str = "default") -> bool:
        """
        验证API密钥格式
        
        Args:
            api_key: API密钥
            service: 服务名称
            
        Returns:
            bool: 是否格式正确
        """
        if not api_key or not isinstance(api_key, str):
            return False
        
        # 基本长度检查
        if len(api_key.strip()) < 10:
            return False
        
        # 服务特定的格式检查
        if service == "siliconflow":
            # SiliconFlow API密钥通常以sk-开头
            return api_key.startswith("sk-") and len(api_key) >= 20
        
        # 通用格式检查：只包含字母、数字、连字符和下划线
        import re
        return bool(re.match(r'^[a-zA-Z0-9_-]+$', api_key))
    
    def generate_client_id(self, request_info: Dict[str, Any]) -> str:
        """
        生成客户端标识
        
        Args:
            request_info: 请求信息（IP、User-Agent等）
            
        Returns:
            str: 客户端标识
        """
        # 使用IP和User-Agent生成唯一标识
        ip = request_info.get("ip", "unknown")
        user_agent = request_info.get("user_agent", "unknown")
        
        # 创建哈希
        hash_input = f"{ip}:{user_agent}"
        return hashlib.md5(hash_input.encode()).hexdigest()[:16]
    
    def is_ip_blocked(self, ip: str) -> bool:
        """
        检查IP是否被阻止
        
        Args:
            ip: IP地址
            
        Returns:
            bool: 是否被阻止
        """
        return ip in self.blocked_ips
    
    def block_ip(self, ip: str, reason: str = "security_violation") -> bool:
        """
        阻止IP地址
        
        Args:
            ip: IP地址
            reason: 阻止原因
            
        Returns:
            bool: 是否阻止成功
        """
        try:
            self.blocked_ips.add(ip)
            self.logger.warning(f"IP {ip} 已被阻止，原因: {reason}")
            return True
        except Exception as e:
            self.logger.error(f"阻止IP失败: {e}")
            return False
    
    def unblock_ip(self, ip: str) -> bool:
        """
        解除IP阻止
        
        Args:
            ip: IP地址
            
        Returns:
            bool: 是否解除成功
        """
        try:
            if ip in self.blocked_ips:
                self.blocked_ips.remove(ip)
                self.logger.info(f"IP {ip} 已解除阻止")
                return True
            return False
        except Exception as e:
            self.logger.error(f"解除IP阻止失败: {e}")
            return False


# 创建全局实例
api_key_manager = APIKeyManager()
rate_limiter = RateLimiter(max_requests=100, window_seconds=60)
security_validator = SecurityValidator()