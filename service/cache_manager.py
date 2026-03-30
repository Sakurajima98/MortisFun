#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
缓存管理模块
提供统一的缓存接口和多种缓存策略
"""

import time
import json
import hashlib
import logging
import threading
from typing import Any, Optional, Dict, List
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class CacheEntry:
    """缓存条目"""
    key: str
    value: Any
    created_at: float
    expires_at: Optional[float] = None
    access_count: int = 0
    last_accessed: float = 0
    size: int = 0
    
    def __post_init__(self) -> None:
        self.last_accessed = self.created_at
        if isinstance(self.value, str):
            self.size = len(self.value.encode('utf-8'))
        elif isinstance(self.value, (dict, list)):
            self.size = len(json.dumps(self.value, ensure_ascii=False).encode('utf-8'))
        else:
            self.size = 64  # 默认大小
    
    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    def touch(self) -> None:
        """更新访问信息"""
        self.access_count += 1
        self.last_accessed = time.time()


class CacheStrategy(ABC):
    """缓存策略抽象基类"""
    
    @abstractmethod
    def should_evict(self, entries: Dict[str, CacheEntry], max_size: int, current_size: int) -> List[str]:
        """决定应该驱逐哪些缓存条目"""
        pass


class LRUStrategy(CacheStrategy):
    """最近最少使用策略"""
    
    def should_evict(self, entries: Dict[str, CacheEntry], max_size: int, current_size: int) -> List[str]:
        if current_size <= max_size:
            return []
        
        # 按最后访问时间排序
        sorted_entries = sorted(entries.items(), key=lambda x: x[1].last_accessed)
        
        evict_keys = []
        size_to_free = current_size - max_size
        freed_size = 0
        
        for key, entry in sorted_entries:
            evict_keys.append(key)
            freed_size += entry.size
            if freed_size >= size_to_free:
                break
        
        return evict_keys


class LFUStrategy(CacheStrategy):
    """最少使用频率策略"""
    
    def should_evict(self, entries: Dict[str, CacheEntry], max_size: int, current_size: int) -> List[str]:
        if current_size <= max_size:
            return []
        
        # 按访问次数排序
        sorted_entries = sorted(entries.items(), key=lambda x: x[1].access_count)
        
        evict_keys = []
        size_to_free = current_size - max_size
        freed_size = 0
        
        for key, entry in sorted_entries:
            evict_keys.append(key)
            freed_size += entry.size
            if freed_size >= size_to_free:
                break
        
        return evict_keys


class TTLStrategy(CacheStrategy):
    """基于TTL的策略"""
    
    def should_evict(self, entries: Dict[str, CacheEntry], max_size: int, current_size: int) -> List[str]:
        # 首先移除过期的条目
        expired_keys = [key for key, entry in entries.items() if entry.is_expired()]
        
        if current_size <= max_size:
            return expired_keys
        
        # 如果还需要更多空间，按过期时间排序
        non_expired = {k: v for k, v in entries.items() if not v.is_expired()}
        sorted_entries = sorted(non_expired.items(), 
                              key=lambda x: x[1].expires_at or float('inf'))
        
        evict_keys = expired_keys.copy()
        size_to_free = current_size - max_size
        freed_size = sum(entries[key].size for key in expired_keys)
        
        for key, entry in sorted_entries:
            if freed_size >= size_to_free:
                break
            evict_keys.append(key)
            freed_size += entry.size
        
        return evict_keys


class MemoryCache:
    """
    内存缓存实现
    支持多种驱逐策略和TTL
    """
    
    def __init__(self, 
                 max_size: int = 1000,
                 max_memory_mb: int = 100,
                 default_ttl: Optional[int] = None,
                 strategy: str = "lru") -> None:
        """
        初始化内存缓存
        
        Args:
            max_size: 最大条目数
            max_memory_mb: 最大内存使用量（MB）
            default_ttl: 默认TTL（秒）
            strategy: 驱逐策略（lru/lfu/ttl）
        """
        self.max_size: int = max_size
        self.max_memory: int = max_memory_mb * 1024 * 1024  # 转换为字节
        self.default_ttl: Optional[int] = default_ttl
        
        # 选择驱逐策略
        strategies: Dict[str, CacheStrategy] = {
            "lru": LRUStrategy(),
            "lfu": LFUStrategy(),
            "ttl": TTLStrategy()
        }
        self.strategy: CacheStrategy = strategies.get(strategy, LRUStrategy())
        
        # 缓存存储
        self.entries: Dict[str, CacheEntry] = {}
        self.current_memory: int = 0
        
        # 线程锁
        self.lock: threading.RLock = threading.RLock()
        
        # 统计信息
        self.stats: Dict[str, int] = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "expired": 0
        }
        
        self.logger: logging.Logger = logging.getLogger(__name__)
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存值
        
        Args:
            key: 缓存键
            
        Returns:
            Optional[Any]: 缓存值，如果不存在或过期则返回None
        """
        with self.lock:
            if key not in self.entries:
                self.stats["misses"] += 1
                return None
            
            entry = self.entries[key]
            
            # 检查是否过期
            if entry.is_expired():
                del self.entries[key]
                self.current_memory -= entry.size
                self.stats["expired"] += 1
                self.stats["misses"] += 1
                return None
            
            # 更新访问信息
            entry.touch()
            self.stats["hits"] += 1
            
            return entry.value
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        设置缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
            ttl: 生存时间（秒），如果为None则使用默认TTL
            
        Returns:
            bool: 是否设置成功
        """
        try:
            with self.lock:
                current_time = time.time()
                
                # 计算过期时间
                expires_at = None
                if ttl is not None:
                    expires_at = current_time + ttl
                elif self.default_ttl is not None:
                    expires_at = current_time + self.default_ttl
                
                # 创建缓存条目
                entry = CacheEntry(
                    key=key,
                    value=value,
                    created_at=current_time,
                    expires_at=expires_at
                )
                
                # 如果键已存在，先移除旧条目
                if key in self.entries:
                    old_entry = self.entries[key]
                    self.current_memory -= old_entry.size
                
                # 检查是否需要驱逐
                projected_memory = self.current_memory + entry.size
                projected_count = len(self.entries) + (1 if key not in self.entries else 0)
                
                if (projected_memory > self.max_memory or 
                    projected_count > self.max_size):
                    self._evict_entries()
                
                # 添加新条目
                self.entries[key] = entry
                self.current_memory += entry.size
                
                return True
                
        except Exception as e:
            self.logger.error(f"设置缓存失败: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        删除缓存条目
        
        Args:
            key: 缓存键
            
        Returns:
            bool: 是否删除成功
        """
        with self.lock:
            if key in self.entries:
                entry = self.entries[key]
                del self.entries[key]
                self.current_memory -= entry.size
                return True
            return False
    
    def clear(self) -> bool:
        """
        清空所有缓存
        
        Returns:
            bool: 是否清空成功
        """
        try:
            with self.lock:
                self.entries.clear()
                self.current_memory = 0
                return True
        except Exception as e:
            self.logger.error(f"清空缓存失败: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """
        检查键是否存在且未过期
        
        Args:
            key: 缓存键
            
        Returns:
            bool: 是否存在
        """
        with self.lock:
            if key not in self.entries:
                return False
            
            entry = self.entries[key]
            if entry.is_expired():
                del self.entries[key]
                self.current_memory -= entry.size
                self.stats["expired"] += 1
                return False
            
            return True
    
    def keys(self) -> List[str]:
        """
        获取所有有效的键
        
        Returns:
            List[str]: 键列表
        """
        with self.lock:
            # 清理过期条目
            self._cleanup_expired()
            return list(self.entries.keys())
    
    def size(self) -> int:
        """
        获取缓存条目数量
        
        Returns:
            int: 条目数量
        """
        with self.lock:
            self._cleanup_expired()
            return len(self.entries)
    
    def memory_usage(self) -> Dict[str, Any]:
        """
        获取内存使用情况
        
        Returns:
            Dict[str, Any]: 内存使用信息
        """
        with self.lock:
            return {
                "current_memory_bytes": self.current_memory,
                "current_memory_mb": self.current_memory / (1024 * 1024),
                "max_memory_mb": self.max_memory / (1024 * 1024),
                "memory_usage_percent": (self.current_memory / self.max_memory) * 100,
                "entry_count": len(self.entries),
                "max_entries": self.max_size
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        with self.lock:
            total_requests = self.stats["hits"] + self.stats["misses"]
            hit_rate = (self.stats["hits"] / total_requests * 100) if total_requests > 0 else 0
            
            return {
                **self.stats,
                "total_requests": total_requests,
                "hit_rate_percent": hit_rate,
                **self.memory_usage()
            }
    
    def _evict_entries(self):
        """
        根据策略驱逐缓存条目
        """
        # 首先清理过期条目
        self._cleanup_expired()
        
        # 如果仍然超出限制，使用驱逐策略
        if (self.current_memory > self.max_memory or 
            len(self.entries) > self.max_size):
            
            keys_to_evict = self.strategy.should_evict(
                self.entries, 
                self.max_size, 
                self.current_memory
            )
            
            for key in keys_to_evict:
                if key in self.entries:
                    entry = self.entries[key]
                    del self.entries[key]
                    self.current_memory -= entry.size
                    self.stats["evictions"] += 1
    
    def _cleanup_expired(self):
        """
        清理过期的缓存条目
        """
        current_time = time.time()
        expired_keys = []
        
        for key, entry in self.entries.items():
            if entry.is_expired():
                expired_keys.append(key)
        
        for key in expired_keys:
            entry = self.entries[key]
            del self.entries[key]
            self.current_memory -= entry.size
            self.stats["expired"] += 1


class CacheManager:
    """
    缓存管理器
    提供统一的缓存接口，支持多个缓存实例
    """
    
    def __init__(self):
        self.caches: Dict[str, MemoryCache] = {}
        self.logger = logging.getLogger(__name__)
    
    def create_cache(self, 
                    name: str,
                    max_size: int = 1000,
                    max_memory_mb: int = 100,
                    default_ttl: Optional[int] = None,
                    strategy: str = "lru") -> bool:
        """
        创建缓存实例
        
        Args:
            name: 缓存名称
            max_size: 最大条目数
            max_memory_mb: 最大内存使用量（MB）
            default_ttl: 默认TTL（秒）
            strategy: 驱逐策略
            
        Returns:
            bool: 是否创建成功
        """
        try:
            if name in self.caches:
                self.logger.warning(f"缓存 {name} 已存在")
                return False
            
            cache = MemoryCache(
                max_size=max_size,
                max_memory_mb=max_memory_mb,
                default_ttl=default_ttl,
                strategy=strategy
            )
            
            self.caches[name] = cache
            self.logger.info(f"缓存 {name} 创建成功")
            return True
            
        except Exception as e:
            self.logger.error(f"创建缓存失败: {e}")
            return False
    
    def get_cache(self, name: str) -> Optional[MemoryCache]:
        """
        获取缓存实例
        
        Args:
            name: 缓存名称
            
        Returns:
            Optional[MemoryCache]: 缓存实例
        """
        return self.caches.get(name)
    
    def delete_cache(self, name: str) -> bool:
        """
        删除缓存实例
        
        Args:
            name: 缓存名称
            
        Returns:
            bool: 是否删除成功
        """
        if name in self.caches:
            del self.caches[name]
            self.logger.info(f"缓存 {name} 已删除")
            return True
        return False
    
    def list_caches(self) -> List[str]:
        """
        列出所有缓存名称
        
        Returns:
            List[str]: 缓存名称列表
        """
        return list(self.caches.keys())
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有缓存的统计信息
        
        Returns:
            Dict[str, Dict[str, Any]]: 所有缓存的统计信息
        """
        return {name: cache.get_stats() for name, cache in self.caches.items()}
    
    def generate_cache_key(self, *args, **kwargs) -> str:
        """
        生成缓存键
        
        Args:
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            str: 缓存键
        """
        # 创建一个包含所有参数的字符串
        key_parts = []
        
        # 添加位置参数
        for arg in args:
            if isinstance(arg, (dict, list)):
                key_parts.append(json.dumps(arg, sort_keys=True, ensure_ascii=False))
            else:
                key_parts.append(str(arg))
        
        # 添加关键字参数
        if kwargs:
            sorted_kwargs = sorted(kwargs.items())
            for k, v in sorted_kwargs:
                if isinstance(v, (dict, list)):
                    key_parts.append(f"{k}:{json.dumps(v, sort_keys=True, ensure_ascii=False)}")
                else:
                    key_parts.append(f"{k}:{v}")
        
        # 生成哈希
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode('utf-8')).hexdigest()


# 创建全局缓存管理器实例
cache_manager = CacheManager()

# 创建默认缓存实例
cache_manager.create_cache(
    name="default",
    max_size=1000,
    max_memory_mb=50,
    default_ttl=3600,  # 1小时
    strategy="lru"
)

cache_manager.create_cache(
    name="api_responses",
    max_size=500,
    max_memory_mb=30,
    default_ttl=1800,  # 30分钟
    strategy="lru"
)

cache_manager.create_cache(
    name="user_sessions",
    max_size=200,
    max_memory_mb=10,
    default_ttl=7200,  # 2小时
    strategy="ttl"
)