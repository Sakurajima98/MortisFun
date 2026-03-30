#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NapCat图片处理器模块

本模块负责处理来自NapCat的图片消息，包括：
1. 图片数据获取和下载
2. 图片格式转换和优化
3. 图片缓存管理
4. 调用大模型进行图片分析
5. 图片描述生成和存储

参考示例项目MaiBot的图片处理机制，适配当前项目需求。

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

import os
import io
import hashlib
import base64
import logging
import asyncio
import aiohttp
import aiofiles
from PIL import Image, ImageSequence
from typing import Dict, Any, Optional, List, Tuple, Union
from datetime import datetime, timedelta
import traceback
import json
import sqlite3
from pathlib import Path


class ImageCache:
    """
    图片缓存管理类
    
    负责管理图片的本地缓存，包括存储、检索和清理。
    """
    
    def __init__(self, cache_dir: str, db_path: str):
        """
        初始化图片缓存
        
        Args:
            cache_dir (str): 缓存目录路径
            db_path (str): 数据库文件路径
        """
        self.cache_dir = Path(cache_dir)
        self.db_path = db_path
        self.logger = logging.getLogger("ImageCache")
        
        # 创建缓存目录
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化数据库
        self._init_database()
    
    def _init_database(self):
        """
        初始化缓存数据库
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建图片缓存表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS image_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_hash TEXT UNIQUE NOT NULL,
                    file_path TEXT NOT NULL,
                    original_url TEXT,
                    file_size INTEGER,
                    width INTEGER,
                    height INTEGER,
                    format TEXT,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    access_count INTEGER DEFAULT 1
                )
            ''')
            
            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_image_hash ON image_cache(image_hash)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON image_cache(created_at)')
            
            conn.commit()
            conn.close()
            
            self.logger.info("图片缓存数据库初始化完成")
            
        except Exception as e:
            self.logger.error(f"初始化缓存数据库时出错: {e}")
    
    async def get_cached_image(self, image_hash: str) -> Optional[Dict[str, Any]]:
        """
        获取缓存的图片信息
        
        Args:
            image_hash (str): 图片哈希值
            
        Returns:
            Optional[Dict[str, Any]]: 缓存的图片信息
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT file_path, original_url, file_size, width, height, format, description
                FROM image_cache WHERE image_hash = ?
            ''', (image_hash,))
            
            result = cursor.fetchone()
            
            if result:
                # 更新访问时间和次数
                cursor.execute('''
                    UPDATE image_cache 
                    SET accessed_at = CURRENT_TIMESTAMP, access_count = access_count + 1
                    WHERE image_hash = ?
                ''', (image_hash,))
                conn.commit()
                
                file_path, original_url, file_size, width, height, format_type, description = result
                
                # 检查文件是否存在
                if os.path.exists(file_path):
                    conn.close()
                    return {
                        'file_path': file_path,
                        'original_url': original_url,
                        'file_size': file_size,
                        'width': width,
                        'height': height,
                        'format': format_type,
                        'description': description,
                        'cached': True
                    }
                else:
                    # 文件不存在，删除缓存记录
                    cursor.execute('DELETE FROM image_cache WHERE image_hash = ?', (image_hash,))
                    conn.commit()
            
            conn.close()
            return None
            
        except Exception as e:
            self.logger.error(f"获取缓存图片时出错: {e}")
            return None
    
    async def save_cached_image(self, image_hash: str, file_path: str, 
                              image_info: Dict[str, Any], description: str = None):
        """
        保存图片到缓存
        
        Args:
            image_hash (str): 图片哈希值
            file_path (str): 文件路径
            image_info (Dict[str, Any]): 图片信息
            description (str): 图片描述
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO image_cache 
                (image_hash, file_path, original_url, file_size, width, height, format, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                image_hash,
                file_path,
                image_info.get('original_url', ''),
                image_info.get('file_size', 0),
                image_info.get('width', 0),
                image_info.get('height', 0),
                image_info.get('format', ''),
                description or ''
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"保存缓存图片时出错: {e}")
    
    async def cleanup_old_cache(self, days: int = 30):
        """
        清理旧的缓存文件
        
        Args:
            days (int): 保留天数
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 查找过期的缓存记录
            cursor.execute('''
                SELECT file_path FROM image_cache 
                WHERE created_at < datetime('now', '-{} days')
            '''.format(days))
            
            old_files = cursor.fetchall()
            
            # 删除文件和数据库记录
            for (file_path,) in old_files:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as e:
                    self.logger.warning(f"删除缓存文件失败: {file_path}, {e}")
            
            # 删除数据库记录
            cursor.execute('''
                DELETE FROM image_cache 
                WHERE created_at < datetime('now', '-{} days')
            '''.format(days))
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            self.logger.info(f"清理了 {deleted_count} 个过期缓存文件")
            
        except Exception as e:
            self.logger.error(f"清理缓存时出错: {e}")


class ImageProcessor:
    """
    NapCat图片处理器类
    
    负责处理图片消息，包括下载、转换、缓存和分析。
    参考示例项目的图片处理逻辑，适配当前项目架构。
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化图片处理器
        
        Args:
            config (Dict[str, Any]): 配置字典
        """
        self.config = config
        self.logger = logging.getLogger("ImageProcessor")
        
        # 处理统计
        self.processed_count = 0
        self.cached_count = 0
        self.downloaded_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        
        # 配置参数
        self.cache_dir = config.get('image_cache_dir', './cache/images')
        self.db_path = config.get('cache_db_path', './cache/image_cache.db')
        self.max_file_size = config.get('max_image_size', 10 * 1024 * 1024)  # 10MB
        self.max_width = config.get('max_image_width', 2048)
        self.max_height = config.get('max_image_height', 2048)
        self.supported_formats = config.get('supported_formats', ['JPEG', 'PNG', 'GIF', 'WEBP', 'BMP'])
        self.enable_gif_processing = config.get('enable_gif_processing', True)
        self.max_gif_frames = config.get('max_gif_frames', 10)
        self.gif_similarity_threshold = config.get('gif_similarity_threshold', 0.9)
        
        # HTTP请求配置
        self.request_timeout = config.get('request_timeout', 30)
        self.max_retries = config.get('max_retries', 3)
        self.user_agent = config.get('user_agent', 
                                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        # 初始化缓存管理器
        self.cache = ImageCache(self.cache_dir, self.db_path)
        
        # 创建HTTP会话
        self.session = None
        
        self.logger.info("NapCat图片处理器初始化完成")
    
    async def __aenter__(self):
        """
        异步上下文管理器入口
        """
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.request_timeout),
            headers={'User-Agent': self.user_agent}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        异步上下文管理器出口
        """
        if self.session:
            await self.session.close()
    
    async def process_image_segment(self, image_segment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理图片消息段
        
        Args:
            image_segment (Dict[str, Any]): 图片消息段数据
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果，包含图片信息和描述
        """
        try:
            self.processed_count += 1
            
            # 提取图片信息
            image_data = image_segment.get('data', {})
            image_url = image_data.get('url', '')
            image_file = image_data.get('file', '')
            
            if not image_url and not image_file:
                self.logger.warning("图片消息段缺少URL或文件信息")
                return None
            
            # 生成图片哈希
            image_hash = self._generate_image_hash(image_url or image_file)
            
            # 检查缓存
            cached_info = await self.cache.get_cached_image(image_hash)
            if cached_info:
                self.cached_count += 1
                self.logger.debug(f"使用缓存图片: {image_hash}")
                return {
                    'hash': image_hash,
                    'cached': True,
                    'file_path': cached_info['file_path'],
                    'description': cached_info['description'],
                    'width': cached_info['width'],
                    'height': cached_info['height'],
                    'format': cached_info['format'],
                    'file_size': cached_info['file_size']
                }
            
            # 下载图片
            image_path, image_info = await self._download_image(image_url, image_hash)
            if not image_path:
                return None
            
            # 处理图片
            processed_info = await self._process_downloaded_image(image_path, image_info)
            if not processed_info:
                return None
            
            # 生成图片描述（这里需要集成大模型）
            description = await self._generate_image_description(processed_info['file_path'])
            
            # 保存到缓存
            await self.cache.save_cached_image(
                image_hash, 
                processed_info['file_path'], 
                processed_info, 
                description
            )
            
            self.downloaded_count += 1
            
            return {
                'hash': image_hash,
                'cached': False,
                'file_path': processed_info['file_path'],
                'description': description,
                'width': processed_info['width'],
                'height': processed_info['height'],
                'format': processed_info['format'],
                'file_size': processed_info['file_size'],
                'original_url': image_url
            }
            
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"处理图片消息段时出错: {e}")
            self.logger.error(traceback.format_exc())
            return None
    
    def _generate_image_hash(self, image_identifier: str) -> str:
        """
        生成图片哈希值
        
        Args:
            image_identifier (str): 图片标识符（URL或文件名）
            
        Returns:
            str: 哈希值
        """
        return hashlib.md5(image_identifier.encode()).hexdigest()
    
    async def _download_image(self, image_url: str, image_hash: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        下载图片
        
        Args:
            image_url (str): 图片URL
            image_hash (str): 图片哈希值
            
        Returns:
            Tuple[Optional[str], Optional[Dict[str, Any]]]: 文件路径和图片信息
        """
        try:
            if not self.session:
                self.logger.error("HTTP会话未初始化")
                return None, None
            
            # 重试下载
            for attempt in range(self.max_retries):
                try:
                    async with self.session.get(image_url) as response:
                        if response.status == 200:
                            content = await response.read()
                            
                            # 检查文件大小
                            if len(content) > self.max_file_size:
                                self.logger.warning(f"图片文件过大: {len(content)} > {self.max_file_size}")
                                return None, None
                            
                            # 保存原始文件
                            file_extension = self._get_file_extension_from_content(content)
                            file_name = f"{image_hash}_original.{file_extension}"
                            file_path = os.path.join(self.cache_dir, file_name)
                            
                            async with aiofiles.open(file_path, 'wb') as f:
                                await f.write(content)
                            
                            # 获取图片信息
                            image_info = await self._get_image_info(file_path)
                            image_info['original_url'] = image_url
                            image_info['file_size'] = len(content)
                            
                            return file_path, image_info
                        else:
                            self.logger.warning(f"下载图片失败，状态码: {response.status}")
                            
                except asyncio.TimeoutError:
                    self.logger.warning(f"下载图片超时，尝试 {attempt + 1}/{self.max_retries}")
                except Exception as e:
                    self.logger.warning(f"下载图片出错，尝试 {attempt + 1}/{self.max_retries}: {e}")
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)  # 重试前等待1秒
            
            return None, None
            
        except Exception as e:
            self.logger.error(f"下载图片时出错: {e}")
            return None, None
    
    def _get_file_extension_from_content(self, content: bytes) -> str:
        """
        从文件内容判断文件扩展名
        
        Args:
            content (bytes): 文件内容
            
        Returns:
            str: 文件扩展名
        """
        try:
            # 检查文件头
            if content.startswith(b'\xff\xd8\xff'):
                return 'jpg'
            elif content.startswith(b'\x89PNG\r\n\x1a\n'):
                return 'png'
            elif content.startswith(b'GIF87a') or content.startswith(b'GIF89a'):
                return 'gif'
            elif content.startswith(b'RIFF') and b'WEBP' in content[:12]:
                return 'webp'
            elif content.startswith(b'BM'):
                return 'bmp'
            else:
                return 'jpg'  # 默认为jpg
        except:
            return 'jpg'
    
    async def _get_image_info(self, file_path: str) -> Dict[str, Any]:
        """
        获取图片信息
        
        Args:
            file_path (str): 图片文件路径
            
        Returns:
            Dict[str, Any]: 图片信息
        """
        try:
            with Image.open(file_path) as img:
                return {
                    'width': img.width,
                    'height': img.height,
                    'format': img.format,
                    'mode': img.mode,
                    'has_transparency': img.mode in ('RGBA', 'LA') or 'transparency' in img.info
                }
        except Exception as e:
            self.logger.error(f"获取图片信息时出错: {e}")
            return {
                'width': 0,
                'height': 0,
                'format': 'UNKNOWN',
                'mode': 'UNKNOWN',
                'has_transparency': False
            }
    
    async def _process_downloaded_image(self, file_path: str, image_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理下载的图片
        
        Args:
            file_path (str): 图片文件路径
            image_info (Dict[str, Any]): 图片信息
            
        Returns:
            Optional[Dict[str, Any]]: 处理后的图片信息
        """
        try:
            # 检查是否需要处理
            needs_processing = (
                image_info['width'] > self.max_width or
                image_info['height'] > self.max_height or
                image_info['format'] not in self.supported_formats or
                (image_info['format'] == 'GIF' and self.enable_gif_processing)
            )
            
            if not needs_processing:
                return {
                    'file_path': file_path,
                    'width': image_info['width'],
                    'height': image_info['height'],
                    'format': image_info['format'],
                    'file_size': image_info['file_size'],
                    'processed': False
                }
            
            # 处理图片
            processed_path = await self._transform_image(file_path, image_info)
            if not processed_path:
                return None
            
            # 获取处理后的图片信息
            processed_info = await self._get_image_info(processed_path)
            processed_info['file_path'] = processed_path
            processed_info['file_size'] = os.path.getsize(processed_path)
            processed_info['processed'] = True
            
            return processed_info
            
        except Exception as e:
            self.logger.error(f"处理图片时出错: {e}")
            return None
    
    async def _transform_image(self, file_path: str, image_info: Dict[str, Any]) -> Optional[str]:
        """
        转换图片格式和尺寸
        
        Args:
            file_path (str): 原始图片路径
            image_info (Dict[str, Any]): 图片信息
            
        Returns:
            Optional[str]: 处理后的图片路径
        """
        try:
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            processed_path = os.path.join(self.cache_dir, f"{base_name}_processed.jpg")
            
            with Image.open(file_path) as img:
                # 处理GIF
                if image_info['format'] == 'GIF' and self.enable_gif_processing:
                    img = await self._process_gif(img)
                
                # 调整尺寸
                if img.width > self.max_width or img.height > self.max_height:
                    img.thumbnail((self.max_width, self.max_height), Image.Resampling.LANCZOS)
                
                # 转换为RGB模式（用于JPEG保存）
                if img.mode in ('RGBA', 'LA', 'P'):
                    # 创建白色背景
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                    img = background
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # 保存处理后的图片
                img.save(processed_path, 'JPEG', quality=85, optimize=True)
            
            return processed_path
            
        except Exception as e:
            self.logger.error(f"转换图片时出错: {e}")
            return None
    
    async def _process_gif(self, gif_image: Image.Image) -> Image.Image:
        """
        处理GIF图片，提取关键帧
        
        Args:
            gif_image (Image.Image): GIF图片对象
            
        Returns:
            Image.Image: 处理后的图片
        """
        try:
            frames = []
            frame_count = 0
            
            # 提取帧
            for frame in ImageSequence.Iterator(gif_image):
                if frame_count >= self.max_gif_frames:
                    break
                
                # 转换为RGB
                frame_rgb = frame.convert('RGB')
                
                # 检查帧相似性（简单实现）
                if not frames or not self._frames_similar(frames[-1], frame_rgb):
                    frames.append(frame_rgb)
                
                frame_count += 1
            
            # 返回第一帧或中间帧
            if frames:
                return frames[len(frames) // 2] if len(frames) > 1 else frames[0]
            else:
                return gif_image.convert('RGB')
                
        except Exception as e:
            self.logger.error(f"处理GIF时出错: {e}")
            return gif_image.convert('RGB')
    
    def _frames_similar(self, frame1: Image.Image, frame2: Image.Image) -> bool:
        """
        检查两帧是否相似（简单实现）
        
        Args:
            frame1 (Image.Image): 第一帧
            frame2 (Image.Image): 第二帧
            
        Returns:
            bool: 是否相似
        """
        try:
            # 简单的相似性检查：比较缩略图
            thumb1 = frame1.resize((8, 8)).convert('L')
            thumb2 = frame2.resize((8, 8)).convert('L')
            
            # 计算像素差异
            diff = sum(abs(p1 - p2) for p1, p2 in zip(thumb1.getdata(), thumb2.getdata()))
            max_diff = 255 * 64  # 8x8 = 64 pixels
            
            similarity = 1 - (diff / max_diff)
            return similarity > self.gif_similarity_threshold
            
        except Exception:
            return False
    
    async def _generate_image_description(self, image_path: str) -> str:
        """
        生成图片描述（需要集成大模型）
        
        Args:
            image_path (str): 图片文件路径
            
        Returns:
            str: 图片描述
        """
        try:
            # TODO: 这里需要集成大模型API来生成图片描述
            # 目前返回占位符描述
            
            # 获取图片基本信息
            image_info = await self._get_image_info(image_path)
            
            # 生成基础描述
            description = f"这是一张{image_info['format']}格式的图片，"
            description += f"尺寸为{image_info['width']}x{image_info['height']}像素。"
            
            # 这里应该调用大模型API进行图片分析
            # 例如：
            # description = await self._call_llm_for_image_analysis(image_path)
            
            return description
            
        except Exception as e:
            self.logger.error(f"生成图片描述时出错: {e}")
            return "图片描述生成失败"
    
    async def _call_llm_for_image_analysis(self, image_path: str) -> str:
        """
        调用大模型进行图片分析（待实现）
        
        Args:
            image_path (str): 图片文件路径
            
        Returns:
            str: 大模型生成的图片描述
        """
        # TODO: 实现大模型调用逻辑
        # 这里需要根据项目中使用的大模型API进行实现
        # 可能需要：
        # 1. 将图片转换为base64
        # 2. 构建请求参数
        # 3. 调用大模型API
        # 4. 解析响应结果
        
        try:
            # 读取图片并转换为base64
            async with aiofiles.open(image_path, 'rb') as f:
                image_data = await f.read()
                image_base64 = base64.b64encode(image_data).decode()
            
            # 这里应该调用大模型API
            # 示例代码结构：
            # response = await self._make_llm_request(image_base64)
            # return response.get('description', '无法识别图片内容')
            
            return "待实现：大模型图片分析"
            
        except Exception as e:
            self.logger.error(f"调用大模型分析图片时出错: {e}")
            return "图片分析失败"
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取处理统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        uptime = datetime.now() - self.start_time
        
        return {
            'processed_count': self.processed_count,
            'cached_count': self.cached_count,
            'downloaded_count': self.downloaded_count,
            'error_count': self.error_count,
            'cache_hit_rate': (self.cached_count / max(self.processed_count, 1)) * 100,
            'success_rate': ((self.processed_count - self.error_count) / max(self.processed_count, 1)) * 100,
            'uptime_seconds': uptime.total_seconds(),
            'uptime_str': str(uptime),
            'images_per_minute': (self.processed_count / max(uptime.total_seconds() / 60, 1))
        }
    
    def reset_statistics(self):
        """
        重置统计信息
        """
        self.processed_count = 0
        self.cached_count = 0
        self.downloaded_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        self.logger.info("图片处理器统计信息已重置")
    
    async def cleanup_cache(self, days: int = 30):
        """
        清理缓存
        
        Args:
            days (int): 保留天数
        """
        await self.cache.cleanup_old_cache(days)