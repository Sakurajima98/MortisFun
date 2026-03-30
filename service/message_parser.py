#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import logging
import base64
import hashlib
import time
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime
import traceback
import asyncio


class MessageSegment:
    """
    消息段数据类
    
    用于封装单个消息段的信息，便于后续处理。
    """
    
    def __init__(self, segment_type: str, data: Dict[str, Any], raw_segment: Dict[str, Any]):
        """
        初始化消息段
        
        Args:
            segment_type (str): 消息段类型
            data (Dict[str, Any]): 消息段数据
            raw_segment (Dict[str, Any]): 原始消息段数据
        """
        self.type = segment_type
        self.data = data
        self.raw = raw_segment
        self.processed_at = time.time()
        
        # 根据类型设置特定属性
        if segment_type == 'text':
            self.text = data.get('text', '')
            self.length = len(self.text)
        elif segment_type == 'image':
            self.file = data.get('file', '')
            self.url = data.get('url', '')
            self.file_size = data.get('file_size', 0)
            self.width = data.get('width', 0)
            self.height = data.get('height', 0)
        elif segment_type == 'at':
            self.qq = data.get('qq', '')
            self.is_all = self.qq == 'all'
        elif segment_type == 'face':
            self.id = data.get('id', '')
        elif segment_type == 'reply':
            self.id = data.get('id', '')
    
    def __str__(self):
        return f"MessageSegment(type={self.type}, data={self.data})"
    
    def __repr__(self):
        return self.__str__()


class MessageParser:
    """
    NapCat消息解析器类
    
    负责深度解析消息内容，提取各种类型的消息段信息，
    参考示例项目的解析逻辑，适配当前项目架构。
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化消息解析器
        
        Args:
            config (Dict[str, Any]): 配置字典
        """
        self.config = config
        self.logger = logging.getLogger("MessageParser")
        
        # 解析统计
        self.parsed_count = 0
        self.text_segments_count = 0
        self.image_segments_count = 0
        self.other_segments_count = 0
        self.start_time = datetime.now()
        
        # 文本处理配置
        self.max_text_length = config.get('max_text_length', 2000)
        self.enable_text_filtering = config.get('enable_text_filtering', True)
        
        # 图片处理配置
        self.max_image_size = config.get('max_image_size', 10 * 1024 * 1024)  # 10MB
        self.supported_image_formats = config.get('supported_image_formats', 
                                                 ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'])
        
        # 敏感词过滤（简单示例）
        self.sensitive_words = set(config.get('sensitive_words', []))
        
        # 编译常用正则表达式
        self._compile_regex_patterns()
        
        self.logger.info("NapCat消息解析器初始化完成")
    
    def _compile_regex_patterns(self):
        """
        编译常用的正则表达式模式
        """
        # URL匹配
        self.url_pattern = re.compile(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        )
        
        # QQ号匹配
        self.qq_pattern = re.compile(r'\b[1-9][0-9]{4,10}\b')
        
        # 邮箱匹配
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
        # 手机号匹配（简单版本）
        self.phone_pattern = re.compile(r'\b1[3-9]\d{9}\b')
        
        # 表情符号匹配
        self.emoji_pattern = re.compile(
            r'[\U0001F600-\U0001F64F]|[\U0001F300-\U0001F5FF]|[\U0001F680-\U0001F6FF]|[\U0001F1E0-\U0001F1FF]'
        )
    
    async def parse_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        解析完整消息
        
        Args:
            message_data (Dict[str, Any]): 标准化后的消息数据
            
        Returns:
            Dict[str, Any]: 解析后的消息数据，包含详细的解析结果
        """
        try:
            self.parsed_count += 1
            
            # 创建解析结果容器
            parsed_result = {
                'original_message': message_data,
                'parsed_at': time.time(),
                'parsed_datetime': datetime.now().isoformat(),
                'segments': [],
                'text_content': '',
                'images': [],
                'mentions': [],
                'replies': [],
                'faces': [],
                'analysis': {
                    'total_segments': 0,
                    'text_segments': 0,
                    'image_segments': 0,
                    'mention_segments': 0,
                    'face_segments': 0,
                    'other_segments': 0,
                    'total_text_length': 0,
                    'has_url': False,
                    'has_email': False,
                    'has_phone': False,
                    'has_emoji': False,
                    'contains_sensitive': False
                }
            }
            
            # 解析消息段
            message_segments = message_data.get('message', [])
            text_parts = []
            
            for segment_data in message_segments:
                segment = await self._parse_single_segment(segment_data)
                if segment:
                    parsed_result['segments'].append(segment)
                    
                    # 根据类型分类处理
                    if segment.type == 'text':
                        self.text_segments_count += 1
                        parsed_result['analysis']['text_segments'] += 1
                        text_parts.append(segment.text)
                        parsed_result['analysis']['total_text_length'] += segment.length
                        
                        # 文本内容分析
                        await self._analyze_text_content(segment.text, parsed_result['analysis'])
                        
                    elif segment.type == 'image':
                        self.image_segments_count += 1
                        parsed_result['analysis']['image_segments'] += 1
                        parsed_result['images'].append({
                            'file': segment.file,
                            'url': segment.url,
                            'file_size': segment.file_size,
                            'width': segment.width,
                            'height': segment.height,
                            'segment_index': len(parsed_result['segments']) - 1
                        })
                        
                    elif segment.type == 'at':
                        parsed_result['analysis']['mention_segments'] += 1
                        parsed_result['mentions'].append({
                            'qq': segment.qq,
                            'is_all': segment.is_all,
                            'segment_index': len(parsed_result['segments']) - 1
                        })
                        
                    elif segment.type == 'face':
                        parsed_result['analysis']['face_segments'] += 1
                        parsed_result['faces'].append({
                            'id': segment.id,
                            'segment_index': len(parsed_result['segments']) - 1
                        })
                        
                    elif segment.type == 'reply':
                        parsed_result['replies'].append({
                            'id': segment.id,
                            'segment_index': len(parsed_result['segments']) - 1
                        })
                        
                    else:
                        self.other_segments_count += 1
                        parsed_result['analysis']['other_segments'] += 1
            
            # 合并文本内容
            parsed_result['text_content'] = ''.join(text_parts).strip()
            parsed_result['analysis']['total_segments'] = len(parsed_result['segments'])
            
            # 敏感词检测
            if self.enable_text_filtering and parsed_result['text_content']:
                parsed_result['analysis']['contains_sensitive'] = self._check_sensitive_words(
                    parsed_result['text_content']
                )
            
            # 添加消息摘要
            parsed_result['summary'] = self._generate_message_summary(parsed_result)
            
            self.logger.debug(f"成功解析消息，包含 {parsed_result['analysis']['total_segments']} 个消息段")
            
            return parsed_result
            
        except Exception as e:
            self.logger.error(f"解析消息时出错: {e}")
            self.logger.error(traceback.format_exc())
            return {
                'original_message': message_data,
                'error': str(e),
                'parsed_at': time.time(),
                'success': False
            }
    
    async def _parse_single_segment(self, segment_data: Dict[str, Any]) -> Optional[MessageSegment]:
        """
        解析单个消息段
        
        Args:
            segment_data (Dict[str, Any]): 消息段数据
            
        Returns:
            Optional[MessageSegment]: 解析后的消息段对象
        """
        try:
            segment_type = segment_data.get('type', '')
            data = segment_data.get('data', {})
            
            if not segment_type:
                return None
            
            # 创建消息段对象
            segment = MessageSegment(segment_type, data, segment_data)
            
            # 根据类型进行特殊处理
            if segment_type == 'text':
                await self._process_text_segment(segment)
            elif segment_type == 'image':
                await self._process_image_segment(segment)
            elif segment_type == 'at':
                await self._process_at_segment(segment)
            elif segment_type == 'face':
                await self._process_face_segment(segment)
            elif segment_type == 'reply':
                await self._process_reply_segment(segment)
            
            return segment
            
        except Exception as e:
            self.logger.error(f"解析消息段时出错: {e}")
            return None
    
    async def _process_text_segment(self, segment: MessageSegment):
        """
        处理文本消息段
        
        Args:
            segment (MessageSegment): 文本消息段
        """
        try:
            text = segment.text
            
            # 文本长度限制
            if len(text) > self.max_text_length:
                self.logger.warning(f"文本长度超过限制: {len(text)} > {self.max_text_length}")
                segment.text = text[:self.max_text_length] + "..."
                segment.truncated = True
            else:
                segment.truncated = False
            
            # 文本清理
            segment.cleaned_text = text.strip()
            
            # 提取特殊内容
            segment.urls = self.url_pattern.findall(text)
            segment.qq_numbers = self.qq_pattern.findall(text)
            segment.emails = self.email_pattern.findall(text)
            segment.phones = self.phone_pattern.findall(text)
            segment.emojis = self.emoji_pattern.findall(text)
            
        except Exception as e:
            self.logger.error(f"处理文本消息段时出错: {e}")
    
    async def _process_image_segment(self, segment: MessageSegment):
        """
        处理图片消息段
        
        Args:
            segment (MessageSegment): 图片消息段
        """
        try:
            # 提取图片信息
            file_info = segment.file
            
            # 解析文件名和格式
            if file_info:
                # 从文件名提取格式
                if '.' in file_info:
                    segment.format = file_info.split('.')[-1].lower()
                else:
                    segment.format = 'unknown'
                
                # 检查格式支持
                segment.format_supported = segment.format in self.supported_image_formats
                
                # 文件大小检查
                if segment.file_size > self.max_image_size:
                    self.logger.warning(f"图片文件过大: {segment.file_size} > {self.max_image_size}")
                    segment.size_exceeded = True
                else:
                    segment.size_exceeded = False
            
            # 生成图片唯一标识
            if segment.url:
                segment.hash = hashlib.md5(segment.url.encode()).hexdigest()
            elif segment.file:
                segment.hash = hashlib.md5(segment.file.encode()).hexdigest()
            else:
                segment.hash = hashlib.md5(str(time.time()).encode()).hexdigest()
            
        except Exception as e:
            self.logger.error(f"处理图片消息段时出错: {e}")
    
    async def _process_at_segment(self, segment: MessageSegment):
        """
        处理@消息段
        
        Args:
            segment (MessageSegment): @消息段
        """
        try:
            # 验证QQ号格式
            if segment.qq != 'all':
                if not segment.qq.isdigit():
                    self.logger.warning(f"无效的QQ号格式: {segment.qq}")
                    segment.valid = False
                else:
                    segment.valid = True
            else:
                segment.valid = True
            
        except Exception as e:
            self.logger.error(f"处理@消息段时出错: {e}")
    
    async def _process_face_segment(self, segment: MessageSegment):
        """
        处理表情消息段
        
        Args:
            segment (MessageSegment): 表情消息段
        """
        try:
            # 验证表情ID
            if segment.id.isdigit():
                segment.face_id = int(segment.id)
                segment.valid = True
            else:
                self.logger.warning(f"无效的表情ID: {segment.id}")
                segment.valid = False
            
        except Exception as e:
            self.logger.error(f"处理表情消息段时出错: {e}")
    
    async def _process_reply_segment(self, segment: MessageSegment):
        """
        处理回复消息段
        
        Args:
            segment (MessageSegment): 回复消息段
        """
        try:
            # 验证回复消息ID
            if segment.id:
                segment.valid = True
            else:
                self.logger.warning("回复消息段缺少消息ID")
                segment.valid = False
            
        except Exception as e:
            self.logger.error(f"处理回复消息段时出错: {e}")
    
    async def _analyze_text_content(self, text: str, analysis: Dict[str, Any]):
        """
        分析文本内容
        
        Args:
            text (str): 文本内容
            analysis (Dict[str, Any]): 分析结果字典
        """
        try:
            # URL检测
            if self.url_pattern.search(text):
                analysis['has_url'] = True
            
            # 邮箱检测
            if self.email_pattern.search(text):
                analysis['has_email'] = True
            
            # 手机号检测
            if self.phone_pattern.search(text):
                analysis['has_phone'] = True
            
            # 表情符号检测
            if self.emoji_pattern.search(text):
                analysis['has_emoji'] = True
            
        except Exception as e:
            self.logger.error(f"分析文本内容时出错: {e}")
    
    def _check_sensitive_words(self, text: str) -> bool:
        """
        检查敏感词
        
        Args:
            text (str): 文本内容
            
        Returns:
            bool: 是否包含敏感词
        """
        try:
            text_lower = text.lower()
            for word in self.sensitive_words:
                if word.lower() in text_lower:
                    return True
            return False
        except Exception as e:
            self.logger.error(f"检查敏感词时出错: {e}")
            return False
    
    def _generate_message_summary(self, parsed_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成消息摘要
        
        Args:
            parsed_result (Dict[str, Any]): 解析结果
            
        Returns:
            Dict[str, Any]: 消息摘要
        """
        try:
            analysis = parsed_result['analysis']
            
            summary = {
                'type': 'unknown',
                'description': '',
                'priority': 'normal',
                'requires_processing': False
            }
            
            # 判断消息类型
            if analysis['image_segments'] > 0 and analysis['text_segments'] > 0:
                summary['type'] = 'mixed'
                summary['description'] = f"包含 {analysis['image_segments']} 张图片和文本内容"
                summary['requires_processing'] = True
            elif analysis['image_segments'] > 0:
                summary['type'] = 'image_only'
                summary['description'] = f"纯图片消息，包含 {analysis['image_segments']} 张图片"
                summary['requires_processing'] = True
            elif analysis['text_segments'] > 0:
                summary['type'] = 'text_only'
                summary['description'] = f"纯文本消息，长度 {analysis['total_text_length']} 字符"
                summary['requires_processing'] = analysis['total_text_length'] > 0
            elif analysis['mention_segments'] > 0:
                summary['type'] = 'mention'
                summary['description'] = f"包含 {analysis['mention_segments']} 个@提及"
                summary['requires_processing'] = True
            else:
                summary['type'] = 'other'
                summary['description'] = "其他类型消息"
            
            # 设置优先级
            if analysis['contains_sensitive']:
                summary['priority'] = 'high'
            elif analysis['mention_segments'] > 0:
                summary['priority'] = 'high'
            elif analysis['image_segments'] > 0:
                summary['priority'] = 'medium'
            
            return summary
            
        except Exception as e:
            self.logger.error(f"生成消息摘要时出错: {e}")
            return {
                'type': 'error',
                'description': '摘要生成失败',
                'priority': 'normal',
                'requires_processing': False
            }
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取解析统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        uptime = datetime.now() - self.start_time
        
        return {
            'parsed_count': self.parsed_count,
            'text_segments_count': self.text_segments_count,
            'image_segments_count': self.image_segments_count,
            'other_segments_count': self.other_segments_count,
            'uptime_seconds': uptime.total_seconds(),
            'uptime_str': str(uptime),
            'messages_per_minute': (self.parsed_count / max(uptime.total_seconds() / 60, 1))
        }
    
    def reset_statistics(self):
        """
        重置统计信息
        """
        self.parsed_count = 0
        self.text_segments_count = 0
        self.image_segments_count = 0
        self.other_segments_count = 0
        self.start_time = datetime.now()
        self.logger.info("消息解析器统计信息已重置")