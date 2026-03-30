#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目消息接收器类
"""

import json
import logging
import time
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import traceback


class MessageReceiver:
    """
    NapCat消息接收器类
    
    负责接收来自NapCat的原始消息，进行基础验证和格式化处理。
    参考示例项目中的消息接收机制，适配当前项目架构。
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化消息接收器
        
        Args:
            config (Dict[str, Any]): 配置字典
        """
        self.config = config
        self.logger = logging.getLogger("MessageReceiver")
        
        # 消息统计
        self.received_count = 0
        self.processed_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        
        # 支持的消息类型
        self.supported_message_types = {
            'private',  # 私聊消息
            'group',    # 群聊消息
            'temp'      # 临时会话消息
        }
        
        # 支持的消息段类型
        self.supported_segment_types = {
            'text',     # 文本消息
            'image',    # 图片消息
            'face',     # QQ表情
            'record',   # 语音消息
            'video',    # 视频消息
            'at',       # @某人
            'reply',    # 回复消息
            'forward',  # 转发消息
            'file',     # 文件消息
            'json',     # JSON消息
            'xml',      # XML消息
            'poke',     # 戳一戳
            'gift',     # 礼物消息
            'market_face',  # 商城表情
            'music',    # 音乐分享
            'share',    # 链接分享
            'contact',  # 推荐好友/群
            'location', # 位置消息
            'shake',    # 窗口抖动
            'anonymous' # 匿名消息
        }
        
        self.logger.info("NapCat消息接收器初始化完成")
    
    async def receive_message(self, raw_message: str, websocket=None) -> Optional[Dict[str, Any]]:
        """
        接收并处理原始消息
        
        Args:
            raw_message (str): 原始JSON消息字符串
            websocket: WebSocket连接对象（可选）
            
        Returns:
            Optional[Dict[str, Any]]: 处理后的消息数据，如果处理失败返回None
        """
        try:
            self.received_count += 1
            
            # 解析JSON消息
            message_data = self._parse_json_message(raw_message)
            if not message_data:
                return None
            
            # 验证消息格式
            if not self._validate_message_format(message_data):
                return None
            
            # 标准化消息格式
            standardized_message = self._standardize_message_format(message_data)
            if not standardized_message:
                return None
            
            # 添加接收时间戳和元数据
            standardized_message['received_at'] = time.time()
            standardized_message['received_datetime'] = datetime.now().isoformat()
            if websocket:
                standardized_message['websocket'] = websocket
            
            self.processed_count += 1
            self.logger.debug(f"成功处理消息，类型: {standardized_message.get('message_type')}, "
                            f"用户: {standardized_message.get('user_id')}")
            
            return standardized_message
            
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"接收消息时出错: {e}")
            self.logger.error(traceback.format_exc())
            return None
    
    def _parse_json_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        """
        解析JSON消息
        
        Args:
            raw_message (str): 原始JSON消息字符串
            
        Returns:
            Optional[Dict[str, Any]]: 解析后的消息数据
        """
        try:
            message_data = json.loads(raw_message)
            return message_data
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析失败: {e}")
            self.logger.error(f"原始消息: {raw_message[:200]}...")  # 只记录前200个字符
            return None
        except Exception as e:
            self.logger.error(f"解析消息时出现未知错误: {e}")
            return None
    
    def _validate_message_format(self, message_data: Dict[str, Any]) -> bool:
        """
        验证消息格式是否符合NapCat标准
        
        Args:
            message_data (Dict[str, Any]): 消息数据
            
        Returns:
            bool: 验证是否通过
        """
        try:
            # 检查基本字段
            post_type = message_data.get('post_type')
            if not post_type:
                self.logger.warning("消息缺少post_type字段")
                return False
            
            # 只处理message类型的消息
            if post_type != 'message':
                self.logger.debug(f"跳过非消息类型: {post_type}")
                return False
            
            # 检查消息类型
            message_type = message_data.get('message_type')
            if not message_type or message_type not in self.supported_message_types:
                self.logger.warning(f"不支持的消息类型: {message_type}")
                return False
            
            # 检查必需字段
            required_fields = ['user_id', 'message', 'time']
            for field in required_fields:
                if field not in message_data:
                    self.logger.warning(f"消息缺少必需字段: {field}")
                    return False
            
            # 检查群聊消息的群号
            if message_type == 'group' and 'group_id' not in message_data:
                self.logger.warning("群聊消息缺少group_id字段")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证消息格式时出错: {e}")
            return False
    
    def _standardize_message_format(self, message_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        标准化消息格式，确保字段类型和格式一致
        
        Args:
            message_data (Dict[str, Any]): 原始消息数据
            
        Returns:
            Optional[Dict[str, Any]]: 标准化后的消息数据
        """
        try:
            standardized = {}
            
            # 基本信息
            standardized['post_type'] = message_data.get('post_type')
            standardized['message_type'] = message_data.get('message_type')
            standardized['sub_type'] = message_data.get('sub_type', 'normal')
            standardized['message_id'] = message_data.get('message_id')
            standardized['user_id'] = str(message_data.get('user_id', ''))
            standardized['time'] = message_data.get('time')
            
            # 群聊相关信息
            if message_data.get('message_type') == 'group':
                standardized['group_id'] = str(message_data.get('group_id', ''))
                standardized['anonymous'] = message_data.get('anonymous')
            
            # 发送者信息
            sender = message_data.get('sender', {})
            standardized['sender'] = {
                'user_id': str(sender.get('user_id', standardized['user_id'])),
                'nickname': sender.get('nickname', ''),
                'card': sender.get('card', ''),
                'sex': sender.get('sex', 'unknown'),
                'age': sender.get('age', 0),
                'area': sender.get('area', ''),
                'level': sender.get('level', ''),
                'role': sender.get('role', 'member'),
                'title': sender.get('title', '')
            }
            
            # 消息内容处理
            message_content = message_data.get('message', [])
            if isinstance(message_content, str):
                # 如果是字符串，转换为标准的消息段格式
                standardized['message'] = [{
                    'type': 'text',
                    'data': {'text': message_content}
                }]
                standardized['raw_message'] = message_content
            elif isinstance(message_content, list):
                # 验证和标准化消息段
                standardized_segments = []
                raw_text_parts = []
                
                for segment in message_content:
                    if not isinstance(segment, dict):
                        continue
                    
                    segment_type = segment.get('type', '')
                    segment_data = segment.get('data', {})
                    
                    # 验证消息段类型
                    if segment_type not in self.supported_segment_types:
                        self.logger.warning(f"不支持的消息段类型: {segment_type}")
                        continue
                    
                    # 标准化消息段
                    standardized_segment = {
                        'type': segment_type,
                        'data': segment_data
                    }
                    standardized_segments.append(standardized_segment)
                    
                    # 提取文本内容用于raw_message
                    if segment_type == 'text':
                        raw_text_parts.append(segment_data.get('text', ''))
                    elif segment_type == 'at':
                        qq = segment_data.get('qq', '')
                        if qq == 'all':
                            raw_text_parts.append('@全体成员')
                        else:
                            raw_text_parts.append(f'@{qq}')
                    elif segment_type == 'face':
                        raw_text_parts.append(f'[表情:{segment_data.get("id", "")}]')
                    elif segment_type == 'image':
                        raw_text_parts.append('[图片]')
                    elif segment_type == 'record':
                        raw_text_parts.append('[语音]')
                    elif segment_type == 'video':
                        raw_text_parts.append('[视频]')
                    else:
                        raw_text_parts.append(f'[{segment_type}]')
                
                standardized['message'] = standardized_segments
                standardized['raw_message'] = ''.join(raw_text_parts)
            else:
                self.logger.error(f"不支持的消息格式: {type(message_content)}")
                return None
            
            # 添加消息统计信息
            standardized['message_stats'] = {
                'segment_count': len(standardized['message']),
                'has_text': any(seg['type'] == 'text' for seg in standardized['message']),
                'has_image': any(seg['type'] == 'image' for seg in standardized['message']),
                'has_at': any(seg['type'] == 'at' for seg in standardized['message']),
                'has_face': any(seg['type'] == 'face' for seg in standardized['message']),
                'text_length': len(standardized['raw_message'])
            }
            
            return standardized
            
        except Exception as e:
            self.logger.error(f"标准化消息格式时出错: {e}")
            self.logger.error(traceback.format_exc())
            return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取消息接收统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        uptime = datetime.now() - self.start_time
        
        return {
            'received_count': self.received_count,
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'success_rate': (self.processed_count / max(self.received_count, 1)) * 100,
            'uptime_seconds': uptime.total_seconds(),
            'uptime_str': str(uptime),
            'messages_per_minute': (self.received_count / max(uptime.total_seconds() / 60, 1))
        }
    
    def reset_statistics(self):
        """
        重置统计信息
        """
        self.received_count = 0
        self.processed_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        self.logger.info("消息接收器统计信息已重置")