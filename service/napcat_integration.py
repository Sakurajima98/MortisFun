#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import logging
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
import traceback

from message_receiver import MessageReceiver
from message_parser import MessageParser
from image_processor import ImageProcessor


class NapCatIntegration:
    """
    NapCat集成类
    
    负责整合消息接收、解析和处理功能，提供统一的接口给主应用程序使用。
    """
    
    def __init__(self, config: Dict[str, Any], chat_service=None):
        """
        初始化NapCat集成模块
        
        Args:
            config (Dict[str, Any]): 配置字典
            chat_service: 对话服务实例
        """
        self.config = config
        self.logger = logging.getLogger("NapCatIntegration")
        
        # 初始化各个组件
        self.message_receiver = MessageReceiver(config.get('message_receiver', {}))
        self.message_parser = MessageParser(config.get('message_parser', {}))
        
        # 图片处理器需要异步初始化
        self.image_processor = None
        self.image_processor_config = config.get('image_processor', {})
        
        # 对话服务
        self.chat_service = chat_service
        
        # 处理统计
        self.total_messages = 0
        self.successful_messages = 0
        self.failed_messages = 0
        self.image_messages = 0
        self.text_messages = 0
        self.chat_responses = 0
        self.start_time = datetime.now()
        
        # 配置参数
        self.enable_image_processing = config.get('enable_image_processing', True)
        self.enable_chat_service = config.get('enable_chat_service', True)
        self.enable_detailed_logging = config.get('enable_detailed_logging', False)
        self.max_concurrent_processing = config.get('max_concurrent_processing', 10)
        
        # 创建信号量限制并发处理
        self.processing_semaphore = asyncio.Semaphore(self.max_concurrent_processing)
        
        self.logger.info("NapCat集成模块初始化完成")
    
    def set_chat_service(self, chat_service):
        """
        设置对话服务实例
        
        Args:
            chat_service: 对话服务实例
        """
        self.chat_service = chat_service
        self.logger.info("对话服务已设置")
    
    async def initialize(self):
        """
        异步初始化
        """
        try:
            # 初始化图片处理器
            if self.enable_image_processing:
                self.image_processor = ImageProcessor(self.image_processor_config)
                await self.image_processor.__aenter__()
                self.logger.info("图片处理器初始化完成")
            
            self.logger.info("NapCat集成模块异步初始化完成")
            
        except Exception as e:
            self.logger.error(f"异步初始化失败: {e}")
            raise
    
    async def cleanup(self):
        """
        清理资源
        """
        try:
            if self.image_processor:
                await self.image_processor.__aexit__(None, None, None)
                self.logger.info("图片处理器资源已清理")
            
            self.logger.info("NapCat集成模块资源清理完成")
            
        except Exception as e:
            self.logger.error(f"清理资源时出错: {e}")
    
    async def process_napcat_message(self, raw_message: str, websocket=None) -> Optional[Dict[str, Any]]:
        """
        处理来自NapCat的消息
        
        Args:
            raw_message (str): 原始消息JSON字符串
            websocket: WebSocket连接对象
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果
        """
        async with self.processing_semaphore:
            try:
                self.total_messages += 1
                
                if self.enable_detailed_logging:
                    self.logger.debug(f"开始处理消息: {raw_message[:100]}...")
                
                # 第一步：消息接收和基础验证
                received_message = await self.message_receiver.receive_message(raw_message, websocket)
                if not received_message:
                    self.failed_messages += 1
                    self.logger.warning("消息接收失败")
                    return None
                
                # 第二步：消息解析
                parsed_message = await self.message_parser.parse_message(received_message)
                if not parsed_message or not parsed_message.get('success', True):
                    self.failed_messages += 1
                    self.logger.warning("消息解析失败")
                    return None
                
                # 第三步：图片处理（如果有图片）
                if self.enable_image_processing and parsed_message.get('images'):
                    await self._process_images_in_message(parsed_message)
                    self.image_messages += 1
                else:
                    self.text_messages += 1
                
                # 第四步：构建最终结果
                result = await self._build_processing_result(received_message, parsed_message)
                
                self.successful_messages += 1
                
                if self.enable_detailed_logging:
                    self.logger.debug(f"消息处理完成: {result.get('summary', {}).get('type', 'unknown')}")
                
                return result
                
            except Exception as e:
                self.failed_messages += 1
                self.logger.error(f"处理NapCat消息时出错: {e}")
                self.logger.error(traceback.format_exc())
                return None
    
    async def _process_images_in_message(self, parsed_message: Dict[str, Any]):
        """
        处理消息中的图片
        
        Args:
            parsed_message (Dict[str, Any]): 解析后的消息
        """
        try:
            if not self.image_processor:
                self.logger.warning("图片处理器未初始化，跳过图片处理")
                return
            
            images = parsed_message.get('images', [])
            processed_images = []
            
            for image_info in images:
                # 获取对应的消息段
                segment_index = image_info.get('segment_index', -1)
                if segment_index >= 0 and segment_index < len(parsed_message.get('segments', [])):
                    segment = parsed_message['segments'][segment_index]
                    
                    # 处理图片
                    processed_image = await self.image_processor.process_image_segment(segment.raw)
                    if processed_image:
                        processed_images.append({
                            **image_info,
                            'processed': processed_image,
                            'description': processed_image.get('description', ''),
                            'file_path': processed_image.get('file_path', ''),
                            'cached': processed_image.get('cached', False)
                        })
                    else:
                        processed_images.append({
                            **image_info,
                            'processed': None,
                            'error': '图片处理失败'
                        })
            
            # 更新解析结果
            parsed_message['processed_images'] = processed_images
            
        except Exception as e:
            self.logger.error(f"处理消息中的图片时出错: {e}")
    
    async def _build_processing_result(self, received_message: Dict[str, Any], 
                                     parsed_message: Dict[str, Any]) -> Dict[str, Any]:
        """
        构建处理结果
        
        Args:
            received_message (Dict[str, Any]): 接收到的消息
            parsed_message (Dict[str, Any]): 解析后的消息
            
        Returns:
            Dict[str, Any]: 最终处理结果
        """
        try:
            # 基础信息
            result = {
                'success': True,
                'processed_at': datetime.now().isoformat(),
                'message_id': received_message.get('message_id'),
                'user_id': received_message.get('user_id'),
                'message_type': received_message.get('message_type'),
                'group_id': received_message.get('group_id'),
                'sender': received_message.get('sender', {}),
                'raw_message': received_message.get('raw_message', ''),
                'time': received_message.get('time')
            }
            
            # 解析结果
            result['parsed'] = {
                'text_content': parsed_message.get('text_content', ''),
                'segments_count': parsed_message.get('analysis', {}).get('total_segments', 0),
                'has_text': parsed_message.get('analysis', {}).get('text_segments', 0) > 0,
                'has_image': parsed_message.get('analysis', {}).get('image_segments', 0) > 0,
                'has_mention': parsed_message.get('analysis', {}).get('mention_segments', 0) > 0,
                'mentions': parsed_message.get('mentions', []),
                'summary': parsed_message.get('summary', {})
            }
            
            # 图片处理结果
            if parsed_message.get('processed_images'):
                result['images'] = []
                for img in parsed_message['processed_images']:
                    image_result = {
                        'description': img.get('description', ''),
                        'cached': img.get('cached', False),
                        'width': img.get('width', 0),
                        'height': img.get('height', 0),
                        'format': img.get('format', ''),
                        'file_size': img.get('file_size', 0)
                    }
                    
                    if img.get('processed'):
                        image_result['processed'] = True
                        image_result['file_path'] = img['processed'].get('file_path', '')
                    else:
                        image_result['processed'] = False
                        image_result['error'] = img.get('error', '处理失败')
                    
                    result['images'].append(image_result)
            
            # 生成用于后续处理的格式化文本
            result['formatted_text'] = self._generate_formatted_text(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"构建处理结果时出错: {e}")
            return {
                'success': False,
                'error': str(e),
                'processed_at': datetime.now().isoformat()
            }
    
    def _generate_formatted_text(self, result: Dict[str, Any]) -> str:
        """
        生成格式化文本，用于后续的AI处理
        
        Args:
            result (Dict[str, Any]): 处理结果
            
        Returns:
            str: 格式化文本
        """
        try:
            parts = []
            
            # 添加文本内容
            text_content = result.get('parsed', {}).get('text_content', '').strip()
            if text_content:
                parts.append(text_content)
            
            # 添加图片描述
            images = result.get('images', [])
            for i, img in enumerate(images, 1):
                if img.get('description'):
                    if len(images) == 1:
                        parts.append(f"[图片描述: {img['description']}]")
                    else:
                        parts.append(f"[图片{i}描述: {img['description']}]")
            
            # 添加提及信息
            mentions = result.get('parsed', {}).get('mentions', [])
            if mentions:
                mention_texts = []
                for mention in mentions:
                    if mention.get('is_all'):
                        mention_texts.append("@全体成员")
                    else:
                        mention_texts.append(f"@{mention.get('qq', '')}")
                if mention_texts:
                    parts.append(f"[提及: {', '.join(mention_texts)}]")
            
            return ' '.join(parts) if parts else ''
            
        except Exception as e:
            self.logger.error(f"生成格式化文本时出错: {e}")
            return result.get('parsed', {}).get('text_content', '')
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取处理统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        uptime = datetime.now() - self.start_time
        
        stats = {
            'total_messages': self.total_messages,
            'successful_messages': self.successful_messages,
            'failed_messages': self.failed_messages,
            'image_messages': self.image_messages,
            'text_messages': self.text_messages,
            'success_rate': (self.successful_messages / max(self.total_messages, 1)) * 100,
            'uptime_seconds': uptime.total_seconds(),
            'uptime_str': str(uptime),
            'messages_per_minute': (self.total_messages / max(uptime.total_seconds() / 60, 1))
        }
        
        # 添加子组件统计
        stats['receiver_stats'] = self.message_receiver.get_statistics()
        stats['parser_stats'] = self.message_parser.get_statistics()
        
        if self.image_processor:
            stats['image_processor_stats'] = self.image_processor.get_statistics()
        
        return stats
    
    def reset_statistics(self):
        """
        重置统计信息
        """
        self.total_messages = 0
        self.successful_messages = 0
        self.failed_messages = 0
        self.image_messages = 0
        self.text_messages = 0
        self.start_time = datetime.now()
        
        # 重置子组件统计
        self.message_receiver.reset_statistics()
        self.message_parser.reset_statistics()
        
        if self.image_processor:
            self.image_processor.reset_statistics()
        
        self.logger.info("NapCat集成模块统计信息已重置")
    
    async def health_check(self) -> Dict[str, Any]:
        """
        健康检查
        
        Returns:
            Dict[str, Any]: 健康状态
        """
        try:
            health = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'components': {
                    'message_receiver': 'healthy',
                    'message_parser': 'healthy',
                    'image_processor': 'healthy' if self.image_processor else 'disabled'
                },
                'statistics': self.get_statistics()
            }
            
            # 检查各组件状态
            if self.failed_messages > self.successful_messages:
                health['status'] = 'degraded'
                health['warning'] = '失败消息数量过多'
            
            return health
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }


# 配置示例
DEFAULT_CONFIG = {
    'message_receiver': {
        'max_message_size': 1024 * 1024,  # 1MB
        'enable_validation': True,
        'supported_message_types': ['private', 'group', 'temp']
    },
    'message_parser': {
        'max_text_length': 2000,
        'enable_text_filtering': True,
        'sensitive_words': []
    },
    'image_processor': {
        'image_cache_dir': './cache/images',
        'cache_db_path': './cache/image_cache.db',
        'max_image_size': 10 * 1024 * 1024,  # 10MB
        'max_image_width': 2048,
        'max_image_height': 2048,
        'supported_formats': ['JPEG', 'PNG', 'GIF', 'WEBP', 'BMP'],
        'enable_gif_processing': True,
        'max_gif_frames': 10,
        'gif_similarity_threshold': 0.9,
        'request_timeout': 30,
        'max_retries': 3
    },
    'enable_image_processing': True,
    'enable_detailed_logging': False,
    'max_concurrent_processing': 10
}


async def create_napcat_integration(config: Dict[str, Any] = None) -> NapCatIntegration:
    """
    创建并初始化NapCat集成实例
    
    Args:
        config (Dict[str, Any]): 配置字典，如果为None则使用默认配置
        
    Returns:
        NapCatIntegration: 初始化完成的集成实例
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    integration = NapCatIntegration(config)
    await integration.initialize()
    
    return integration