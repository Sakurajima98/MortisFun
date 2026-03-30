#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目语音服务模块

本文件实现语音消息发送功能，包括：
1. 语音文件管理
2. 语音消息发送
3. 语音文件格式验证
4. 语音文件路径处理

作者: Mortisfun Team
创建时间: 2025
"""

import os
from typing import Dict, Any, Optional, List
from .base_service import BaseService


class VoiceService(BaseService):
    """
    语音服务类
    
    负责处理语音消息的发送和管理，包括语音文件的验证、
    路径处理和消息构建等功能。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None):
        """
        初始化语音服务
        
        Args:
            config (Dict[str, Any]): 配置信息
            data_manager: 数据管理器
            text_formatter: 文本格式化器
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 语音文件配置
        voice_config = config.get('services', {}).get('voice', {})
        self.voice_directory = voice_config.get('voice_directory', 'output')
        self.supported_formats = voice_config.get('supported_formats', ['.wav', '.mp3', '.ogg', '.m4a'])
        self.max_file_size = voice_config.get('max_file_size_mb', 10) * 1024 * 1024  # 转换为字节
        
        # 确保语音文件夹存在
        if not os.path.exists(self.voice_directory):
            os.makedirs(self.voice_directory)
            self.log_unified("INFO", f"创建语音文件夹: {self.voice_directory}", group_id="system", user_id="system")
    
    def get_available_voices(self) -> List[str]:
        """
        获取可用的语音文件列表
        
        Returns:
            List[str]: 语音文件名列表
        """
        try:
            voice_files = []
            if os.path.exists(self.voice_directory):
                for file in os.listdir(self.voice_directory):
                    file_path = os.path.join(self.voice_directory, file)
                    if os.path.isfile(file_path) and self._is_valid_voice_file(file_path):
                        voice_files.append(file)
            
            self.log_unified("INFO", f"找到 {len(voice_files)} 个可用语音文件", group_id="system", user_id="system")
            return sorted(voice_files)
            
        except Exception as e:
            self.log_unified("ERROR", f"获取语音文件列表失败: {e}", group_id="system", user_id="system")
            return []
    
    def _is_valid_voice_file(self, file_path: str) -> bool:
        """
        验证语音文件是否有效
        
        Args:
            file_path (str): 文件路径
            
        Returns:
            bool: 文件是否有效
        """
        try:
            # 检查文件扩展名
            _, ext = os.path.splitext(file_path)
            if ext.lower() not in self.supported_formats:
                return False
            
            # 检查文件大小
            file_size = os.path.getsize(file_path)
            if file_size > self.max_file_size:
                self.logger.warning(f"语音文件过大: {file_path} ({file_size} bytes)")
                return False
            
            # 检查文件是否可读
            if not os.access(file_path, os.R_OK):
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证语音文件失败: {file_path} - {e}")
            return False
    
    def get_voice_file_path(self, filename: str) -> Optional[str]:
        """
        获取语音文件的完整路径
        
        Args:
            filename (str): 文件名
            
        Returns:
            Optional[str]: 文件的完整路径，如果文件不存在则返回None
        """
        try:
            # 如果已经是绝对路径，直接验证
            if os.path.isabs(filename):
                if self._is_valid_voice_file(filename):
                    return filename
                else:
                    return None
            
            # 构建完整路径
            full_path = os.path.join(self.voice_directory, filename)
            
            # 验证文件
            if os.path.exists(full_path) and self._is_valid_voice_file(full_path):
                return os.path.abspath(full_path)
            
            return None
            
        except Exception as e:
            self.logger.error(f"获取语音文件路径失败: {filename} - {e}")
            return None
    
    def send_voice_message(self, target_type: str, target_id: str, voice_filename: str, 
                          text_content: str = "") -> Dict[str, Any]:
        """
        发送语音消息
        
        Args:
            target_type (str): 目标类型 ('group' 或 'private')
            target_id (str): 目标ID（群号或用户ID）
            voice_filename (str): 语音文件名
            text_content (str): 附加的文本内容
            
        Returns:
            Dict[str, Any]: 发送结果
        """
        try:
            # 获取语音文件路径
            voice_path = self.get_voice_file_path(voice_filename)
            if not voice_path:
                return {
                    'success': False,
                    'message': f'语音文件不存在或无效: {voice_filename}',
                    'available_voices': self.get_available_voices()
                }
            
            # 构建响应数据
            if target_type == 'group':
                response_data = {
                    'target_type': 'group',
                    'target_id': target_id,
                    'content': text_content,
                    'voice_path': voice_path
                }
            elif target_type == 'private':
                response_data = {
                    'target_type': 'private',
                    'target_id': target_id,
                    'content': text_content,
                    'voice_path': voice_path
                }
            else:
                return {
                    'success': False,
                    'message': f'不支持的目标类型: {target_type}'
                }
            
            self.logger.info(f"准备发送语音消息: {voice_filename} 到 {target_type}:{target_id}")
            
            return {
                'success': True,
                'message': f'语音消息准备完成: {voice_filename}',
                'response_data': response_data
            }
            
        except Exception as e:
            self.logger.error(f"发送语音消息失败: {e}")
            return {
                'success': False,
                'message': f'发送语音消息失败: {str(e)}'
            }
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理语音相关命令
        
        Args:
            message (str): 用户消息
            user_id (str): 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果
        """
        try:
            message = message.strip()
            
            # 语音列表命令
            if message in ['/语音列表', '/voice', '/voices']:
                voices = self.get_available_voices()
                if not voices:
                    return {
                        "content": "❌ 当前没有可用的语音文件",
                        "voice_path": None
                    }
                
                voice_list = "\n".join([f"📢 {voice}" for voice in voices])
                content = f"🎵 可用语音文件列表：\n{voice_list}\n\n💡 使用方法：/播放语音 文件名"
                return {
                    "content": content,
                    "voice_path": None
                }
            
            # 语音输出命令
            if message.startswith('/语音输出') or message.startswith('/voice_output'):
                parts = message.split(' ', 1)
                if len(parts) < 2 or not parts[1].strip():
                    return {
                        "content": "❌ 请指定语音文件名\n💡 使用方法：/语音输出 文件名",
                        "voice_path": None
                    }
                
                voice_filename = parts[1].strip()
                voice_path = self.get_voice_file_path(voice_filename)
                
                if voice_path:
                    return {
                        "content": f"🎵 语音输出: {voice_filename}",
                        "voice_path": voice_path
                    }
                else:
                    voices = self.get_available_voices()
                    error_msg = f"❌ 找不到语音文件: {voice_filename}"
                    if voices:
                        voice_list = "\n".join([f"📢 {voice}" for voice in voices[:5]])  # 只显示前5个
                        error_msg += f"\n\n🎵 可用语音文件：\n{voice_list}"
                        if len(voices) > 5:
                            error_msg += f"\n... 还有 {len(voices) - 5} 个文件"
                    return {
                        "content": error_msg,
                        "voice_path": None
                    }
            
            # 播放语音命令
            if message.startswith('/播放语音 ') or message.startswith('/play '):
                parts = message.split(' ', 1)
                if len(parts) < 2:
                    return {
                        "content": "❌ 请指定语音文件名\n💡 使用方法：/播放语音 文件名",
                        "voice_path": None
                    }
                
                voice_filename = parts[1].strip()
                voice_path = self.get_voice_file_path(voice_filename)
                
                if voice_path:
                    return {
                        "content": f"🎵 正在播放语音: {voice_filename}",
                        "voice_path": voice_path
                    }
                else:
                    voices = self.get_available_voices()
                    error_msg = f"❌ 找不到语音文件: {voice_filename}"
                    if voices:
                        voice_list = "\n".join([f"📢 {voice}" for voice in voices[:5]])  # 只显示前5个
                        error_msg += f"\n\n🎵 可用语音文件：\n{voice_list}"
                        if len(voices) > 5:
                            error_msg += f"\n... 还有 {len(voices) - 5} 个文件"
                    return {
                        "content": error_msg,
                        "voice_path": None
                    }
            
            return None
            
        except Exception as e:
            self.log_unified("ERROR", f"处理语音命令失败: {e}", user_id=user_id)
            return {
                "content": f"❌ 处理语音命令时发生错误: {str(e)}",
                "voice_path": None
            }
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取帮助文本
        
        Returns:
            Dict[str, Any]: 帮助信息
        """
        voices = self.get_available_voices()
        voice_count = len(voices)
        
        help_content = f"""🎵 语音服务帮助

📋 可用命令：
• /语音列表 - 查看所有可用语音文件
• /语音输出 <文件名> - 输出指定语音文件
• /播放语音 <文件名> - 播放指定语音文件

📊 当前状态：
• 语音文件目录: {self.voice_directory}
• 可用语音文件: {voice_count} 个
• 支持格式: {', '.join(self.supported_formats)}

💡 使用示例：
/语音列表
/语音输出 example.wav
/播放语音 example.wav

📝 注意事项：
• 语音文件需要放在 {self.voice_directory} 目录下
• 支持的格式: {', '.join(self.supported_formats)}
• 文件大小限制: {self.max_file_size // (1024*1024)}MB"""
        
        return {
            "content": help_content,
            "image_path": None
        }
    
    async def handle_message(self, message: str, user_id: str, context: Dict[str, Any]) -> Optional[str]:
        """
        处理语音相关命令
        
        Args:
            message (str): 用户消息
            user_id (str): 用户ID
            context (Dict[str, Any]): 消息上下文
            
        Returns:
            Optional[str]: 处理结果
        """
        try:
            message = message.strip()
            
            # 语音列表命令
            if message in ['/语音列表', '/voice', '/voices']:
                voices = self.get_available_voices()
                if not voices:
                    return "❌ 当前没有可用的语音文件"
                
                voice_list = "\n".join([f"📢 {voice}" for voice in voices])
                return f"🎵 可用语音文件列表：\n{voice_list}\n\n💡 使用方法：/播放语音 文件名"
            
            # 播放语音命令
            if message.startswith('/播放语音 ') or message.startswith('/play '):
                parts = message.split(' ', 1)
                if len(parts) < 2:
                    return "❌ 请指定语音文件名\n💡 使用方法：/播放语音 文件名"
                
                voice_filename = parts[1].strip()
                
                # 确定目标类型和ID
                target_type = context.get('message_type', 'private')
                if target_type == 'group':
                    target_id = context.get('group_id')
                else:
                    target_id = user_id
                
                # 发送语音消息
                result = self.send_voice_message(target_type, target_id, voice_filename)
                
                if result['success']:
                    # 返回特殊格式，让主程序知道这是语音消息
                    return f"VOICE_MESSAGE:{result['response_data']['voice_path']}"
                else:
                    error_msg = result['message']
                    if 'available_voices' in result:
                        voices = result['available_voices']
                        if voices:
                            voice_list = "\n".join([f"📢 {voice}" for voice in voices[:5]])  # 只显示前5个
                            error_msg += f"\n\n🎵 可用语音文件：\n{voice_list}"
                            if len(voices) > 5:
                                error_msg += f"\n... 还有 {len(voices) - 5} 个文件"
                    return error_msg
            
            return None
            
        except Exception as e:
            self.logger.error(f"处理语音命令失败: {e}")
            return f"❌ 处理语音命令时发生错误: {str(e)}"
    
    def get_service_info(self) -> Dict[str, Any]:
        """
        获取服务信息
        
        Returns:
            Dict[str, Any]: 服务信息
        """
        voices = self.get_available_voices()
        return {
            'name': '语音服务',
            'description': '发送语音消息',
            'commands': [
                '/语音列表 - 查看可用语音文件',
                '/播放语音 <文件名> - 播放指定语音文件'
            ],
            'voice_directory': self.voice_directory,
            'available_voices': len(voices),
            'supported_formats': self.supported_formats
        }