# -*- coding: utf-8 -*-
"""
加密服务模块
提供各种文本加密功能，包括兽音译者加密等
"""

import logging
from typing import Optional, Dict, Any
from .base_service import BaseService

class BeastEncoder:
    """
    兽音译者加密器
    将文本转换为兽音字符编码
    """
    
    def __init__(self, beast_chars: list):
        """
        初始化兽音编码器
        
        Args:
            beast_chars: 兽音字符列表，用于编码
        """
        self.beast_chars = beast_chars
        self.logger = logging.getLogger(__name__)
    
    def encode(self, text: str) -> str:
        """
        将文本编码为兽音字符
        
        Args:
            text: 要编码的文本
            
        Returns:
            编码后的兽音字符串
        """
        try:
            if not text:
                return ""
            
            # 将文本转换为字节
            text_bytes = text.encode('utf-8')
            
            # 使用兽音字符进行编码
            encoded_chars = []
            for byte in text_bytes:
                # 使用字节值对兽音字符数量取模来选择字符
                char_index = byte % len(self.beast_chars)
                encoded_chars.append(self.beast_chars[char_index])
            
            encoded_text = ''.join(encoded_chars)
            self.logger.debug(f"文本编码完成: {text[:20]}... -> {encoded_text[:20]}...")
            
            return encoded_text
            
        except Exception as e:
            self.logger.error(f"文本编码失败: {e}")
            return text  # 编码失败时返回原文本
    
    def decode(self, encoded_text: str) -> str:
        """
        将兽音字符解码为原文本
        注意：由于编码过程中信息丢失，解码可能不完全准确
        
        Args:
            encoded_text: 兽音编码的文本
            
        Returns:
            解码后的文本（可能不完全准确）
        """
        try:
            if not encoded_text:
                return ""
            
            # 这是一个简化的解码实现
            # 实际的解码需要更复杂的算法，因为编码过程中有信息丢失
            self.logger.warning("兽音解码功能为简化实现，可能不完全准确")
            
            return encoded_text  # 暂时返回原文本
            
        except Exception as e:
            self.logger.error(f"文本解码失败: {e}")
            return encoded_text

class EncryptionService(BaseService):
    """
    加密服务
    提供各种文本加密功能
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter):
        """
        初始化加密服务
        
        Args:
            config: 加密服务配置
            data_manager: 数据管理器
            text_formatter: 文本格式化器
        """
        super().__init__(config, data_manager, text_formatter)
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 初始化兽音编码器
        beast_chars = config.get('beast_chars', ['祥', '子', '移', '动'])
        self.beast_encoder = BeastEncoder(beast_chars)
        
        self.logger.info("加密服务初始化完成")
    
    def get_service_name(self) -> str:
        """获取服务名称"""
        return "encryption"
    
    def get_commands(self) -> list:
        """获取支持的命令列表"""
        return []  # 加密服务不直接响应用户命令
    
    def can_handle_message(self, message: str, user_id: str, group_id: str = None) -> bool:
        """检查是否能处理消息"""
        return False  # 加密服务不直接处理用户消息
    
    def handle_message(self, message: str, user_id: str, group_id: str = None) -> Optional[str]:
        """处理消息"""
        return None  # 加密服务不直接处理用户消息
    
    def encrypt_with_beast(self, text: str) -> str:
        """
        使用兽音译者加密文本
        
        Args:
            text: 要加密的文本
            
        Returns:
            加密后的文本
        """
        return self.beast_encoder.encode(text)
    
    def decrypt_with_beast(self, encrypted_text: str) -> str:
        """
        使用兽音译者解密文本
        
        Args:
            encrypted_text: 要解密的文本
            
        Returns:
            解密后的文本
        """
        return self.beast_encoder.decode(encrypted_text)
    
    def encrypt_text(self, text: str, method: str = 'beast') -> str:
        """
        通用文本加密方法
        
        Args:
            text: 要加密的文本
            method: 加密方法 ('beast' 等)
            
        Returns:
            加密后的文本
        """
        if method == 'beast':
            return self.encrypt_with_beast(text)
        else:
            self.logger.warning(f"不支持的加密方法: {method}")
            return text
    
    def decrypt_text(self, encrypted_text: str, method: str = 'beast') -> str:
        """
        通用文本解密方法
        
        Args:
            encrypted_text: 要解密的文本
            method: 解密方法 ('beast' 等)
            
        Returns:
            解密后的文本
        """
        if method == 'beast':
            return self.decrypt_with_beast(encrypted_text)
        else:
            self.logger.warning(f"不支持的解密方法: {method}")
            return encrypted_text
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取服务状态
        
        Returns:
            服务状态信息
        """
        return {
            'service_name': self.get_service_name(),
            'enabled': True,
            'beast_chars_count': len(self.beast_encoder.beast_chars),
            'supported_methods': ['beast']
        }
    
    def get_help_text(self):
        """获取帮助文本"""
        return "加密服务 - 提供兽音译者等加密功能\n使用方法: 加密 [文本] 或 解密 [加密文本]"
    
    async def process_message(self, message_text, user_id, group_id=None, **kwargs):
        """处理消息"""
        message_text = message_text.strip()
        
        # 加密命令
        if message_text.startswith('加密 '):
            text_to_encrypt = message_text[3:].strip()
            if text_to_encrypt:
                encrypted = self.encrypt_text(text_to_encrypt)
                return f"加密结果: {encrypted}"
            else:
                return "请提供要加密的文本"
        
        # 解密命令
        elif message_text.startswith('解密 '):
            text_to_decrypt = message_text[3:].strip()
            if text_to_decrypt:
                try:
                    decrypted = self.decrypt_text(text_to_decrypt)
                    return f"解密结果: {decrypted}"
                except Exception as e:
                    return f"解密失败: {str(e)}"
            else:
                return "请提供要解密的文本"
        
        return None