.3#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目兽音加密工具类

本文件实现兽音加密和解密功能，包括：
1. 字符串到十六进制的转换
2. 十六进制到字符串的转换
3. 兽音编码和解码
4. 支持从配置文件读取加密字符

基于兽音译者（兽语翻译）SDK的Python实现
作者: Mortisfun Team
创建时间: 2025
"""

import logging
import os
from typing import List, Optional, Dict, Any
from datetime import datetime


class BeastEncoder:
    """
    兽音加密器类
    
    实现兽音加密和解密功能，支持自定义加密字符集与可选的头尾标记（与常见“兽音译者”兼容）。
    默认使用['祥', '，', '移', '动']作为加密字符。
    """
    
    def __init__(self, beast_chars: Optional[List[str]] = None, use_header_footer: bool = True):
        """
        初始化兽音加密器
        
        Args:
            beast_chars (Optional[List[str]]): 自定义加密字符集，长度必须为4，依次对应0/1/2/3
            use_header_footer (bool): 是否在编码结果中添加头尾标记（与常见“兽音译者”前缀/后缀兼容）
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 设置默认加密字符
        if beast_chars is None:
            self.beast = ['祥', '，', '移', '动']
        else:
            if len(beast_chars) != 4:
                raise ValueError("加密字符集必须包含4个字符")
            self.beast = beast_chars
        
        # 是否使用头尾标记
        self.use_header_footer = use_header_footer
        
        # 记录初始化日志
        try:
            log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
            os.makedirs(log_dir, exist_ok=True)
            log_filename = f"beast_encoder_{datetime.now().strftime('%Y%m%d')}.log"
            log_filepath = os.path.join(log_dir, log_filename)
            self._log_unified("INFO", f"兽音加密器初始化完成，使用字符集: {self.beast}，头尾标记: {self.use_header_footer}", "system", "system", log_filepath)
        except Exception:
            pass
    
    def _log_unified(self, level: str, message: str, group_id: str = "system", user_id: str = "system", log_file_path: str = None):
        """
        统一日志记录方法
        
        Args:
            level: 日志级别 (INFO, WARNING, ERROR, DEBUG)
            message: 日志消息
            group_id: 群组ID
            user_id: 用户ID
            log_file_path: 日志文件路径
        """
        try:
            current_time = datetime.now()
            timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            log_msg = f"[{timestamp}][{level}][G:{group_id}][U:{user_id}]: {message}"
            
            # 打印到控制台
            print(log_msg)
            
            # 写入日志文件
            if log_file_path:
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(log_msg + "\n")
            
            # 使用logging模块记录
            logger = logging.getLogger(__name__)
            if level == "INFO":
                logger.info(message)
            elif level == "WARNING":
                logger.warning(message)
            elif level == "ERROR":
                logger.error(message)
            elif level == "DEBUG":
                logger.debug(message)
        except Exception:
            pass

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'BeastEncoder':
        """
        从配置文件创建兽音加密器实例
        
        Args:
            config (Dict[str, Any]): 配置字典
            
        Returns:
            BeastEncoder: 兽音加密器实例
        """
        beast_config = config.get('beast_encoder', {})
        beast_chars = beast_config.get('chars', ['祥', '，', '移', '动'])
        use_header_footer = beast_config.get('use_header_footer', True)
        return cls(beast_chars, use_header_footer)
    
    def str2hex(self, text: str) -> str:
        """
        将字符串转换为十六进制表示（UTF-16BE 编码）
        
        Args:
            text (str): 要转换的字符串
            
        Returns:
            str: 十六进制字符串（长度为原字符数×4）
        """
        try:
            # 使用 UTF-16BE 能稳定地将任意 Unicode 文本转换为“每个字符2字节”的十六进制串
            # 这样在 hex2str 中按字节解码可以保证还原一致，不会出现高位码点截断问题
            ret = text.encode('utf-16-be').hex()
            return ret
        except Exception as e:
            self.logger.error(f"字符串转十六进制失败: {e}")
            raise
    
    def hex2str(self, text: str) -> str:
        """
        将十六进制字符串转换为普通字符串（UTF-16BE 解码）
        
        Args:
            text (str): 十六进制字符串
            
        Returns:
            str: 转换后的字符串
        """
        try:
            # 与 str2hex 配套：以 UTF-16BE 从十六进制还原为原始字符串
            return bytes.fromhex(text).decode('utf-16-be')
        except Exception as e:
            self.logger.error(f"十六进制转字符串失败: {e}")
            raise
    
    def encode(self, text: str) -> str:
        """
        将字符串编码为兽音
        
        采用每个十六进制半字节(nibble)按位置偏移(位置对16取模)后映射为两位兽音字符的算法；
        当启用头尾标记时，会在编码结果前添加索引为[3,1,0]的三字符前缀，在末尾添加索引为[2]的一字符后缀，
        以与常见“兽音译者”的格式保持兼容。
        
        Args:
            text (str): 要编码的字符串
            
        Returns:
            str: 兽音编码后的字符串
        """
        try:
            if not text:
                return ""
            
            # UTF-16BE -> hex 序列（每字符4个hex）
            hexArray = list(self.str2hex(text))
            code = ""
            n = 0
            for x in hexArray:
                k = int(x, 16) + n % 16
                if k >= 16:
                    k -= 16
                # 将4位的半字节拆分为两位(高2位 / 低2位)，映射到字符表
                code += self.beast[k >> 2] + self.beast[k & 0x3]
                n += 1
            
            if self.use_header_footer:
                prefix = self.beast[3] + self.beast[1] + self.beast[0]
                suffix = self.beast[2]
                return prefix + code + suffix
            
            return code
        except Exception as e:
            self.logger.error(f"兽音编码失败: {e}")
            raise
    
    def decode(self, text: str) -> str:
        """
        将兽音解码为普通字符串
        
        解码时自动兼容两种格式：
        - 含头尾标记：前缀为索引[3,1,0]字符，后缀为索引[2]字符，仅对中间主体进行按对(2字符)解码；
        - 不含头尾标记：对全串按对(2字符)解码；
        
        Args:
            text (str): 兽音编码的字符串
            
        Returns:
            str: 解码后的字符串
        """
        try:
            if not text:
                return ""
            
            # 识别并剥离头尾标记（即使未启用use_header_footer，也做兼容处理）
            prefix = self.beast[3] + self.beast[1] + self.beast[0]
            suffix = self.beast[2]
            body = text
            if len(text) >= 4 and text.startswith(prefix) and text.endswith(suffix):
                body = text[3:-1]
            
            if len(body) % 2 != 0:
                raise ValueError("无效的兽音编码：主体长度应为偶数")
            
            hex_str = ""
            for i in range(0, len(body), 2):
                a = body[i]
                b = body[i + 1]
                pos1 = self.beast.index(a)
                pos2 = self.beast.index(b)
                v = ((pos1 << 2) | pos2) - ((i // 2) % 16)
                if v < 0:
                    v += 16
                hex_str += format(v, 'x')
            
            return self.hex2str(hex_str)
        except Exception as e:
            self.logger.error(f"兽音解码失败: {e}")
            raise
    
    def is_beast_encoded(self, text: str) -> bool:
        """
        检查字符串是否为兽音编码（兼容有无头尾标记的两种格式）
        
        Args:
            text (str): 要检查的字符串
            
        Returns:
            bool: 如果是兽音编码返回True，否则返回False
        """
        try:
            if not text:
                return False
            
            # 所有字符都应在字符表内
            for ch in text:
                if ch not in self.beast:
                    return False
            
            # 识别主体（剥离可能存在的头尾标记）
            prefix = self.beast[3] + self.beast[1] + self.beast[0]
            suffix = self.beast[2]
            body = text
            if len(text) >= 4 and text.startswith(prefix) and text.endswith(suffix):
                body = text[3:-1]
            
            # 主体长度应为偶数
            if len(body) % 2 != 0:
                return False
            
            # 尝试解码
            try:
                _ = self.decode(text)
                return True
            except Exception:
                return False
        except Exception:
            return False
    
    def get_beast_chars(self) -> List[str]:
        """
        获取当前使用的兽音字符集
        
        Returns:
            List[str]: 兽音字符集
        """
        return self.beast.copy()
    
    def set_beast_chars(self, beast_chars: List[str]) -> None:
        """
        设置新的兽音字符集
        
        Args:
            beast_chars (List[str]): 新的兽音字符集，必须包含4个字符
        """
        if len(beast_chars) != 4:
            raise ValueError("加密字符集必须包含4个字符")
        
        self.beast = beast_chars
        self.logger.info(f"兽音字符集已更新为: {self.beast}")


# 便捷函数
def create_beast_encoder_from_config(config: Dict[str, Any]) -> BeastEncoder:
    """
    从配置创建兽音加密器的便捷函数
    
    Args:
        config (Dict[str, Any]): 配置字典
        
    Returns:
        BeastEncoder: 兽音加密器实例
    """
    return BeastEncoder.from_config(config)


if __name__ == '__main__':
    # 测试代码
    encoder = BeastEncoder()
    
    # 测试编码和解码
    test_text = "你好"
    encoded = encoder.encode(test_text)
    decoded = encoder.decode(encoded)
    
    log_msg1 = f"原文: {test_text}"
    log_msg2 = f"编码: {encoded}"
    log_msg3 = f"解码: {decoded}"
    log_msg4 = f"编码解码是否一致: {test_text == decoded}"
    log_msg5 = f"是否为兽音编码: {encoder.is_beast_encoded(encoded)}"
    log_msg6 = f"普通文本是否为兽音编码: {encoder.is_beast_encoded('普通文本')}"
    
    # 使用统一日志记录方法
    encoder._log_unified("INFO", log_msg1, "system", "system")
    encoder._log_unified("INFO", log_msg2, "system", "system")
    encoder._log_unified("INFO", log_msg3, "system", "system")
    encoder._log_unified("INFO", log_msg4, "system", "system")
    encoder._log_unified("INFO", log_msg5, "system", "system")
    encoder._log_unified("INFO", log_msg6, "system", "system")
    
    # 记录测试日志
    try:
        log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_filename = f"beast_encoder_{datetime.now().strftime('%Y%m%d')}.log"
        log_filepath = os.path.join(log_dir, log_filename)
        with open(log_filepath, 'a', encoding='utf-8') as f:
            f.write(log_msg1 + '\n')
            f.write(log_msg2 + '\n')
            f.write(log_msg3 + '\n')
            f.write(log_msg4 + '\n')
            f.write(log_msg5 + '\n')
            f.write(log_msg6 + '\n')
    except Exception:
        pass