#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
倍率计算服务模块

该模块提供倍率计算相关功能，支持用户输入多个数值进行特定公式的计算。
"""

from service.base_service import BaseService
from typing import Optional, Dict, Any

class CalculatorService(BaseService):
    """倍率计算服务类
    
    提供倍率计算功能，支持用户通过指令输入数值并按照特定公式计算结果。
    
    支持的指令格式：
    /倍率计算 [第一个数] [第二个数] [[第三个数] [第四个数] [[第五个数]
    
    计算公式：
    1 + 第一个数/100 + (第二个数 + 第三个数 + 第四个数 + 第五个数)/500
    """
    
    def __init__(self, config, data_manager, text_formatter, server=None):
        """初始化倍率计算服务
        
        Args:
            config: 配置对象
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.command_prefix = "/倍率计算"
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """处理用户消息
        
        Args:
            message: 用户消息字符串
            user_id: 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[Dict[str, Any]]: 计算结果消息，如果不是倍率计算指令则返回None
        """
        if not message.startswith(self.command_prefix):
            return None
            
        # 解析参数
        parts = message.split()
        if len(parts) < 3:  # 至少需要命令和两个数字
            return None
            
        try:
            # 获取输入的数字，如果没有提供则默认为0
            numbers = [float(x) for x in parts[1:6]]  # 最多取5个数字
            while len(numbers) < 5:
                numbers.append(0)
                
            # 计算倍率
            # 1 + 第一个数/100 + 剩余四个数总和/500
            result = 1 + (numbers[0] / 100) + (sum(numbers[1:]) / 500)
            
            # 格式化结果，保留两位小数
            return {"content": f"你的倍率是：{result:.2f}", "image_path": None}
            
        except ValueError:
            return None
            
    def get_help_text(self):
        """获取帮助文本
        
        Returns:
            Dict[str, Any]: 包含帮助文本的字典
        """
        help_text = (
            "倍率计算功能帮助：\n"
            "- 使用方法：/倍率计算 [第一个数] [第二个数] [[第三个数] [第四个数] [[第五个数]\n"
            "- 示例：/倍率计算 130 110 110 80 80\n"
            "- 计算公式：1 + 第一个数/100 + (其余数字之和)/500\n"
            "- 说明：第三个数到第五个数可选，未提供时默认为0"
        )
        return {"content": help_text, "image_path": None}