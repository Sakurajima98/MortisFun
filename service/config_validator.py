#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置验证模块
提供配置文件验证和类型检查功能
"""

import json
import logging
from typing import Dict, Any, List, Union
from pathlib import Path


class ConfigValidator:
    """
    配置验证器
    用于验证配置文件的完整性和正确性
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 定义配置模式
        self.config_schema = {
            "api_settings": {
                "required": True,
                "type": dict,
                "schema": {
                    "connect_timeout": {"type": (int, float), "min": 1, "max": 60},
                    "read_timeout": {"type": (int, float), "min": 1, "max": 300},
                    "total_timeout": {"type": (int, float), "min": 1, "max": 600},
                    "retry": {
                        "type": dict,
                        "schema": {
                            "max_retries": {"type": int, "min": 0, "max": 10},
                            "backoff_factor": {"type": (int, float), "min": 0, "max": 10},
                            "retry_status_codes": {"type": list}
                        }
                    }
                }
            },
            "siliconflow": {
                "required": True,
                "type": dict,
                "schema": {
                    "api_key": {"type": str, "min_length": 10},
                    "base_url": {"type": str, "min_length": 10},
                    "model": {"type": str, "min_length": 1},
                    "max_tokens": {"type": int, "min": 1, "max": 32000},
                    "temperature": {"type": (int, float), "min": 0, "max": 2}
                }
            },
            "chat": {
                "required": True,
                "type": dict,
                "schema": {
                    "enable_cache": {"type": bool},
                    "cache_ttl_minutes": {"type": int, "min": 1, "max": 1440}
                }
            },
            "tarot": {
                "required": False,
                "type": dict
            },
            "logging": {
                "required": False,
                "type": dict,
                "schema": {
                    "level": {"type": str, "choices": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]},
                    "format": {"type": str},
                    "file": {"type": str}
                }
            }
        }
    
    def validate_config(self, config_path: Union[str, Path]) -> Dict[str, Any]:
        """
        验证配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            Dict[str, Any]: 验证结果，包含是否有效、错误信息等
        """
        try:
            # 检查文件是否存在
            config_file = Path(config_path)
            if not config_file.exists():
                return {
                    "valid": False,
                    "error": "config_file_not_found",
                    "message": f"配置文件不存在: {config_path}"
                }
            
            # 读取配置文件
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            except json.JSONDecodeError as e:
                return {
                    "valid": False,
                    "error": "invalid_json",
                    "message": f"配置文件JSON格式错误: {e}"
                }
            except Exception as e:
                return {
                    "valid": False,
                    "error": "read_error",
                    "message": f"读取配置文件失败: {e}"
                }
            
            # 验证配置结构
            validation_result = self._validate_structure(config)
            if not validation_result["valid"]:
                return validation_result
            
            # 验证配置值
            value_validation = self._validate_values(config)
            if not value_validation["valid"]:
                return value_validation
            
            return {
                "valid": True,
                "config": config,
                "message": "配置验证通过"
            }
            
        except Exception as e:
            self.logger.error(f"配置验证异常: {e}")
            return {
                "valid": False,
                "error": "validation_exception",
                "message": f"配置验证异常: {e}"
            }
    
    def _validate_structure(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        验证配置结构
        
        Args:
            config: 配置字典
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        errors = []
        
        # 检查必需的顶级字段
        for field_name, field_schema in self.config_schema.items():
            if field_schema.get("required", False) and field_name not in config:
                errors.append(f"缺少必需字段: {field_name}")
                continue
            
            if field_name in config:
                # 检查字段类型
                expected_type = field_schema.get("type")
                if expected_type and not isinstance(config[field_name], expected_type):
                    errors.append(f"字段 {field_name} 类型错误，期望 {expected_type.__name__}，实际 {type(config[field_name]).__name__}")
                    continue
                
                # 检查嵌套字段
                if "schema" in field_schema and isinstance(config[field_name], dict):
                    nested_errors = self._validate_nested_fields(
                        config[field_name], 
                        field_schema["schema"], 
                        field_name
                    )
                    errors.extend(nested_errors)
        
        if errors:
            return {
                "valid": False,
                "error": "structure_validation_failed",
                "message": "配置结构验证失败",
                "details": errors
            }
        
        return {"valid": True}
    
    def _validate_nested_fields(self, config_section: Dict[str, Any], 
                               schema: Dict[str, Any], 
                               parent_name: str) -> List[str]:
        """
        验证嵌套字段
        
        Args:
            config_section: 配置段
            schema: 字段模式
            parent_name: 父字段名
            
        Returns:
            List[str]: 错误列表
        """
        errors = []
        
        for field_name, field_schema in schema.items():
            full_field_name = f"{parent_name}.{field_name}"
            
            if field_schema.get("required", True) and field_name not in config_section:
                errors.append(f"缺少必需字段: {full_field_name}")
                continue
            
            if field_name in config_section:
                value = config_section[field_name]
                expected_type = field_schema.get("type")
                
                # 类型检查
                if expected_type:
                    if isinstance(expected_type, tuple):
                        if not isinstance(value, expected_type):
                            type_names = " 或 ".join([t.__name__ for t in expected_type])
                            errors.append(f"字段 {full_field_name} 类型错误，期望 {type_names}，实际 {type(value).__name__}")
                    else:
                        if not isinstance(value, expected_type):
                            errors.append(f"字段 {full_field_name} 类型错误，期望 {expected_type.__name__}，实际 {type(value).__name__}")
                
                # 递归验证嵌套字段
                if "schema" in field_schema and isinstance(value, dict):
                    nested_errors = self._validate_nested_fields(
                        value, 
                        field_schema["schema"], 
                        full_field_name
                    )
                    errors.extend(nested_errors)
        
        return errors
    
    def _validate_values(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        验证配置值
        
        Args:
            config: 配置字典
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        errors = []
        
        # 验证API设置
        if "api_settings" in config:
            api_errors = self._validate_api_settings(config["api_settings"])
            errors.extend(api_errors)
        
        # 验证SiliconFlow设置
        if "siliconflow" in config:
            sf_errors = self._validate_siliconflow_settings(config["siliconflow"])
            errors.extend(sf_errors)
        
        # 验证聊天设置
        if "chat" in config:
            chat_errors = self._validate_chat_settings(config["chat"])
            errors.extend(chat_errors)
        
        if errors:
            return {
                "valid": False,
                "error": "value_validation_failed",
                "message": "配置值验证失败",
                "details": errors
            }
        
        return {"valid": True}
    
    def _validate_api_settings(self, api_settings: Dict[str, Any]) -> List[str]:
        """
        验证API设置
        
        Args:
            api_settings: API设置字典
            
        Returns:
            List[str]: 错误列表
        """
        errors = []
        
        # 验证超时设置
        timeouts = ["connect_timeout", "read_timeout", "total_timeout"]
        for timeout_name in timeouts:
            if timeout_name in api_settings:
                value = api_settings[timeout_name]
                if not isinstance(value, (int, float)) or value <= 0:
                    errors.append(f"API设置中的 {timeout_name} 必须是正数")
        
        # 验证重试设置
        if "retry" in api_settings:
            retry_settings = api_settings["retry"]
            
            if "max_retries" in retry_settings:
                max_retries = retry_settings["max_retries"]
                if not isinstance(max_retries, int) or max_retries < 0:
                    errors.append("最大重试次数必须是非负整数")
            
            if "backoff_factor" in retry_settings:
                backoff = retry_settings["backoff_factor"]
                if not isinstance(backoff, (int, float)) or backoff < 0:
                    errors.append("退避因子必须是非负数")
            
            if "retry_status_codes" in retry_settings:
                status_codes = retry_settings["retry_status_codes"]
                if not isinstance(status_codes, list):
                    errors.append("重试状态码必须是列表")
                else:
                    for code in status_codes:
                        if not isinstance(code, int) or code < 100 or code > 599:
                            errors.append(f"无效的HTTP状态码: {code}")
        
        return errors
    
    def _validate_siliconflow_settings(self, sf_settings: Dict[str, Any]) -> List[str]:
        """
        验证SiliconFlow设置
        
        Args:
            sf_settings: SiliconFlow设置字典
            
        Returns:
            List[str]: 错误列表
        """
        errors = []
        
        # 验证API密钥
        if "api_key" in sf_settings:
            api_key = sf_settings["api_key"]
            if not isinstance(api_key, str) or len(api_key.strip()) < 10:
                errors.append("API密钥长度不足或格式错误")
        
        # 验证基础URL
        if "base_url" in sf_settings:
            base_url = sf_settings["base_url"]
            if not isinstance(base_url, str) or not base_url.startswith(("http://", "https://")):
                errors.append("基础URL格式错误")
        
        # 验证最大token数
        if "max_tokens" in sf_settings:
            max_tokens = sf_settings["max_tokens"]
            if not isinstance(max_tokens, int) or max_tokens <= 0 or max_tokens > 32000:
                errors.append("最大token数必须在1-32000之间")
        
        # 验证温度参数
        if "temperature" in sf_settings:
            temperature = sf_settings["temperature"]
            if not isinstance(temperature, (int, float)) or temperature < 0 or temperature > 2:
                errors.append("温度参数必须在0-2之间")
        
        return errors
    
    def _validate_chat_settings(self, chat_settings: Dict[str, Any]) -> List[str]:
        """
        验证聊天设置
        
        Args:
            chat_settings: 聊天设置字典
            
        Returns:
            List[str]: 错误列表
        """
        errors = []
        
        # 验证缓存TTL
        if "cache_ttl_minutes" in chat_settings:
            ttl = chat_settings["cache_ttl_minutes"]
            if not isinstance(ttl, int) or ttl <= 0 or ttl > 1440:
                errors.append("缓存TTL必须在1-1440分钟之间")
        
        return errors
    
    def get_config_template(self) -> Dict[str, Any]:
        """
        获取配置模板
        
        Returns:
            Dict[str, Any]: 配置模板
        """
        return {
            "api_settings": {
                "connect_timeout": 10,
                "read_timeout": 30,
                "total_timeout": 60,
                "retry": {
                    "max_retries": 3,
                    "backoff_factor": 1.0,
                    "retry_status_codes": [500, 502, 503, 504, 429]
                }
            },
            "siliconflow": {
                "api_key": "your_api_key_here",
                "base_url": "https://api.siliconflow.cn/v1",
                "model": "deepseek-chat",
                "max_tokens": 2000,
                "temperature": 0.7
            },
            "chat": {
                "enable_cache": True,
                "cache_ttl_minutes": 30
            },
            "tarot": {
                "enable_ai_interpretation": True,
                "default_spread": "three_card"
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file": "logs/app.log"
            }
        }


# 创建全局配置验证器实例
config_validator = ConfigValidator()