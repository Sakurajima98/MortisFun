#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目API客户端基类

本文件实现统一的API调用管理，包括：
1. 超时配置管理
2. 重试机制实现
3. 连接池管理
4. 统一错误处理
5. API调用监控

作者: Mortisfun Team
创建时间: 2024
"""

import time
import logging
import requests
from typing import Dict, Any, Optional, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from .monitoring import APIMonitor, StructuredLogger

# 初始化全局监控实例
api_monitor = APIMonitor()
structured_logger = StructuredLogger("api_client")


class APIClient:
    """
    统一API客户端基类
    
    提供统一的API调用接口，包含超时管理、重试机制、
    连接池管理和错误处理等功能。
    """
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None) -> None:
        """
        初始化API客户端
        
        Args:
            config (Dict[str, Any]): API配置信息
            logger (Optional[logging.Logger]): 日志记录器
        """
        self.config: Dict[str, Any] = config
        self.logger: logging.Logger = logger or logging.getLogger(__name__)
        
        # API设置
        api_settings: Dict[str, Any] = config.get('api_settings', {})
        self.connect_timeout: int = api_settings.get('connect_timeout', 10)
        self.read_timeout: int = api_settings.get('read_timeout', 60)
        self.total_timeout: int = api_settings.get('total_timeout', 90)
        
        # 重试设置
        retry_settings: Dict[str, Any] = api_settings.get('retry', {})
        self.max_retries: int = retry_settings.get('max_retries', 3)
        self.backoff_factor: float = retry_settings.get('backoff_factor', 0.3)
        self.retry_status_codes: List[int] = retry_settings.get('status_codes', [429, 500, 502, 503, 504])
        
        # 创建会话和配置重试策略
        self.session: requests.Session = self._create_session()
        
        # 监控数据
        self.api_stats: Dict[str, Any] = {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'total_response_time': 0.0
        }
    
    def _create_session(self) -> requests.Session:
        """
        创建配置好的requests会话
        
        Returns:
            requests.Session: 配置好的会话对象
        """
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=self.retry_status_codes,
            backoff_factor=self.backoff_factor,
            raise_on_status=False
        )
        
        # 配置适配器
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def call_api(self, 
                 url: str, 
                 method: str = 'POST',
                 headers: Optional[Dict[str, str]] = None,
                 data: Optional[Dict[str, Any]] = None,
                 params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        统一API调用方法
        
        Args:
            url (str): API端点URL
            method (str): HTTP方法，默认POST
            headers (Optional[Dict[str, str]]): 请求头
            data (Optional[Dict[str, Any]]): 请求数据
            params (Optional[Dict[str, str]]): URL参数
            
        Returns:
            Dict[str, Any]: API响应结果
        """
        start_time = time.time()
        self.api_stats['total_calls'] += 1
        
        try:
            # 记录请求开始
            self.logger.info(f"API调用开始: {method} {url}")
            
            # 发送请求
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                json=data if method.upper() in ['POST', 'PUT', 'PATCH'] else None,
                params=params,
                timeout=(self.connect_timeout, self.read_timeout)
            )
            
            # 计算响应时间
            response_time = time.time() - start_time
            self.api_stats['total_response_time'] += response_time
            
            # 处理响应
            result = self._handle_response(response, response_time)
            
            if result.get('success', False):
                self.api_stats['successful_calls'] += 1
            else:
                self.api_stats['failed_calls'] += 1
                
            return result
            
        except requests.exceptions.Timeout as e:
            self.api_stats['failed_calls'] += 1
            self.logger.error(f"API调用超时: {url} - {str(e)}")
            return {
                'success': False,
                'error': 'timeout',
                'message': '请求超时，请稍后再试',
                'details': str(e)
            }
            
        except requests.exceptions.ConnectionError as e:
            self.api_stats['failed_calls'] += 1
            self.logger.error(f"API连接失败: {url} - {str(e)}")
            return {
                'success': False,
                'error': 'connection_error',
                'message': '网络连接失败，请检查网络状态',
                'details': str(e)
            }
            
        except Exception as e:
            self.api_stats['failed_calls'] += 1
            self.logger.error(f"API调用异常: {url} - {str(e)}")
            return {
                'success': False,
                'error': 'unknown_error',
                'message': 'API调用失败',
                'details': str(e)
            }
    
    def _handle_response(self, response: requests.Response, response_time: float) -> Dict[str, Any]:
        """
        处理API响应
        
        Args:
            response (requests.Response): HTTP响应对象
            response_time (float): 响应时间（秒）
            
        Returns:
            Dict[str, Any]: 处理后的响应结果
        """
        # 记录响应信息
        self.logger.info(f"API响应: {response.status_code} - {response_time:.2f}s")
        
        # 检查状态码
        if response.status_code == 200:
            try:
                data = response.json()
                return {
                    'success': True,
                    'data': data,
                    'status_code': response.status_code,
                    'response_time': response_time
                }
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON解析失败: {str(e)}")
                return {
                    'success': False,
                    'error': 'json_decode_error',
                    'message': 'API响应格式错误',
                    'details': str(e)
                }
        
        elif response.status_code == 401:
            return {
                'success': False,
                'error': 'unauthorized',
                'message': 'API密钥无效或已过期',
                'status_code': response.status_code
            }
        
        elif response.status_code == 429:
            return {
                'success': False,
                'error': 'rate_limit',
                'message': 'API调用频率超限，请稍后再试',
                'status_code': response.status_code
            }
        
        elif response.status_code >= 500:
            return {
                'success': False,
                'error': 'server_error',
                'message': 'API服务器错误，请稍后再试',
                'status_code': response.status_code
            }
        
        else:
            # 4xx 客户端错误：尽力解析返回体以提供更清晰的诊断信息
            try:
                payload = response.json()
                # OpenAI/SiliconFlow 通常返回 {"error": {"message": "...", "type": "invalid_request_error"}}
                err_obj = payload.get('error') if isinstance(payload, dict) else None
                err_msg = ''
                if isinstance(err_obj, dict):
                    err_msg = err_obj.get('message') or err_obj.get('code') or ''
                return {
                    'success': False,
                    'error': (err_obj.get('type') if isinstance(err_obj, dict) and err_obj.get('type') else 'http_error'),
                    'message': err_msg or f'API调用失败，状态码: {response.status_code}',
                    'status_code': response.status_code,
                    'data': payload
                }
            except Exception:
                return {
                    'success': False,
                    'error': 'http_error',
                    'message': f'API调用失败，状态码: {response.status_code}',
                    'status_code': response.status_code,
                    'details': response.text
                }
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取API调用统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        total_calls = self.api_stats['total_calls']
        if total_calls == 0:
            return {
                'total_calls': 0,
                'success_rate': 0.0,
                'average_response_time': 0.0
            }
        
        return {
            'total_calls': total_calls,
            'successful_calls': self.api_stats['successful_calls'],
            'failed_calls': self.api_stats['failed_calls'],
            'success_rate': (self.api_stats['successful_calls'] / total_calls) * 100,
            'average_response_time': self.api_stats['total_response_time'] / total_calls
        }
    
    def close(self):
        """
        关闭会话，释放资源
        """
        if hasattr(self, 'session'):
            self.session.close()
            self.logger.info("API客户端会话已关闭")


class SiliconFlowClient(APIClient):
    """
    硅基流动API客户端
    
    专门用于调用硅基流动的AI服务API。
    """
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        初始化硅基流动客户端
        
        Args:
            config (Dict[str, Any]): 配置信息
            logger (Optional[logging.Logger]): 日志记录器
        """
        super().__init__(config, logger)
        
        # 硅基流动特定配置
        siliconflow_config = config.get('siliconflow', {})
        self.api_key = siliconflow_config.get('api_key', '')
        self.base_url = siliconflow_config.get('base_url', 'https://api.siliconflow.cn/v1')
        self.model = siliconflow_config.get('model', 'deepseek-ai/DeepSeek-V3')
        self.max_tokens = siliconflow_config.get('max_tokens', 2000)
        self.temperature = siliconflow_config.get('temperature', 0.7)
        self.enable_thinking = siliconflow_config.get('enable_thinking', False)
    
    def chat_completion(self, messages: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        调用聊天完成API
        
        Args:
            messages (List[Dict[str, str]]): 对话消息列表
            **kwargs: 其他参数
            
        Returns:
            Dict[str, Any]: API响应结果
        """
        start_time = time.time()
        endpoint = "/chat/completions"
        model_name = kwargs.get('model', self.model)
        
        if not self.api_key:
            return {
                'success': False,
                'error': 'config_error',
                'message': 'API密钥未配置'
            }
        
        try:
            # 规范化消息以兼容 /chat/completions：
            # - 将非标准的 `input_image` 类型转换为 `image_url`
            # - 对不支持多模态的文本模型降级为纯文本（附加图片URL说明）
            normalized_messages = self._normalize_messages_for_chat_completions(messages, model_name)

            # 构建请求数据（移除不被 OpenAI 兼容端点识别的字段）
            data = {
                'model': model_name,
                'messages': normalized_messages,
                'max_tokens': kwargs.get('max_tokens', self.max_tokens),
                'temperature': kwargs.get('temperature', self.temperature)
            }
            
            # 设置请求头
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            # 调用API
            url = f"{self.base_url}/chat/completions"
            result = self.call_api(url, 'POST', headers, data)
            
            response_time = time.time() - start_time
            
            # 处理成功响应
            if result.get('success', False):
                api_data = result.get('data', {})
                if 'choices' in api_data and len(api_data['choices']) > 0:
                    content = api_data['choices'][0]['message']['content']
                    usage = api_data.get('usage', {})
                    
                    # 记录成功的API调用
                    api_monitor.record_call(
                        endpoint=endpoint,
                        success=True,
                        response_time=response_time,
                        usage=usage
                    )
                    
                    structured_logger.log_api_call(
                        endpoint=endpoint,
                        status='success',
                        response_time=response_time,
                        usage=usage,
                        model=model_name,
                        message_count=len(messages)
                    )
                    
                    return {
                        'success': True,
                        'content': content.strip(),
                        'usage': usage,
                        'response_time': response_time,
                        'model': model_name
                    }
                else:
                    error_type = 'invalid_response'
                    error_message = 'API响应格式错误'
                    
                    # 记录失败的API调用
                    api_monitor.record_call(
                        endpoint=endpoint,
                        success=False,
                        response_time=response_time,
                        error_type=error_type
                    )
                    
                    structured_logger.log_api_call(
                        endpoint=endpoint,
                        status='error',
                        response_time=response_time,
                        error=error_message,
                        model=model_name
                    )
                    
                    return {
                        'success': False,
                        'error': error_type,
                        'message': error_message,
                        'response_time': response_time
                    }
            else:
                error_type = result.get('error', 'unknown')
                error_message = result.get('message', 'API调用失败')
                
                # 记录失败的API调用
                api_monitor.record_call(
                    endpoint=endpoint,
                    success=False,
                    response_time=response_time,
                    error_type=error_type
                )
                
                structured_logger.log_api_call(
                    endpoint=endpoint,
                    status='error',
                    response_time=response_time,
                    error=error_message,
                    model=model_name
                )
                
                return result
                
        except Exception as e:
            response_time = time.time() - start_time
            error_message = str(e)
            
            # 记录异常的API调用
            api_monitor.record_call(
                endpoint=endpoint,
                success=False,
                response_time=response_time,
                error_type='exception'
            )
            
            structured_logger.log_api_call(
                endpoint=endpoint,
                status='exception',
                response_time=response_time,
                error=error_message,
                model=model_name
            )
            
            self.logger.error(f"聊天完成API调用异常: {e}")
            return {
                'success': False,
                'error': 'exception',
                'message': error_message,
                'response_time': response_time
            }

    def _normalize_messages_for_chat_completions(self, messages: List[Dict[str, Any]], model_name: str) -> List[Dict[str, Any]]:
        """
        为 OpenAI 兼容的 /chat/completions 端点规范化消息结构。

        - 将 `input_image` 类型统一为 `image_url`
        - 若模型看起来不支持多模态（不包含 VL/vision/gpt-4o 等关键词），则把图片块降级为文本描述

        Args:
            messages (List[Dict[str, Any]]): 原始消息列表
            model_name (str): 当前模型名

        Returns:
            List[Dict[str, Any]]: 规范化后的消息列表
        """
        vision_flag = False
        try:
            lower = (model_name or '').lower()
            vision_keywords = ['vl', 'vision', 'gpt-4o', 'omni', 'multimodal']
            vision_flag = any(k in lower for k in vision_keywords)
        except Exception:
            vision_flag = False

        normalized: List[Dict[str, Any]] = []
        for m in messages:
            role = m.get('role', 'user')
            content = m.get('content')

            # 系统/助手/用户消息统一处理
            if isinstance(content, list):
                text_parts: List[str] = []
                image_blocks: List[Dict[str, Any]] = []

                for b in content:
                    try:
                        btype = b.get('type')
                        if btype == 'text' and 'text' in b:
                            text_parts.append(str(b.get('text', '')))
                        elif btype in ('image_url', 'input_image') and isinstance(b.get('image_url'), dict):
                            url = str(b['image_url'].get('url', '')).strip()
                            if not url:
                                continue
                            if vision_flag:
                                image_blocks.append({'type': 'image_url', 'image_url': {'url': url}})
                            else:
                                text_parts.append(f"图片: {url}")
                        else:
                            # 未知块忽略
                            pass
                    except Exception:
                        continue

                if vision_flag and (image_blocks or text_parts):
                    new_content: List[Dict[str, Any]] = []
                    if text_parts:
                        new_content.append({'type': 'text', 'text': '\n'.join(text_parts)})
                    new_content.extend(image_blocks)
                    normalized.append({'role': role, 'content': new_content})
                else:
                    normalized.append({'role': role, 'content': '\n'.join(text_parts)})
            else:
                normalized.append({'role': role, 'content': content})

        return normalized
