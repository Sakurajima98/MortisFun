#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目网盘搜索服务模块

本文件实现网盘搜索功能，包括：
1. 调用PanSou API进行网盘资源搜索
2. 处理搜索结果并格式化
3. 使用兽音加密保护敏感信息
4. 通过napcat合并转发消息发送结果

作者: Mortisfun Team
创建时间: 2025
"""

import asyncio
import aiohttp
import re
import random
from typing import Dict, Any, Optional, List
from .base_service import BaseService
from utils.beast_encoder import BeastEncoder


class PanSearchService(BaseService):
    """
    网盘搜索服务类
    
    负责处理网盘搜索请求，调用PanSou API获取搜索结果，
    并通过合并转发消息的形式发送给用户。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, napcat_api_caller, server=None):
        """
        初始化网盘搜索服务
        
        Args:
            config (Dict[str, Any]): 配置信息
            data_manager: 数据管理器
            text_formatter: 文本格式化器
            napcat_api_caller: Napcat API调用器
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.napcat_api_caller = napcat_api_caller
        
        # 初始化兽音加密器
        self.beast_encoder = BeastEncoder.from_config(config)
        
        # 网盘搜索服务配置
        self.pan_search_config = config.get('services', {}).get('pan_search', {})
        self.pansou_config = config.get('pansou', {})
        
        # API配置
        self.api_url = self.pansou_config.get('api_url', 'https://so.252035.xyz')
        self.timeout = self.pansou_config.get('timeout', 300)  # 增加超时时间到300s
        self.default_channels = self.pansou_config.get('default_channels', ['tgsearchers3'])
        self.enabled_plugins = self.pansou_config.get('enabled_plugins', [])
        self.max_concurrent_requests = self.pansou_config.get('max_concurrent_requests', 5)
        
        # 结果限制
        self.max_results_per_type = self.pan_search_config.get('max_results_per_type', 10)
        
        # 网盘类型映射
        self.cloud_type_names = {
            'baidu': '百度网盘',
            'aliyun': '阿里云盘',
            'quark': '夸克网盘',
            'tianyi': '天翼云盘',
            'uc': 'UC网盘',
            'mobile': '移动云盘',
            '115': '115网盘',
            'pikpak': 'PikPak',
            'xunlei': '迅雷网盘',
            '123': '123网盘',
            'magnet': '磁力链接',
            'ed2k': '电驴链接',
            'others': '其他'
        }
        
        self.log_unified("INFO", "网盘搜索服务初始化完成", group_id="system", user_id="system")
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理网盘搜索和解密消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果
        """
        try:
            # 先处理帮助指令，支持 /help 网盘搜索 与 /帮助 网盘搜索 两种形式
            stripped = message.strip()
            if stripped == "/help 网盘搜索" or stripped == "/帮助 网盘搜索":
                try:
                    from .help_service import HelpService
                    helper = HelpService(self.config, self.data_manager, self.text_formatter)
                    help_dict = helper.get_service_help("网盘搜索")
                    # 使用HelpService的格式化输出
                    formatted = helper._format_help_dict(help_dict, service_name="网盘搜索")
                    return {"content": formatted, "image_path": None}
                except Exception as he:
                    group_id = kwargs.get('group_id', '')
                    self.log_unified("ERROR", f"委托获取网盘搜索帮助失败: {he}", group_id=group_id, user_id=user_id)
                    return {"content": "❌ 获取帮助信息失败，请稍后重试", "image_path": None}
            
            # 检查是否为解密命令
            decrypt_result = self._parse_decrypt_command(message)
            if decrypt_result:
                # 检查服务是否启用
                if not self.is_enabled():
                    return {
                        "content": "🚫 网盘搜索服务当前未启用",
                        "image_path": None
                    }
                
                # 检查每日使用限制
                if not self.check_daily_limit(user_id, 'decrypt'):
                    daily_limit = self.pan_search_config.get('daily_limit', 20)
                    return {
                        "content": f"⏰ 您今日的解密次数已达上限({daily_limit}次)，请明天再试。",
                        "image_path": None
                    }
                
                # 记录服务使用
                self.log_service_usage(user_id, 'pan_search', 'decrypt')
                
                return decrypt_result
            
            # 解析搜索命令
            parsed = self._parse_search_command(message)
            if parsed:
                # 检查服务是否启用
                if not self.is_enabled():
                    return {
                        "content": "🚫 网盘搜索服务当前未启用",
                        "image_path": None
                    }
                
                # 检查每日使用限制
                if not self.check_daily_limit(user_id, 'search'):
                    daily_limit = self.pan_search_config.get('daily_limit', 20)
                    return {
                        "content": f"⏰ 您今日的搜索次数已达上限({daily_limit}次)，请明天再试。",
                        "image_path": None
                    }
                
                # 记录服务使用
                self.log_service_usage(user_id, 'pan_search', 'search')
                
                # 创建异步搜索响应（立即回复提示，后台继续查询）
                context = kwargs.get('context', {})
                return self._create_async_search_response(parsed, user_id, context)
            
            return None
            
        except Exception as e:
            group_id = kwargs.get('group_id', '')
            self.log_unified("ERROR", f"处理消息失败: {e}", group_id=group_id, user_id=user_id)
            return {
                "content": "❌ 处理请求时出现错误，请稍后重试",
                "image_path": None
            }


    def _parse_search_command(self, message: str) -> Optional[Dict[str, Any]]:
        """
        解析搜索命令
        
        Args:
            message (str): 用户消息
            
        Returns:
            Optional[Dict[str, Any]]: 解析后的搜索参数
        """
        try:
            # 匹配命令格式: /网盘搜索 [搜索内容] [频道列表]
            pattern = r'^/网盘搜索\s+(.+?)(?:\s+频道:([^\s]+(?:,[^\s]+)*))?$'
            match = re.match(pattern, message.strip())
            
            if not match:
                return None
            
            keyword = match.group(1).strip()
            channels_str = match.group(2)
            
            # 验证关键词
            if not keyword or len(keyword) > 100:
                return None
            
            # 解析频道列表
            channels = self.default_channels.copy()
            if channels_str:
                custom_channels = [ch.strip() for ch in channels_str.split(',') if ch.strip()]
                if custom_channels:
                    channels = custom_channels
            
            return {
                'keyword': keyword,
                'channels': channels
            }
            
        except Exception as e:
            self.logger.error(f"解析搜索命令失败: {e}")
            return None
    
    def _parse_decrypt_command(self, message: str) -> Optional[Dict[str, Any]]:
        """
        解析解密命令
        
        Args:
            message (str): 用户消息
            
        Returns:
            Optional[Dict[str, Any]]: 解密结果或None
        """
        try:
            # 匹配命令格式: /解密 [加密内容]
            pattern = r'^/解密\s+(.+)$'
            match = re.match(pattern, message.strip())
            
            if not match:
                return None
            
            encrypted_text = match.group(1).strip()
            
            # 验证加密内容
            if not encrypted_text:
                return {
                    "content": "❌ 请提供需要解密的内容",
                    "image_path": None
                }
            
            # 去除用户粘贴内容中可能混入的“删除”占位及空白
            cleaned_cipher = encrypted_text.replace('删除', '').replace('\u5220\u9664', '').strip()
            cleaned_cipher = re.sub(r"\s+", "", cleaned_cipher)
            
            # 如果不是有效的兽音字符序列，直接失败
            if not self.beast_encoder.is_beast_encoded(cleaned_cipher):
                return {
                    "content": "❌ 解密失败，内容格式不正确或已损坏",
                    "image_path": None
                }
            
            # 执行解密
            try:
                decrypted_url = self.beast_encoder.decode(cleaned_cipher)
                
                # 验证解密结果是否为有效URL（支持多种网盘/协议/无协议格式）
                if not decrypted_url or not self._is_valid_url(decrypted_url):
                    return {
                        "content": "❌ 解密失败，请检查加密内容是否正确",
                        "image_path": None
                    }
                
                # 直接返回干净的URL（不再二次加入“删除”以免影响使用）
                return {
                    "content": f"🔓 解密成功：\n{decrypted_url}",
                    "image_path": None
                }
                
            except Exception as decode_error:
                self.logger.error(f"解密失败: {decode_error}")
                return {
                    "content": "❌ 解密失败，请检查加密内容是否正确",
                    "image_path": None
                }
            
        except Exception as e:
            self.logger.error(f"解析解密命令失败: {e}")
            return None
    
    def _create_async_search_response(self, search_params: Dict[str, Any], user_id: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建异步搜索响应
        
        Args:
            search_params (Dict[str, Any]): 搜索参数
            user_id (str): 用户ID
            context (Dict[str, Any]): 上下文信息
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        # 使用线程池执行异步任务
        import threading
        
        def run_async_search():
            try:
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(
                        self._perform_search_and_send_results(search_params, user_id, context)
                    )
                finally:
                    loop.close()
            except Exception as e:
                self.logger.error(f"异步搜索任务执行失败: {e}")
        
        # 在后台线程中执行
        thread = threading.Thread(target=run_async_search, daemon=True)
        thread.start()
        
        # 不返回搜索提示消息，直接返回None表示无需立即响应
        return None
    
    async def _perform_search_and_send_results(self, search_params: Dict[str, Any], user_id: str, context: Dict[str, Any]):
        """
        执行搜索并发送结果
        
        Args:
            search_params (Dict[str, Any]): 搜索参数
            user_id (str): 用户ID
            context (Dict[str, Any]): 上下文信息
        """
        try:
            # 调用PanSou API
            search_results = await self._call_pansou_api(search_params)
            
            if not search_results:
                await self._send_no_results_message(user_id, search_params['keyword'], context)
                return
            
            # 构建合并转发消息
            forward_messages = self._build_forward_messages(search_results, search_params['keyword'])
            
            if not forward_messages:
                await self._send_no_results_message(user_id, search_params['keyword'], context)
                return
            
            # 发送合并转发消息
            await self._send_forward_message(user_id, forward_messages, search_params['keyword'], context)
            
        except Exception as e:
            group_id = context.get('group_id', '')
            self.log_unified("ERROR", f"执行搜索并发送结果失败: {e}", group_id=group_id, user_id=user_id)
            await self._send_error_message(user_id, str(e), context)
    
    async def _call_pansou_api(self, search_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        调用PanSou API进行搜索
        
        Args:
            search_params (Dict[str, Any]): 搜索参数
            
        Returns:
            Optional[Dict[str, Any]]: 搜索结果
        """
        try:
            api_url = f"{self.api_url}/api/search"
            
            # 官方完整的频道列表
            official_channels = [
                "tgsearchers3", "yunpanxunlei", "tianyifc", "BaiduCloudDisk", "txtyzy", "peccxinpd", 
                "gotopan", "xingqiump4", "yunpanqk", "PanjClub", "kkxlzy", "baicaoZY", "MCPH01", 
                "share_aliyun", "bdwpzhpd", "ysxb48", "jdjdn1111", "yggpan", "MCPH086", "zaihuayun", 
                "Q66Share", "NewAliPan", "ypquark", "Oscar_4Kmovies", "ucwpzy", "alyp_TV", "alyp_4K_Movies", 
                "shareAliyun", "alyp_1", "dianyingshare", "Quark_Movies", "XiangxiuNBB", "NewQuark", 
                "ydypzyfx", "kuakeyun", "ucquark", "xx123pan", "yingshifenxiang123", "zyfb123", 
                "tyypzhpd", "tianyirigeng", "cloudtianyi", "hdhhd21", "Lsp115", "oneonefivewpfx", 
                "Maidanglaocom", "qixingzhenren", "taoxgzy", "tgsearchers115", "Channel_Shares_115", 
                "tyysypzypd", "vip115hot", "wp123zy", "yunpan139", "yunpan189", "yunpanuc", 
                "yydf_hzl", "alyp_Animation", "alyp_JLP", "leoziyuan"
            ]
            
            # 官方完整的插件列表
            official_plugins = [
                "ddys", "erxiao", "hdr4k", "labi", "libvio", "panta", "susu", "wanou", "xuexizhinan", 
                "zhizhen", "clxiong", "duoduo", "hdmoli", "huban", "leijing", "muou", "ouge", "panyq", 
                "shandian", "cldi", "clmao", "cyg", "fox4k", "hunhepan", "jikepan", "miaoso", 
                "pansearch", "panwiki", "pianku", "qupansou", "thepiratebay", "wuji", "xb6v", 
                "xiaozhang", "xys", "yuhuage", "javdb", "u3c3"
            ]
            
            # 构建GET请求参数，包含完整的频道和插件信息
            params = {
                "kw": search_params['keyword'],
                "channels": ",".join(official_channels),
                "plugins": ",".join(official_plugins),
                "res": "merge",
                "src": "all",
                "conc": "20",  # 转换为字符串
                "refresh": "true",  # 转换为字符串
                "ext": '{"referer":"https://dm.xueximeng.com"}'
            }
            
            self.log_unified("INFO", f"调用PanSou API: {api_url}, 关键词: {search_params['keyword']}, 频道数: {len(official_channels)}, 插件数: {len(official_plugins)}", group_id="system", user_id="system")
            
            # 增加超时时间到2分钟(120秒)
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url, params=params) as response:
                    if response.status == 200:
                        result = await response.json()
                        total = result.get('data', {}).get('total', 0)
                        self.log_unified("INFO", f"PanSou API调用成功，返回 {total} 个结果", group_id="system", user_id="system")
                        return result
                    else:
                        error_text = await response.text()
                        self.log_unified("ERROR", f"PanSou API调用失败: {response.status}, {error_text}", group_id="system", user_id="system")
                        return None
                        
        except asyncio.TimeoutError:
            self.log_unified("ERROR", "PanSou API调用超时(2分钟)", group_id="system", user_id="system")
            return None
        except Exception as e:
            self.log_unified("ERROR", f"PanSou API调用异常: {e}", group_id="system", user_id="system")
            return None
    
    def _build_forward_messages(self, search_results: Dict[str, Any], keyword: str) -> List[Dict[str, Any]]:
        """
        构建三层结构的合并转发消息
        第一层：搜索结果概览
        第二层：按网盘类型分类的消息
        第三层：每个搜索结果的独立消息
        
        Args:
            search_results (Dict[str, Any]): 搜索结果
            keyword (str): 搜索关键词
            
        Returns:
            List[Dict[str, Any]]: 合并转发消息列表
        """
        try:
            # 正确解析API返回的数据结构
            if 'data' in search_results:
                data = search_results.get('data', {})
                merged_by_type = data.get('merged_by_type', {})
            else:
                # 兼容直接返回结果的情况
                merged_by_type = search_results.get('merged_by_type', {})
            
            # 第一层：创建搜索结果概览消息
            overview_message = self._build_overview_message(merged_by_type, keyword)
            
            # 第二层：为每个网盘类型创建分类消息，包含第三层的独立结果消息
            type_messages = []
            for cloud_type, links in merged_by_type.items():
                if not links:
                    continue
                
                # 限制每种类型的结果数量
                limited_links = links[:self.max_results_per_type]
                
                # 构建该类型的分类消息（第二层）和独立结果消息（第三层）
                type_message_group = self._build_cloud_type_message_group(cloud_type, limited_links, keyword)
                if type_message_group:
                    type_messages.extend(type_message_group)
            
            # 组合所有消息：概览 + 各类型消息组
            all_messages = [overview_message] + type_messages
            return all_messages
            
        except Exception as e:
            self.logger.error(f"构建合并转发消息失败: {e}")
            return []
    
    def _build_overview_message(self, merged_by_type: Dict[str, List], keyword: str) -> Dict[str, Any]:
        """
        构建搜索结果概览消息（第一层）
        
        Args:
            merged_by_type (Dict[str, List]): 按类型分组的搜索结果
            keyword (str): 搜索关键词
            
        Returns:
            Dict[str, Any]: 概览消息节点
        """
        try:
            total_results = sum(len(links) for links in merged_by_type.values())
            type_count = len([links for links in merged_by_type.values() if links])
            
            overview_lines = [
                f"🔍 搜索结果如下：",
                f"关键词：{keyword}",
                f"总计：{total_results} 个结果",
                f"类型：{type_count} 种网盘",
                "",
                "📋 分类概览："
            ]
            
            for cloud_type, links in merged_by_type.items():
                if links:
                    cloud_name = self.cloud_type_names.get(cloud_type, cloud_type)
                    overview_lines.append(f"• {cloud_name}：{len(links)} 个结果")
            
            overview_lines.extend([
                "",
                "💡 点击下方消息查看详细结果"
            ])
            
            return {
                "type": "node",
                "data": {
                    "user_id": "1145141919",
                    "nickname": "Mortis",
                    "content": [
                        {
                            "type": "text",
                            "data": {
                                "text": "\n".join(overview_lines)
                            }
                        }
                    ]
                }
            }
            
        except Exception as e:
            self.log_unified("ERROR", f"构建概览消息失败: {e}", group_id="system", user_id="system")
            return {}
    
    def _build_cloud_type_message_group(self, cloud_type: str, links: List[Dict[str, Any]], keyword: str) -> List[Dict[str, Any]]:
        """
        构建单个网盘类型的消息组（第二层 + 第三层）
        
        Args:
            cloud_type (str): 网盘类型
            links (List[Dict[str, Any]]): 链接列表
            keyword (str): 搜索关键词
            
        Returns:
            List[Dict[str, Any]]: 消息组列表
        """
        try:
            messages = []
            cloud_name = self.cloud_type_names.get(cloud_type, cloud_type)
            
            # 第二层：网盘类型分类消息
            type_header_lines = [
                f"📁 {cloud_name} ({len(links)}个结果)",
                "=" * 30,
                f"搜索关键词：{keyword}",
                "",
                "📄 详细结果如下："
            ]
            
            type_header_message = {
                "type": "node",
                "data": {
                    "user_id": "1145141919",
                    "nickname": "Mortis",
                    "content": [
                        {
                            "type": "text",
                            "data": {
                                "text": "\n".join(type_header_lines)
                            }
                        }
                    ]
                }
            }
            messages.append(type_header_message)
            
            # 第三层：每个搜索结果的独立消息
            for i, link in enumerate(links, 1):
                result_message = self._build_single_result_message(link, i, cloud_name)
                if result_message:
                    messages.append(result_message)
            
            # 添加解密说明消息
            decrypt_message = {
                "type": "node",
                "data": {
                    "user_id": "1145141919",
                    "nickname": "Mortis",
                    "content": [
                        {
                            "type": "text",
                            "data": {
                                "text": "💡 提示：链接已使用兽音加密，请使用 /解密 指令解密后使用"
                            }
                        }
                    ]
                }
            }
            messages.append(decrypt_message)
            
            return messages
            
        except Exception as e:
            self.logger.error(f"构建网盘类型消息组失败: {e}")
            return []
    
    def _build_single_result_message(self, link: Dict[str, Any], index: int, cloud_name: str) -> Dict[str, Any]:
        """
        构建单个搜索结果的独立消息（第三层）
        
        Args:
            link (Dict[str, Any]): 链接信息
            index (int): 结果序号
            cloud_name (str): 网盘名称
            
        Returns:
            Dict[str, Any]: 单个结果消息节点
        """
        try:
            url = link.get('url', '')
            password = link.get('password', '')
            note = link.get('note', '').strip()
            datetime_str = link.get('datetime', '')
            
            # 使用兽音加密URL
            encrypted_url = self.beast_encoder.encode(url) if url else ""
            
            # 构建单个结果的消息内容
            result_lines = [
                f"📄 结果 #{index}",
                f"📁 网盘：{cloud_name}",
                ""
            ]
            
            if encrypted_url:
                result_lines.append(f"🔗 链接：{encrypted_url}")
            
            if note:
                result_lines.append(f"📝 名称：{note}")
            
            if password:
                result_lines.append(f"🔑 密码：{password}")
            
            if datetime_str:
                result_lines.append(f"⏰ 时间：{datetime_str}")
            
            return {
                "type": "node",
                "data": {
                    "user_id": "1145141919",
                    "nickname": "Mortis",
                    "content": [
                        {
                            "type": "text",
                            "data": {
                                "text": "\n".join(result_lines)
                            }
                        }
                    ]
                }
            }
            
        except Exception as e:
            self.log_unified("ERROR", f"构建单个结果消息失败: {e}", group_id="system", user_id="system")
            return {}
    
    async def _send_forward_message(self, user_id: str, messages: List[Dict[str, Any]], keyword: str, context: Dict[str, Any]):
        """
        发送嵌套合并转发消息
        最外层：一个合并转发消息
        中间层：按网盘类型分组的合并转发消息
        最内层：每个搜索结果的详细信息消息
        
        Args:
            user_id (str): 用户ID
            messages (List[Dict[str, Any]]): 消息列表（包含概览和各类型消息组）
            keyword (str): 搜索关键词
            context (Dict[str, Any]): 上下文信息
        """
        try:
            message_type = context.get('message_type', 'private')
            
            if not messages:
                await self._send_error_message(user_id, "没有可发送的消息", context)
                return
            
            # 构建嵌套的合并转发消息结构
            nested_forward_messages = []
            
            # 第一条消息是概览消息，添加到最外层
            overview_message = messages[0]
            nested_forward_messages.append(overview_message)
            
            # 处理剩余消息，按网盘类型分组并创建嵌套合并转发
            remaining_messages = messages[1:]
            if remaining_messages:
                # 按网盘类型分组消息
                type_groups = {}
                current_type = None
                current_group = []
                
                for message in remaining_messages:
                    # 检查是否是类型头部消息（包含网盘类型信息）
                    message_text = self._extract_message_text(message)
                    if "📁" in message_text and "个结果" in message_text:
                        # 保存上一个分组
                        if current_type and current_group:
                            type_groups[current_type] = current_group
                        
                        # 开始新的分组
                        current_type = self._extract_cloud_type_from_message(message_text)
                        current_group = [message]
                    else:
                        # 添加到当前分组
                        if current_group is not None:
                            current_group.append(message)
                
                # 保存最后一个分组
                if current_type and current_group:
                    type_groups[current_type] = current_group
                
                # 为每个网盘类型创建合并转发消息节点
                for cloud_type, type_messages in type_groups.items():
                    cloud_name = self.cloud_type_names.get(cloud_type, cloud_type)
                    
                    # 创建该网盘类型的合并转发消息节点
                    type_forward_node = {
                        "type": "node",
                        "data": {
                            "user_id": "1145141919",
                            "nickname": "Mortis",
                            "content": type_messages  # 包含该类型的所有消息
                        }
                    }
                    nested_forward_messages.append(type_forward_node)
            
            # 直接发送合并转发消息，不需要最外层包装
            forward_request = {
                "action": "send_forward_msg",
                "params": {
                    "messages": nested_forward_messages
                }
            }
            
            # 根据消息类型添加目标
            message_type = context.get('message_type', 'private')
            if message_type == 'group':
                group_id = context.get('group_id')
                if group_id:
                    forward_request["params"]["group_id"] = str(group_id)
            else:
                forward_request["params"]["user_id"] = str(user_id)
            
            # 发送请求
            result = await self.napcat_api_caller(forward_request)
            
            if result and result.get('status') == 'ok':
                group_id = context.get('group_id', '')
                self.log_unified("INFO", f"嵌套合并转发消息发送成功: 用户{user_id}, 关键词{keyword}, 消息数{len(nested_forward_messages)}", group_id=group_id, user_id=user_id)
            else:
                from datetime import datetime
                group_id = context.get('group_id', '')
                self.log_unified("ERROR", f"嵌套合并转发消息发送失败: {result}", group_id=group_id, user_id=user_id)
                
        except Exception as e:
            group_id = context.get('group_id', '')
            self.log_unified("ERROR", f"发送嵌套合并转发消息失败: {e}", group_id=group_id, user_id=user_id)
            await self._send_error_message(user_id, str(e), context)
    
    async def _send_single_message(self, user_id: str, message: Dict[str, Any], context: Dict[str, Any]):
        """
        发送单条消息（支持普通消息和合并转发消息）
        
        Args:
            user_id (str): 用户ID
            message (Dict[str, Any]): 消息内容
            context (Dict[str, Any]): 上下文信息
        """
        try:
            message_type = context.get('message_type', 'private')
            
            # 检查是否是合并转发消息
            if message.get('type') == 'node':
                # 发送合并转发消息
                send_request = {
                    "action": "send_forward_msg",
                    "params": {
                        "messages": [message]  # 将单个node消息包装在数组中
                    }
                }
            else:
                # 提取普通消息文本
                text_content = message.get('data', {}).get('content', [{}])[0].get('data', {}).get('text', '')
                
                # 构建普通消息发送请求
                send_request = {
                    "action": "send_msg",
                    "params": {
                        "message": text_content
                    }
                }
            
            # 根据消息类型添加目标
            if message_type == 'group':
                group_id = context.get('group_id')
                if group_id:
                    send_request["params"]["group_id"] = str(group_id)
            else:
                send_request["params"]["user_id"] = str(user_id)
            
            # 发送请求
            result = await self.napcat_api_caller(send_request)
            
            if result and result.get('status') == 'ok':
                group_id = context.get('group_id', '')
                self.log_unified("INFO", f"消息发送成功: 用户{user_id}", group_id=group_id, user_id=user_id)
            else:
                group_id = context.get('group_id', '')
                self.log_unified("ERROR", f"消息发送失败: {result}", group_id=group_id, user_id=user_id)
                
        except Exception as e:
            group_id = context.get('group_id', '')
            self.log_unified("ERROR", f"发送消息失败: {e}", group_id=group_id, user_id=user_id)
    

    
    def _extract_message_text(self, message: Dict[str, Any]) -> str:
        """
        从消息对象中提取文本内容
        
        Args:
            message (Dict[str, Any]): 消息对象
            
        Returns:
            str: 提取的文本内容
        """
        try:
            if message.get('type') == 'node':
                content = message.get('data', {}).get('content', [])
                if content and len(content) > 0:
                    first_content = content[0]
                    if first_content.get('type') == 'text':
                        return first_content.get('data', {}).get('text', '')
            elif message.get('type') == 'text':
                return message.get('data', {}).get('text', '')
            return ''
        except Exception as e:
            self.logger.error(f"提取消息文本失败: {e}")
            return ''
    
    def _extract_cloud_type_from_message(self, message_text: str) -> str:
        """
        从消息文本中提取网盘类型
        
        Args:
            message_text (str): 消息文本
            
        Returns:
            str: 网盘类型名称
        """
        try:
            # 查找 📁 后面的网盘名称
            import re
            match = re.search(r'📁\s*([^\s\(]+)', message_text)
            if match:
                return match.group(1)
            return "未知网盘"
        except Exception as e:
            self.log_unified("ERROR", f"提取网盘类型失败: {e}", group_id="system", user_id="system")
            return "未知网盘"
    
    async def _send_no_results_message(self, user_id: str, keyword: str, context: Dict[str, Any]):
        """
        发送无结果消息
        
        Args:
            user_id (str): 用户ID
            keyword (str): 搜索关键词
            context (Dict[str, Any]): 上下文信息
        """
        try:
            message_type = context.get('message_type', 'private')
            content = f"😔 未找到关于 \"{keyword}\" 的网盘资源，请尝试其他关键词。"
            
            # 构建消息请求
            if message_type == 'group':
                group_id = context.get('group_id')
                if group_id:
                    request = {
                        "action": "send_group_msg",
                        "params": {
                            "group_id": str(group_id),
                            "message": [{"type": "text", "data": {"text": content}}]
                        }
                    }
            else:
                request = {
                    "action": "send_private_msg",
                    "params": {
                        "user_id": str(user_id),
                        "message": [{"type": "text", "data": {"text": content}}]
                    }
                }
            
            await self.napcat_api_caller(request)
            
        except Exception as e:
            group_id = context.get('group_id', '')
            self.log_unified("ERROR", f"发送无结果消息失败: {e}", group_id=group_id, user_id=user_id)
    
    async def _send_error_message(self, user_id: str, error_msg: str, context: Dict[str, Any]):
        """
        发送错误消息
        
        Args:
            user_id (str): 用户ID
            error_msg (str): 错误信息
            context (Dict[str, Any]): 上下文信息
        """
        try:
            message_type = context.get('message_type', 'private')
            content = f"❌ 搜索过程中出现错误: {error_msg}"
            
            # 构建消息请求
            if message_type == 'group':
                group_id = context.get('group_id')
                if group_id:
                    request = {
                        "action": "send_group_msg",
                        "params": {
                            "group_id": str(group_id),
                            "message": [{"type": "text", "data": {"text": content}}]
                        }
                    }
            else:
                request = {
                    "action": "send_private_msg",
                    "params": {
                        "user_id": str(user_id),
                        "message": [{"type": "text", "data": {"text": content}}]
                    }
                }
            
            await self.napcat_api_caller(request)
            
        except Exception as e:
            group_id = context.get('group_id', '')
            self.log_unified("ERROR", f"发送错误消息失败: {e}", group_id=group_id, user_id=user_id)
    
    def _is_valid_url(self, url: str) -> bool:
        """
        验证URL格式是否有效
        
        支持：
        - http/https 标准链接
        - 常见网盘域名的无协议链接（自动识别域名）
        - magnet/ed2k/thunder 等协议
        
        Args:
            url (str): 待验证的URL
            
        Returns:
            bool: URL是否有效
        """
        try:
            u = url.strip()
            if not u:
                return False
            
            # 标准 http/https
            if re.match(r'^https?://[^\s]+$', u, re.IGNORECASE):
                return True
            
            # BT/电驴/迅雷等协议
            if re.match(r'^(magnet:|ed2k://|thunder://)[^\s]+$', u, re.IGNORECASE):
                return True
            
            # 常见网盘域名（可缺省协议）
            cloud_domains = (
                r'(?:pan\.baidu\.com|yun\.baidu\.com|www\.aliyundrive\.com|www\.123pan\.com|cloud\.189\.cn|pan\.quark\.cn|pan\.xunlei\.com|115\.com|www\.115\.com|drive\.pikpak\.me|mypikpak\.com|share\.weiyun\.com)'
            )
            if re.match(rf'^(?:https?://)?{cloud_domains}/[^\s]+$', u, re.IGNORECASE):
                return True
            
            # 允许部分不带斜杠但明显是网盘分享（如 115.com/t/xxxx）
            if re.match(rf'^(?:https?://)?{cloud_domains}[^\s]*$', u, re.IGNORECASE):
                return True
            
            return False
        except Exception:
            return False
    
    def _add_random_deletions(self, url: str) -> str:
        """
        在URL中随机添加删除字段
        
        Args:
            url (str): 原始URL
            
        Returns:
            str: 添加删除字段后的URL
        """
        try:
            # 确保URL长度足够进行操作
            if len(url) < 10:
                return url
            
            # 找到协议部分的结束位置（避免在头部添加删除字段）
            protocol_end = url.find('://') + 3 if '://' in url else 0
            
            # 确定可以插入删除字段的范围（避免头部和尾部）
            start_pos = max(protocol_end, 1)
            end_pos = len(url) - 1
            
            if start_pos >= end_pos:
                return url
            
            # 随机确定删除字段的数量（1-4个）
            deletion_count = random.randint(1, 4)
            
            # 生成随机插入位置
            positions = set()
            attempts = 0
            while len(positions) < deletion_count and attempts < 20:
                pos = random.randint(start_pos, end_pos)
                positions.add(pos)
                attempts += 1
            
            # 按位置从大到小排序，避免插入时位置偏移
            positions = sorted(positions, reverse=True)
            
            # 在指定位置插入删除字段
            result = url
            for pos in positions:
                result = result[:pos] + '删除' + result[pos:]
            
            return result
            
        except Exception as e:
            self.logger.error(f"添加删除字段失败: {e}")
            return url
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取帮助文本（委托 HelpService 统一管理）
        
        Returns:
            Dict[str, Any]: 帮助信息（与HelpService保持一致）
        """
        try:
            # 延迟导入以避免循环依赖
            from .help_service import HelpService
            helper = HelpService(self.config, self.data_manager, self.text_formatter)
            return helper.get_service_help("网盘搜索")
        except Exception as e:
            self.log_unified("ERROR", f"获取帮助信息失败: {e}", group_id="system", user_id="system")
            # 移除内置回退，保持单一来源
            return {"error": "获取帮助失败", "message": "请稍后重试"}