#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目塔罗牌服务

本文件实现塔罗牌功能，包括：
1. 简单塔罗牌抽取（1张或多张）
2. 专业塔罗牌占卜（结合AI分析）
3. 塔罗牌图片和解析的返回
4. 专业占卜的历史记录管理
5. 牌阵推荐和选择

支持的指令：
- /抽塔罗牌 - 抽取一张塔罗牌
- /抽塔罗牌 [数量]张 - 抽取指定数量的塔罗牌
- /抽塔罗牌专业 [问题] - 专业占卜模式
- /抽塔罗牌专业牌阵选择 [牌阵] - 选择牌阵进行占卜

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

from typing import Dict, List, Any, Optional, Tuple
import asyncio
import random
import re
import os
from datetime import datetime
from .base_service import BaseService
from .api_client import SiliconFlowClient


class TarotService(BaseService):
    """
    塔罗牌服务类
    
    负责处理塔罗牌相关的请求，包括简单抽牌、专业占卜、
    牌阵选择、AI分析等功能。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """
        初始化塔罗牌服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 初始化API客户端
        self.api_client = SiliconFlowClient(config, self.logger)
        
        # 塔罗牌数据（78张标准塔罗牌）
        self.tarot_cards: List[Dict[str, Any]] = self._initialize_tarot_cards()
        
        # 牌阵配置
        self.spreads: Dict[str, Dict[str, Any]] = {
            "三牌时间流牌阵": {
                "positions": ["过去", "现在", "未来"],
                "description": "展现竞争态势演变",
                "card_count": 3
            },
            "成功之路牌阵": {
                "positions": ["基础条件", "障碍", "助力", "短期发展", "最终结果"],
                "description": "分析成功路径中的各种因素",
                "card_count": 5
            },
            "凯尔特十字竞技场变阵": {
                "positions": ["现状", "挑战", "远景", "基础", "过去", "可能", "你的方法", "外界影响", "希望恐惧", "结果"],
                "description": "深度解析竞争格局中的能量流动",
                "card_count": 10
            }
        }
        
        # AI占卜提示词模板
        self.ai_prompt_template: str = self._get_ai_prompt_template()
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理塔罗牌请求消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[str]: 塔罗牌结果，如果无法处理则返回None
        """
        try:
            message = message.strip()
            
            # 检查是否是塔罗牌相关的消息
            if not self._is_tarot_related(message):
                return None
            
            # 解析塔罗牌指令
            command_type, count, question_or_spread = self._parse_tarot_command(message)
            
            if command_type == "simple":
                # 简单抽牌
                result = self.draw_simple_tarot(count or 1)
                return result
                
            elif command_type == "professional":
                # 专业占卜 - 开始阶段（后台执行并推送）
                if question_or_spread:
                    context = kwargs.get("context", {})
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(self._background_start_professional(user_id, question_or_spread, context))
                    except RuntimeError:
                        asyncio.create_task(self._background_start_professional(user_id, question_or_spread, context))
                    return None
                else:
                    return {
                        "content": "……要问什么问题呢。格式：/抽塔罗牌专业 [问题]",
                        "image_path": None
                    }
                    
            elif command_type == "professional_select":
                # 选择牌阵进行占卜
                if question_or_spread:
                    context = kwargs.get("context", {})
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(self._background_select_spread_and_read(user_id, question_or_spread, context))
                    except RuntimeError:
                        asyncio.create_task(self._background_select_spread_and_read(user_id, question_or_spread, context))
                    return None
                else:
                    return {
                        "content": "……要选哪个牌阵呢。格式：/抽塔罗牌专业牌阵选择 [牌阵]",
                        "image_path": None
                    }
                    
            return None
            
        except Exception:
            return {
                "content": "……塔罗牌出问题了。稍后再试吧",
                "image_path": None
            }
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取塔罗牌服务帮助文本
        
        Returns:
            str: 帮助文本
        """
        help_text = """
🔮 塔罗牌占卜服务

📖 可用指令：

1. 简单抽牌：
   /抽塔罗牌 - 抽取1张塔罗牌
   /抽塔罗牌 3张 - 抽取指定数量的塔罗牌（1-10张）

2. 专业占卜：
   /抽塔罗牌专业 [问题] - 开始专业塔罗占卜
   例如：/抽塔罗牌专业 我的爱情运势如何？
   
   然后选择牌阵：
   /抽塔罗牌专业牌阵选择 [牌阵名称]
   例如：/抽塔罗牌专业牌阵选择 三牌阵

🎯 可用牌阵：
• 三牌阵 - 适用于简单的过去、现在、未来问题
• 凯尔特十字 - 适用于复杂的人生问题分析
• 六芒星预测 - 适用于爱情、关系类问题
• 四要素 - 适用于全面的生活状况分析
• 时间流 - 适用于时间线相关的问题

✨ 专业占卜将提供AI深度解读，包括牌面解读、占卜结果、建议和谶语。
"""
        return help_text.strip()

    async def _background_start_professional(self, user_id: str, question: str, context: Dict[str, Any]) -> None:
        try:
            # 复用现有逻辑生成文本
            def _sync_start():
                return self.start_professional_reading(user_id, question)
            result = await asyncio.to_thread(_sync_start)
            content = (result or {}).get("content", "")
            if not content:
                return
            await self._send_text(content, context, user_id)
        except Exception:
            pass

    async def _background_select_spread_and_read(self, user_id: str, spread_name: str, context: Dict[str, Any]) -> None:
        try:
            # 加载会话与校验
            session_data = self.load_reading_session(user_id)
            if not session_data or session_data.get("status") != "waiting_spread_selection":
                await self._send_text("……先用 /抽塔罗牌专业 [问题] 开始占卜吧", context, user_id)
                return
            if spread_name not in self.spreads:
                available_spreads = "、".join(self.spreads.keys())
                await self._send_text(f"……没有 '{spread_name}' 这个牌阵，可用的：{available_spreads}", context, user_id)
                return
            spread_info = self.spreads[spread_name]
            question = session_data["question"]
            drawn_cards = self._draw_cards_for_spread(spread_info["card_count"])
            # AI解读放到线程池
            ai_interpretation = await asyncio.to_thread(self._call_ai_for_reading, question, spread_name, spread_info, drawn_cards)
            result = self.format_card_interpretation(question, spread_name, spread_info, drawn_cards, ai_interpretation)
            session_data["status"] = "completed"
            session_data["spread_name"] = spread_name
            session_data["drawn_cards"] = drawn_cards
            session_data["ai_interpretation"] = ai_interpretation
            session_data["result"] = result
            session_data["completed_time"] = datetime.now().isoformat()
            self.save_reading_session(user_id, session_data)
            # 推送图片与文本
            images = result.get("image_paths", [])
            text = result.get("text", "")
            await self._send_images_then_text(images, text, context, user_id)
        except Exception:
            pass

    async def _send_text(self, content: str, context: Dict[str, Any], user_id: str) -> None:
        if not content:
            return
        mt = context.get("message_type")
        if mt == "private":
            target_id = str(context.get("user_id", user_id))
            payload = {
                "action": "send_private_msg",
                "params": {
                    "user_id": target_id,
                    "message": [{"type": "text", "data": {"text": content}}]
                }
            }
        else:
            group_id = str(context.get("group_id", ""))
            if not group_id:
                return
            payload = {
                "action": "send_group_msg",
                "params": {
                    "group_id": group_id,
                    "message": [{"type": "text", "data": {"text": content}}]
                }
            }
        if hasattr(self, "server") and self.server:
            await self.server.send_response_to_napcat(payload)

    async def _send_images_then_text(self, image_paths: List[str], text: str, context: Dict[str, Any], user_id: str) -> None:
        try:
            # 先发图片
            for p in image_paths or []:
                abs_path = p
                if not os.path.isabs(abs_path):
                    abs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', p))
                mt = context.get("message_type")
                if mt == "private":
                    target_id = str(context.get("user_id", user_id))
                    payload = {
                        "action": "send_private_msg",
                        "params": {
                            "user_id": target_id,
                            "message": [{"type": "image", "data": {"file": f"file://{abs_path}"}}]
                        }
                    }
                else:
                    group_id = str(context.get("group_id", ""))
                    if not group_id:
                        continue
                    payload = {
                        "action": "send_group_msg",
                        "params": {
                            "group_id": group_id,
                            "message": [{"type": "image", "data": {"file": f"file://{abs_path}"}}]
                        }
                    }
                if hasattr(self, "server") and self.server:
                    await self.server.send_response_to_napcat(payload)
            # 再发文本
            if text:
                await self._send_text(text, context, user_id)
        except Exception:
            pass
    
    def draw_simple_tarot(self, count: int = 1) -> Dict[str, Any]:
        """
        简单塔罗牌抽取
        
        Args:
            count (int): 抽取数量
            
        Returns:
            Dict[str, Any]: 包含抽牌结果和图片路径的字典
        """
        drawn_cards = []
        available_cards = self.tarot_cards.copy()
        
        for _ in range(count):
            if not available_cards:
                break
                
            # 随机选择一张牌
            card = random.choice(available_cards)
            available_cards.remove(card)
            
            # 随机决定正位或逆位
            is_reversed = random.choice([True, False])
            
            # 创建抽牌结果
            drawn_card = card.copy()
            drawn_card["is_reversed"] = is_reversed
            drawn_card["position_meaning"] = drawn_card["reversed_meaning"] if is_reversed else drawn_card["upright_meaning"]
            
            drawn_cards.append(drawn_card)
            
        # 格式化结果并返回包含图片路径的字典
        return self._format_simple_tarot_result(drawn_cards)
    
    def _format_simple_tarot_result(self, cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        格式化简单抽牌结果
        
        Args:
            cards (List[Dict[str, Any]]): 抽取的塔罗牌
            
        Returns:
            Dict[str, Any]: 包含文本和图片路径的结果字典
        """
        if not cards:
            return {
                "text": "抽牌失败，请重试",
                "image_paths": []
            }
            
        result_parts = []
        image_paths = []
        
        if len(cards) == 1:
            card = cards[0]
            position_text = "逆位" if card["is_reversed"] else "正位"
            result_parts.append(f"🔮 您抽到的塔罗牌是：\n\n")
            result_parts.append(f"【{card['name']}】({position_text})\n")
            result_parts.append(f"📖 含义：{card['position_meaning']}\n")
            result_parts.append(f"✨ 描述：{card['description']}")
            
            # 获取图片路径
            image_path = self.get_card_image_path(card['name'], card['is_reversed'])
            image_paths.append(image_path)
        else:
            result_parts.append(f"🔮 您抽到了{len(cards)}张塔罗牌：\n\n")
            # 按抽牌顺序处理每张牌，确保图片路径顺序正确
            for i, card in enumerate(cards, 1):
                position_text = "逆位" if card["is_reversed"] else "正位"
                result_parts.append(f"第{i}张：【{card['name']}】({position_text})\n")
                result_parts.append(f"含义：{card['position_meaning']}\n\n")
                
                # 按顺序获取每张牌的图片路径，确保与抽牌顺序一致
                image_path = self.get_card_image_path(card['name'], card['is_reversed'])
                image_paths.append(image_path)
                
        return {
            "text": "".join(result_parts),
            "image_paths": image_paths
        }
    
    def start_professional_reading(self, user_id: str, question: str) -> Dict[str, Any]:
        """
        开始专业塔罗牌占卜
        
        Args:
            user_id (str): 用户ID
            question (str): 用户问题
            
        Returns:
            Dict[str, Any]: 推荐的牌阵信息
        """
        if not question:
            return {
                "content": "……要问什么问题呢，例如：/抽塔罗牌专业 我的运势如何",
                "image_path": None
            }
            
        # 清除旧的会话记录
        self.clear_reading_session(user_id)
        
        # 保存新的会话状态
        session_data = {
            "user_id": user_id,
            "question": question,
            "status": "waiting_spread_selection",
            "timestamp": datetime.now().isoformat()
        }
        self.save_reading_session(user_id, session_data)
        
        # 调用AI推荐牌阵
        ai_recommendation = self._get_ai_spread_recommendation(question)
        
        # 构建推荐牌阵的回复
        result_parts = []
        result_parts.append(f"……塔罗占卜\n\n")
        result_parts.append(f"问题：{question}\n\n")
        
        if ai_recommendation:
            result_parts.append("……推荐的牌阵：\n\n")
            result_parts.append(ai_recommendation)
        else:
            result_parts.append("……选个牌阵吧：\n\n")
            for i, (spread_name, spread_info) in enumerate(self.spreads.items(), 1):
                result_parts.append(f"{i}. 【{spread_name}】\n")
                result_parts.append(f"   ……{spread_info['description']}\n")
                result_parts.append(f"   {spread_info['card_count']}张牌\n\n")
            
        result_parts.append("\n回复：/抽塔罗牌专业牌阵选择 [牌阵名称]\n")
        result_parts.append("例如：/抽塔罗牌专业牌阵选择 三牌时间流牌阵")
        
        return {
            "content": "".join(result_parts),
            "image_path": None
        }
    
    def select_spread_and_read(self, user_id: str, spread_name: str) -> Dict[str, Any]:
        """
        选择牌阵并进行占卜
        
        Args:
            user_id (str): 用户ID
            spread_name (str): 选择的牌阵名称
            
        Returns:
            Dict[str, Any]: 包含占卜结果和图片路径的字典
        """
        # 加载会话数据
        session_data = self.load_reading_session(user_id)
        if not session_data or session_data.get("status") != "waiting_spread_selection":
            return {
            "text": "……先用 /抽塔罗牌专业 [问题] 开始占卜吧",
            "image_paths": []
        }
            
        # 验证牌阵名称
        if spread_name not in self.spreads:
            available_spreads = "、".join(self.spreads.keys())
            return {
            "text": f"……没有 '{spread_name}' 这个牌阵，可用的：{available_spreads}",
            "image_paths": []
        }
            
        spread_info = self.spreads[spread_name]
        question = session_data["question"]
        
        # 抽取塔罗牌
        drawn_cards = self._draw_cards_for_spread(spread_info["card_count"])
        
        # 调用AI进行解读
        ai_interpretation = self._call_ai_for_reading(question, spread_name, spread_info, drawn_cards)
        
        # 格式化结果
        result = self.format_card_interpretation(question, spread_name, spread_info, drawn_cards, ai_interpretation)
        
        # 更新会话状态为完成
        session_data["status"] = "completed"
        session_data["spread_name"] = spread_name
        session_data["drawn_cards"] = drawn_cards
        session_data["ai_interpretation"] = ai_interpretation
        session_data["result"] = result
        session_data["completed_time"] = datetime.now().isoformat()
        
        self.save_reading_session(user_id, session_data)
        
        return result
    
    def get_card_image_path(self, card_name: str, is_reversed: bool = False) -> str:
        """
        获取塔罗牌图片路径
        
        Args:
            card_name (str): 塔罗牌名称
            is_reversed (bool): 是否为逆位
            
        Returns:
            str: 图片文件路径
        """
        # 构建图片文件名
        # 将中文名称转换为文件名格式
        filename = card_name.replace(" ", "_")
        if is_reversed:
            filename += "_reversed"
        
        # 支持多种图片格式
        image_extensions = [".jpg", ".jpeg", ".png", ".webp"]
        
        # 构建完整路径
        image_dir = os.path.join(self.data_manager.base_path, "tarot", "images")
        
        # 尝试找到存在的图片文件
        for ext in image_extensions:
            image_path = os.path.join(image_dir, filename + ext)
            if os.path.exists(image_path):
                return f"data/tarot/images/{filename}{ext}"
        
        # 如果没有找到对应图片，返回默认图片路径
        for ext in image_extensions:
            default_image = os.path.join(image_dir, f"default_tarot{ext}")
            if os.path.exists(default_image):
                return f"data/tarot/images/default_tarot{ext}"
                
        return "data/tarot/images/default_tarot.jpg"
    
    def format_card_interpretation(self, question: str, spread_name: str, spread_info: Dict[str, Any], cards: List[Dict[str, Any]], ai_interpretation: str) -> Dict[str, Any]:
        """
        格式化塔罗牌解读结果
        
        Args:
            question (str): 占卜问题
            spread_name (str): 牌阵名称
            spread_info (Dict[str, Any]): 牌阵信息
            cards (List[Dict[str, Any]]): 抽取的牌
            ai_interpretation (str): AI解读结果
            
        Returns:
            Dict[str, Any]: 包含文本和图片路径的结果字典
        """
        result_parts = []
        image_paths = []
        
        # 标题和问题
        result_parts.append("……占卜结果\n\n")
        result_parts.append(f"问题：{question}\n")
        result_parts.append(f"牌阵：{spread_name}\n\n")
        
        # 抽到的牌面信息
        result_parts.append("抽到的牌：\n")
        # 按牌阵位置顺序处理每张牌，确保图片路径顺序与牌阵位置一致
        for i, card in enumerate(cards):
            position_name = spread_info["positions"][i] if i < len(spread_info["positions"]) else f"位置{i+1}"
            position_text = "逆位" if card["is_reversed"] else "正位"
            result_parts.append(f"  {position_name}：【{card['name']}】({position_text})\n")
            
            # 按牌阵位置顺序获取每张牌的图片路径
            image_path = self.get_card_image_path(card['name'], card['is_reversed'])
            image_paths.append(image_path)
        
        result_parts.append("\n")
        
        # AI解读结果
        if ai_interpretation:
            result_parts.append("……解读：\n")
            result_parts.append(ai_interpretation)
        else:
            result_parts.append("……解读出问题了。稍后再试吧")
        
        return {
            "text": "".join(result_parts),
            "image_paths": image_paths
        }
    
    def save_reading_session(self, user_id: str, session_data: Dict[str, Any]):
        """
        保存塔罗牌占卜会话
        
        Args:
            user_id (str): 用户ID
            session_data (Dict[str, Any]): 会话数据
        """
        try:
            self.data_manager.save_tarot_record(user_id, session_data)
        except Exception as e:
            self.log_unified("ERROR", f"保存塔罗会话失败: {e}", group_id="system", user_id="system")
    
    def load_reading_session(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        加载塔罗牌占卜会话
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Optional[Dict[str, Any]]: 会话数据
        """
        try:
            return self.data_manager.load_tarot_record(user_id)
        except Exception as e:
            self.log_unified("ERROR", f"加载塔罗会话失败: {e}", group_id="system", user_id="system")
            return None
    
    def clear_reading_session(self, user_id: str):
        """
        清除塔罗牌占卜会话
        
        Args:
            user_id (str): 用户ID
        """
        try:
            # 删除会话文件
            session_file = os.path.join(self.data_manager.tarot_path, f"tarot_{user_id}.json")
            if os.path.exists(session_file):
                os.remove(session_file)
        except Exception as e:
            self.log_unified("ERROR", f"清除塔罗会话失败: {e}", group_id="system", user_id="system")
    
    def _initialize_tarot_cards(self) -> List[Dict[str, Any]]:
        """
        初始化78张塔罗牌数据
        
        Returns:
            List[Dict[str, Any]]: 塔罗牌数据列表
        """
        cards = []
        
        # 大阿卡纳牌（22张）
        major_arcana = [
            {"name": "愚人", "english_name": "The Fool", "number": 0, "type": "major",
             "upright_meaning": "新的开始、冒险、纯真、自由",
             "reversed_meaning": "鲁莽、愚蠢、缺乏方向",
             "description": "一个年轻人站在悬崖边，手持白玫瑰，身边有一只小狗。象征着新的开始和无畏的冒险精神。"},
            {"name": "魔术师", "english_name": "The Magician", "number": 1, "type": "major",
             "upright_meaning": "意志力、创造力、技能、专注",
             "reversed_meaning": "操控、欺骗、缺乏技能",
             "description": "一个男子举起权杖，桌上摆放着四大元素的象征物。代表着将想法转化为现实的能力。"},
            {"name": "女祭司", "english_name": "The High Priestess", "number": 2, "type": "major",
             "upright_meaning": "直觉、神秘、内在智慧、潜意识",
             "reversed_meaning": "缺乏直觉、秘密、表面知识",
             "description": "一位神秘的女性坐在两根柱子之间，背后是神秘的帷幕。象征着内在智慧和直觉。"},
            {"name": "皇后", "english_name": "The Empress", "number": 3, "type": "major",
             "upright_meaning": "丰饶、母性、创造力、自然",
             "reversed_meaning": "依赖、空虚、缺乏成长",
             "description": "一位优雅的女性坐在自然环境中，周围充满生机。代表着母性和创造力。"},
            {"name": "皇帝", "english_name": "The Emperor", "number": 4, "type": "major",
             "upright_meaning": "权威、结构、控制、父性",
             "reversed_meaning": "专制、缺乏纪律、不成熟",
             "description": "一位威严的男性坐在石制王座上，手持权杖。象征着权威和秩序。"},
            {"name": "教皇", "english_name": "The Hierophant", "number": 5, "type": "major",
             "upright_meaning": "传统、精神指导、宗教、教育",
             "reversed_meaning": "反叛、非传统、个人信仰",
             "description": "一位宗教领袖坐在神圣的环境中，手持权杖。代表着传统智慧和精神指导。"},
            {"name": "恋人", "english_name": "The Lovers", "number": 6, "type": "major",
             "upright_meaning": "爱情、关系、选择、和谐",
             "reversed_meaning": "不和谐、错误选择、分离",
             "description": "一对恋人在天使的见证下相拥。象征着爱情和重要的选择。"},
            {"name": "战车", "english_name": "The Chariot", "number": 7, "type": "major",
             "upright_meaning": "胜利、意志力、控制、决心",
             "reversed_meaning": "失控、缺乏方向、失败",
             "description": "一位战士驾驶着由两匹马拉的战车。代表着通过意志力获得胜利。"},
            {"name": "力量", "english_name": "Strength", "number": 8, "type": "major",
             "upright_meaning": "内在力量、勇气、耐心、控制",
             "reversed_meaning": "软弱、自我怀疑、缺乏勇气",
             "description": "一位女性温柔地抚摸着狮子。象征着内在力量和温柔的控制。"},
            {"name": "隐者", "english_name": "The Hermit", "number": 9, "type": "major",
             "upright_meaning": "内省、寻找、指导、智慧",
             "reversed_meaning": "孤立、迷失、拒绝帮助",
             "description": "一位老者手持灯笼在黑暗中前行。代表着内在的寻找和智慧的指引。"},
            {"name": "命运之轮", "english_name": "Wheel of Fortune", "number": 10, "type": "major",
             "upright_meaning": "命运、机会、循环、转折点",
             "reversed_meaning": "厄运、缺乏控制、破坏循环",
             "description": "一个巨大的轮子在天空中转动，周围有神秘的符号。象征着命运的变化。"},
            {"name": "正义", "english_name": "Justice", "number": 11, "type": "major",
             "upright_meaning": "公正、平衡、真理、法律",
             "reversed_meaning": "不公、偏见、缺乏责任",
             "description": "一位女性手持天平和宝剑，象征着公正和平衡。"},
            {"name": "倒吊人", "english_name": "The Hanged Man", "number": 12, "type": "major",
             "upright_meaning": "牺牲、等待、新视角、启发",
             "reversed_meaning": "拖延、抗拒、无意义的牺牲",
             "description": "一个人倒挂在树上，但表情平静。代表着通过牺牲获得新的视角。"},
            {"name": "死神", "english_name": "Death", "number": 13, "type": "major",
             "upright_meaning": "结束、转变、重生、释放",
             "reversed_meaning": "抗拒变化、停滞、恐惧",
             "description": "死神骑着白马，象征着必要的结束和新的开始。"},
            {"name": "节制", "english_name": "Temperance", "number": 14, "type": "major",
             "upright_meaning": "平衡、节制、耐心、和谐",
             "reversed_meaning": "不平衡、过度、缺乏耐心",
             "description": "一位天使在两个杯子之间倒水，象征着平衡和节制。"},
            {"name": "恶魔", "english_name": "The Devil", "number": 15, "type": "major",
             "upright_meaning": "束缚、诱惑、物质主义、依赖",
             "reversed_meaning": "释放、觉醒、摆脱束缚",
             "description": "恶魔坐在王座上，下方有被锁链束缚的人。代表着物质的束缚和诱惑。"},
            {"name": "塔", "english_name": "The Tower", "number": 16, "type": "major",
             "upright_meaning": "突然变化、破坏、启示、觉醒",
             "reversed_meaning": "避免灾难、恐惧变化、内在动荡",
             "description": "闪电击中高塔，人们从塔中坠落。象征着突然的变化和觉醒。"},
            {"name": "星星", "english_name": "The Star", "number": 17, "type": "major",
             "upright_meaning": "希望、灵感、治愈、指引",
             "reversed_meaning": "绝望、缺乏信心、失去方向",
             "description": "一位女性在星空下倒水，象征着希望和精神的指引。"},
            {"name": "月亮", "english_name": "The Moon", "number": 18, "type": "major",
             "upright_meaning": "幻觉、恐惧、潜意识、直觉",
             "reversed_meaning": "释放恐惧、内在平静、直觉清晰",
             "description": "月亮照耀着神秘的景象，有狼和狗在嚎叫。代表着潜意识和幻觉。"},
            {"name": "太阳", "english_name": "The Sun", "number": 19, "type": "major",
             "upright_meaning": "快乐、成功、活力、积极",
             "reversed_meaning": "暂时的挫折、缺乏热情、延迟",
             "description": "太阳照耀着快乐的孩子，象征着纯真的快乐和成功。"},
            {"name": "审判", "english_name": "Judgement", "number": 20, "type": "major",
             "upright_meaning": "重生、内在呼唤、宽恕、觉醒",
             "reversed_meaning": "自我怀疑、严厉判断、缺乏宽恕",
             "description": "天使吹响号角，死者从坟墓中复活。象征着精神的重生和觉醒。"},
            {"name": "世界", "english_name": "The World", "number": 21, "type": "major",
             "upright_meaning": "完成、成就、旅程结束、满足",
             "reversed_meaning": "未完成、缺乏成就、延迟",
             "description": "一位舞者在花环中央，四个角落有四大元素的象征。代表着完成和成就。"}
        ]
        
        cards.extend(major_arcana)
        
        # 小阿卡纳牌（56张）
        suits = {
            "权杖": {"english": "Wands", "element": "火", "meaning": "创造力、激情、事业"},
            "圣杯": {"english": "Cups", "element": "水", "meaning": "情感、关系、直觉"},
            "宝剑": {"english": "Swords", "element": "风", "meaning": "思想、沟通、冲突"},
            "星币": {"english": "Pentacles", "element": "土", "meaning": "物质、金钱、实用"}
        }
        
        # 数字牌（1-10）
        for suit_name, suit_info in suits.items():
            for number in range(1, 11):
                card_name = f"{suit_name}{self._number_to_chinese(number)}"
                cards.append({
                    "name": card_name,
                    "english_name": f"{number} of {suit_info['english']}",
                    "number": number,
                    "suit": suit_name,
                    "type": "minor",
                    "element": suit_info["element"],
                    "upright_meaning": self._get_minor_upright_meaning(suit_name, number),
                    "reversed_meaning": self._get_minor_reversed_meaning(suit_name, number),
                    "description": f"{suit_name}的{self._number_to_chinese(number)}，代表{suit_info['meaning']}领域的相关含义。"
                })
        
        # 宫廷牌
        court_cards = ["侍从", "骑士", "王后", "国王"]
        court_english = ["Page", "Knight", "Queen", "King"]
        
        for suit_name, suit_info in suits.items():
            for i, court in enumerate(court_cards):
                card_name = f"{suit_name}{court}"
                cards.append({
                    "name": card_name,
                    "english_name": f"{court_english[i]} of {suit_info['english']}",
                    "number": 11 + i,
                    "suit": suit_name,
                    "type": "court",
                    "element": suit_info["element"],
                    "upright_meaning": self._get_court_upright_meaning(suit_name, court),
                    "reversed_meaning": self._get_court_reversed_meaning(suit_name, court),
                    "description": f"{suit_name}的{court}，代表{suit_info['meaning']}领域的人物特质。"
                })
        
        return cards
    
    def _number_to_chinese(self, number: int) -> str:
        """将数字转换为中文"""
        chinese_numbers = {
            1: "一", 2: "二", 3: "三", 4: "四", 5: "五",
            6: "六", 7: "七", 8: "八", 9: "九", 10: "十"
        }
        return chinese_numbers.get(number, str(number))
    
    def _get_minor_upright_meaning(self, suit: str, number: int) -> str:
        """获取小阿卡纳正位含义"""
        meanings = {
            "权杖": {
                1: "新的开始、创造力、灵感", 2: "计划、等待、个人力量", 3: "扩展、远见、领导力",
                4: "庆祝、和谐、家庭", 5: "竞争、冲突、挑战", 6: "胜利、成功、认可",
                7: "防御、坚持、挑战", 8: "快速行动、进展、消息", 9: "坚韧、毅力、最后的努力",
                10: "负担、责任、努力工作"
            },
            "圣杯": {
                1: "新的爱情、情感开始、直觉", 2: "伙伴关系、爱情、和谐", 3: "友谊、庆祝、创造力",
                4: "冷漠、沉思、重新评估", 5: "失望、悲伤、失落", 6: "怀旧、童年、纯真",
                7: "幻想、选择、愿望", 8: "放弃、寻找、内心召唤", 9: "满足、愿望实现、快乐",
                10: "家庭幸福、情感满足、和谐"
            },
            "宝剑": {
                1: "新想法、心智清晰、突破", 2: "困难决定、平衡、僵局", 3: "心碎、悲伤、痛苦",
                4: "休息、沉思、恢复", 5: "冲突、失败、不和", 6: "过渡、旅行、前进",
                7: "欺骗、策略、逃避", 8: "限制、困境、无力感", 9: "焦虑、恐惧、噩梦",
                10: "背叛、痛苦、结束"
            },
            "星币": {
                1: "新机会、表现、财富", 2: "平衡、适应、时间管理", 3: "团队合作、技能、工作",
                4: "安全、控制、保守", 5: "财务困难、不安全感、担忧", 6: "慷慨、分享、互惠",
                7: "耐心、投资、努力", 8: "技能发展、勤奋、专注", 9: "独立、财务安全、成就",
                10: "财富、家庭、传承"
            }
        }
        return meanings.get(suit, {}).get(number, "未知含义")
    
    def _get_minor_reversed_meaning(self, suit: str, number: int) -> str:
        """获取小阿卡纳逆位含义"""
        meanings = {
            "权杖": {
                1: "缺乏方向、延迟、创造力受阻", 2: "缺乏计划、恐惧、个人目标不明", 3: "缺乏远见、延迟、计划失败",
                4: "不和谐、缺乏稳定、家庭问题", 5: "避免冲突、内在冲突、协议", 6: "延迟成功、缺乏认可、自我怀疑",
                7: "屈服、缺乏勇气、感到不知所措", 8: "延迟、挫折、缺乏进展", 9: "偏执、固执、缺乏信心",
                10: "委派、寻求帮助、释放负担"
            },
            "圣杯": {
                1: "情感封闭、缺乏爱、创造力受阻", 2: "关系不平衡、缺乏和谐、分离", 3: "孤独、内向、缺乏庆祝",
                4: "新机会、动机、重新参与", 5: "接受、宽恕、从失望中恢复", 6: "活在当下、新机会、前进",
                7: "现实、做出选择、集中注意力", 8: "恐惧改变、回归、寻找快乐", 9: "内在不满、物质主义、傲慢",
                10: "家庭不和、价值观冲突、关系破裂"
            },
            "宝剑": {
                1: "混乱、缺乏清晰、错误信息", 2: "信息过载、犹豫不决、情感决定", 3: "恢复、宽恕、从痛苦中前进",
                4: "躁动、缺乏休息、倦怠", 5: "和解、宽恕、从冲突中前进", 6: "抗拒改变、个人过渡、释放过去",
                7: "坦诚、诚实、寻求帮助", 8: "自我限制、寻求帮助、新视角", 9: "内在力量、面对恐惧、寻求帮助",
                10: "恢复、重生、从背叛中前进"
            },
            "星币": {
                1: "失去机会、缺乏计划、财务损失", 2: "失衡、过度承诺、时间管理不善", 3: "缺乏团队合作、技能不匹配、质量差",
                4: "过度控制、贪婪、财务不安全", 5: "财务改善、寻求帮助、克服困难", 6: "自私、债务、一方面的关系",
                7: "缺乏耐心、缺乏回报、质疑投资", 8: "缺乏专注、完美主义、技能不匹配", 9: "过度工作、财务依赖、缺乏自我照顾",
                10: "财务失败、家庭冲突、缺乏传承"
            }
        }
        return meanings.get(suit, {}).get(number, "未知含义")
    
    def _get_court_upright_meaning(self, suit: str, court: str) -> str:
        """获取宫廷牌正位含义"""
        meanings = {
            "权杖": {
                "侍从": "热情、冒险、自由精神", "骑士": "冲动、冒险、无畏",
                "王后": "自信、外向、温暖", "国王": "自然领导者、愿景、企业家"
            },
            "圣杯": {
                "侍从": "创造性机会、直觉信息、好奇心", "骑士": "浪漫、魅力、想象力",
                "王后": "同情心、关怀、直觉", "国王": "情感平衡、慷慨、外交"
            },
            "宝剑": {
                "侍从": "新想法、好奇心、警觉", "骑士": "雄心、冲动、行动导向",
                "王后": "独立、清晰思维、直接沟通", "国王": "心智清晰、知识权威、真理"
            },
            "星币": {
                "侍从": "表现、雄心、技能发展", "骑士": "效率、努力工作、保守",
                "王后": "实用、慷慨、安全", "国王": "财务安全、商业头脑、安全"
            }
        }
        return meanings.get(suit, {}).get(court, "未知含义")
    
    def _get_court_reversed_meaning(self, suit: str, court: str) -> str:
        """获取宫廷牌逆位含义"""
        meanings = {
            "权杖": {
                "侍从": "缺乏方向、拖延、缺乏想法", "骑士": "鲁莽、缺乏耐心、冲动",
                "王后": "自私、嫉妒、缺乏信心", "国王": "专制、缺乏耐心、残酷"
            },
            "圣杯": {
                "侍从": "情感不成熟、缺乏创造力、直觉受阻", "骑士": "情绪化、喜怒无常、嫉妒",
                "王后": "情感依赖、缺乏信心、不安全感", "国王": "情感操控、喜怒无常、缺乏同情心"
            },
            "宝剑": {
                "侍从": "缺乏想法、缺乏计划、间谍活动", "骑士": "鲁莽、缺乏方向、冲动",
                "王后": "冷酷、残忍、缺乏同情心", "国王": "专制、操控、滥用权力"
            },
            "星币": {
                "侍从": "缺乏进展、拖延、缺乏目标", "骑士": "懒惰、不负责任、粗心",
                "王后": "自我照顾不足、金融依赖、嫉妒", "国王": "贪婪、物质主义、财务不安全"
            }
        }
        return meanings.get(suit, {}).get(court, "未知含义")
    
    def _get_ai_prompt_template(self) -> str:
        """
        获取AI占卜的提示词模板
        
        Returns:
            str: 提示词模板
        """
        # 从配置文件读取AI提示词
        try:
            return self.config.get('ai_prompts', {}).get('tarot_reading_prompt', 
                '你是一位专业的塔罗牌占卜师，请根据用户的问题和抽到的牌进行占卜解读。')
        except Exception as e:
            self.logger.error(f"获取塔罗牌占卜提示词失败: {e}")
            return '你是一位专业的塔罗牌占卜师，请根据用户的问题和抽到的牌进行占卜解读。'
    
    def _get_ai_spread_recommendation(self, question: str) -> str:
        """
        调用AI推荐合适的牌阵
        
        Args:
            question (str): 用户问题
            
        Returns:
            str: AI推荐的牌阵信息
        """
        try:
            # 构建可用牌阵信息
            spreads_info = []
            for spread_name, spread_data in self.spreads.items():
                spreads_info.append(f"【{spread_name}】- {spread_data['description']} ({spread_data['card_count']}张牌)")
            
            spreads_text = "\n".join(spreads_info)
            
            # 构建用户消息
            user_message = f"""用户问题：{question}

可用的塔罗牌阵：
{spreads_text}

请根据用户的问题，推荐最合适的1-2个牌阵，并说明推荐理由。请用简洁明了的格式回复，包含牌阵名称、推荐理由。"""
            
            # 从配置文件获取系统提示词
            try:
                system_prompt = self.config.get('ai_prompts', {}).get('tarot_spread_recommendation_prompt', 
                    '你是一位专业的塔罗牌占卜师，请根据用户的问题推荐最合适的牌阵。')
            except Exception as e:
                self.logger.error(f"获取塔罗牌阵推荐提示词失败: {e}")
                system_prompt = '你是一位专业的塔罗牌占卜师，请根据用户的问题推荐最合适的牌阵。'
            
            # 调用API客户端
            result = self.api_client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.7,
                max_tokens=500
            )
            
            if result.get('success', False):
                return result.get('content', '')
            else:
                self.logger.error(f"AI牌阵推荐失败: {result.get('error', 'unknown')} - {result.get('message', '')}")
                return None
                
        except Exception as e:
            self.logger.error(f"调用AI牌阵推荐失败: {e}")
            return None
    
    def _call_ai_for_reading(self, question: str, spread_name: str, spread_info: Dict[str, Any], cards: List[Dict[str, Any]]) -> str:
        """
        调用AI进行塔罗牌解读
        
        Args:
            question (str): 占卜问题
            spread_name (str): 牌阵名称
            spread_info (Dict[str, Any]): 牌阵信息
            cards (List[Dict[str, Any]]): 抽取的牌
            
        Returns:
            str: AI解读结果
        """
        try:
            # 构建牌面信息
            cards_info = []
            for i, card in enumerate(cards):
                position_name = spread_info["positions"][i] if i < len(spread_info["positions"]) else f"位置{i+1}"
                position_text = "逆位" if card["is_reversed"] else "正位"
                cards_info.append(f"{position_name}：{card['name']}（{position_text}）")
            
            # 构建用户消息
            user_message = f"""
问题：{question}
牌阵：{spread_name}
抽到的牌：
{chr(10).join(cards_info)}

请根据以上信息进行塔罗占卜解读。
"""
            
            # 获取AI提示词模板
            system_prompt = self._get_ai_prompt_template()
            
            # 调用API客户端
            result = self.api_client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=1.2,
                max_tokens=2000
            )
            
            if result.get('success', False):
                content = result.get('content', '')
                if content:
                    return content
                else:
                    return "AI解读失败，响应内容为空。"
            else:
                error_type = result.get('error', 'unknown')
                error_message = result.get('message', 'AI服务调用失败')
                self.logger.error(f"AI塔罗解读失败: {error_type} - {error_message}")
                return f"AI解读失败：{error_message}"
                
        except Exception as e:
            self.logger.error(f"AI解读过程中发生错误: {e}")
            return f"AI解读过程中发生错误：{str(e)}"
    
    def _draw_cards_for_spread(self, card_count: int) -> List[Dict[str, Any]]:
        """
        为牌阵抽取指定数量的塔罗牌
        
        Args:
            card_count (int): 需要抽取的牌数
            
        Returns:
            List[Dict[str, Any]]: 抽取的塔罗牌列表
        """
        drawn_cards = []
        available_cards = self.tarot_cards.copy()
        
        for i in range(card_count):
            if not available_cards:
                break
                
            # 随机选择一张牌
            card = random.choice(available_cards)
            available_cards.remove(card)
            
            # 随机决定正位或逆位
            is_reversed = random.choice([True, False])
            
            # 创建抽牌结果，包含位置信息
            drawn_card = card.copy()
            drawn_card["is_reversed"] = is_reversed
            drawn_card["position_meaning"] = drawn_card["reversed_meaning"] if is_reversed else drawn_card["upright_meaning"]
            drawn_card["position_index"] = i + 1  # 牌在牌阵中的位置
            
            drawn_cards.append(drawn_card)
            
        return drawn_cards
    
    def _parse_tarot_command(self, message: str) -> Tuple[str, Optional[int], Optional[str]]:
        """
        解析塔罗牌指令
        
        Args:
            message (str): 用户消息
            
        Returns:
            Tuple[str, Optional[int], Optional[str]]: (命令类型, 数量, 问题/牌阵)
        """
        message = message.strip()
        
        # 专业占卜牌阵选择
        if message.startswith("/抽塔罗牌专业牌阵选择"):
            spread_match = re.search(r"/抽塔罗牌专业牌阵选择\s+(.+)", message)
            spread_name = spread_match.group(1).strip() if spread_match else None
            return "professional_select", None, spread_name
            
        # 专业占卜
        elif message.startswith("/抽塔罗牌专业"):
            question_match = re.search(r"/抽塔罗牌专业\s+(.+)", message)
            question = question_match.group(1).strip() if question_match else None
            return "professional", None, question
            
        # 简单抽牌
        elif message.startswith("/抽塔罗牌"):
            # 检查是否指定数量
            count_match = re.search(r"/抽塔罗牌\s+(\d+)张", message)
            if count_match:
                count = int(count_match.group(1))
                # 限制抽牌数量
                count = min(max(count, 1), 10)  # 最少1张，最多10张
                return "simple", count, None
            else:
                # 默认抽1张
                return "simple", 1, None
                
        return "unknown", None, None
    
    def _is_tarot_related(self, message: str) -> bool:
        """
        检查消息是否与塔罗牌相关
        
        Args:
            message (str): 用户消息
            
        Returns:
            bool: 是否与塔罗牌相关
        """
        message = message.strip().lower()
        
        # 塔罗牌相关关键词
        tarot_keywords = [
            "/抽塔罗牌",
            "/help 塔罗牌",
            "/抽塔罗牌专业",
            "/抽塔罗牌专业牌阵选择"
        ]
        
        for keyword in tarot_keywords:
            if keyword.lower() in message:
                return True
                
        return False
