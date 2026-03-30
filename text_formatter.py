#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目文本格式化器
"""

from typing import Dict, Any, Optional
import re


class TextFormatter:
    """
    文本格式化器类
    """
    
    def __init__(self):
        """
        初始化文本格式化器
        """
        # 格式化配置
        self.max_message_length = 1000  # QQ消息最大长度限制
        self.line_separator = "\n"
        self.section_separator = "\n" + "─" * 20 + "\n"
        
        # 表情符号映射
        self.emoji_map = {
            "fortune": "🔮",
            "zodiac": "⭐",
            "tarot": "🃏",
            "quote": "💫",
            "chat": "💬",
            "error": "❌",
            "warning": "⚠️",
            "success": "✅",
            "info": "ℹ️",
            "heart": "❤️",
            "star": "⭐",
            "moon": "🌙",
            "sun": "☀️"
        }
        
        # 运势等级映射
        self.fortune_level_map = {
            "excellent": {"name": "极佳", "emoji": "🌟", "color": "金色"},
            "good": {"name": "良好", "emoji": "😊", "color": "绿色"},
            "average": {"name": "一般", "emoji": "😐", "color": "黄色"},
            "poor": {"name": "较差", "emoji": "😔", "color": "橙色"},
            "bad": {"name": "糟糕", "emoji": "😰", "color": "红色"}
        }
    
    def format_help_message(self, service_name: Optional[str] = None) -> str:
        """
        格式化帮助信息
        
        Args:
            service_name (Optional[str]): 服务名称，如果为None则返回总体帮助
            
        Returns:
            str: 格式化后的帮助信息
        """
        if service_name is None:
            # 总体帮助信息
            help_text = f"{self.emoji_map['info']} Mortisfun 功能菜单\n\n"
            help_text += "📋 可用功能：\n"
            help_text += "• 每日运势 - 输入 '运势' 或 'fortune'\n"
            help_text += "• 星座运势 - 输入 '星座 [星座名]' 或 'zodiac [星座名]'\n"
            help_text += "• 塔罗占卜 - 输入 '塔罗' 或 'tarot'\n"
            help_text += "• 励志语录 - 输入 '语录' 或 'quote'\n"
            help_text += "• AI对话 - 输入 '聊天' 或 'chat'\n\n"
            help_text += "💡 提示：输入 '帮助 [功能名]' 获取详细说明"
        else:
            # 特定服务的帮助信息
            service_help = {
                "fortune": f"{self.emoji_map['fortune']} 每日运势功能\n\n" +
                          "🎯 功能说明：\n" +
                          "• 每日运势：获取今日整体运势\n" +
                          "• 运势对比：对比不同日期的运势\n" +
                          "• 运势历史：查看历史运势记录\n\n" +
                          "📝 使用方法：\n" +
                          "• 运势 - 获取今日运势\n" +
                          "• 运势 对比 - 对比运势\n" +
                          "• 运势 历史 - 查看历史",
                
                "zodiac": f"{self.emoji_map['zodiac']} 星座运势功能\n\n" +
                         "🎯 功能说明：\n" +
                         "• 12星座今日运势查询\n" +
                         "• 星座运势对比分析\n" +
                         "• 星座运势历史记录\n\n" +
                         "📝 使用方法：\n" +
                         "• 星座 白羊座 - 查看白羊座运势\n" +
                         "• 星座 对比 白羊座 金牛座 - 对比两个星座\n" +
                         "• 星座 历史 白羊座 - 查看历史记录",
                
                "tarot": f"{self.emoji_map['tarot']} 塔罗占卜功能\n\n" +
                        "🎯 功能说明：\n" +
                        "• 简单抽牌占卜\n" +
                        "• 专业牌阵占卜\n" +
                        "• AI智能解读\n\n" +
                        "📝 使用方法：\n" +
                        "• 塔罗 - 简单抽牌\n" +
                        "• 塔罗 专业 - 专业占卜\n" +
                        "• 塔罗 牌阵 [牌阵名] - 指定牌阵",
                
                "quote": f"{self.emoji_map['quote']} 励志语录功能\n\n" +
                        "🎯 功能说明：\n" +
                        "• 随机励志语录\n" +
                        "• 分类语录查询\n" +
                        "• 自定义语录添加\n\n" +
                        "📝 使用方法：\n" +
                        "• 语录 - 随机语录\n" +
                        "• 语录 [分类] - 指定分类\n" +
                        "• 语录 添加 [内容] - 添加语录",
                
                "chat": f"{self.emoji_map['chat']} AI对话功能\n\n" +
                       "🎯 功能说明：\n" +
                       "• 智能AI对话\n" +
                       "• 对话历史记录\n" +
                       "• 对话状态管理\n\n" +
                       "📝 使用方法：\n" +
                       "• 聊天 [内容] - 开始对话\n" +
                       "• 聊天 结束 - 结束对话\n" +
                       "• 聊天 重置 - 重置对话"
            }
            
            help_text = service_help.get(service_name.lower(), 
                                       f"{self.emoji_map['error']} 未找到 '{service_name}' 的帮助信息")
        
        return self.truncate_text(help_text)
    
    def format_fortune_result(self, fortune_data: Dict[str, Any]) -> str:
        """
        格式化运势结果
        
        Args:
            fortune_data (Dict[str, Any]): 运势数据
            
        Returns:
            str: 格式化后的运势结果
        """
        if not fortune_data:
            return f"{self.emoji_map['error']} 今日运势数据为空"
        
        try:
            date = fortune_data.get('date', '今日')
            overall_level = fortune_data.get('overall_level', 'average')
            description = fortune_data.get('description', '暂无描述')
            
            # 获取运势等级信息
            level_info = self.fortune_level_map.get(overall_level, self.fortune_level_map['average'])
            
            # 构建运势结果
            result = f"{self.emoji_map['fortune']} {date} 运势报告\n\n"
            result += f"🎯 整体运势：{level_info['emoji']} {level_info['name']}\n\n"
            
            # 添加详细运势信息
            if 'details' in fortune_data:
                details = fortune_data['details']
                result += "📊 详细运势：\n"
                
                for category, info in details.items():
                    if isinstance(info, dict):
                        level = info.get('level', 'average')
                        level_emoji = self.fortune_level_map.get(level, self.fortune_level_map['average'])['emoji']
                        result += f"• {category}：{level_emoji} {info.get('description', '暂无')}\n"
                    else:
                        result += f"• {category}：{info}\n"
                
                result += "\n"
            
            # 添加运势描述
            result += f"💭 运势解读：\n{description}\n\n"
            
            # 添加幸运信息
            if 'lucky' in fortune_data:
                lucky = fortune_data['lucky']
                result += "🍀 今日幸运：\n"
                if 'color' in lucky:
                    result += f"• 幸运颜色：{lucky['color']}\n"
                if 'number' in lucky:
                    result += f"• 幸运数字：{lucky['number']}\n"
                if 'direction' in lucky:
                    result += f"• 幸运方位：{lucky['direction']}\n"
                result += "\n"
            
            # 添加建议
            if 'advice' in fortune_data:
                result += f"💡 今日建议：\n{fortune_data['advice']}"
            
            return self.truncate_text(result)
            
        except Exception as e:
            return f"{self.emoji_map['error']} 运势格式化失败：{str(e)}"
    
    def format_zodiac_fortune(self, zodiac: str, fortune_data: Dict[str, Any]) -> str:
        """
        格式化星座运势结果
        
        Args:
            zodiac (str): 星座名称
            fortune_data (Dict[str, Any]): 运势数据
            
        Returns:
            str: 格式化后的星座运势结果
        """
        if not fortune_data:
            return f"{self.emoji_map['error']} {zodiac} 运势数据为空"
        
        try:
            date = fortune_data.get('date', '今日')
            overall_score = fortune_data.get('overall_score', 0)
            
            # 星座符号映射
            zodiac_symbols = {
                "白羊座": "♈", "金牛座": "♉", "双子座": "♊", "巨蟹座": "♋",
                "狮子座": "♌", "处女座": "♍", "天秤座": "♎", "天蝎座": "♏",
                "射手座": "♐", "摩羯座": "♑", "水瓶座": "♒", "双鱼座": "♓"
            }
            
            zodiac_symbol = zodiac_symbols.get(zodiac, "⭐")
            
            # 构建星座运势结果
            result = f"{self.emoji_map['zodiac']} {zodiac_symbol} {zodiac} {date}运势\n\n"
            result += f"🎯 综合评分：{overall_score}/100\n\n"
            
            # 添加各项运势
            if 'categories' in fortune_data:
                categories = fortune_data['categories']
                result += "📊 运势详情：\n"
                
                category_icons = {
                    "爱情运势": "💕", "事业运势": "💼", "财运": "💰", "健康运势": "🏥",
                    "学业运势": "📚", "人际关系": "👥", "家庭运势": "🏠", "旅行运势": "✈️"
                }
                
                for category, info in categories.items():
                    icon = category_icons.get(category, "•")
                    if isinstance(info, dict):
                        score = info.get('score', 0)
                        desc = info.get('description', '暂无描述')
                        result += f"{icon} {category}：{score}/100\n   {desc}\n\n"
                    else:
                        result += f"{icon} {category}：{info}\n\n"
            
            # 添加今日建议
            if 'advice' in fortune_data:
                result += f"💡 今日建议：\n{fortune_data['advice']}\n\n"
            
            # 添加幸运信息
            if 'lucky' in fortune_data:
                lucky = fortune_data['lucky']
                result += "🍀 今日幸运：\n"
                if 'color' in lucky:
                    result += f"• 幸运颜色：{lucky['color']}\n"
                if 'number' in lucky:
                    result += f"• 幸运数字：{lucky['number']}\n"
                if 'stone' in lucky:
                    result += f"• 幸运宝石：{lucky['stone']}\n"
                if 'constellation' in lucky:
                    result += f"• 守护星座：{lucky['constellation']}\n"
            
            return self.truncate_text(result)
            
        except Exception as e:
            return f"{self.emoji_map['error']} {zodiac} 运势格式化失败：{str(e)}"
    
    def format_tarot_result(self, tarot_data: Dict[str, Any]) -> str:
        """
        格式化塔罗牌结果
        
        Args:
            tarot_data (Dict[str, Any]): 塔罗牌数据
            
        Returns:
            str: 格式化后的塔罗牌结果
        """
        if not tarot_data:
            return f"{self.emoji_map['error']} 塔罗牌数据为空"
        
        try:
            reading_type = tarot_data.get('type', '简单抽牌')
            spread_name = tarot_data.get('spread_name', '')
            question = tarot_data.get('question', '')
            
            # 构建塔罗牌结果标题
            result = f"{self.emoji_map['tarot']} 塔罗占卜结果\n\n"
            
            if spread_name:
                result += f"🔮 牌阵：{spread_name}\n"
            
            result += f"📝 占卜类型：{reading_type}\n"
            
            if question:
                result += f"❓ 占卜问题：{question}\n"
            
            result += "\n" + "─" * 15 + "\n\n"
            
            # 处理抽到的牌
            if 'cards' in tarot_data:
                cards = tarot_data['cards']
                if isinstance(cards, list):
                    for i, card in enumerate(cards, 1):
                        if isinstance(card, dict):
                            card_name = card.get('name', '未知牌')
                            position = card.get('position', '正位')
                            meaning = card.get('meaning', '暂无解释')
                            
                            # 位置符号
                            pos_symbol = "⬆️" if position == '正位' else "⬇️"
                            
                            result += f"🃏 第{i}张牌：{card_name} {pos_symbol}\n"
                            result += f"📖 牌义：{meaning}\n\n"
                        else:
                            result += f"🃏 第{i}张牌：{card}\n\n"
                else:
                    # 单张牌的情况
                    if isinstance(cards, dict):
                        card_name = cards.get('name', '未知牌')
                        position = cards.get('position', '正位')
                        meaning = cards.get('meaning', '暂无解释')
                        
                        pos_symbol = "⬆️" if position == '正位' else "⬇️"
                        result += f"🃏 抽到的牌：{card_name} {pos_symbol}\n"
                        result += f"📖 牌义：{meaning}\n\n"
            
            # 添加AI解读
            if 'ai_interpretation' in tarot_data:
                result += "🤖 AI智能解读：\n"
                result += f"{tarot_data['ai_interpretation']}\n\n"
            
            # 添加总体解读
            if 'overall_interpretation' in tarot_data:
                result += "🔍 综合解读：\n"
                result += f"{tarot_data['overall_interpretation']}\n\n"
            
            # 添加建议
            if 'advice' in tarot_data:
                result += "💡 塔罗建议：\n"
                result += f"{tarot_data['advice']}\n\n"
            
            # 添加注意事项
            if 'notes' in tarot_data:
                result += "⚠️ 注意事项：\n"
                result += f"{tarot_data['notes']}"
            else:
                result += "⚠️ 注意：塔罗占卜仅供娱乐参考，请理性对待。"
            
            return self.truncate_text(result)
            
        except Exception as e:
            return f"{self.emoji_map['error']} 塔罗牌格式化失败：{str(e)}"
    
    def format_quote_message(self, quote: str, category: Optional[str] = None) -> str:
        """
        格式化励志语录
        
        Args:
            quote (str): 语录内容
            category (Optional[str]): 语录分类
            
        Returns:
            str: 格式化后的语录消息
        """
        if not quote:
            return f"{self.emoji_map['error']} 语录内容为空"
        
        try:
            # 清理语录内容
            clean_quote = self.clean_text(quote)
            
            # 构建语录消息
            result = f"{self.emoji_map['quote']} 今日语录\n\n"
            
            # 添加分类信息
            if category:
                category_icons = {
                    "励志": "💪", "成功": "🏆", "智慧": "🧠", "爱情": "💕",
                    "友谊": "👫", "人生": "🌟", "梦想": "✨", "坚持": "🔥",
                    "成长": "🌱", "快乐": "😊", "勇气": "🦁", "希望": "🌈"
                }
                icon = category_icons.get(category, "📝")
                result += f"{icon} 分类：{category}\n\n"
            
            # 添加语录内容（使用引号包围）
            result += f"\"💭 {clean_quote}\"\n\n"
            
            # 添加装饰性分隔线
            result += "─" * 15 + "\n"
            result += f"{self.emoji_map['heart']} 愿这句话能给你带来力量与启发"
            
            return self.truncate_text(result)
            
        except Exception as e:
            return f"{self.emoji_map['error']} 语录格式化失败：{str(e)}"
    
    def format_chat_response(self, response: str) -> str:
        """
        格式化对话响应
        
        Args:
            response (str): AI响应内容
            
        Returns:
            str: 格式化后的响应消息
        """
        if not response:
            return f"{self.emoji_map['error']} AI响应为空"
        
        try:
            # 清理响应内容
            clean_response = self.clean_text(response)
            
            # 构建对话响应
            result = f"{self.emoji_map['chat']} AI助手回复\n\n"
            result += clean_response
            
            # 如果响应过长，添加提示
            if len(clean_response) > 800:
                result += "\n\n💡 回复较长，如需更多信息请继续提问"
            
            return self.truncate_text(result)
            
        except Exception as e:
            return f"{self.emoji_map['error']} 对话响应格式化失败：{str(e)}"
    
    def format_error_message(self, error_type: str, details: str = "") -> str:
        """
        格式化错误消息
        
        Args:
            error_type (str): 错误类型
            details (str): 错误详情
            
        Returns:
            str: 格式化后的错误消息
        """
        try:
            # 错误类型映射
            error_types = {
                "command_not_found": "命令未找到",
                "invalid_parameter": "参数无效",
                "service_error": "服务错误",
                "network_error": "网络错误",
                "data_error": "数据错误",
                "permission_error": "权限错误",
                "timeout_error": "超时错误",
                "unknown_error": "未知错误"
            }
            
            error_name = error_types.get(error_type, error_type)
            
            # 构建错误消息
            result = f"{self.emoji_map['error']} 操作失败\n\n"
            result += f"🔍 错误类型：{error_name}\n"
            
            if details:
                clean_details = self.clean_text(details)
                result += f"📝 错误详情：{clean_details}\n"
            
            result += "\n💡 建议：\n"
            
            # 根据错误类型提供建议
            suggestions = {
                "command_not_found": "请检查命令格式，输入 '帮助' 查看可用命令",
                "invalid_parameter": "请检查参数格式，输入 '帮助 [功能名]' 查看使用方法",
                "service_error": "服务暂时不可用，请稍后重试",
                "network_error": "网络连接异常，请检查网络状态后重试",
                "data_error": "数据处理出错，请重新尝试",
                "permission_error": "权限不足，请联系管理员",
                "timeout_error": "请求超时，请稍后重试",
                "unknown_error": "遇到未知错误，请重新尝试或联系技术支持"
            }
            
            suggestion = suggestions.get(error_type, "请重新尝试，如问题持续请联系技术支持")
            result += f"• {suggestion}"
            
            return self.truncate_text(result)
            
        except Exception as e:
            return f"{self.emoji_map['error']} 错误消息格式化失败：{str(e)}"
    
    def clean_text(self, text: str) -> str:
        """
        清理文本内容，移除特殊字符和多余空白
        
        Args:
            text (str): 原始文本
            
        Returns:
            str: 清理后的文本
        """
        if not text:
            return ""
        
        try:
            # 移除HTML标签
            text = re.sub(r'<[^>]+>', '', text)
            
            # 移除多余的空白字符
            text = re.sub(r'\s+', ' ', text)
            
            # 移除行首行尾空白
            text = text.strip()
            
            # 移除特殊控制字符（保留常用的换行符、制表符）
            text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
            
            # 规范化换行符
            text = text.replace('\r\n', '\n').replace('\r', '\n')
            
            # 移除连续的换行符（超过2个）
            text = re.sub(r'\n{3,}', '\n\n', text)
            
            # 移除可能导致QQ消息显示问题的特殊字符
            problematic_chars = ['\u200b', '\u200c', '\u200d', '\ufeff']
            for char in problematic_chars:
                text = text.replace(char, '')
            
            return text
            
        except Exception:
            # 如果清理失败，返回原始文本
            return str(text)
    
    def truncate_text(self, text: str, max_length: int = 500) -> str:
        """
        截断过长的文本
        
        Args:
            text (str): 原始文本
            max_length (int): 最大长度
            
        Returns:
            str: 截断后的文本
        """
        if not text:
            return ""
        
        # 使用配置的最大长度，如果没有指定则使用默认值
        if max_length <= 0:
            max_length = self.max_message_length
        
        try:
            # 如果文本长度在限制内，直接返回
            if len(text) <= max_length:
                return text
            
            # 计算截断位置（留出省略号的空间）
            ellipsis = "...\n\n📝 内容过长已截断，如需完整信息请分段查询"
            available_length = max_length - len(ellipsis)
            
            if available_length <= 0:
                return ellipsis
            
            # 尝试在合适的位置截断（优先在句号、换行符等位置）
            truncate_pos = available_length
            
            # 寻找最近的合适截断点
            good_break_chars = ['。', '！', '？', '\n', '；', '，']
            for i in range(min(available_length - 50, len(text)), 
                          min(available_length + 1, len(text))):
                if i < len(text) and text[i] in good_break_chars:
                    truncate_pos = i + 1
                    break
            
            # 确保不会截断emoji或特殊字符
            while truncate_pos > 0 and truncate_pos < len(text):
                char = text[truncate_pos]
                # 检查是否是emoji的一部分或特殊Unicode字符
                if ord(char) >= 0x1F000:  # emoji范围
                    truncate_pos -= 1
                else:
                    break
            
            # 执行截断
            truncated_text = text[:truncate_pos].rstrip()
            
            return truncated_text + ellipsis
            
        except Exception:
            # 如果截断失败，使用简单截断
            simple_truncate = text[:max_length - 20] + "...（截断失败）"
            return simple_truncate
    
    def add_decorative_border(self, text: str, style: str = "simple") -> str:
        """
        为文本添加装饰性边框
        
        Args:
            text (str): 原始文本
            style (str): 边框样式 (simple, fancy, minimal)
            
        Returns:
            str: 添加边框后的文本
        """
        if not text:
            return text
        
        borders = {
            "simple": {"top": "─" * 20, "bottom": "─" * 20},
            "fancy": {"top": "✨" + "─" * 18 + "✨", "bottom": "✨" + "─" * 18 + "✨"},
            "minimal": {"top": "·" * 10, "bottom": "·" * 10}
        }
        
        border = borders.get(style, borders["simple"])
        
        return f"{border['top']}\n{text}\n{border['bottom']}"