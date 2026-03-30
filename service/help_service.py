#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目帮助服务

本文件实现帮助服务功能，包括：
1. 总体帮助指南的提供
2. 各个服务的详细帮助信息
3. 指令示例和使用说明
4. 服务状态和可用性信息

支持的指令：
- /help - 显示总体帮助指南
- /help [服务名称] - 显示特定服务的详细帮助

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
import re
from .base_service import BaseService


class HelpService(BaseService):
    """
    帮助服务类
    
    负责处理用户的帮助请求，提供项目功能介绍和使用指南。
    支持总体帮助和特定服务的详细帮助。
    """
    # 集中定义：有效服务白名单，避免多处硬编码导致不一致
    VALID_SERVICES: List[str] = [
        "今日运势", "星座运势", "塔罗牌", "抽卡", "励志语录", "倍率计算", "对话", "冲榜游戏", "车队", "推时", "今日老婆",
        "音乐", "网盘", "网盘搜索", "搜索",
        "pjsk", "pjsk排名", "pjsk抽卡记录",
        "语音", "画廊", "图库", "群成员", "成员获取", "审核", "mc", "mc消息",
        "每日车队统计", "每日推时统计", "若叶睦",
        "选择服务", "施法", "注册"
    ]
    
    def __init__(self, config: Dict[str, Any], data_manager: Any, text_formatter: Any, server=None) -> None:
        """
        初始化帮助服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 服务帮助信息映射
        self.service_help_map: Dict[str, Callable[[], Dict[str, Any]]] = {
            "今日运势": self._get_fortune_help,
            "星座运势": self._get_zodiac_help,
            "塔罗牌": self._get_tarot_help,
            "抽卡": self._get_gacha_help,
            "励志语录": self._get_quote_help,
            "倍率计算": self._get_calculator_help,
            "对话": self._get_chat_help,
            "冲榜游戏": self._get_ranking_game_help,
            "车队": self._get_team_help,
            "推时": self._get_push_time_help,
            "今日老婆": self._get_daily_wife_help,
            "音乐": self._get_music_help,
            "网盘": self._get_pan_search_help,
            "网盘搜索": self._get_pan_search_help,
            "搜索": self._get_pan_search_help,
            "pjsk": self._get_pjsk_help,
            "pjsk排名": self._get_pjskranking_help,
            "pjsk抽卡记录": self._get_pjskgachashistory_help,
            "语音": self._get_voice_help,
            "画廊": self._get_gallery_help,
            "图库": self._get_gallery_help,
            "群成员": self._get_group_member_help,
            "成员获取": self._get_group_member_help,
            "审核": self._get_audit_help,
            "mc": self._get_mc_help,
            "mc消息": self._get_mc_help,
            "每日车队统计": self._get_daily_team_statistics_help,
            "每日推时统计": self._get_daily_push_time_statistics_help,
            "若叶睦": self._get_mutsmi_help,
            "选择服务": self._get_choice_help,
            "施法": self._get_spell_help,
            "注册": self._get_user_management_help
        }
        
        # 初始化帮助服务特定配置
        self.service_name = "帮助服务"
        self.version = "1.0.0"
        
        # 支持的指令模式
        self.help_patterns: List[str] = [
            "/help",
            "帮助",
            "help",
            "使用说明",
            "功能介绍"
        ]
        
        # 服务状态映射
        self.service_status: Dict[str, bool] = {
            "今日运势": True,
            "星座运势": True,
            "塔罗牌": True,
            "抽卡": True,
            "励志语录": True,
            "倍率计算": True,
            "对话": True,
            "冲榜游戏": True,
            "车队": True,
            "推时": True,
            "今日老婆": True,
            "音乐": True,
            "网盘": True,
            "网盘搜索": True,
            "搜索": True,
            "pjsk": True,
            "pjsk排名": True,
            "pjsk抽卡记录": True,
            "语音": True,
            "画廊": True,
            "图库": True,
            "群成员": True,
            "成员获取": True,
            "审核": True,
            "mc": True,
            "mc消息": True,
            "每日车队统计": True,
            "每日推时统计": True,
            "若叶睦": True,
            "选择服务": True,
            "施法": True,
            "注册": True
        }
        
        self.log_unified("INFO", f"{self.service_name} 初始化完成", group_id="system", user_id="system")
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理帮助请求消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[str]: 帮助信息，如果无法处理则返回None
        """
        try:
            # 清理和标准化消息
            clean_message = message.strip().lower()
            
            # 检查是否为帮助请求
            if not self._is_help_request(clean_message):
                return None
            
            # 记录服务使用
            self.log_service_usage(user_id, self.service_name, "help_request")
            
            # 解析帮助请求类型
            service_name = self._extract_service_name(clean_message)
            
            if service_name:
                # 获取特定服务帮助
                self.log_unified("DEBUG", f"用户{user_id}请求{service_name}服务帮助", user_id, "help")
                help_text = self.get_service_help(service_name)
            else:
                # 获取总体帮助
                self.log_unified("DEBUG", f"用户{user_id}请求总体帮助", user_id, "help")
                help_text = self.get_general_help()
            
            # 格式化帮助信息
            if isinstance(help_text, dict):
                # 检查是否是包含content字段的结构化响应
                if 'content' in help_text and isinstance(help_text['content'], dict):
                    # 使用content字段中的数据进行格式化
                    formatted_help = self._format_help_dict(help_text['content'], service_name)
                else:
                    # 直接使用字典进行格式化
                    formatted_help = self._format_help_dict(help_text, service_name)
            else:
                # 如果是字符串，直接使用
                formatted_help = str(help_text)
            
            self.log_unified("INFO", f"为用户{user_id}提供了帮助信息: {service_name or '总体帮助'}", user_id, "help")
            return {
                "content": formatted_help,
                "image_path": None
            }
            
        except Exception as e:
            self.log_unified("ERROR", f"处理帮助请求失败: {e}", user_id, "help")
            error_message = self.text_formatter.format_error_message(
                "service_error", 
                "获取帮助信息时出现错误，请稍后重试"
            )
            return {
                "content": error_message,
                "image_path": None
            }
    
    def _get_calculator_help(self) -> Dict[str, Any]:
        """
        获取倍率计算服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 倍率计算服务的帮助信息
        """
        return {
            "title": "🧮 倍率计算功能详细指南 🧮",
            "description": "倍率计算工具",
            "commands": {
                "倍率计算": {
                    "format": "/倍率计算 [数1] [数2] [数3] [数4] [数5]",
                    "examples": [
                        "/倍率计算 50 100 150 200 250",
                        "/倍率计算 30 80 120 160 200",
                        "/倍率计算 40 90 110 140 180"
                    ],
                    "description": "倍率计算"
                }
            },
            "features": [
                "快速计算"
            ]
        }

    def get_help_text(self) -> Dict[str, Any]:
        """
        获取帮助服务的帮助文本
        
        Returns:
            str: 帮助文本
        """
        return {
            "service_name": "帮助服务",
            "description": "提供项目功能介绍和使用指南",
            "usage": [
                "/help - 显示总体帮助指南",
                "/help [服务名] - 显示特定服务帮助",
                "帮助 - 显示总体帮助指南",
                "使用说明 - 显示总体帮助指南"
            ],
            "examples": [
                "/help",
                "/help 今日运势",
                "帮助",
                "塔罗牌使用说明"
            ]
        }
    
    def _get_push_time_help(self) -> Dict[str, Any]:
        """
        获取推时统计服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 推时统计服务帮助信息
        """
        return {
            "title": "📊 推车时长统计功能详细指南 📊",
            "description": "推车时长统计服务，管理和统计用户的推车时长、被推时长（跑者时长）以及净推车时长",
            "commands": {
                "手动管理指令": {
                    "format": "/增加推时 <CN> <小时数>\n/减少推时 <CN> <小时数>\n/增加跑时 <CN> <小时数>\n/减少跑时 <CN> <小时数>",
                    "examples": [
                        "/增加推时 小明 2.5",
                        "/减少推时 小红 1.0",
                        "/增加跑时 小李 3.0",
                        "/减少跑时 小王 0.5"
                    ],
                    "description": "手动调整用户的推车时长和被推时长"
                },
                "查询指令": {
                    "format": "/推时统计 <CN>",
                    "examples": [
                        "/推时统计 小明",
                        "/推时统计 小红"
                    ],
                    "description": "查询指定用户的推车时长统计信息"
                },
                "用户管理": {
                    "format": "/上传cn <CN>",
                    "examples": [
                        "/上传cn 小明",
                        "/上传cn 新用户"
                    ],
                    "description": "将新的CN（用户名）上传到系统中"
                },
                "管理员指令": {
                    "format": "/推时查询\n/推时负数查询\n/推时结算\n/底标结算",
                    "examples": [
                        "/推时查询",
                        "/推时负数查询",
                        "/推时结算",
                        "/底标结算",
                    ],
                    "description": "管理员专用功能，包括数据查询、结算和导出"
                }
            },
            "features": [
                "📈 自动记录车队活动中的推车和跑者时长",
                "✏️ 支持手动增减时长管理",
                "📊 详细的时长统计查询功能",
                "👤 CN（用户名）管理和查重功能",
                "🔒 管理员权限控制的结算功能",
                "📋 支持导出推车时长统计表格",
                "💾 数据持久化存储，支持CSV格式"
            ],
            "tips": [
                "CN为用户名称，时长单位为小时，支持小数（如2.5小时）",
                "系统会自动记录车队活动中的推车和跑者时长，无需手动操作",
                "推时统计包括：推车时长、被推时长（跑者时长）、净推车时长",
                "净推车时长 = 推车时长 - 跑时/2（跑时即被推时长）",
                "结算功能仅管理员可用，用于定期清算和统计",
                "查询功能返回对应群聊的数据文件，方便数据管理",
                "导出的表格保存在data/tuiche目录下，支持Excel查看",
                "支持多群组独立统计，每个群组的数据互不影响",
                "跑推计入推车时长的0.2倍（包含替补跑推）"
            ]
        }
    
    def get_general_help(self) -> Dict[str, Any]:
        """
        获取总体帮助指南
        
        Returns:
            str: 总体帮助指南
        """
        return {
            "title": "🎵 Mortisfun 功能指南 🎵",
            "description": "欢迎使用Mortisfun！这里有各种有趣的功能等你探索~",
            "services": {
                "今日运势": {
                    "description": "每日运势预测，了解今天的运气如何"
                },
                "星座运势": {
                    "description": "星座运势查询和配对分析"
                },
                "塔罗牌": {
                    "description": "神秘的塔罗牌占卜，探索未知的答案"
                },
                "抽卡": {
                    "description": "Project SEKAI卡牌模拟抽取"
                },
                "倍率计算": {
                    "description": "快速计算倍率"
                },
                "对话": {
                     "description": "AI智能对话"
                 },
                "冲榜游戏": {
                    "description": "🎵 Project SEKAI 冲榜大作战 🎵"
                },
                "车队": {
                    "description": "车队报班功能，支持报班、上车、撤回、车队查询"
                },
                "推时": {
                    "description": "推车时长统计服务，管理推车时长、被推时长和净推车时长"
                },
                "今日老婆": {
                    "description": "今日老婆功能，随机选择群成员建立恋爱关系"
                },
                "语音": {
                    "description": "语音服务，列出与播放本地语音文件"
                },
                "画廊": {
                    "description": "画廊服务，上传与查看图片集合（含别名与查重）"
                },
                "群成员": {
                    "description": "群成员获取服务，将群成员导出为Excel并生成统计"
                },
                "审核": {
                    "description": "审核题库服务，随机抽取或列出全部题目"
                },
                "mc": {
                    "description": "MC消息发送服务，/send 转发到MC服务器"
                },
                "施法": {
                    "description": "随机从 data/shifa 发送一张图片"
                },
                "选择服务": {
                    "description": "无前缀选择功能，随机帮你做决定"
                },
                "注册": {
                    "description": "网页端用户注册、密码修改和群聊管理功能"
                },
                "每日车队统计": {
                    "description": "每日统计昨日结束车队并可发送甘特图"
                },
                "每日推时统计": {
                    "description": "每日统计昨日推时操作记录并保存日志"
                },
                "若叶睦": {
                    "description": "若叶睦互动与小游戏（摸头/买礼物/种黄瓜/打工/演唱会）"
                },
                "pjsk排名": {
                    "description": "PJSK排名查询（wcnsk... / wcn 活动/预测等）"
                },
                "pjsk抽卡记录": {
                    "description": "PJSK抽卡历史记录查询与图片渲染（wcn抽卡记录...）"
                }
            },
            "tips": [
                "🎵 锵锵，这里是Mortisfun的小助手！",
                "想要详细了解某个功能？试试 /help [功能名] 吧~",
                "比如：/help 推时 就能看到推时功能的详细用法哦！",
                "所有功能都在这里啦：今日运势、星座运势、塔罗牌、抽卡、倍率计算、对话、冲榜游戏、车队、推时、今日老婆、音乐、网盘搜索、语音、画廊、群成员、审核、mc、每日车队统计、每日推时统计、若叶睦、pjsk、pjsk排名、pjsk抽卡记录、选择服务、施法、注册",
                "有什么问题随时问我，我会尽力帮助你的！ desuwa~"
            ]
        }
    
    def get_service_help(self, service_name: str) -> Dict[str, Any]:
        """
        获取特定服务的详细帮助
        
        说明：
        - 由于 /help [服务名] 在入口已使用 VALID_SERVICES 严格校验，这里直接使用传入的 service_name。
        - 移除了名称归一化与映射逻辑，减少冗余与不一致风险。
        """
        try:
            # 直接使用传入的服务名称；前置校验已保证合法
            normalized_name = service_name
            
            if normalized_name in self.service_help_map:
                help_function = self.service_help_map[normalized_name]
                return help_function()
            else:
                # 返回可用服务列表
                available_services = self.list_available_services()
                return {
                    "error": "服务未找到",
                    "message": f"未找到服务 '{service_name}'",
                    "available_services": available_services,
                    "suggestion": "请检查服务名称或查看可用服务列表"
                }
                
        except Exception as e:
            self.log_unified("ERROR", f"获取服务帮助失败: {e}", group_id="system", user_id="system")
            return {
                "error": "获取帮助失败",
                "message": "获取服务帮助信息时出现错误"
            }
    
    def _get_fortune_help(self) -> Dict[str, Any]:
        """
        获取今日运势服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 今日运势服务帮助信息
        """
        return {
            "title": "🔮 今日运势功能详细指南 🔮",
            "description": "每日运势预测系统，都是随机数啦~不用太在意",
            "commands": {
                "今日运势": {
                    "format": "/今日运势",
                    "examples": [
                        "/今日运势",
                    ],
                    "description": "获取今天的综合运势预测，包含多个维度的详细分析"
                }
            },
            "features": [
                "🌟 综合运势评分，直观了解今日整体运势",
                "💕 爱情运势分析，单身和恋爱状态都有针对性建议",
                "💼 事业运势指导，工作学习方面的运势提醒",
                "💰 财运预测，理财和消费方面的建议",
                "🏥 健康运势关注，身心健康状态提醒",
                "🎨 每日幸运元素，包括幸运颜色、数字、方位等"
            ],
            "tips": [
                "每天的运势都会有所不同，建议每日查看",
                "运势仅供参考，重要决定还需理性思考",
                "保持积极心态，好运势会助力，坏运势也能化解",
                "可以根据幸运元素调整当日的穿搭和行程",
                "运势预测基于传统命理学，娱乐性质为主"
            ]
        }
    
    def _get_zodiac_help(self) -> Dict[str, Any]:
        """
        获取星座运势服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 星座运势服务帮助信息
        """
        return {
            "title": "⭐ 星座运势功能详细指南 ⭐",
            "description": "十二星座运势预测系统，提供单个星座运势查询和星座配对分析功能",
            "commands": {
                "星座运势查询": {
                    "format": "/星座运势 [星座名称]",
                    "examples": [
                        "/星座运势 白羊座",
                        "/星座运势 双子座",
                        "/星座运势 天蝎座"
                    ],
                    "description": "查询指定星座的详细运势信息，包含爱情、事业、财运等方面"
                },
                "星座运势对比": {
                    "format": "/星座运势对比 [星座名称]\n/星座运势对比 [星座1] + [星座2]",
                    "examples": [
                        "/星座运势对比 双子座",
                        "/星座运势对比 白羊座 + 狮子座",
                        "/星座运势对比 处女座 + 摩羯座"
                    ],
                    "description": "分析星座运势走向或进行两个星座的配对分析"
                }
            },
            "features": [
                "🌟 支持全部12个星座的运势查询",
                "💫 详细的运势分析，包含多个生活维度",
                "💕 星座配对功能，分析两个星座的相性",
                "📈 运势走向分析，了解星座运势变化趋势",
                "🎯 个性化建议，根据星座特点提供针对性指导",
                "🔮 基于传统占星学理论的专业分析"
            ],
            "tips": [
                "支持的星座：白羊座、金牛座、双子座、巨蟹座、狮子座、处女座、天秤座、天蝎座、射手座、摩羯座、水瓶座、双鱼座",
                "星座配对分析可以用于了解人际关系和恋爱匹配度",
                "运势对比功能可以单独查看一个星座的运势走向",
                "星座运势会根据天体运行周期性更新",
                "建议结合个人实际情况理性参考运势建议"
            ]
        }
    
    def _get_tarot_help(self) -> Dict[str, Any]:
        """
        获取塔罗牌服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 塔罗牌服务帮助信息
        """
        return {
            "title": "🔮 塔罗牌占卜功能详细指南 🔮",
            "description": "专业塔罗牌占卜系统，提供多种牌阵和占卜方式，帮助您探索内心和未来",
            "commands": {
                "简单抽牌": {
                    "format": "/抽塔罗牌\n/抽塔罗牌 [数量]张",
                    "examples": [
                        "/抽塔罗牌",
                        "/抽塔罗牌 3张",
                        "/抽塔罗牌 5张"
                    ],
                    "description": "随机抽取塔罗牌进行简单占卜，可指定抽牌数量"
                },
                "专业占卜": {
                    "format": "/抽塔罗牌专业 [问题]",
                    "examples": [
                        "/抽塔罗牌专业 我的运势如何",
                        "/抽塔罗牌专业 感情方面的建议",
                        "/抽塔罗牌专业 事业发展方向"
                    ],
                    "description": "针对具体问题进行专业塔罗牌占卜分析"
                },
                "牌阵占卜": {
                    "format": "/抽塔罗牌专业牌阵选择 [牌阵名称]",
                    "examples": [
                        "/抽塔罗牌专业牌阵选择 三牌阵",
                        "/抽塔罗牌专业牌阵选择 十字牌阵",
                        "/抽塔罗牌专业牌阵选择 简单抽牌"
                    ],
                    "description": "使用特定牌阵进行深度占卜分析"
                }
            },
            "features": [
                "🃏 完整的78张塔罗牌库，包含大阿卡纳和小阿卡纳",
                "🔮 多种牌阵选择：简单抽牌、三牌阵、十字牌阵等",
                "💫 专业占卜解读，每张牌都有详细的含义解释",
                "🎯 针对性问题占卜，可以就具体问题进行咨询",
                "📚 丰富的牌意解释，正位和逆位都有不同含义",
                "🌟 个性化占卜体验，每次抽牌都是独特的"
            ],
            "tips": [
                "占卜前建议先明确自己想要咨询的问题",
                "三牌阵代表过去、现在、未来，适合了解事情发展脉络",
                "十字牌阵提供更全面的分析，包含多个维度的信息",
                "塔罗牌占卜结果仅供参考，重要决定还需理性思考",
                "保持开放的心态，塔罗牌更多是提供思考的角度"
            ]
        }
    

    
    def _get_chat_help(self) -> Dict[str, Any]:
        """
        获取对话服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 对话服务帮助信息
        """
        return {
            "title": "💬 智能对话功能详细指南 💬",
            "description": "AI智能对话系统，支持自然语言交流、上下文记忆和多话题讨论",
            "commands": {
                "对话指令": {
                    "format": "/对话 [消息内容]\n/聊天 [消息内容]",
                    "examples": [
                        "/对话 你好",
                        "/对话 能帮我解答一个问题吗"
                    ],
                    "description": "使用指令形式开始对话，可以询问任何问题"
                },
                "切换角色": {
                    "format": "/切换角色 [角色名]",
                    "examples": [
                        "/切换角色",
                        "/切换角色 默认"
                    ],
                    "description": "切换不同对话人设，不同角色会分别保存对话历史"
                },
                "直接对话": {
                    "format": "使用 /开始对话 后直接发送消息内容",
                    "examples": [
                        "/开始对话"
                        "你能帮我做什么？",
                        "请介绍一下这个机器人的功能",
                        "我想了解一些有趣的知识",
                        "/关闭对话"
                    ],
                    "description": "直接发送消息即可对话"
                }
            },
            "features": [
                "🧠 先进的自然语言理解能力，支持复杂对话",
                "💭 上下文记忆功能，能够理解对话的连贯性",
                "🌍 多话题交流支持，从日常闲聊到专业问题",
                "🎯 智能回复生成，提供有用和相关的回答",
                "📚 知识问答能力，可以回答各种领域的问题",
                "😊 友好的对话体验，支持情感化交流"
            ],
            "tips": [
                "想聊什么就聊什么，我会尽力提供有帮助的回答",
                "支持连续对话，我会记住之前的对话内容",
                "如果回答不满意，可以换个方式重新提问",
                "可以询问各种类型的问题：知识问答、生活建议、技术问题等",
                "对话过程中可以随时切换话题，我会适应新的对话方向"
            ]
        }

    def _get_gacha_help(self) -> Dict[str, Any]:
        """
        获取抽卡服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 抽卡服务帮助信息
        """
        return {
            "title": "🎲 抽卡模拟功能详细指南 🎲",
            "description": "Project SEKAI抽卡模拟系统，支持日服和国服卡池，真实还原游戏抽卡体验",
            "commands": {
                "默认抽卡日服卡池": {
                    "format": "/模拟抽卡",
                    "examples": [
                        "/模拟抽卡"
                    ],
                    "description": "使用默认设置（日服）进行抽卡"
                },
                "指定地区抽卡": {
                    "format": "/地区简称模拟抽卡",
                    "examples": [
                        "/jp模拟抽卡",
                        "/cn模拟抽卡"
                    ],
                    "description": "指定服务器地区进行抽卡，支持英文简写"
                },
            },
            "features": [
                "🌸 支持日服和国服双卡池",
                "⭐ 实时UP角色信息，与游戏内活动同步",
                "🎯 真实概率模拟，完全还原游戏抽卡机制",
                "📊 详细抽卡结果展示，包含卡片信息和稀有度",
            ],
            "tips": [
                "UP角色在活动期间概率更高，建议关注活动信息",
                "日服和国服卡池内容不同，可以体验不同的角色",
                "支持地区简写：jp（日服）、cn（国服）",
                "抽卡结果完全随机，仅供娱乐，不代表真实游戏结果"
            ]
        }
    
    def list_available_services(self) -> List[str]:
        """
        列出所有可用的服务
        
        Returns:
            List[str]: 可用服务列表
        """
        available_services = []
        
        for service_name, is_enabled in self.service_status.items():
            if is_enabled:
                available_services.append(service_name)
        
        return available_services
    
    def _is_help_request(self, message: str) -> bool:
        """
        判断消息是否为帮助请求
        严格按照文档定义的指令格式进行识别
        
        Args:
            message (str): 用户消息
            
        Returns:
            bool: 是否为帮助请求
        """
        # 严格按照文档定义的指令格式
        message = message.strip()
        
        # 只识别精确的 "/help" 指令
        if message == "/help":
            return True
        
        # 或者 "/help [有效服务名称]" 格式
        if message.startswith("/help "):
            service_part = message[6:].strip()
            # 使用集中白名单进行校验，移除局部重复的 valid_services 定义
            return service_part in self.VALID_SERVICES
        
        return False
    
    def _extract_service_name(self, message: str) -> Optional[str]:
        """
        从消息中提取服务名称
        严格按照文档定义的指令格式进行识别
        
        Args:
            message (str): 用户消息
            
        Returns:
            Optional[str]: 提取的服务名称，如果没有则返回None
        """
        # 严格按照文档定义的 "/help [服务名称]" 格式
        message = message.strip()
        
        # 检查是否为 "/help [服务名称]" 格式
        if message.startswith("/help "):
            # 提取服务名称部分
            service_part = message[6:].strip()
            
            # 只返回完全匹配白名单的服务名称
            if service_part in self.VALID_SERVICES:
                return service_part
            else:
                return None
        
        return None
    
    def _format_help_info(self, help_data: Dict) -> str:
        """
        格式化帮助信息为用户友好的文本
        
        Args:
            help_data (Dict): 帮助信息数据
            
        Returns:
            str: 格式化后的帮助文本
        """
        if not help_data:
            return "❌ 帮助信息不可用"
        
        formatted_text = []
        
        # 服务标题
        if "service_name" in help_data:
            formatted_text.append(f"🔧 **{help_data['service_name']}**")
            formatted_text.append("="*50)
        
        # 服务描述
        if "description" in help_data:
            formatted_text.append(f"📝 **服务描述：**")
            formatted_text.append(f"   {help_data['description']}")
            formatted_text.append("")
        
        # 功能特性
        if "features" in help_data and help_data["features"]:
            formatted_text.append("✨ **主要功能：**")
            for feature in help_data["features"]:
                formatted_text.append(f"   • {feature}")
            formatted_text.append("")
        
        # 支持的星座（仅星座服务）
        if "supported_zodiac" in help_data:
            formatted_text.append("⭐ **支持星座：**")
            zodiac_list = ", ".join(help_data["supported_zodiac"])
            formatted_text.append(f"   {zodiac_list}")
            formatted_text.append("")
        
        # 占卜类型（仅塔罗服务）
        if "divination_types" in help_data:
            formatted_text.append("🔮 **占卜类型：**")
            for div_type in help_data["divination_types"]:
                formatted_text.append(f"   • {div_type}")
            formatted_text.append("")
        
        # 语录分类（仅励志语录服务）
        if "categories" in help_data:
            formatted_text.append("📚 **语录分类：**")
            categories = ", ".join(help_data["categories"])
            formatted_text.append(f"   {categories}")
            formatted_text.append("")
        
        # 对话能力（仅对话服务）
        if "capabilities" in help_data:
            formatted_text.append("🤖 **对话能力：**")
            for capability in help_data["capabilities"]:
                formatted_text.append(f"   • {capability}")
            formatted_text.append("")
        
        # 使用方法
        if "usage" in help_data and help_data["usage"]:
            formatted_text.append("📖 **使用方法：**")
            for usage in help_data["usage"]:
                formatted_text.append(f"   • {usage}")
            formatted_text.append("")
        
        # 使用示例
        if "examples" in help_data and help_data["examples"]:
            formatted_text.append("💡 **使用示例：**")
            for example in help_data["examples"]:
                formatted_text.append(f"   • \"{example}\"")
            formatted_text.append("")
        
        # 指令详情
        if "commands" in help_data and help_data["commands"]:
            formatted_text.append("📋 **指令详情：**")
            # 处理字典格式的commands（车队服务等）
            if isinstance(help_data["commands"], dict):
                for cmd_name, cmd_info in help_data["commands"].items():
                    formatted_text.append(f"   🔸 **{cmd_name}：**")
                    if "format" in cmd_info:
                        formatted_text.append(f"      格式：{cmd_info['format']}")
                    if "examples" in cmd_info and cmd_info["examples"]:
                        formatted_text.append(f"      示例：")
                        for example in cmd_info["examples"]:
                            formatted_text.append(f"        • {example}")
                    if "description" in cmd_info:
                        formatted_text.append(f"      说明：{cmd_info['description']}")
                    formatted_text.append("")
            # 处理列表格式的commands（音乐服务等）
            elif isinstance(help_data["commands"], list):
                for cmd_info in help_data["commands"]:
                    if "command" in cmd_info:
                        formatted_text.append(f"   🔸 **{cmd_info['command']}：**")
                        if "description" in cmd_info:
                            formatted_text.append(f"      说明：{cmd_info['description']}")
                        if "example" in cmd_info:
                            formatted_text.append(f"      示例：{cmd_info['example']}")
                        if "parameters" in cmd_info and cmd_info["parameters"]:
                            formatted_text.append(f"      参数：")
                            for param in cmd_info["parameters"]:
                                param_desc = f"        • {param['name']} ({param['type']})"
                                if param.get('required', False):
                                    param_desc += " [必需]"
                                if 'description' in param:
                                    param_desc += f" - {param['description']}"
                                formatted_text.append(param_desc)
                        formatted_text.append("")
        
        # 使用提示
        if "tips" in help_data and help_data["tips"]:
            formatted_text.append("💫 **使用提示：**")
            for tip in help_data["tips"]:
                formatted_text.append(f"   • {tip}")
            formatted_text.append("")
        
        # 使用技巧（音乐服务等）
        if "usage_tips" in help_data and help_data["usage_tips"]:
            formatted_text.append("💡 **使用技巧：**")
            for tip in help_data["usage_tips"]:
                formatted_text.append(f"   {tip}")
            formatted_text.append("")
        
        # 限制说明（音乐服务等）
        if "limitations" in help_data and help_data["limitations"]:
            formatted_text.append("⚠️ **注意事项：**")
            for limitation in help_data["limitations"]:
                formatted_text.append(f"   {limitation}")
            formatted_text.append("")
        
        return "\n".join(formatted_text)
    
    def _is_service_available(self, service_name: str) -> bool:
        """
        检查指定服务是否可用
        
        Args:
            service_name (str): 服务名称
            
        Returns:
            bool: 服务是否可用
        """
        return self.service_status.get(service_name, False)
    
    def _record_service_usage(self, user_id: str, service_name: str) -> None:
        """
        记录服务使用情况
        
        Args:
            user_id (str): 用户ID
            service_name (str): 服务名称
        """
        try:
            from datetime import datetime
            # 记录帮助服务使用
            usage_data = {
                "timestamp": datetime.now().isoformat(),
                "user_id": user_id,
                "service_requested": service_name,
                "help_type": "specific" if service_name != "general" else "general"
            }
            
            # 使用数据管理器保存使用记录
            if hasattr(self, 'data_manager') and self.data_manager:
                # 保存到用户的帮助使用历史
                self.data_manager.save_user_data(
                    user_id, 
                    "help_usage", 
                    usage_data
                )
        except Exception as e:
            # 记录失败不应影响主要功能
            self.log_unified("ERROR", f"记录服务使用失败: {e}", group_id="system", user_id="system")
    
    def _clean_message(self, message: str) -> str:
        """
        清理和标准化用户消息
        
        Args:
            message (str): 原始消息
            
        Returns:
            str: 清理后的消息
        """
        if not message:
            return ""
        
        # 去除首尾空白
        cleaned = message.strip()
        
        # 转换为小写以便匹配
        cleaned = cleaned.lower()
        
        # 移除常见的标点符号
        import re
        cleaned = re.sub(r'[？?！!。.，,；;：:]', '', cleaned)
        
        # 移除多余的空格
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned
    
    def _determine_help_type(self, message: str) -> tuple:
        """
        确定帮助请求的类型和目标服务
        
        Args:
            message (str): 用户消息
            
        Returns:
            tuple: (帮助类型, 目标服务)
        """
        cleaned_message = self._clean_message(message)
        
        # 检查是否是特定服务的帮助请求
        for service_name in self.service_help_map.keys():
            if service_name in cleaned_message:
                return "specific", service_name
        
        # 检查是否是总体帮助请求
        general_patterns = [
            "帮助", "help", "功能", "怎么用", "使用方法", 
            "指南", "说明", "介绍", "菜单", "命令"
        ]
        
        for pattern in general_patterns:
            if pattern in cleaned_message:
                return "general", "general"
        
        # 默认返回总体帮助
        return "general", "general"
    
    def _format_help_dict(self, help_dict: dict, service_name: str = None) -> str:
        """
        将帮助信息字典格式化为字符串
        
        Args:
            help_dict (dict): 帮助信息字典
            service_name (str): 服务名称
            
        Returns:
            str: 格式化后的帮助信息字符串
        """
        if not help_dict:
            return "❌ 帮助信息为空"
        
        try:
            if service_name:
                # 特定服务的帮助信息
                result = f"📋 {help_dict.get('service_name', service_name)} 详细帮助\n\n"
                
                # 服务描述
                if 'description' in help_dict:
                    result += f"📝 功能描述：\n{help_dict['description']}\n\n"
                
                # 功能特性
                if 'features' in help_dict:
                    result += "✨ 功能特性：\n"
                    for feature in help_dict['features']:
                        result += f"  {feature}\n"
                    result += "\n"
                
                # 使用方法
                if 'usage' in help_dict:
                    result += "🎯 使用方法：\n"
                    for usage in help_dict['usage']:
                        result += f"  • {usage}\n"
                    result += "\n"
                
                # 使用示例
                if 'examples' in help_dict:
                    result += "💡 使用示例：\n"
                    for example in help_dict['examples']:
                        result += f"  • {example}\n"
                    result += "\n"
                
                # 指令详情
                if 'commands' in help_dict:
                    result += "📝 指令详情：\n\n"
                    commands = help_dict['commands']
                    
                    # 处理字典格式的commands（车队服务等）
                    if isinstance(commands, dict):
                        for cmd_name, cmd_info in commands.items():
                            result += f"🔸 {cmd_name}\n"
                            result += f"   格式：{cmd_info.get('format', '无格式')}\n"
                            
                            examples = cmd_info.get('examples', [])
                            if examples:
                                result += "   示例：\n"
                                for example in examples:
                                    result += f"     • {example}\n"
                            
                            description = cmd_info.get('description', '')
                            if description:
                                result += f"   说明：{description}\n"
                            result += "\n"
                    
                    # 处理列表格式的commands（音乐服务等）
                    elif isinstance(commands, list):
                        for cmd_info in commands:
                            if isinstance(cmd_info, dict):
                                cmd_name = cmd_info.get('command', cmd_info.get('name', '未知指令'))
                                result += f"🔸 {cmd_name}\n"
                                
                                description = cmd_info.get('description', '')
                                if description:
                                    result += f"   说明：{description}\n"
                                
                                example = cmd_info.get('example', '')
                                if example:
                                    result += f"   示例：{example}\n"
                                
                                params = cmd_info.get('parameters', cmd_info.get('params', []))
                                if params:
                                    result += f"   参数：\n"
                                    for param in params:
                                        if isinstance(param, dict):
                                            param_desc = f"     • {param.get('name', '未知参数')} ({param.get('type', '未知类型')})"
                                            if param.get('required', False):
                                                param_desc += " [必需]"
                                            if 'description' in param:
                                                param_desc += f" - {param['description']}"
                                            result += param_desc + "\n"
                                
                                result += "\n"
                
                # 使用提示
                if 'tips' in help_dict:
                    result += "💭 使用提示：\n"
                    for tip in help_dict['tips']:
                        result += f"  {tip}\n"
                    result += "\n"
                
                # 使用技巧（音乐服务等）
                if 'usage_tips' in help_dict:
                    result += "💡 使用技巧：\n"
                    for tip in help_dict['usage_tips']:
                        result += f"  {tip}\n"
                    result += "\n"
                
                # 限制说明（音乐服务等）
                if 'limitations' in help_dict:
                    result += "⚠️ 使用限制：\n"
                    for limitation in help_dict['limitations']:
                        result += f"  {limitation}\n"
                    result += "\n"
                
                # 支持的星座（仅星座运势）
                if 'supported_zodiac' in help_dict:
                    result += "⭐ 支持的星座：\n"
                    zodiac_list = help_dict['supported_zodiac']
                    # 每行显示4个星座
                    for i in range(0, len(zodiac_list), 4):
                        line_zodiacs = zodiac_list[i:i+4]
                        result += f"  {' | '.join(line_zodiacs)}\n"
                    result += "\n"
                
                # 占卜类型（仅塔罗牌）
                if 'divination_types' in help_dict:
                    result += "🔮 占卜类型：\n"
                    for div_type in help_dict['divination_types']:
                        result += f"  • {div_type}\n"
                    result += "\n"
                
                # 语录分类（仅励志语录）
                if 'categories' in help_dict:
                    result += "📚 语录分类：\n"
                    categories = help_dict['categories']
                    # 每行显示3个分类
                    for i in range(0, len(categories), 3):
                        line_categories = categories[i:i+3]
                        result += f"  {' | '.join(line_categories)}\n"
                    result += "\n"
                
                # 对话能力（仅智能对话）
                if 'capabilities' in help_dict:
                    result += "🤖 对话能力：\n"
                    for capability in help_dict['capabilities']:
                        result += f"  • {capability}\n"
                    result += "\n"
                
                # 相关链接（PJSK等服务）
                if 'links' in help_dict:
                    result += "🔗 相关链接：\n"
                    links = help_dict['links']
                    if isinstance(links, dict):
                        for link_key, link_url in links.items():
                            result += f"  • {link_key}: {link_url}\n"
                    result += "\n"
                
                # 错误信息处理
                if 'error' in help_dict:
                    result = f"❌ {help_dict['error']}\n\n"
                    if 'message' in help_dict:
                        result += f"📝 详细信息：{help_dict['message']}\n\n"
                    if 'available_services' in help_dict:
                        result += "📋 可用服务：\n"
                        for service in help_dict['available_services']:
                            result += f"  • {service}\n"
                        result += "\n"
                    if 'suggestion' in help_dict:
                        result += f"💡 建议：{help_dict['suggestion']}\n"
                
            else:
                # 总体帮助信息
                result = f"🎯 {help_dict.get('title', 'Mortisfun 帮助指南')}\n\n"
                
                # 描述
                if 'description' in help_dict:
                    result += f"{help_dict['description']}\n\n"
                
                # 可用服务
                if 'services' in help_dict:
                    result += "📋 可用服务：\n"
                    services = help_dict['services']
                    for service_name, service_info in services.items():
                        result += f"\n🔹 {service_name}\n"
                        result += f"   {service_info.get('description', '暂无描述')}\n"
                        if 'commands' in service_info:
                            commands = service_info['commands']
                            result += f"   指令：{' | '.join(commands)}\n"
                    result += "\n"
                
                # 使用提示
                if 'tips' in help_dict:
                    result += "💭 使用提示：\n"
                    for tip in help_dict['tips']:
                        result += f"  {tip}\n"
                    result += "\n"
            
            return result.strip()
        
        except Exception as e:
            return f"❌ 格式化帮助信息时出现错误：{str(e)}"
    
    def _get_daily_wife_help(self) -> Dict[str, Any]:
        """
        获取今日老婆服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 今日老婆服务帮助信息
        """
        return {
            "title": "💕 今日老婆功能详细指南 💕",
            "description": "今日老婆功能 - 纯爱版本，支持群成员间建立恋爱关系",
            "commands": {
                "今日老婆": {
                    "format": "/今日老婆",
                    "examples": [
                        "/今日老婆"
                    ],
                    "description": "获取今日老婆（建立恋爱关系）"
                },
                "指定老婆": {
                    "format": "/今日老婆@用户",
                    "examples": [
                        "/今日老婆@小明",
                        "/今日老婆@123456789"
                    ],
                    "description": "指定目标用户（40%概率成功）"
                },
                "分手": {
                    "format": "/分手",
                    "examples": [
                        "/分手"
                    ],
                    "description": "解除当前恋爱关系"
                }
            },
            "features": [
                "💖 双向绑定：A抽到B时，B使用功能结果也是A",
                "💕 专一关系：一旦确定关系就不会改变",
                "🚫 防止自恋：不会抽到自己",
                "🔒 排他性：已有恋爱关系的用户不会被选中",
                "🌐 真实数据：通过napcat API获取真实群成员信息",
                "🏠 独立管理：每个群聊独立管理恋爱关系",
                "🎯 指定功能：支持指定目标用户，增加成功概率",
                "🖼️ 头像显示：自动获取用户昵称和头像"
            ],
            "tips": [
                "恋爱关系是双向的，彼此专一",
                "分手后可以重新寻找真爱",
                "指定功能不保证100%成功，增加趣味性",
                "头像获取可能需要一定时间",
                "每个群聊独立记录成员信息",
                "需要napcat正常运行并连接",
                "机器人需要在目标群聊中"
            ]
        }
    
    def _get_pan_search_help(self) -> Dict[str, Any]:
        """
        获取网盘搜索服务的帮助信息
        
        Returns:
            Dict[str, Any]: 网盘搜索帮助信息
        """
        return {
            "service_name": "网盘搜索",
            "description": "搜索各大网盘平台的资源，支持多种网盘类型和TG频道搜索",
            "commands": [
                {
                    "command": "/网盘搜索 [搜索内容]",
                    "description": "搜索网盘资源，使用默认频道",
                    "example": "/网盘搜索 速度与激情"
                },
                {
                    "command": "/网盘搜索 [搜索内容] [频道列表]",
                    "description": "在指定频道搜索网盘资源",
                    "example": "/网盘搜索 速度与激情 tgsearchers3,xxx"
                },
                {
                    "command": "/解密 [加密内容]",
                    "description": "解密搜索结果中的兽音加密链接或密码，返回可直接使用的原始内容",
                    "example": "/解密 ㊙獣音加密内容示例"
                }
            ],
            "features": [
                "🔍 支持多平台网盘搜索（百度网盘、阿里云盘、夸克网盘等）",
                "📱 支持55个TG频道和42个搜索插件",
                "🔐 搜索结果兽音加密保护链接和密码",
                "📋 合并转发消息展示，每个结果单独占一个消息",
                "⚡ 异步搜索，响应迅速",
                "🎯 智能排序和结果分类"
            ],
            "supported_platforms": [
                "百度网盘 (baidu)",
                "阿里云盘 (aliyun)", 
                "夸克网盘 (quark)",
                "天翼云盘 (tianyi)",
                "UC网盘 (uc)",
                "移动云盘 (mobile)",
                "115网盘 (115)",
                "PikPak (pikpak)",
                "迅雷网盘 (xunlei)",
                "123网盘 (123)",
                "磁力链接 (magnet)",
                "电驴链接 (ed2k)"
            ],
            "usage_notes": [
                "💡 搜索结果中的链接和密码已使用兽音加密",
                "🔓 请使用 /解密 指令解密后使用",
                "⏰ 每日搜索限制：20次",
                "📝 频道列表用英文逗号分隔，不提供则使用默认配置",
                "🚀 支持实时搜索，结果按网盘类型分类展示"
            ],
            "daily_limit": 20,
            "enabled": True
        }

    def _get_ranking_game_help(self) -> Dict[str, Any]:
        """
        获取冲榜游戏的详细帮助信息
        
        Returns:
            Dict[str, Any]: 冲榜游戏帮助信息
        """
        return {
            "title": "🏆 冲榜游戏功能详细指南 🏆",
            "description": "Project SEKAI冲榜大作战多人运气游戏",
            "commands": {
                "游戏报名": {
                    "format": "/报名 [玩家名称]",
                    "examples": [
                        "/报名 初音未来",
                        "/报名 巡音流歌",
                        "/报名 镜音铃",
                    ],
                    "description": "报名参加冲榜游戏，可指定角色名称"
                }
            },
            "features": [
                "👥 支持2-8人多人竞技，自动填充虚拟歌手NPC",
                "🎲 丰富的随机事件系统，影响游戏进程和排名",
                "🎭 个性化角色扮演，每个虚拟歌手都有独特表现",
                "🏅 详细的排名系统和胜利条件判定",
                "💬 NPC获胜角色会发表个性化的胜利感言"
            ],
            "tips": [
                "报名时间限制：开始后60秒内完成报名",
                "密切关注随机事件，它们会大幅影响最终排名",
                "游戏结束后可以查看详细的排名"
            ]
        }
    
    def _get_team_help(self) -> Dict[str, Any]:
        """
        获取车队服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 车队服务帮助信息
        """
        return {
            "title": "🚗 车队报班功能详细指南 🚗",
            "description": "车队报班系统",
            "commands": {
                "报班": {
                    "format": "/报班 日期 时间 歌曲名称 车队队长名称 车主综合 [周回] [倍率] [描述]",
                    "examples": [
                        "/报班 8.3 13:00-15:00 龙 Mortis 31.6w",
                        "/报班 8.3 13:00-15:00 龙 Mortis 29w 3 2.0 新手友好车队",
                        "/报班 8.3-8.7 13:00-15:00 龙 Mortis 29657 多天报班",
                        "/报班 9.12 14:00-16:00 龙 七七 80 5 (仅周回)",
                        "/报班 9.12 14:00-16:00 龙 七七 80 2.5 (仅倍率)"
                    ],
                    "description": "创建新的车队，支持单日或多日报班；车主综合为必填，支持纯数字或w后缀（表示万）；可选填写周回、倍率以及车队描述"
                },
                "上车": {
                    "format": "/推车 车队队长名称/班号 日期 时间 车队队员名称\n/跑推 车队队长名称/班号 日期 时间 车队队员名称\n/共跑 车队队长名称 日期 时间 车队队员名称",
                    "examples": [
                        "/推车 Mortis 8.3 13:00-15:00 小明",
                        "/跑推 Mortis 8.3 13:00-15:00 小明",
                        "/推车 1 8.3 13:00-15:00 小明 (按班号推车)",
                        "/跑推 1 8.3 13:00-15:00 小明 (按班号跑推)",
                        "/跑推 Mortis 8.3-8.7 13:00-15:00 小明 (多天车队)",
                        "/推车 Mortis 8.3-8.7 13:00-15:00 小明 (多天车队)",
                        "/共跑 Mortis 8.3 13:00-15:00 小红"
                    ],
                    "description": "加入现有车队，支持按队长名称或班号操作"
                },
                "替补上车": {
                    "format": "/替补推车 车队队长名称/班号 日期 时间 替补队员名称\n/替补共跑 车队队长名称/班号 日期 时间 替补队员名称",
                    "examples": [
                        "/替补推车 Mortis 8.3 13:00-15:00 小王",
                        "/替补推车 1 8.3 13:00-15:00 小王 (按班号替补推车)",
                        "/替补共跑 Mortis 8.3 13:00-15:00 小李",
                        "/替补共跑 1 8.3 13:00-15:00 小李 (按班号替补共跑)"
                    ],
                    "description": "作为替补成员加入车队，当正式成员不足时可以参与推车或共跑"
                },
                "撤回班车": {
                    "format": "/撤回班车 车队队长名称/班号 日期 时间",
                    "examples": [
                        "/撤回班车 Mortis 8.3 13:00-15:00",
                        "/撤回班车 1 8.3 13:00-15:00"
                    ],
                    "description": "撤回整个车队，只有报班队长可以操作"
                },
                "队员撤回": {
                    "format": "/队员撤回 车队队长名称/班号 日期 时间 队员名称",
                    "examples": [
                        "/队员撤回 Mortis 8.3 13:00-15:00 小明",
                        "/队员撤回 1 8.3 13:00-15:00 小明",
                        "/队员撤回 Mortis 8.3 13:00-15:00 替补推车 (撤回替补成员)",
                        "/队员撤回 1 8.3 13:00-15:00 替补共跑 (撤回替补成员)"
                    ],
                    "description": "撤回指定队员（包括替补成员），只有队员本人或队长可以操作"
                },
                "车队查询": {
                    "format": "/车队查询 [车队号/队长名称/日期/时间段/歌曲名]\n/可报车队查询 [搜索条件]",
                    "examples": [
                        "/车队查询 (查看所有车队)",
                        "/车队查询 1 (查看1号车队)",
                        "/车队查询 Mortis (查看Mortis的所有车队)",
                        "/车队查询 9.12 (查看9月12日的车队)",
                        "/车队查询 龙 (查看歌曲名包含'龙'的车队)",
                        "/车队查询 14-15 (查看14-15点时间段的车队)",
                        "/车队查询 9.12 14-15 (查看9月12日14-15点的车队)",
                        "/车队查询 9.12 14-15 七七 (查看9月12日14-15点队长为七七的车队)",
                        "/可报车队查询 (查看可报名车队)",
                        "/可报车队查询 9.12 龙 (查看9月12日歌曲名包含'龙'的可报名车队)"
                    ],
                    "description": "查询车队信息，支持多维度组合搜索：队长名称、日期(如9.12)、时间段(如14-15)、歌曲名称，参数不分先后顺序"
                },
                "车队图片查询": {
                    "format": "/车队图片查询 车队号",
                    "examples": [
                        "/车队图片查询 1",
                        "/车队图片查询 2"
                    ],
                    "description": "生成指定车队的甘特图，直观显示车队成员的推车时间安排，包括正式成员和替补成员"
                },
                "车牌管理": {
                    "format": "/上传车牌 车队号 车牌号",
                    "examples": [
                        "/上传车牌 1 12345",
                        "/上传车牌 2 67890"
                    ],
                    "description": "为指定车队上传车牌信息，队长和成员都可以上传"
                }
            },
            "features": [
                "每个车队最多容纳5人（包括队长），另外支持无限替补成员",
                "替补成员系统：当正式成员不足时，替补成员可以参与推车/跑推/共跑",
                "支持多天报班，如8.3-8.7表示从8月3日到8月7日",
                "权限管理：撤回班车仅限报班人，队员撤回限队员本人或队长",
                "自动过期管理，过期车队自动隐藏但保留数据",
                "多种查询方式：按车队号、队长名称、可报名状态",
                "甘特图生成：可视化显示车队成员的推车时间安排",
                "甘特图颜色区分成员类型（推车/跑推/共跑及对应替补）"
            ],
            "tips": [
                "推车指令支持按队长名称或班号操作，班号更快捷",
                "多天报班格式：8.3-8.7，系统会自动处理日期范围",
                "车队号会自动分配，无需手动指定",
                "过期车队不会显示在可报名列表中",
                "替补成员不占用正式成员名额，可以无限添加",
                "替补成员撤回使用相同的 /队员撤回 指令",
                "甘特图会显示所有成员（包括替补）的时间安排，便于协调",
                "车主综合显示：若未手动填写车主综合，将自动显示 周回×倍率",
                "跑推计入推车时长的0.2倍（跑推替补同样适用）"
            ]
        }
        
        return {
            "type": "text",
            "content": self._format_help_info(help_data),
            "metadata": {
                "service": "help",
                "help_type": "team",
                "timestamp": self._get_current_time()
            }
        }
    
    def _get_music_help(self) -> Dict[str, Any]:
        """
        获取音乐服务帮助信息
        
        Returns:
            Dict[str, Any]: 音乐服务帮助信息
        """
        help_data = {
            "service_name": "音乐服务",
            "description": "提供在线音乐搜索和播放功能，支持搜索网易云音乐歌曲并获取播放链接",
            "commands": [
                {
                    "command": "/听歌 [歌曲名称]",
                    "description": "搜索指定歌曲并获取播放链接",
                    "example": "/听歌 稻香",
                    "parameters": [
                        {
                            "name": "歌曲名称",
                            "type": "string",
                            "required": True,
                            "description": "要搜索的歌曲名称，支持歌手名+歌曲名"
                        }
                    ]
                },
                {
                    "command": "/音乐源列表",
                    "description": "查看当前支持的音乐源",
                    "example": "/音乐源列表",
                    "parameters": []
                }
            ],
            "features": [
                "🎵 在线音乐搜索",
                "🎶 多音乐源支持",
                "🔗 直接播放链接获取",
                "📱 移动端兼容",
                "⚡ 快速响应"
            ],
            "usage_tips": [
                "💡 搜索时可以包含歌手名，如：'/听歌 周杰伦 稻香'",
                "💡 支持中英文歌曲搜索",
                "💡 如果搜索结果较多，会返回最匹配的歌曲",
                "💡 播放链接有时效性，建议及时使用"
            ],
            "limitations": [
                "⚠️ 依赖网络连接质量",
                "⚠️ 部分歌曲可能因版权限制无法获取",
                "⚠️ 播放链接具有时效性"
            ]
        }
        
        return {
            "type": "text",
            "content": help_data,
            "metadata": {
                "service": "help",
                "help_type": "music",
                "timestamp": datetime.now().isoformat()
            }
        }

    def _get_choice_help(self) -> Dict[str, Any]:
        """
        获取选择服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 选择服务帮助信息
        """
        return {
            "service_name": "选择服务",
            "description": "识别诸如 A或者B、A或B或C、A、B、C选哪个 等消息，无需前缀，随机选择一个并返回语录。",
            "features": [
                "识别多种连接词：或者/或/还是/、/or",
                "支持多个候选项，自动去重与清洗",
                "随机返回预设语录，可在配置中扩展",
                "无前缀触发，仅在识别到选择语境时响应",
                "询问关键词可配置：支持‘哪个好/怎么选/推荐哪个/选哪个/哪个更好’等"
            ],
            "usage": [
                "直接发送：A或者B",
                "支持：A或B或C",
                "支持：A、B、C选哪个",
                "无需添加 '/' 前缀"
            ],
            "examples": [
                "晚饭吃火锅或者烧烤？",
                "去A地还是B地还是C地？",
                "iOS、安卓、鸿蒙选哪个更好？"
            ],
            "tips": [
                "列举时可用中文顿号或逗号，都会识别（逗号会被统一为首个分隔符）",
                "可以混合使用‘或者/或/还是/or’等连接词",
                "可在 config.json 的 services.choice 下配置：phrases/connectors/separators/question_keywords",
                "英文 or 也会识别（需保留在 connectors 中）",
                "语录模板中的 {opt} 会替换为最终选择的选项"
            ]
        }

    def _get_pjsk_help(self) -> Dict[str, Any]:
        """
        获取PJSK服务的帮助信息
        
        Returns:
            Dict[str, Any]: 包含PJSK帮助信息的字典
        """
        help_data = {
            "title": "🎵 PJSK 帮助信息",
            "description": "Project Sekai 相关资源链接",
            "links": {
                "m": "https://8823.resona.cn/bot-commands",
                "s": "https://help.mikuware.top/",
                "h": "https://docs.haruki.seiunx.com/"
            }
        }
        
        return {
            "type": "text",
            "content": help_data,
            "metadata": {
                "service": "help",
                "help_type": "spell",
                "timestamp": datetime.now().isoformat()
            }
        }

    def _get_user_management_help(self) -> Dict[str, Any]:
        """
        获取用户管理服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 用户管理服务帮助信息
        """
        return {
            "title": "👤 注册 功能指南",
            "description": "网页端用户注册、密码修改和群聊管理功能",
            "commands": {
                "注册": {
                    "format": "/注册 <群聊号> <中文名> <密码>",
                    "examples": [
                        "/注册 123456789 张三 mypassword123",
                        "/注册 987654321 李四 securepass456"
                    ],
                    "description": "注册新的网页端用户账户",
                    "parameters": {
                        "群聊号": "用户所属的群聊号码，必须为数字",
                        "中文名": "用户的名称",
                        "密码": "用户登录密码，建议使用强密码"
                    }
                },
                "修改密码": {
                    "format": "/修改密码 <旧密码> <新密码>",
                    "examples": [
                        "/修改密码 oldpass123 newpass456",
                        "/修改密码 myoldpassword mynewpassword"
                    ],
                    "description": "修改当前用户的登录密码",
                    "parameters": {
                        "旧密码": "当前使用的密码",
                        "新密码": "要设置的新密码"
                    }
                },
                "增加群聊": {
                    "format": "/增加群聊 <群号>",
                    "examples": [
                        "/增加群聊 123456789",
                        "/增加群聊 987654321"
                    ],
                    "description": "为当前用户添加新的群聊权限",
                    "parameters": {
                        "群号": "要添加的群聊号码，必须为数字"
                    }
                }
            },
            "features": [
                "就是注册，成功后去网站walnutmortis.top试试吧！"
            ],
            "notes": [
                "注册成功后记得去网站登录试试哦！！！"
            ],
            "type": "text",
            "metadata": {
                "service": "help",
                "help_type": "user_management",
                "timestamp": datetime.now().isoformat()
            }
        }

    def _get_spell_help(self) -> Dict[str, Any]:
        """
        获取施法服务的详细帮助信息
        
        Returns:
            Dict[str, Any]: 施法服务帮助信息
        """
        return {
            "title": "🪄 施法 功能指南",
            "description": "随机从数据目录 data/shifa 发送一张图片",
            "commands": {
                "施法": {
                    "format": "/施法",
                    "examples": ["/施法", "施法"],
                    "description": "随机发送一张施法图片"
                }
            },
            "notes": [
                "将图片放在 data/shifa 目录下",
                "支持格式：jpg、jpeg、png、gif、webp"
            ]
        }

    def _get_quote_help(self) -> Dict[str, Any]:
        """
        获取励志语录相关帮助信息
        
        Returns:
            Dict[str, Any]: 励志语录帮助信息
        """
        return {
            "service_name": "励志语录",
            "description": "随机发送一条励志/鸡汤语录（根据项目内置语录数据与格式化器输出）。",
            "usage": [
                "发送：语录",
                "发送：quote"
            ],
            "examples": [
                "语录",
                "quote"
            ],
            "tips": [
                "该功能是否启用取决于当前运行实例的服务路由配置",
                "若发送后无回复，可能当前未加载对应服务或被禁用"
            ]
        }

    def _get_voice_help(self) -> Dict[str, Any]:
        """
        获取语音服务的帮助信息
        
        Returns:
            Dict[str, Any]: 语音服务帮助信息
        """
        return {
            "service_name": "语音",
            "description": "管理并播放本地语音文件，支持列出文件与按名称播放。",
            "commands": {
                "语音列表": {
                    "format": "/语音列表",
                    "examples": ["/语音列表"],
                    "description": "查看所有可用语音文件"
                },
                "语音输出": {
                    "format": "/语音输出 <文件名>",
                    "examples": ["/语音输出 example.wav"],
                    "description": "输出指定语音文件（以语音消息形式发送）"
                },
                "播放语音": {
                    "format": "/播放语音 <文件名>",
                    "examples": ["/播放语音 example.wav"],
                    "description": "播放指定语音文件（等价于语音输出）"
                }
            },
            "tips": [
                "语音文件需放置在语音目录下，文件名需精确匹配（含扩展名）",
                "如提示找不到文件，先使用 /语音列表 确认可用文件名"
            ]
        }

    def _get_gallery_help(self) -> Dict[str, Any]:
        """
        获取画廊服务的帮助信息
        
        Returns:
            Dict[str, Any]: 画廊服务帮助信息
        """
        return {
            "service_name": "画廊",
            "description": "通过回复图片并发送命令，将图片保存到画廊目录；支持查看、拼图预览、别名与查重。",
            "commands": {
                "上传画廊": {
                    "format": "/上传画廊 <名称>\n/上传 <名称>\n/上传名称（无空格，如：/上传猪）",
                    "examples": [
                        "/上传画廊 mnr",
                        "/上传 猪",
                        "/上传猪"
                    ],
                    "description": "将被回复消息或当前消息内图片保存到 data/gallery/<名称>"
                },
                "看图": {
                    "format": "/看 <名称>\n/看名称id（无空格）",
                    "examples": [
                        "/看 mnr",
                        "/看mnr1"
                    ],
                    "description": "随机查看该画廊的单张图片或指定编号图片"
                },
                "看所有": {
                    "format": "/看所有 <名称>",
                    "examples": [
                        "/看所有 mnr"
                    ],
                    "description": "生成该画廊的拼图预览并发送"
                },
                "画廊别名": {
                    "format": "/画廊别名 <名称A> <名称B>",
                    "examples": [
                        "/画廊别名 mnr 莫莫"
                    ],
                    "description": "将名称B并入名称A（以后B会被解析为A）"
                },
                "强制上传": {
                    "format": "/强制上传画廊",
                    "examples": ["/强制上传画廊"],
                    "description": "当查重提示疑似重复时，3分钟内可用此命令强制保存"
                },
                "退回上传": {
                    "format": "/退回上传",
                    "examples": ["/退回上传"],
                    "description": "撤销最近一次待确认/刚完成的上传流程"
                }
            },
            "tips": [
                "推荐流程：先发送图片 → 回复该图片消息 → 再发送 /上传画廊 名称",
                "名称会做简单清理以适配Windows文件系统，避免非法字符导致失败",
                "若未回复图片也未附带图片，将提示使用方法"
            ]
        }

    def _get_group_member_help(self) -> Dict[str, Any]:
        """
        获取群成员获取服务的帮助信息
        
        Returns:
            Dict[str, Any]: 群成员服务帮助信息
        """
        return {
            "service_name": "群成员",
            "description": "从群聊获取成员列表并导出为Excel，同时生成角色分布等统计信息。",
            "commands": {
                "成员获取": {
                    "format": "/成员获取",
                    "examples": ["/成员获取"],
                    "description": "获取当前群聊成员列表并保存到 data/group_members 目录"
                }
            },
            "tips": [
                "需要NapCat正常连接且机器人在目标群内",
                "导出Excel依赖 pandas/openpyxl，若缺失可能无法生成文件"
            ]
        }

    def _get_audit_help(self) -> Dict[str, Any]:
        """
        获取审核题库服务的帮助信息
        
        Returns:
            Dict[str, Any]: 审核服务帮助信息
        """
        return {
            "service_name": "审核",
            "description": "审核题库查询服务，支持随机抽取与全部列出。",
            "commands": {
                "审核问答": {
                    "format": "/审核问答",
                    "examples": ["/审核问答"],
                    "description": "随机返回三条题目"
                },
                "审核题目": {
                    "format": "/审核题目",
                    "examples": ["/审核题目"],
                    "description": "返回全部题目"
                }
            },
            "tips": [
                "题库文件位于 data/SIN/question.json",
                "题库为空或缺失时会提示数据错误"
            ]
        }

    def _get_mc_help(self) -> Dict[str, Any]:
        """
        获取MC消息发送服务的帮助信息
        
        Returns:
            Dict[str, Any]: MC消息发送服务帮助信息
        """
        return {
            "service_name": "mc",
            "description": "将群聊/私聊消息通过RCON转发到MC服务器。",
            "commands": {
                "send": {
                    "format": "/send <消息内容>\n/send\\n<消息内容>\n/send消息内容",
                    "examples": [
                        "/send Hello World",
                        "/send\nHello World",
                        "/sendHello World"
                    ],
                    "description": "将内容发送到MC服务器聊天频道"
                }
            },
            "tips": [
                "目标服务器地址与端口由配置 services.mc 决定",
                "可配置 allowed_groups 限制允许发送的群聊"
            ]
        }

    def _get_daily_team_statistics_help(self) -> Dict[str, Any]:
        """
        获取每日车队统计服务的帮助信息
        
        Returns:
            Dict[str, Any]: 每日车队统计服务帮助信息
        """
        return {
            "service_name": "每日车队统计",
            "description": "每天自动统计昨日结束车队（自然过期+撤回），并可在统计消息中生成甘特图。",
            "commands": {
                "手动统计车队": {
                    "format": "/手动统计车队",
                    "examples": ["/手动统计车队"],
                    "description": "手动执行一次“昨日结束车队”统计任务"
                }
            },
            "tips": [
                "自动执行时间与群聊映射由服务配置决定",
                "统计口径包含“自然过期”和“撤回”两种结束类型"
            ]
        }

    def _get_daily_push_time_statistics_help(self) -> Dict[str, Any]:
        """
        获取每日推时统计服务的帮助信息
        
        Returns:
            Dict[str, Any]: 每日推时统计服务帮助信息
        """
        return {
            "service_name": "每日推时统计",
            "description": "每日自动统计昨日的手动推时操作记录，并按群组保存日志文件。",
            "commands": {
                "推时统计": {
                    "format": "/推时统计",
                    "examples": ["/推时统计"],
                    "description": "手动触发昨日推时统计"
                },
                "推时统计状态": {
                    "format": "/推时统计状态",
                    "examples": ["/推时统计状态"],
                    "description": "查看服务配置与运行状态"
                }
            },
            "tips": [
                "统计结果默认保存到 data/log/push_time/ 目录（以服务配置为准）",
                "仅统计日志中记录到的手动增减操作"
            ]
        }

    def _get_mutsmi_help(self) -> Dict[str, Any]:
        """
        获取若叶睦互动服务的帮助信息
        
        Returns:
            Dict[str, Any]: 若叶睦帮助信息
        """
        return {
            "service_name": "若叶睦",
            "description": "若叶睦互动与金币/好感小游戏集合。",
            "commands": {
                "摸摸": {
                    "format": "摸摸睦头人 / 摸摸莫莫 / 摸摸木木 / 摸摸睦睦 / 摸摸若叶睦",
                    "examples": ["摸摸睦睦"],
                    "description": "当日首次随机提升好感并获得金币奖励"
                },
                "商品一览": {
                    "format": "商品一览",
                    "examples": ["商品一览"],
                    "description": "查看可购买礼物列表"
                },
                "买礼物": {
                    "format": "买<商品名称>",
                    "examples": ["买苹果"],
                    "description": "消耗金币购买礼物并提升好感"
                },
                "种黄瓜": {
                    "format": "/种黄瓜",
                    "examples": ["/种黄瓜"],
                    "description": "开始种植任务，到期后再次输入结算"
                },
                "打工": {
                    "format": "/打工",
                    "examples": ["/打工"],
                    "description": "开始打工任务，到期后再次输入结算"
                },
                "演唱会": {
                    "format": "/live\n/演唱会",
                    "examples": ["/live", "/演唱会"],
                    "description": "开始演出任务，到期后再次输入结算"
                },
                "放弃活动": {
                    "format": "/放弃活动",
                    "examples": ["/放弃活动"],
                    "description": "立即解除当前额外金币任务（不保留进度）"
                }
            },
            "tips": [
                "任务为两段式：首次触发开始计时，到期后再次发送同指令结算",
                "奖励与概率规则以服务内实现为准"
            ]
        }

    def _get_pjskranking_help(self) -> Dict[str, Any]:
        """
        获取PJSK排名查询服务的帮助信息
        
        Returns:
            Dict[str, Any]: PJSK排名帮助信息
        """
        return {
            "service_name": "pjsk排名",
            "description": "查询PJSK指定排名/区间数据，并提供活动/预测等接口信息。",
            "usage": [
                "wcnsk100 或 wcnsk 100",
                "wcnsk10-20 或 wcnsk 10-20",
                "wcn 活动列表",
                "wcn 预测",
                "wcn 预测 <活动ID>",
                "wcn 预测历史 <活动ID> <rank>"
            ],
            "examples": [
                "wcnsk100",
                "wcnsk 10-20",
                "wcn 活动列表",
                "wcn 预测 123"
            ],
            "tips": [
                "部分查询会返回图片结果（排名信息整合图）",
                "数据来源与缓存位置在 data/pjsk/ranking 目录"
            ]
        }

    def _get_pjskgachashistory_help(self) -> Dict[str, Any]:
        """
        获取PJSK抽卡历史服务的帮助信息
        
        Returns:
            Dict[str, Any]: PJSK抽卡历史帮助信息
        """
        return {
            "service_name": "pjsk抽卡记录",
            "description": "查询并渲染PJSK抽卡历史记录，支持绑定用户与修复未知卡池。",
            "usage": [
                "wcn抽卡记录 [数量]",
                "wcn抽卡记录 全",
                "wcn抽卡修复",
                "pjskgacha <user_id> [数量]",
                "pjsk未知卡池 <user_id>"
            ],
            "examples": [
                "wcn抽卡记录",
                "wcn抽卡记录 20",
                "wcn抽卡记录 全",
                "wcn抽卡修复"
            ],
            "tips": [
                "首次使用可能会抓取更多历史以完成本地缓存",
                "如未绑定用户ID，请先按服务提示完成绑定流程"
            ]
        }
