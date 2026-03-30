#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目星座运势服务

本文件实现星座运势功能，包括：
1. 12星座的今日运势生成
2. 星座运势数据的存储和管理
3. 星座运势对比功能
4. 多星座对比分析
5. 过期数据的自动清理

支持的指令：
- /星座运势 [星座] - 获取指定星座今日运势
- /星座运势对比 [星座] - 查看指定星座近3天运势走向
- /星座运势对比 [星座] + [星座] - 对比两个星座今日运势

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import random
import json
from pathlib import Path
from .base_service import BaseService


class FortuneService(BaseService):
    """
    星座运势服务类
    
    负责处理星座运势相关的请求，包括单个星座运势查询、
    历史对比、多星座对比等功能。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """
        初始化星座运势服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 12星座信息
        self.zodiac_signs: Dict[str, Dict[str, str]] = {
            "白羊座": {"symbol": "♈", "element": "火", "dates": "3.21-4.19"},
            "金牛座": {"symbol": "♉", "element": "土", "dates": "4.20-5.20"},
            "双子座": {"symbol": "♊", "element": "风", "dates": "5.21-6.21"},
            "巨蟹座": {"symbol": "♋", "element": "水", "dates": "6.22-7.22"},
            "狮子座": {"symbol": "♌", "element": "火", "dates": "7.23-8.22"},
            "处女座": {"symbol": "♍", "element": "土", "dates": "8.23-9.22"},
            "天秤座": {"symbol": "♎", "element": "风", "dates": "9.23-10.23"},
            "天蝎座": {"symbol": "♏", "element": "水", "dates": "10.24-11.22"},
            "射手座": {"symbol": "♐", "element": "火", "dates": "11.23-12.21"},
            "摩羯座": {"symbol": "♑", "element": "土", "dates": "12.22-1.19"},
            "水瓶座": {"symbol": "♒", "element": "风", "dates": "1.20-2.18"},
            "双鱼座": {"symbol": "♓", "element": "水", "dates": "2.19-3.20"}
        }
        
        # 运势维度
        self.fortune_aspects: Dict[str, str] = {
            "overall": "综合运势",
            "love": "爱情运势",
            "career": "事业运势",
            "wealth": "财富运势",
            "health": "健康运势"
        }
        
        # 初始化外部数据容器
        self.fortune_levels: Dict[str, Any] = {}
        self.fortune_descriptions: Dict[str, Any] = {}
        self.lucky_elements: Dict[str, Any] = {}
        self.fortune_ratings: List[Dict[str, Any]] = []
        
        # 加载外部数据文件
        self._load_zodiac_data()
        
        # 确保数据目录存在
        self._ensure_zodiac_directories()
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理星座运势请求消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[Dict[str, Any]]: 包含内容和图片路径的响应字典，如果无法处理则返回None
        """
        try:
            # 解析指令
            command_type, zodiac1, zodiac2 = self._parse_zodiac_command(message)
            
            # 如果命令类型为None，说明消息不相关，直接返回None
            if command_type is None:
                return None
            
            if command_type == "single":
                return self.get_zodiac_fortune(zodiac1)
            elif command_type == "compare":
                return self.compare_zodiac_fortune(zodiac1)
            elif command_type == "dual_compare":
                return self.compare_two_zodiacs(zodiac1, zodiac2)
            elif command_type == "help":
                return {"content": self.get_help_text(), "image_path": None}
            else:
                return None
                
        except Exception:
            return None
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取星座运势服务的帮助文本
        
        Returns:
            str: 帮助文本
        """
        help_text = "……星座运势服务\n\n"
        help_text += "指令：\n"
        help_text += "• /星座运势 [星座名] - ……看星座运势\n"
        help_text += "• /星座运势对比 [星座名] - ……看运势走向\n"
        help_text += "• /星座运势对比 [星座1] + [星座2] - ……对比两个星座\n\n"
        
        help_text += "支持的星座：\n"
        zodiac_list = []
        for zodiac, info in self.zodiac_signs.items():
            zodiac_list.append(f"{info['symbol']} {zodiac} ({info['dates']})")
        
        # 每行显示3个星座
        for i in range(0, len(zodiac_list), 3):
            line_zodiacs = zodiac_list[i:i+3]
            help_text += "  " + "  ".join(line_zodiacs) + "\n"
        
        help_text += "\n运势维度：综合、爱情、事业、财富、健康\n"
        help_text += "运势等级：★☆☆☆☆ 到 ★★★★★\n\n"
        help_text += "示例：\n"
        help_text += "• /星座运势 白羊座\n"
        help_text += "• /星座运势对比 金牛座\n"
        help_text += "• /星座运势对比 双子座 + 巨蟹座"
        
        return help_text
    
    def get_zodiac_fortune(self, zodiac: str) -> Dict[str, Any]:
        """
        获取指定星座的今日运势
        
        Args:
            zodiac (str): 星座名称
            
        Returns:
            Dict[str, Any]: 包含内容和图片路径的响应字典
        """
        if not self._validate_zodiac(zodiac):
            return {
                "content": f"……不支持这个星座：{zodiac}\n\n用 /星座运势 看支持的星座列表吧",
                "image_path": None
            }
        
        # 生成今日运势
        fortune_data = self._generate_zodiac_fortune(zodiac)
        
        # 格式化结果
        result = self._format_zodiac_fortune(zodiac, fortune_data)
        
        return result
    
    def compare_zodiac_fortune(self, zodiac: str) -> Dict[str, Any]:
        """
        对比指定星座的运势走向（近3天）
        
        Args:
            zodiac (str): 星座名称
            
        Returns:
            Dict[str, Any]: 包含内容和图片路径的响应字典
        """
        if not self._validate_zodiac(zodiac):
            return {
                "content": f"……不支持这个星座：{zodiac}\n\n用 /星座运势 看支持的星座列表吧",
                "image_path": None
            }
        
        # 获取近3天的运势数据
        fortune_history = []
        today = datetime.now()
        
        for i in range(3):
            date = today - timedelta(days=2-i)
            fortune_data = self._generate_zodiac_fortune(zodiac, date)
            fortune_history.append((date, fortune_data))
        
        # 格式化对比结果
        content = self._format_zodiac_comparison(zodiac, fortune_history)
        return {"content": content, "image_path": None}
    
    def compare_two_zodiacs(self, zodiac1: str, zodiac2: str) -> Dict[str, Any]:
        """
        对比两个星座的今日运势
        
        Args:
            zodiac1 (str): 第一个星座名称
            zodiac2 (str): 第二个星座名称
            
        Returns:
            Dict[str, Any]: 包含内容和图片路径的响应字典
        """
        if not self._validate_zodiac(zodiac1):
            return {
                "content": f"……不支持这个星座：{zodiac1}\n\n用 /星座运势 看支持的星座列表吧",
                "image_path": None
            }
        
        if not self._validate_zodiac(zodiac2):
            return {
                "content": f"……不支持这个星座：{zodiac2}\n\n用 /星座运势 看支持的星座列表吧",
                "image_path": None
            }
        
        # 生成两个星座的今日运势
        fortune1 = self._generate_zodiac_fortune(zodiac1)
        fortune2 = self._generate_zodiac_fortune(zodiac2)
        
        # 格式化双星座对比结果
        content = self._format_dual_zodiac_comparison(zodiac1, fortune1, zodiac2, fortune2)
        return {"content": content, "image_path": None}
    
    def generate_zodiac_fortune(self, zodiac: str) -> Dict[str, Any]:
        """
        生成指定星座的运势数据
        
        Args:
            zodiac (str): 星座名称
            
        Returns:
            Dict[str, Any]: 星座运势数据
        """
        return self._generate_zodiac_fortune(zodiac)
    
    def validate_zodiac(self, zodiac: str) -> bool:
        """
        验证星座名称是否有效
        
        Args:
            zodiac (str): 星座名称
            
        Returns:
            bool: 是否为有效星座
        """
        return zodiac in self.zodiac_signs
    
    def get_zodiac_history(self, zodiac: str, days: int = 3) -> List[Dict[str, Any]]:
        """
        获取指定星座的历史运势数据
        
        Args:
            zodiac (str): 星座名称
            days (int): 获取天数，默认3天
            
        Returns:
            List[Dict[str, Any]]: 运势历史列表
        """
        history = []
        today = datetime.now()
        
        for i in range(days):
            date = today - timedelta(days=i)
            fortune_data = self._generate_zodiac_fortune(zodiac, date)
            history.append({
                "date": date.strftime("%Y-%m-%d"),
                "fortune_data": fortune_data
            })
        
        return history
    
    def cleanup_old_zodiac_data(self) -> None:
        """
        清理超过3天的星座运势数据
        """
        self._cleanup_old_data()
    
    def _parse_zodiac_command(self, message: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        解析星座运势指令
        
        Args:
            message (str): 用户消息
            
        Returns:
            Tuple[Optional[str], Optional[str], Optional[str]]: (命令类型, 星座1, 星座2)
        """
        message = message.strip()
        
        # 必须以星座运势指令开头，否则不处理
        if not (message.startswith("/星座运势") or message.startswith("星座运势")):
            return None, None, None
        
        # 帮助指令
        if message in ["/星座运势", "星座运势"]:
            return "help", None, None
        
        # 对比指令
        if "对比" in message:
            # 双星座对比：/星座运势对比 白羊座 + 金牛座
            if "+" in message:
                parts = message.replace("/星座运势对比", "").replace("星座运势对比", "").strip()
                zodiac_parts = [z.strip() for z in parts.split("+")]
                if len(zodiac_parts) == 2:
                    return "dual_compare", zodiac_parts[0], zodiac_parts[1]
            else:
                # 单星座历史对比：/星座运势对比 白羊座
                zodiac = message.replace("/星座运势对比", "").replace("星座运势对比", "").strip()
                if zodiac:
                    return "compare", zodiac, None
        
        # 单个星座运势：/星座运势 白羊座
        else:
            zodiac = message.replace("/星座运势", "").replace("星座运势", "").strip()
            if zodiac:
                return "single", zodiac, None
        
        # 如果解析失败，返回None表示不处理
        return None, None, None
    
    def _generate_fortune_advice(self, zodiac: str, fortune_data: Dict[str, Any]) -> str:
        """
        生成星座运势建议
        
        Args:
            zodiac (str): 星座名称
            fortune_data (Dict[str, Any]): 运势数据
            
        Returns:
            str: 运势建议
        """
        advice_templates = {
            "high": [
                "今天是展现你才华的好时机，勇敢地追求你的目标吧！",
                "运势极佳，适合做重要决定和开始新的计划。",
                "今天的你充满正能量，是实现梦想的绝佳时机。"
            ],
            "medium": [
                "保持平常心，稳步前进会有不错的收获。",
                "今天适合巩固现有成果，为未来做好准备。",
                "虽然运势平稳，但细心观察会发现新的机会。"
            ],
            "low": [
                "今天宜静不宜动，多思考少行动会更安全。",
                "运势略有波动，建议保持谨慎的态度。",
                "今天是反思和调整的好时机，为明天积蓄力量。"
            ]
        }
        
        overall_score = fortune_data.get("overall", {}).get("score", 50)
        
        if overall_score >= 80:
            advice_type = "high"
        elif overall_score >= 60:
            advice_type = "medium"
        else:
            advice_type = "low"
        
        return random.choice(advice_templates[advice_type])
    
    def _load_zodiac_data(self) -> None:
        """
        从外部文件加载星座运势相关数据
        """
        try:
            # 加载运势等级数据
            levels_file = Path("data/zodiac/zodiac_fortune_levels.json")
            if levels_file.exists():
                with open(levels_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 设置fortune_levels为zodiac_fortune_levels数组
                    if "zodiac_fortune_levels" in data:
                        self.fortune_levels = data["zodiac_fortune_levels"]
                        # 转换为fortune_ratings格式
                        self.fortune_ratings = []
                        for level_data in data["zodiac_fortune_levels"]:
                            # 使用min_score作为score，保持兼容性
                            score = level_data.get("min_score", level_data.get("score", 50))
                            self.fortune_ratings.append({
                                "level": level_data["symbol"],
                                "desc": level_data["level"],
                                "score": score
                            })
                    else:
                        self.fortune_levels = []
                        self.fortune_ratings = self._get_default_ratings()
            else:
                # 默认运势等级
                self.fortune_levels = []
                self.fortune_ratings = self._get_default_ratings()
            
            # 加载运势描述数据
            descriptions_file = Path("data/zodiac/zodiac_fortune_descriptions.json")
            if descriptions_file.exists():
                with open(descriptions_file, 'r', encoding='utf-8') as f:
                    self.fortune_descriptions = json.load(f)
            
            # 加载幸运元素数据
            lucky_file = Path("data/zodiac/zodiac_lucky_elements.json")
            if lucky_file.exists():
                with open(lucky_file, 'r', encoding='utf-8') as f:
                    self.lucky_elements = json.load(f)
                    
        except Exception as e:
            self.log_unified("ERROR", f"加载星座数据时出错: {e}", group_id="system", user_id="system")
            # 使用默认数据
            self.fortune_levels = {}
            self.fortune_descriptions = {}
            self.lucky_elements = {}
            self.fortune_ratings = self._get_default_ratings()
    
    def _get_default_ratings(self) -> List[Dict[str, Any]]:
        """
        获取默认的运势等级数据
        
        Returns:
            List[Dict[str, Any]]: 默认运势等级列表
        """
        return [
            {"level": "★★★★★", "desc": "极佳", "score": 95},
            {"level": "★★★★☆", "desc": "很好", "score": 80},
            {"level": "★★★☆☆", "desc": "良好", "score": 65},
            {"level": "★★☆☆☆", "desc": "一般", "score": 50},
            {"level": "★☆☆☆☆", "desc": "较差", "score": 30}
        ]
    
    def _ensure_zodiac_directories(self) -> None:
        """
        确保星座运势相关目录存在
        """
        directories = [
            "data/zodiac",
            "data/zodiac/history",
            "data/zodiac/images"
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def _validate_zodiac(self, zodiac: str) -> bool:
        """
        验证星座名称是否有效
        
        Args:
            zodiac (str): 星座名称
            
        Returns:
            bool: 是否为有效星座
        """
        return zodiac in self.zodiac_signs
    
    def _generate_zodiac_fortune(self, zodiac: str, date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        生成指定星座的运势数据
        
        Args:
            zodiac (str): 星座名称
            date (Optional[datetime]): 指定日期，默认为今天
            
        Returns:
            Dict[str, Any]: 运势数据
        """
        if date is None:
            date = datetime.now()
        
        # 使用日期和星座作为随机种子，确保同一天同一星座的运势一致
        seed = f"{date.strftime('%Y-%m-%d')}_{zodiac}"
        random.seed(hash(seed) % (2**32))
        
        fortune_data = {}
        
        # 为每个运势维度生成数据
        for aspect_key, aspect_name in self.fortune_aspects.items():
            rating = random.choice(self.fortune_ratings)
            fortune_data[aspect_key] = {
                "name": aspect_name,
                "level": rating["level"],
                "desc": rating["desc"],
                "score": rating["score"],
                "advice": self._generate_aspect_advice(aspect_key, rating["score"])
            }
        
        # 生成幸运元素
        fortune_data["lucky"] = self._generate_lucky_elements(zodiac, date)
        
        # 重置随机种子
        random.seed()
        
        return fortune_data
    
    def _generate_aspect_advice(self, aspect: str, score: int) -> str:
        """
        生成特定运势维度的建议
        
        Args:
            aspect (str): 运势维度
            score (int): 运势分数
            
        Returns:
            str: 运势建议
        """
        advice_data = {
            "overall": {
                "high": ["今天整体运势极佳，是实现目标的好时机", "充满正能量的一天，勇敢追求梦想吧"],
                "medium": ["运势平稳，保持积极心态会有不错收获", "稳步前进，机会就在不远处"],
                "low": ["今天宜静不宜动，多思考少冲动", "运势略有波动，保持谨慎为上"]
            },
            "love": {
                "high": ["爱情运势旺盛，单身者有望遇到心仪对象", "感情甜蜜，是表达爱意的好时机"],
                "medium": ["感情运势平稳，适合深入了解彼此", "爱情需要耐心经营，慢慢来会更好"],
                "low": ["感情可能有小波折，多沟通少争执", "爱情运势一般，给彼此一些空间"]
            },
            "career": {
                "high": ["事业运势极佳，工作表现会得到认可", "职场运势旺盛，适合展示才华"],
                "medium": ["工作运势稳定，踏实努力会有进展", "事业平稳发展，保持专注很重要"],
                "low": ["工作中可能遇到挑战，保持冷静应对", "事业运势一般，避免做重大决定"]
            },
            "wealth": {
                "high": ["财运亨通，投资理财会有不错收益", "金钱运势极佳，适合进行财务规划"],
                "medium": ["财运平稳，理性消费是关键", "金钱运势一般，开源节流并重"],
                "low": ["财运略有波动，避免大额支出", "理财需谨慎，保守投资为宜"]
            },
            "health": {
                "high": ["健康运势极佳，精力充沛活力满满", "身体状态很好，适合运动锻炼"],
                "medium": ["健康状况稳定，保持良好作息", "身体运势平稳，注意劳逸结合"],
                "low": ["健康需要关注，多休息少熬夜", "身体可能有小不适，及时调理"]
            }
        }
        
        if score >= 80:
            level = "high"
        elif score >= 60:
            level = "medium"
        else:
            level = "low"
        
        aspect_advice = advice_data.get(aspect, advice_data["overall"])
        return random.choice(aspect_advice[level])
    
    def _generate_lucky_elements(self, zodiac: str, date: datetime) -> Dict[str, Any]:
        """
        生成幸运元素
        
        Args:
            zodiac (str): 星座名称
            date (datetime): 日期
            
        Returns:
            Dict[str, Any]: 幸运元素数据
        """
        colors = ["红色", "蓝色", "绿色", "黄色", "紫色", "橙色", "粉色", "白色", "黑色", "金色"]
        numbers = list(range(1, 10))
        directions = ["东", "南", "西", "北", "东南", "西南", "东北", "西北"]
        
        return {
            "color": random.choice(colors),
            "number": random.choice(numbers),
            "direction": random.choice(directions),
            "time": f"{random.randint(9, 18)}:00-{random.randint(9, 18)}:59"
        }
    
    def _format_zodiac_fortune(self, zodiac: str, fortune_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化单个星座运势信息
        
        Args:
            zodiac (str): 星座名称
            fortune_data (Dict[str, Any]): 运势数据
            
        Returns:
            Dict[str, Any]: 包含内容和图片路径的字典
        """
        zodiac_info = self.zodiac_signs[zodiac]
        today = datetime.now().strftime("%Y年%m月%d日")
        
        result = f"{zodiac_info['symbol']} {zodiac} ……今日运势\n"
        result += f"{today} ({zodiac_info['dates']})\n\n"
        
        # 运势详情
        for aspect_key, aspect_data in fortune_data.items():
            if aspect_key == "lucky":
                continue
            
            result += f"{aspect_data['name']}：{aspect_data['level']} ({aspect_data['desc']})\n"
            result += f"   {aspect_data['advice']}\n\n"
        
        # 幸运元素
        lucky = fortune_data.get("lucky", {})
        result += "今日幸运元素：\n"
        result += f"   颜色：{lucky.get('color', '金色')}\n"
        result += f"   数字：{lucky.get('number', 7)}\n"
        result += f"   方位：{lucky.get('direction', '东')}\n"
        result += f"   时间：{lucky.get('time', '10:00-11:59')}\n\n"
        
        # 总体建议
        overall_advice = self._generate_fortune_advice(zodiac, fortune_data)
        result += f"今日建议：{overall_advice}"
        
        # 根据综合运势等级生成图片路径
        overall_score = fortune_data.get("overall", {}).get("score", 50)
        image_path = self.get_zodiac_image_path(zodiac, overall_score)
        
        return {
            "content": result,
            "image_path": image_path
        }
    
    def get_zodiac_image_path(self, zodiac: str, overall_score: int) -> str:
        """
        根据星座和运势分数获取对应的图片路径
        
        Args:
            zodiac (str): 星座名称
            overall_score (int): 综合运势分数
            
        Returns:
            str: 图片文件路径
        """
        # 根据分数确定运势等级
        for level_data in self.fortune_levels:
            if level_data["min_score"] <= overall_score <= level_data["max_score"]:
                # 获取基础图片文件名（不含扩展名）
                base_image = level_data.get("image", "default.png")
                image_name, image_ext = base_image.rsplit('.', 1) if '.' in base_image else (base_image, 'png')
                
                # 生成星座专属的图片文件名：星座名_运势等级.扩展名
                zodiac_image_filename = f"{zodiac}_{image_name}.{image_ext}"
                zodiac_image_path = f"data/zodiac/images/{zodiac_image_filename}"
                
                # 检查星座专属图片是否存在，如果不存在则使用通用图片
                from pathlib import Path
                if Path(zodiac_image_path).exists():
                    return zodiac_image_path
                else:
                    # 如果星座专属图片不存在，返回通用运势等级图片
                    return f"data/zodiac/images/{base_image}"
        
        # 如果没有找到匹配的等级，返回默认图片
        return "data/zodiac/images/default.png"
    
    def _format_zodiac_comparison(self, zodiac: str, fortune_history: List[Tuple[datetime, Dict[str, Any]]]) -> str:
        """
        格式化星座运势对比信息
        
        Args:
            zodiac (str): 星座名称
            fortune_history (List[Tuple[datetime, Dict[str, Any]]]): 运势历史数据
            
        Returns:
            str: 格式化的对比信息
        """
        zodiac_info = self.zodiac_signs[zodiac]
        
        result = f"{zodiac_info['symbol']} {zodiac} ……运势走向分析\n\n"
        
        # 显示每天的运势
        for i, (date, fortune_data) in enumerate(fortune_history):
            date_str = date.strftime("%m月%d日")
            if i == len(fortune_history) - 1:
                date_str += " (今日)"
            
            result += f"{date_str}\n"
            
            for aspect_key, aspect_data in fortune_data.items():
                if aspect_key == "lucky":
                    continue
                result += f"   {aspect_data['name']}：{aspect_data['level']}\n"
            
            result += "\n"
        
        # 运势趋势分析
        result += "趋势分析：\n"
        
        for aspect_key, aspect_name in self.fortune_aspects.items():
            scores = [fortune[1][aspect_key]['score'] for fortune in fortune_history]
            
            if scores[-1] > scores[0]:
                trend = "上升"
            elif scores[-1] < scores[0]:
                trend = "下降"
            else:
                trend = "平稳"
            
            result += f"   {aspect_name}：{trend}\n"
        
        return result
    
    def _format_dual_zodiac_comparison(self, zodiac1: str, fortune1: Dict[str, Any], 
                                     zodiac2: str, fortune2: Dict[str, Any]) -> str:
        """
        格式化双星座对比信息
        
        Args:
            zodiac1 (str): 第一个星座名称
            fortune1 (Dict[str, Any]): 第一个星座运势数据
            zodiac2 (str): 第二个星座名称
            fortune2 (Dict[str, Any]): 第二个星座运势数据
            
        Returns:
            str: 格式化的双星座对比信息
        """
        info1 = self.zodiac_signs[zodiac1]
        info2 = self.zodiac_signs[zodiac2]
        today = datetime.now().strftime("%Y年%m月%d日")
        
        result = f"……星座运势对比\n"
        result += f"{today}\n\n"
        result += f"{info1['symbol']} {zodiac1} VS {info2['symbol']} {zodiac2}\n\n"
        
        # 逐项对比
        for aspect_key, aspect_name in self.fortune_aspects.items():
            score1 = fortune1[aspect_key]['score']
            score2 = fortune2[aspect_key]['score']
            
            result += f"{aspect_name}对比：\n"
            result += f"   {zodiac1}：{fortune1[aspect_key]['level']} ({score1}分)\n"
            result += f"   {zodiac2}：{fortune2[aspect_key]['level']} ({score2}分)\n"
            
            if score1 > score2:
                result += f"   {zodiac1} 略胜一筹\n\n"
            elif score2 > score1:
                result += f"   {zodiac2} 略胜一筹\n\n"
            else:
                result += f"   两者不相上下\n\n"
        
        # 总体对比
        total1 = sum(fortune1[key]['score'] for key in self.fortune_aspects.keys())
        total2 = sum(fortune2[key]['score'] for key in self.fortune_aspects.keys())
        
        result += "综合评分：\n"
        result += f"   {zodiac1}：{total1}分\n"
        result += f"   {zodiac2}：{total2}分\n\n"
        
        if total1 > total2:
            result += f"今日 {zodiac1} 的整体运势更好"
        elif total2 > total1:
            result += f"今日 {zodiac2} 的整体运势更好"
        else:
            result += "两个星座今日运势差不多……都还不错"
        
        return result