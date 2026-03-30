#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目每日运势服务

本文件实现每日运势功能，包括：
1. 随机生成用户今日运势
2. 运势数据的存储和管理
3. 防止重复测试的机制
4. 运势对比功能
5. 过期数据的自动清理

支持的指令：
- /今日运势 - 获取今日运势
- /运势对比 - 查看近3天运势走向

作者: Mortisfun Team
版本: 1.0.0
创建时间: 2025
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import random
from .base_service import BaseService


class DailyFortuneService(BaseService):
    """
    每日运势服务类
    
    负责处理用户的每日运势请求，包括运势生成、存储、
    对比分析等功能。确保每个用户每天只能测试一次。
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """
        初始化每日运势服务
        
        Args:
            config (Dict[str, Any]): 服务配置
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        
        # 运势类型和描述
        self.fortune_types: Dict[str, str] = {
            "love": "爱情运势",
            "career": "事业运势",
            "wealth": "财富运势",
            "health": "健康运势",
            "study": "学习运势"
        }
        
        # 从数据文件加载运势等级
        self.fortune_levels: List[Dict[str, Any]] = self._load_fortune_levels()
        
        # 从数据文件加载运势描述
        self.fortune_descriptions: Dict[str, Any] = self._load_fortune_descriptions()
        
        # 从数据文件加载幸运元素
        self.lucky_elements: Dict[str, Any] = self._load_lucky_elements()
        
        # 确保数据目录存在
        self._ensure_fortune_directories()
        
        self.log_unified("INFO", "每日运势服务初始化完成", group_id="system", user_id="system")
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理运势请求消息
        
        Args:
            message (str): 用户消息内容
            user_id (str): 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[Dict[str, Any]]: 包含内容和图片路径的响应字典，如果无法处理则返回None
        """
        message = message.strip()
        
        # 处理今日运势请求
        if message == "/今日运势":
            return self.get_daily_fortune(user_id)
        
        # 处理运势对比请求
        elif message == "/运势对比":
            return self.compare_fortune(user_id)
        
        # 不是运势相关指令
        return None
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取每日运势服务的帮助文本
        
        Returns:
            str: 帮助文本
        """
        help_text = """
……运势帮助

指令：
• `/今日运势` - ……今天的运势
• `/运势对比` - ……最近的运势变化

说明：
• 一天只能测一次……重复的话就是之前的结果
• 包含爱情、事业、财富、健康、学习……五个方面
• 每个方面都有等级和建议什么的
• 还有幸运颜色、数字、方位……这些
• 数据保留3天……过期就删了

示例：
• 用户：`/今日运势`
• 回复：……五大运势的分析

• 用户：`/运势对比`
• 回复：……近3天的运势变化

提示：
……仅供娱乐。不要太当真。
        """
        return help_text.strip()
    
    def get_daily_fortune(self, user_id: str) -> Dict[str, Any]:
        """
        获取用户今日运势
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Dict[str, Any]: 包含content和image_path的响应字典
        """
        try:
            # 清理过期数据
            self.cleanup_old_fortune_data()
            
            # 检查今天是否已经测试过
            if self.check_today_fortune_exists(user_id):
                # 获取今天的运势数据
                today = datetime.now().strftime("%Y-%m-%d")
                fortune_file = f"daily_{user_id}_{today}.json"
                file_path = self.data_manager.base_path / "fortune" / "daily" / fortune_file
                fortune_data = self.data_manager._safe_read_json(file_path)
                
                if fortune_data:
                    formatted_result = self._format_fortune_result(fortune_data)
                    content = f"……今天已经测过了\n\n{formatted_result['content']}"
                    return {"content": content, "image_path": formatted_result['image_path']}
            
            # 生成新的运势
            fortune_data = self.generate_fortune(user_id)
            
            # 保存运势数据
            today = datetime.now().strftime("%Y-%m-%d")
            fortune_file = f"daily_{user_id}_{today}.json"
            file_path = self.data_manager.base_path / "fortune" / "daily" / fortune_file
            self.data_manager._safe_write_json(file_path, fortune_data)
            
            formatted_result = self._format_fortune_result(fortune_data)
            return {"content": formatted_result['content'], "image_path": formatted_result['image_path']}
            
        except Exception as e:
            return {"content": f"……运势出问题了：{str(e)}", "image_path": None}
    
    def _format_fortune_result(self, fortune_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化运势结果为可读文本
        
        Args:
            fortune_data (Dict[str, Any]): 运势数据
            
        Returns:
            Dict[str, Any]: 包含content和image_path的字典
        """
        result = []
        result.append(f"……{fortune_data['date']} 运势报告")
        result.append("")
        
        # 总体运势
        overall_score = fortune_data.get('overall_score', 0)
        if overall_score >= 80:
            overall_desc = "……运势很好"
        elif overall_score >= 60:
            overall_desc = "……运势不错"
        elif overall_score >= 40:
            overall_desc = "……运势平稳"
        else:
            overall_desc = "……运势不太好"
        
        result.append(f"总体运势：{overall_score}分 - {overall_desc}")
        result.append("")
        
        # 各项运势
        result.append("详细运势：")
        for fortune_type, fortune_info in fortune_data['fortunes'].items():
            color = fortune_info['color']
            type_name = fortune_info['type']
            level = fortune_info['level']
            score = fortune_info['score']
            description = fortune_info['description']
            
            result.append(f"{color} {type_name}：{level}（{score}分）")
            result.append(f"   {description}")
            result.append("")
        
        # 幸运元素
        lucky = fortune_data['lucky_elements']
        result.append("今日幸运元素：")
        result.append(f"• 颜色：{lucky['color']}")
        result.append(f"• 数字：{lucky['number']}")
        result.append(f"• 方位：{lucky['direction']}")
        result.append(f"• 物品：{lucky['item']}")
        
        # 使用总体运势图片路径
        main_image_path = fortune_data.get('overall_image_path', None)
        
        return {
            "content": "\n".join(result),
            "image_path": main_image_path
        }
    
    def _format_fortune_comparison(self, history: List[Dict[str, Any]]) -> str:
        """
        格式化运势对比结果
        
        Args:
            history (List[Dict[str, Any]]): 运势历史数据
            
        Returns:
            str: 格式化后的对比文本
        """
        result = []
        result.append("……运势走向分析")
        result.append("")
        
        # 显示每天的总体运势
        result.append("总体运势趋势：")
        for i, data in enumerate(history):
            date = data['date']
            score = data.get('overall_score', 0)
            
            # 添加趋势箭头
            if i > 0:
                prev_score = history[i-1].get('overall_score', 0)
                if score > prev_score:
                    trend = "📈"
                elif score < prev_score:
                    trend = "📉"
                else:
                    trend = "➡️"
            else:
                trend = "🔸"
            
            result.append(f"{trend} {date}: {score}分")
        
        result.append("")
        
        # 分析各项运势变化
        result.append("各项运势变化：")
        
        for fortune_type in self.fortune_types.keys():
            type_name = self.fortune_types[fortune_type]
            scores = []
            
            for data in history:
                if fortune_type in data.get('fortunes', {}):
                    score = data['fortunes'][fortune_type].get('score', 0)
                    scores.append(score)
            
            if len(scores) >= 2:
                if scores[0] > scores[-1]:
                    trend = "上升"
                elif scores[0] < scores[-1]:
                    trend = "下降"
                else:
                    trend = "平稳"
                
                result.append(f"• {type_name}: {trend}")
        
        result.append("")
        result.append("建议：……运势有起伏很正常。保持积极心态就好")
        
        return "\n".join(result)
    
    def compare_fortune(self, user_id: str) -> Dict[str, Any]:
        """
        对比用户近3天的运势走向
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            str: 运势对比结果
        """
        try:
            # 清理过期数据
            self.cleanup_old_fortune_data()
            
            # 获取近3天的运势历史
            history = self.get_fortune_history(user_id, 3)
            
            if not history:
                return {"content": "……没有运势记录。先用 /今日运势 获取信息吧", "image_path": None}
            
            if len(history) == 1:
                return {"content": "……记录不够。至少需要2天的数据才能对比", "image_path": None}
            
            content = self._format_fortune_comparison(history)
            return {"content": content, "image_path": None}
            
        except Exception as e:
            return {"content": f"……运势对比出问题了：{str(e)}", "image_path": None}
    
    def generate_fortune(self, user_id: str) -> Dict[str, Any]:
        """
        生成新的运势数据
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            Dict[str, Any]: 运势数据
        """
        fortune_data = {
            "user_id": user_id,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": datetime.now().isoformat(),
            "fortunes": {},
            "lucky_elements": {
                "color": random.choice(self.lucky_elements["colors"]),
                "number": random.choice(self.lucky_elements["numbers"]),
                "direction": random.choice(self.lucky_elements["directions"]),
                "item": random.choice(self.lucky_elements["items"])
            },
            "overall_score": 0
        }
        
        total_score = 0
        
        # 为每个运势类型生成数据（不包含单独的图片路径）
        for fortune_type in self.fortune_types.keys():
            level_info = self._generate_random_fortune_level()
            description = self._generate_fortune_description(fortune_type, level_info["level"])
            
            fortune_data["fortunes"][fortune_type] = {
                "type": self.fortune_types[fortune_type],
                "level": level_info["level"],
                "score": level_info["score"],
                "color": level_info["color"],
                "description": description
            }
            
            total_score += level_info["score"]
        
        # 计算总体运势分数
        fortune_data["overall_score"] = round(total_score / len(self.fortune_types), 1)
        
        # 根据总体运势分数生成对应的运势等级和图片
        if fortune_data["overall_score"] >= 80:
            overall_level = "大吉"
        elif fortune_data["overall_score"] >= 60:
            overall_level = "中吉"
        elif fortune_data["overall_score"] >= 45:
            overall_level = "小吉"
        elif fortune_data["overall_score"] >= 30:
            overall_level = "平"
        elif fortune_data["overall_score"] >= 15:
            overall_level = "小凶"
        elif fortune_data["overall_score"] >= 5:
            overall_level = "中凶"
        else:
            overall_level = "大凶"
        
        # 生成总体运势图片路径
        fortune_data["overall_level"] = overall_level
        fortune_data["overall_image_path"] = self.get_fortune_image_path(overall_level)
        
        return fortune_data
    
    def check_today_fortune_exists(self, user_id: str) -> bool:
        """
        检查用户今天是否已经测试过运势
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            bool: 是否已经测试过
        """
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            fortune_file = f"daily_{user_id}_{today}.json"
            return self.file_exists("fortune/daily", fortune_file)
        except Exception:
            return False
    
    def get_fortune_history(self, user_id: str, days: int = 3) -> List[Dict[str, Any]]:
        """
        获取用户的运势历史记录
        
        Args:
            user_id (str): 用户ID
            days (int): 获取天数
            
        Returns:
            List[Dict[str, Any]]: 运势历史列表
        """
        history = []
        
        try:
            # 获取最近几天的日期
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
                fortune_file = f"daily_{user_id}_{date}.json"
                
                file_path = self.data_manager.base_path / "fortune" / "daily" / fortune_file
                if file_path.exists():
                    fortune_data = self.data_manager._safe_read_json(file_path)
                    if fortune_data:
                        history.append(fortune_data)
            
            # 按日期排序（最新的在前）
            history.sort(key=lambda x: x.get("date", ""), reverse=True)
            
        except Exception as e:
            self.log_unified("ERROR", f"获取运势历史失败: {e}", user_id=user_id)
        
        return history
    
    def cleanup_old_fortune_data(self):
        """
        清理超过3天的运势数据
        """
        try:
            # 获取3天前的日期
            cutoff_date = datetime.now() - timedelta(days=3)
            
            # 获取所有运势文件
            fortune_dir = self.data_manager.base_path / "fortune" / "daily"
            if not fortune_dir.exists():
                return
            
            # 遍历文件并删除过期的
            for file_path in fortune_dir.glob("daily_*.json"):
                try:
                    # 从文件名提取日期
                    filename = file_path.stem
                    parts = filename.split("_")
                    if len(parts) >= 3:
                        date_str = parts[-1]  # 最后一部分是日期
                        file_date = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        # 如果文件日期早于截止日期，删除文件
                        if file_date < cutoff_date:
                            file_path.unlink()
                            self.log_unified("INFO", f"删除过期运势文件: {filename}", group_id="system", user_id="system")
                            
                except (ValueError, IndexError) as e:
                    self.log_unified("WARNING", f"解析运势文件名失败: {filename}, 错误: {e}", group_id="system", user_id="system")
                    
        except Exception as e:
            # 获取当前时间戳
            from datetime import datetime
            self.log_unified("ERROR", f"清理过期运势数据失败: {e}", group_id="system", user_id="system")
    
    def _generate_random_fortune_level(self) -> Dict[str, Any]:
        """
        随机生成运势等级
        
        Returns:
            Dict[str, Any]: 运势等级信息
        """
        return random.choice(self.fortune_levels)
    
    def _generate_fortune_description(self, fortune_type: str, level: str) -> str:
        """
        生成运势描述
        
        Args:
            fortune_type (str): 运势类型
            level (str): 运势等级
            
        Returns:
            str: 运势描述
        """
        descriptions = self.fortune_descriptions.get(fortune_type, {})
        level_descriptions = descriptions.get(level, ["运势平平，保持平常心。"])
        return random.choice(level_descriptions)
    
    def _load_fortune_levels(self) -> List[Dict[str, Any]]:
        """
        从数据文件加载运势等级信息
        
        Returns:
            List[Dict[str, Any]]: 运势等级列表
        """
        try:
            file_path = self.data_manager.base_path / "fortune" / "fortune_levels.json"
            data = self.data_manager._safe_read_json(file_path)
            if data and "fortune_levels" in data:
                self.log_unified("DEBUG", f"成功加载运势等级数据，包含{len(data['fortune_levels'])}个等级", group_id="system", user_id="system")
                return data["fortune_levels"]
            else:
                self.log_unified("DEBUG", "运势等级数据文件不存在或格式错误，使用默认数据", group_id="system", user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"加载运势等级数据失败: {e}", group_id="system", user_id="system")
        
        # 返回默认数据
        return [
            {"level": "大吉", "score": 90, "color": "🟢", "image": "daji.svg", "description": "运势极佳"},
            {"level": "中吉", "score": 75, "color": "🟡", "image": "zhongji.svg", "description": "运势良好"},
            {"level": "小吉", "score": 60, "color": "🔵", "image": "xiaoji.svg", "description": "运势尚可"},
            {"level": "平", "score": 50, "color": "⚪", "image": "ping.svg", "description": "运势平稳"},
            {"level": "小凶", "score": 40, "color": "🟠", "image": "xiaoxiong.svg", "description": "运势稍差"},
            {"level": "中凶", "score": 25, "color": "🟤", "image": "zhongxiong.svg", "description": "运势不佳"},
            {"level": "大凶", "score": 10, "color": "🔴", "image": "daxiong.svg", "description": "运势极差"}
        ]
    
    def _load_fortune_descriptions(self) -> Dict[str, Dict[str, List[str]]]:
        """
        从数据文件加载运势描述信息
        
        Returns:
            Dict[str, Dict[str, List[str]]]: 运势描述字典
        """
        try:
            file_path = self.data_manager.base_path / "fortune" / "fortune_descriptions.json"
            data = self.data_manager._safe_read_json(file_path)
            if data and "fortune_descriptions" in data:
                self.log_unified("DEBUG", "成功加载运势描述数据", group_id="system", user_id="system")
                return data["fortune_descriptions"]
            else:
                self.log_unified("DEBUG", "运势描述数据文件不存在或格式错误，使用默认数据", group_id="system", user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"加载运势描述数据失败: {e}", group_id="system", user_id="system")
        
        # 返回默认数据
        return {
            "love": {"平": ["感情运势平平，保持现状即可"]},
            "career": {"平": ["工作状态平稳，按部就班即可"]},
            "wealth": {"平": ["财运平稳，收支基本平衡"]},
            "health": {"平": ["健康状况平稳，无大碍"]},
            "study": {"平": ["学习运势平稳，按计划进行"]}
        }
    
    def _load_lucky_elements(self) -> Dict[str, List]:
        """
        从数据文件加载幸运元素信息
        
        Returns:
            Dict[str, List]: 幸运元素字典
        """
        try:
            file_path = self.data_manager.base_path / "fortune" / "lucky_elements.json"
            data = self.data_manager._safe_read_json(file_path)
            if data and "lucky_elements" in data:
                self.log_unified("DEBUG", "成功加载幸运元素数据", group_id="system", user_id="system")
                return data["lucky_elements"]
            else:
                self.log_unified("DEBUG", "幸运元素数据文件不存在或格式错误，使用默认数据", group_id="system", user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"加载幸运元素数据失败: {e}", group_id="system", user_id="system")
        
        # 返回默认数据
        return {
            "colors": ["红色", "蓝色", "绿色", "黄色"],
            "numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
            "directions": ["东方", "南方", "西方", "北方"],
            "items": ["水晶", "玉石", "鲜花", "绿植"]
        }
    
    def get_fortune_image_path(self, level: str) -> str:
        """
        根据运势等级获取对应的图片路径
        
        Args:
            level (str): 运势等级
            
        Returns:
            str: 图片文件路径
        """
        # 查找对应等级的图片文件名
        for level_info in self.fortune_levels:
            if level_info['level'] == level:
                image_filename = level_info.get('image', f"{level}.svg")
                return f"data/fortune/images/{image_filename}"
        
        # 如果没找到，返回默认路径
        return f"data/fortune/images/{level}.svg"
    
    def _ensure_fortune_directories(self):
        """
        确保运势相关目录存在
        """
        try:
            fortune_daily_path = self.data_manager.base_path / "fortune" / "daily"
            fortune_images_path = self.data_manager.base_path / "fortune" / "images"
            
            # 检查并创建每日运势目录
            if not fortune_daily_path.exists():
                fortune_daily_path.mkdir(parents=True, exist_ok=True)
                self.log_unified("INFO", f"创建运势数据目录: {fortune_daily_path}", group_id="system", user_id="system")
            else:
                self.log_unified("DEBUG", f"运势数据目录已存在: {fortune_daily_path}", group_id="system", user_id="system")
            
            # 检查并创建运势图片目录
            if not fortune_images_path.exists():
                fortune_images_path.mkdir(parents=True, exist_ok=True)
                self.log_unified("INFO", f"创建运势图片目录: {fortune_images_path}", group_id="system", user_id="system")
            else:
                self.log_unified("DEBUG", f"运势图片目录已存在: {fortune_images_path}", group_id="system", user_id="system")
        except Exception as e:
            self.log_unified("ERROR", f"创建运势目录失败: {e}", group_id="system", user_id="system")
    
    def file_exists(self, directory: str, filename: str) -> bool:
        """
        检查文件是否存在
        
        Args:
            directory (str): 目录路径
            filename (str): 文件名
            
        Returns:
            bool: 文件是否存在
        """
        try:
            file_path = self.data_manager.base_path / directory / filename
            return file_path.exists()
        except Exception:
            return False