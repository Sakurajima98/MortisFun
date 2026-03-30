#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推时计算器工具类

功能说明：
- 提供不同群组的推时计算规则
- 支持基于群组ID的计算规则选择
- 封装推时、被推时长的计算逻辑
- 支持净推车时长的计算

作者：Assistant
创建时间：2025-01-11
"""

from typing import Dict, Any, Optional
from abc import ABC, abstractmethod


class BasePushTimeCalculator(ABC):
    """
    推时计算器基类
    
    定义推时计算的基本接口
    """
    
    @abstractmethod
    def calculate_reduction(self, pushed_time: float) -> float:
        """
        根据被推时长计算需要减少的推车时长
        
        Args:
            pushed_time: 被推时长（小时）
            
        Returns:
            需要减少的推车时长（小时）
        """
        pass
    
    @abstractmethod
    def calculate_net_push_time(self, push_time: float, pushed_time: float) -> float:
        """
        计算净推车时长
        
        Args:
            push_time: 推车时长（小时）
            pushed_time: 被推时长（小时）
            
        Returns:
            净推车时长（小时）
        """
        pass
    
    @abstractmethod
    def get_rule_description(self) -> str:
        """
        获取计算规则描述
        
        Returns:
            规则描述文本
        """
        pass


class DefaultPushTimeCalculator(BasePushTimeCalculator):
    """
    默认推时计算器
    
    适用于群聊：926194902 和 191804984
    
    计算规则：
    - 净推车时长 = 推车时长 - 跑时/2（跑时即被推时长）
    - 减免 = 跑时/2
    """
    
    def calculate_reduction(self, pushed_time: float) -> float:
        """
        根据被推时长计算需要减少的推车时长
        
        新规则：按 1/2 折算
        - 减免 = 被推时长 / 2
        
        Args:
            pushed_time: 被推时长（小时）
            
        Returns:
            需要减少的推车时长（小时）
        """
        return pushed_time / 2.0
    
    def calculate_net_push_time(self, push_time: float, pushed_time: float) -> float:
        """
        计算净推车时长
        
        净推车时长 = 推车时长 - 跑时/2（跑时即被推时长）
        
        Args:
            push_time: 推车时长（小时）
            pushed_time: 被推时长（小时）
            
        Returns:
            净推车时长（小时）
        """
        reduction = self.calculate_reduction(pushed_time)
        return push_time - reduction
    
    def get_rule_description(self) -> str:
        """
        获取计算规则描述
        
        Returns:
            规则描述文本
        """
        return """
默认推时计算规则（统一新规则）：
- 净推车时长 = 推车时长 - 跑时/2（跑时即被推时长）
- 减免 = 跑时/2
"""


class Group926194902Calculator(BasePushTimeCalculator):
    """
    群聊926194902专用推时计算器
    
    适用于群聊：926194902
    
    计算规则：
    - 净推车时长 = 推车时长 - （跑时向上取整）/2
    - 减免 = （跑时向上取整）/2
    """
    
    def calculate_reduction(self, pushed_time: float) -> float:
        """
        根据被推时长计算需要减少的推车时长
        
        新规则：跑时向上取整后按 1/2 折算
        - 减免 = ceil(被推时长) / 2
        
        Args:
            pushed_time: 被推时长（小时）
            
        Returns:
            需要减少的推车时长（小时）
        """
        import math
        return math.ceil(pushed_time) / 2.0
    
    def calculate_net_push_time(self, push_time: float, pushed_time: float) -> float:
        """
        计算净推车时长
        
        净推车时长 = 推车时长 - （跑时向上取整）/2
        
        Args:
            push_time: 推车时长（小时）
            pushed_time: 被推时长（小时）
            
        Returns:
            净推车时长（小时）
        """
        reduction = self.calculate_reduction(pushed_time)
        return push_time - reduction
    
    def get_rule_description(self) -> str:
        """
        获取计算规则描述
        
        Returns:
            规则描述文本
        """
        return """
群聊926194902专用推时计算规则：
- 净推车时长 = 推车时长 - （跑时向上取整）/2
- 减免 = （跑时向上取整）/2
- 注意：跑时会先向上取整，然后再除以2
"""


class SimplePushTimeCalculator(BasePushTimeCalculator):
    """
    简单推时计算器
    
    适用于其他群组的简化计算规则
    
    计算规则：
    - 净推车时长 = 推车时长 - 跑时/2（跑时即被推时长）
    """
    
    def calculate_reduction(self, pushed_time: float) -> float:
        """
        根据被推时长计算需要减少的推车时长
        
        简单规则：按 1/2 折算
        - 减免 = 被推时长 / 2
        
        Args:
            pushed_time: 被推时长（小时）
            
        Returns:
            需要减少的推车时长（小时）
        """
        return pushed_time / 2.0
    
    def calculate_net_push_time(self, push_time: float, pushed_time: float) -> float:
        """
        计算净推车时长
        
        净推车时长 = 推车时长 - 跑时/2（跑时即被推时长）
        
        Args:
            push_time: 推车时长（小时）
            pushed_time: 被推时长（小时）
            
        Returns:
            净推车时长（小时）
        """
        return push_time - (pushed_time / 2.0)
    
    def get_rule_description(self) -> str:
        """
        获取计算规则描述
        
        Returns:
            规则描述文本
        """
        return """
简单推时计算规则（统一新规则）：
- 净推车时长 = 推车时长 - 跑时/2（跑时即被推时长）
- 减免 = 跑时/2
"""


class PushTimeCalculatorFactory:
    """
    推时计算器工厂类
    
    负责根据群组ID创建对应的推时计算器实例
    """
    
    # 群组计算规则映射
    GROUP_CALCULATOR_MAPPING = {
        # 群聊926194902使用专用计算器（跑时向上取整）
        "926194902": Group926194902Calculator,
        # 默认规则群组（191804984 和 805295621）
        "191804984": DefaultPushTimeCalculator,
        "805295621": DefaultPushTimeCalculator,
        # 其他群组使用简单规则
        # 可以在这里添加更多群组的特定规则
    }
    
    @classmethod
    def create_calculator(cls, group_id: str) -> BasePushTimeCalculator:
        """
        根据群组ID创建对应的推时计算器
        
        Args:
            group_id: 群组ID
            
        Returns:
            推时计算器实例
        """
        # 确保群组ID为字符串类型，处理可能的类型不匹配问题
        group_id_str = str(group_id)
        calculator_class = cls.GROUP_CALCULATOR_MAPPING.get(group_id_str, SimplePushTimeCalculator)
        return calculator_class()
    
    @classmethod
    def get_supported_groups(cls) -> Dict[str, str]:
        """
        获取支持的群组及其计算规则类型
        
        Returns:
            群组ID到计算器类型的映射
        """
        result = {}
        for group_id, calculator_class in cls.GROUP_CALCULATOR_MAPPING.items():
            result[str(group_id)] = calculator_class.__name__
        return result
    
    @classmethod
    def add_group_rule(cls, group_id: str, calculator_class: type):
        """
        添加新的群组计算规则
        
        Args:
            group_id: 群组ID
            calculator_class: 计算器类
        """
        if not issubclass(calculator_class, BasePushTimeCalculator):
            raise ValueError("计算器类必须继承自BasePushTimeCalculator")
        
        # 确保群组ID为字符串类型
        group_id_str = str(group_id)
        cls.GROUP_CALCULATOR_MAPPING[group_id_str] = calculator_class
    
    @classmethod
    def remove_group_rule(cls, group_id: str):
        """
        移除群组计算器规则
        
        Args:
            group_id: 群组ID
        """
        # 确保群组ID为字符串类型
        group_id_str = str(group_id)
        if group_id_str in cls.GROUP_CALCULATOR_MAPPING:
            del cls.GROUP_CALCULATOR_MAPPING[group_id_str]


class PushTimeCalculatorManager:
    """
    推时计算器管理器
    
    提供统一的推时计算接口，自动根据群组选择合适的计算器
    """
    
    def __init__(self):
        """初始化推时计算器管理器"""
        self._calculators = {}  # 缓存计算器实例
    
    def get_calculator(self, group_id: str) -> BasePushTimeCalculator:
        """
        获取指定群组的推时计算器
        
        Args:
            group_id: 群组ID
            
        Returns:
            推时计算器实例
        """
        # 确保群组ID为字符串类型，处理可能的类型不匹配问题
        group_id_str = str(group_id)
        if group_id_str not in self._calculators:
            self._calculators[group_id_str] = PushTimeCalculatorFactory.create_calculator(group_id_str)
        
        return self._calculators[group_id_str]
    
    def calculate_reduction(self, group_id: str, pushed_time: float) -> float:
        """
        计算指定群组的被推时长减免
        
        Args:
            group_id: 群组ID
            pushed_time: 被推时长（小时）
            
        Returns:
            需要减少的推车时长（小时）
        """
        calculator = self.get_calculator(group_id)
        return calculator.calculate_reduction(pushed_time)
    
    def calculate_net_push_time(self, group_id: str, push_time: float, pushed_time: float) -> float:
        """
        计算指定群组的净推车时长
        
        Args:
            group_id: 群组ID
            push_time: 推车时长（小时）
            pushed_time: 被推时长（小时）
            
        Returns:
            净推车时长（小时）
        """
        calculator = self.get_calculator(group_id)
        return calculator.calculate_net_push_time(push_time, pushed_time)
    
    def get_rule_description(self, group_id: str) -> str:
        """
        获取指定群组的计算规则描述
        
        Args:
            group_id: 群组ID
            
        Returns:
            规则描述文本
        """
        calculator = self.get_calculator(group_id)
        return calculator.get_rule_description()
    
    def get_all_group_rules(self) -> Dict[str, str]:
        """
        获取所有群组的计算规则信息
        
        Returns:
            群组ID到规则描述的映射
        """
        supported_groups = PushTimeCalculatorFactory.get_supported_groups()
        result = {}
        
        for group_id, calculator_type in supported_groups.items():
            calculator = self.get_calculator(group_id)
            result[group_id] = {
                'calculator_type': calculator_type,
                'rule_description': calculator.get_rule_description()
            }
        
        return result
    
    def clear_cache(self, group_id: str = None):
        """
        清理计算器缓存
        
        Args:
            group_id: 指定群组ID，如果为None则清理所有缓存
        """
        if group_id is None:
            self._calculators.clear()
        else:
            # 确保群组ID为字符串类型
            group_id_str = str(group_id)
            if group_id_str in self._calculators:
                del self._calculators[group_id_str]


# 创建全局推时计算器管理器实例
push_time_calculator_manager = PushTimeCalculatorManager()


def get_push_time_calculator(group_id: str) -> BasePushTimeCalculator:
    """
    获取指定群组的推时计算器（便捷函数）
    
    Args:
        group_id: 群组ID
        
    Returns:
        推时计算器实例
    """
    return push_time_calculator_manager.get_calculator(group_id)


def calculate_push_time_reduction(group_id: str, pushed_time: float) -> float:
    """
    计算指定群组的被推时长减免（便捷函数）
    
    Args:
        group_id: 群组ID
        pushed_time: 被推时长（小时）
        
    Returns:
        需要减少的推车时长（小时）
    """
    return push_time_calculator_manager.calculate_reduction(group_id, pushed_time)


def calculate_net_push_time(group_id: str, push_time: float, pushed_time: float) -> float:
    """
    计算指定群组的净推车时长（便捷函数）
    
    Args:
        group_id: 群组ID
        push_time: 推车时长（小时）
        pushed_time: 被推时长（小时）
        
    Returns:
        净推车时长（小时）
    """
    return push_time_calculator_manager.calculate_net_push_time(group_id, push_time, pushed_time)