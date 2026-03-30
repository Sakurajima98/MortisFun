#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初音未来冲榜小游戏服务

本模块实现了一个模拟初音未来缤纷舞台活动冲榜的小游戏功能。
玩家可以报名参与冲榜挑战，通过5轮随机事件来决定最终排名。

功能特性:
- 玩家报名系统（最多10人，最少5人）
- 自动填充初音未来角色
- 5轮随机事件模拟冲榜过程
- 动态排名计算系统
- 获胜感言系统

作者: Mortisfun Team
创建时间: 2025
版本: 1.0.0
"""

import asyncio
import json
import logging
import random
import time
from typing import Dict, List, Optional, Any
from pathlib import Path

from .base_service import BaseService


class RankingGameService(BaseService):
    """
    冲榜游戏服务类
    
    负责处理初音未来冲榜小游戏的所有逻辑，包括：
    - 玩家报名管理
    - 游戏状态控制
    - 随机事件处理
    - 排名计算
    - 结果展示
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None):
        """
        初始化冲榜游戏服务
        
        Args:
            config (Dict[str, Any]): 配置信息
            data_manager: 数据管理器实例
            text_formatter: 文本格式化器实例
            server: 服务器实例，用于日志格式化
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.logger = logging.getLogger(__name__)
        
        # 游戏状态管理
        self.active_games: Dict[str, Dict] = {}  # 群组ID -> 游戏状态
        self.finished_games: Dict[str, List[Dict]] = {}  # 存储已完成的游戏历史
        self.game_config = config.get('ranking_game', {})
        
        # 状态广播回调
        self.status_callback = None  # 用于自动发送状态更新的回调函数
        
        # 游戏常量（从配置文件读取，如果没有则使用默认值）
        self.MAX_PLAYERS = self.game_config.get('max_players', 10)
        self.MIN_PLAYERS = self.game_config.get('min_players', 5)
        self.TOTAL_ROUNDS = self.game_config.get('total_rounds', 5)
        self.ROUND_DELAY = self.game_config.get('round_delay', 30)  # 秒
        self.REGISTRATION_TIME = self.game_config.get('registration_timeout', 20)  # 秒
        
        # 虚拟歌手名称列表（用于自动生成玩家名）
        self.vocaloid_names = [
            "初音未来", "镜音铃", "镜音连", "巡音流歌", "MEIKO", "KAITO",
            "星乃一歌", "天马咲希", "望月穗波", "日野森志步",
            "花里みのり", "桐谷遥", "桃井愛莉", "日野森雫",
            "小豆沢こはね", "白石杏", "东云彰人", "青柳冬弥",
            "天马司", "鳳えむ", "草薙寧々", "神代類",
            "宵崎奏", "朝比奈まふゆ", "東雲絵名", "暁山瑞希"
        ]
        
        # 初始化数据
        self._load_game_data()
        
        self.log_unified("INFO", "冲榜游戏服务初始化完成", group_id="system", user_id="system")
    
    def set_status_callback(self, callback):
        """
        设置状态广播回调函数
        
        Args:
            callback: 回调函数，接收(group_id, status_message)参数
        """
        self.status_callback = callback
    
    async def _broadcast_status(self, group_id: str, message: str):
        """
        广播游戏状态更新
        
        Args:
            group_id (str): 群组ID
            message (str): 状态消息
        """
        if self.status_callback:
            try:
                await self.status_callback(group_id, message)
            except Exception as e:
                self.log_unified("ERROR", f"状态广播失败: {e}", group_id=group_id)
        else:
            # 如果没有设置回调，使用统一日志记录
            self.log_unified("INFO", message, group_id=group_id, user_id="system")
    
    def _load_game_data(self):
        """
        加载游戏相关数据
        包括随机事件、角色数据、获胜感言等
        """
        try:
            # 加载随机事件数据
            self.random_events = self._load_random_events()
            
            # 加载初音未来角色数据
            self.miku_characters = self._load_miku_characters()
            
            # 加载获胜感言
            self.victory_speeches = self._load_victory_speeches()
            
            # 加载活动名称
            self.activity_name = self.game_config.get('activity_name', '🎵 Colorful Stage! 冲榜挑战赛 🎵')
            
            self.log_unified("INFO", "游戏数据加载完成", group_id="system", user_id="system")
            
        except Exception as e:
            self.log_unified("ERROR", f"加载游戏数据失败: {e}", group_id="system", user_id="system")
            # 使用默认数据
            self._create_default_data()
    
    def _load_random_events(self) -> List[Dict[str, Any]]:
        """
        加载随机事件数据
        
        Returns:
            List[Dict[str, Any]]: 随机事件列表
        """
        events_file = Path(self.data_manager.base_path) / "ranking_events.json"
        
        if events_file.exists():
            try:
                with open(events_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('events', [])
            except Exception as e:
                self.log_unified("ERROR", f"读取随机事件文件失败: {e}", group_id="system", user_id="system")
        
        # 返回默认随机事件
        return self._get_default_random_events()
    
    def _load_miku_characters(self) -> List[Dict[str, str]]:
        """
        加载初音未来角色数据
        
        Returns:
            List[Dict[str, str]]: 角色列表
        """
        # 直接返回默认角色列表，因为角色数据相对固定
        return self._get_default_characters()
    
    def _load_victory_speeches(self) -> Dict[str, List[str]]:
        """
        加载获胜感言数据
        
        Returns:
            Dict[str, List[str]]: 角色获胜感言字典
        """
        speeches_file = Path(self.data_manager.base_path) / "victory_quotes.json"
        
        if speeches_file.exists():
            try:
                with open(speeches_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('quotes', {})
            except Exception as e:
                self.log_unified("ERROR", f"读取获胜感言文件失败: {e}", group_id="system", user_id="system")
        
        # 返回默认获胜感言
        return self._get_default_victory_speeches()
    
    def _create_default_data(self):
        """
        创建默认游戏数据
        """
        self.random_events = self._get_default_random_events()
        self.miku_characters = self._get_default_characters()
        self.victory_speeches = self._get_default_victory_speeches()
        
        # 保存默认数据到文件
        self._save_default_data()
    
    def _get_default_random_events(self) -> List[Dict[str, Any]]:
        """
        获取默认随机事件数据
        
        Returns:
            List[Dict[str, Any]]: 默认随机事件列表
        """
        return [
            {"description": "完美演出！观众为你疯狂！", "effect": "large_boost", "rank_change": -150},
            {"description": "状态极佳，发挥超常！", "effect": "boost", "rank_change": -80},
            {"description": "稳定发挥，保持节奏。", "effect": "normal", "rank_change": -20},
            {"description": "有些紧张，小失误。", "effect": "slight_drop", "rank_change": 30},
            {"description": "设备故障，影响发挥。", "effect": "drop", "rank_change": 80},
            {"description": "严重失误，排名大跌！", "effect": "large_drop", "rank_change": 150},
            {"description": "获得粉丝应援，士气大振！", "effect": "boost", "rank_change": -100},
            {"description": "网络卡顿，错失良机。", "effect": "drop", "rank_change": 60},
            {"description": "灵感爆发，创造奇迹！", "effect": "large_boost", "rank_change": -120},
            {"description": "体力不支，表现下滑。", "effect": "slight_drop", "rank_change": 40}
        ]
    
    def _get_default_characters(self) -> List[Dict[str, str]]:
        """
        获取默认初音未来角色数据
        
        Returns:
            List[Dict[str, str]]: 默认角色列表
        """
        return [
            {"name": "初音未来", "nickname": "miku"},
            {"name": "镜音铃", "nickname": "rin"},
            {"name": "镜音连", "nickname": "len"},
            {"name": "巡音流歌", "nickname": "luka"},
            {"name": "MEIKO", "nickname": "meiko"},
            {"name": "KAITO", "nickname": "kaito"},
            {"name": "星乃一歌", "nickname": "ichika"},
            {"name": "天马咲希", "nickname": "saki"},
            {"name": "望月穗波", "nickname": "honami"},
            {"name": "日野森志步", "nickname": "shiho"}
        ]
    
    def _get_default_victory_speeches(self) -> Dict[str, List[str]]:
        """
        获取默认获胜感言
        
        Returns:
            Dict[str, List[str]]: 默认获胜感言字典
        """
        return {
            "初音未来": [
                "谢谢大家的支持！我会继续努力唱出更好的歌声！",
                "这次的冲榜真的很激烈呢～大家都很厉害！",
                "能获得第一名真是太开心了！让我们一起创造更多美好的回忆吧！"
            ],
            "镜音铃": [
                "哇！我居然拿到第一名了！谢谢大家！",
                "这次的表现超出了我的预期呢～",
                "虽然很累，但是能和大家一起努力真的很开心！"
            ],
            "镜音连": [
                "嗯，这个结果还算不错。",
                "大家都很努力，我只是运气比较好而已。",
                "下次我会更加努力的。"
            ]
        }
    
    def _save_default_data(self):
        """
        保存默认数据到文件
        """
        try:
            # 确保目录存在
            ranking_dir = Path(self.data_manager.base_path) / "ranking_game"
            ranking_dir.mkdir(parents=True, exist_ok=True)
            
            # 保存随机事件
            events_file = ranking_dir / "random_events.json"
            with open(events_file, 'w', encoding='utf-8') as f:
                json.dump(self.random_events, f, ensure_ascii=False, indent=2)
            
            # 保存角色数据
            characters_file = ranking_dir / "miku_characters.json"
            with open(characters_file, 'w', encoding='utf-8') as f:
                json.dump(self.miku_characters, f, ensure_ascii=False, indent=2)
            
            # 保存获胜感言
            speeches_file = ranking_dir / "victory_speeches.json"
            with open(speeches_file, 'w', encoding='utf-8') as f:
                json.dump(self.victory_speeches, f, ensure_ascii=False, indent=2)
            
            self.log_unified("INFO", "默认游戏数据保存完成", group_id="system", user_id="system")
            
        except Exception as e:
            self.log_unified("ERROR", f"保存默认数据失败: {e}", group_id="system", user_id="system")
    
    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        处理消息的同步接口
        
        Args:
            message (str): 用户消息
            user_id (str): 用户ID
            **kwargs: 其他参数
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果
        """
        try:
            message = message.strip()
            
            # 只处理/报名指令
            if not message.startswith('/报名'):
                return None
            
            # 获取群组ID - 从context中获取
            context = kwargs.get('context', {})
            group_id = str(context.get('group_id', 'default'))
            
            # 处理报名逻辑
            result = self._handle_registration_sync(message, user_id, group_id)
            
            return {
                "content": result,
                "image_path": None
            }
            
        except Exception as e:
            group_id = kwargs.get('group_id', '')
            self.log_unified("ERROR", f"处理冲榜游戏消息失败: {e}", group_id=group_id, user_id=user_id)
            return {
                "content": "处理冲榜游戏请求时发生错误，请稍后再试。",
                "image_path": None
            }
    
    async def handle_message(self, message: str, user_id: str, context: Dict[str, Any]) -> Optional[str]:
        """
        处理消息
        
        Args:
            message (str): 用户消息
            user_id (str): 用户ID
            context (Dict[str, Any]): 消息上下文
            
        Returns:
            Optional[str]: 回复消息
        """
        try:
            message = message.strip()
            group_id = context.get('group_id')
            
            # 处理冲榜游戏启动命令（新增简洁命令）
            if message == '/冲榜' or message == '/冲榜游戏' or message == '/冲榜挑战':
                # 自动为用户生成一个虚拟歌手玩家名
                default_name = random.choice(self.vocaloid_names)
                return await self._handle_registration(f"/报名 {default_name}", user_id, group_id)
            
            # 处理报名命令（保持兼容性）
            elif message.startswith('/报名'):
                return await self._handle_registration(message, user_id, group_id)
            
            # 处理游戏状态查询
            elif message == '/冲榜状态' or message == '/游戏状态':
                return self._get_game_status(group_id)
            
            # 处理游戏历史查询
            elif message == '/冲榜历史' or message == '/游戏历史':
                return self._get_game_history(group_id)
            
            # 处理游戏取消
            elif message == '/取消冲榜' or message == '/停止游戏':
                return self._cancel_game(group_id, user_id)
            
            return None
            
        except Exception as e:
            self.logger.error(f"处理消息失败: {e}")
            return "处理请求时发生错误，请稍后再试。"
    
    def _handle_registration_sync(self, message: str, user_id: str, group_id: str) -> str:
        """
        处理玩家报名（同步版本）
        
        Args:
            message (str): 报名消息
            user_id (str): 用户ID
            group_id (str): 群组ID
            
        Returns:
            str: 回复消息
        """
        try:
            # 解析玩家姓名
            parts = message.split()
            if len(parts) < 2:
                return "请使用正确的格式：/报名 玩家姓名"
            
            player_name = ' '.join(parts[1:])
            
            # 检查群组是否已有进行中的游戏
            if group_id in self.active_games:
                game = self.active_games[group_id]
                if game['status'] == 'running':
                    return "当前已有冲榜游戏正在进行中，请等待结束后再开始新游戏。"
                elif game['status'] == 'registering':
                    # 检查是否已报名
                    for player in game['players']:
                        if player['user_id'] == user_id:
                            return f"你已经报名过了！当前玩家姓名：{player['name']}"
                    
                    # 检查人数限制
                    if len(game['players']) >= self.MAX_PLAYERS:
                        return "报名人数已满（10人），无法继续报名。"
                    
                    # 生成唯一的初始排名
                    initial_rank = self._generate_unique_initial_rank(game['players'])
                    
                    # 添加玩家
                    game['players'].append({
                        'user_id': user_id,
                        'name': player_name,
                        'rank': initial_rank,
                        'is_npc': False
                    })
                    
                    # 计算剩余时间
                    elapsed_time = time.time() - game['start_time']
                    remaining_time = max(0, self.REGISTRATION_TIME - int(elapsed_time))
                    
                    return f"报名成功！玩家 {player_name} 已加入冲榜挑战。\n当前报名人数：{len(game['players'])}/{self.MAX_PLAYERS}\n活动将在 {remaining_time} 秒后开始"
            
            # 创建新游戏
            game = {
                'status': 'registering',
                'event_name': self.activity_name,
                'group_id': group_id,  # 添加群组ID字段
                'players': [{
                    'user_id': user_id,
                    'name': player_name,
                    'rank': random.randint(1990, 2000),
                    'is_npc': False
                }],
                'round': 0,
                'start_time': time.time(),
                'creator': user_id
            }
            
            self.active_games[group_id] = game
            
            # 启动报名倒计时（异步）
            asyncio.create_task(self._registration_countdown(group_id))
            
            return f"🎵 {self.activity_name} 冲榜挑战开始报名！\n\n" \
                   f"玩家 {player_name} 率先报名！\n" \
                   f"当前人数：1/{self.MAX_PLAYERS}\n" \
                   f"活动将在 {self.REGISTRATION_TIME} 秒后开始\n\n" \
                   f"使用 /报名 玩家姓名 来参与挑战！"
            
        except Exception as e:
            # 获取当前时间戳
            from datetime import datetime
            self.log_unified("ERROR", f"处理报名失败: {e}", group_id=group_id, user_id=user_id)
            return "报名失败，请稍后再试。"
    
    async def _handle_registration(self, message: str, user_id: str, group_id: str) -> str:
        """
        处理玩家报名
        
        Args:
            message (str): 报名消息
            user_id (str): 用户ID
            group_id (str): 群组ID
            
        Returns:
            str: 回复消息
        """
        try:
            # 解析玩家姓名
            parts = message.split()
            if len(parts) < 2:
                return "请使用正确的格式：/报名 玩家姓名"
            
            player_name = ' '.join(parts[1:])
            
            # 检查群组是否已有进行中的游戏
            if group_id in self.active_games:
                game = self.active_games[group_id]
                if game['status'] == 'running':
                    return "当前已有冲榜游戏正在进行中，请等待结束后再开始新游戏。"
                elif game['status'] == 'registering':
                    # 检查是否已报名
                    for player in game['players']:
                        if player['user_id'] == user_id:
                            return f"你已经报名过了！当前玩家姓名：{player['name']}"
                    
                    # 检查人数限制
                    if len(game['players']) >= self.MAX_PLAYERS:
                        return "报名人数已满（10人），无法继续报名。"
                    
                    # 生成唯一的初始排名
                    initial_rank = self._generate_unique_initial_rank(game['players'])
                    
                    # 添加玩家
                    game['players'].append({
                        'user_id': user_id,
                        'name': player_name,
                        'rank': initial_rank,
                        'is_npc': False
                    })
                    
                    return f"报名成功！玩家 {player_name} 已加入冲榜挑战。\n当前报名人数：{len(game['players'])}/{self.MAX_PLAYERS}"
            
            # 创建新游戏
            game = {
                'status': 'registering',
                'event_name': self.activity_name,
                'group_id': group_id,  # 添加群组ID字段
                'players': [{
                    'user_id': user_id,
                    'name': player_name,
                    'rank': random.randint(1990, 2000),
                    'is_npc': False
                }],
                'round': 0,
                'start_time': time.time(),
                'creator': user_id
            }
            
            self.active_games[group_id] = game
            
            # 启动报名倒计时
            asyncio.create_task(self._registration_countdown(group_id))
            
            return f"🎵 {self.activity_name} 冲榜挑战开始报名！\n\n" \
                   f"玩家 {player_name} 率先报名！\n" \
                   f"报名时间：{self.REGISTRATION_TIME}秒\n" \
                   f"当前人数：1/{self.MAX_PLAYERS}\n" \
                   f"最少需要{self.MIN_PLAYERS}人开始游戏\n\n" \
                   f"使用 /报名 玩家姓名 来参与挑战！"
            
        except Exception as e:
            self.logger.error(f"处理报名失败: {e}")
    
    def _generate_unique_initial_rank(self, existing_players: List[Dict[str, Any]]) -> int:
        """
        生成唯一的初始排名（1990-2000之间）
        
        Args:
            existing_players (List[Dict[str, Any]]): 已有玩家列表
            
        Returns:
            int: 唯一的初始排名
        """
        used_ranks = {player['rank'] for player in existing_players}
        available_ranks = [rank for rank in range(1990, 2001) if rank not in used_ranks]
        
        if not available_ranks:
            # 如果1990-2000都被占用，扩展到1980-2010范围
            extended_range = list(range(1980, 1990)) + list(range(2001, 2011))
            available_ranks = [rank for rank in extended_range if rank not in used_ranks]
        
        return random.choice(available_ranks) if available_ranks else random.randint(1980, 2010)
    
    async def _registration_countdown(self, group_id: str):
        """
        报名倒计时处理
        
        Args:
            group_id (str): 群组ID
        """
        try:
            await asyncio.sleep(self.REGISTRATION_TIME)
            
            if group_id not in self.active_games:
                return
            
            game = self.active_games[group_id]
            if game['status'] != 'registering':
                return
            
            # 检查报名人数
            player_count = len(game['players'])
            
            if player_count < self.MIN_PLAYERS:
                # 用NPC填充到5人
                self._fill_with_npcs(game)
            
            # 开始游戏
            await self._start_game(group_id)
            
        except Exception as e:
            self.logger.error(f"报名倒计时处理失败: {e}")
    
    def _fill_with_npcs(self, game: Dict[str, Any]):
        """
        用NPC角色填充到最少人数
        
        Args:
            game (Dict[str, Any]): 游戏状态
        """
        try:
            current_count = len(game['players'])
            needed_count = self.MIN_PLAYERS - current_count
            
            # 随机选择角色填充
            available_characters = self.miku_characters.copy()
            random.shuffle(available_characters)
            
            for i in range(needed_count):
                if i < len(available_characters):
                    char = available_characters[i]
                    # 为NPC生成唯一的初始排名
                    initial_rank = self._generate_unique_initial_rank(game['players'])
                    game['players'].append({
                        'user_id': f'npc_{char["nickname"]}',
                        'name': char['name'],
                        'rank': initial_rank,
                        'is_npc': True
                    })
            
            # 为所有玩家预设最终排名
            self._preset_final_rankings(game)
            
            self.logger.info(f"已用{needed_count}个NPC角色填充游戏，并预设最终排名")
            
        except Exception as e:
            self.logger.error(f"NPC填充失败: {e}")
    
    def _preset_final_rankings(self, game: Dict[str, Any]):
        """
        预设所有玩家的最终排名
        确保至少1人进入前50名，其余玩家在500名以上（即排名数字小于500）
        
        Args:
            game (Dict[str, Any]): 游戏状态
        """
        try:
            players = game['players']
            num_players = len(players)
            
            # 随机选择1-2名玩家进入前50名
            num_top50 = random.randint(1, min(2, num_players))
            
            # 为所有玩家分配最终排名
            final_rankings = []
            
            # 前50名的排名分配
            for i in range(num_top50):
                rand = random.random()
                if rand < 0.03:  # 3%概率进入前3名
                    rank = random.randint(1, 3)
                elif rand < 0.13:  # 10%概率进入前10名
                    rank = random.randint(4, 10)
                elif rand < 0.33:  # 20%概率进入前20名
                    rank = random.randint(11, 20)
                else:  # 其余在20-50名之间
                    rank = random.randint(21, 50)
                final_rankings.append(rank)
            
            # 其余玩家在51-499名之间（500名以上，即排名数字小于500）
            for i in range(num_players - num_top50):
                rank = random.randint(51, 499)
                final_rankings.append(rank)
            
            # 确保排名唯一
            final_rankings = list(set(final_rankings))
            while len(final_rankings) < num_players:
                rank = random.randint(51, 499)
                if rank not in final_rankings:
                    final_rankings.append(rank)
            
            # 随机分配给玩家
            random.shuffle(final_rankings)
            for i, player in enumerate(players):
                player['target_rank'] = final_rankings[i]
                
            # 获取当前时间戳
            from datetime import datetime
            self.log_unified("INFO", f"已为{num_players}名玩家预设最终排名，{num_top50}人进入前50名", group_id="system", user_id="system")
            
        except Exception as e:
            self.log_unified("ERROR", f"预设最终排名失败: {e}", group_id="system", user_id="system")
    
    async def _start_game(self, group_id: str):
        """
        开始游戏
        
        Args:
            group_id (str): 群组ID
        """
        try:
            if group_id not in self.active_games:
                return
            
            game = self.active_games[group_id]
            game['status'] = 'running'
            game['round'] = 1
            
            # 如果还没有预设最终排名，现在预设
            if not any('target_rank' in player for player in game['players']):
                self._preset_final_rankings(game)
            
            # 游戏开始，直接进入第一轮，不发送额外消息
            self.log_unified("INFO", f"游戏开始: {game['event_name']}", group_id=group_id)
            
            # 广播游戏开始状态
            start_message = f"🎮 {game['event_name']} 正式开始！\n\n参赛玩家：\n"
            for i, player in enumerate(game['players'], 1):
                npc_tag = "(NPC)" if player['is_npc'] else ""
                start_message += f"{i}. {player['name']}{npc_tag}\n"
            start_message += f"\n游戏将进行 {self.TOTAL_ROUNDS} 轮，每轮间隔 {self.ROUND_DELAY} 秒\n准备开始第一轮..."
            
            await self._broadcast_status(game['group_id'], start_message)
            
            # 开始第一轮
            await asyncio.sleep(2)
            await self._run_round(group_id)
            
        except Exception as e:
            # 获取当前时间戳
            self.log_unified("ERROR", f"开始游戏失败: {e}", group_id=group_id)
    
    async def _run_round(self, group_id: str):
        """
        执行游戏回合
        
        Args:
            group_id (str): 群组ID
        """
        try:
            if group_id not in self.active_games:
                return
            
            game = self.active_games[group_id]
            current_round = game['round']
            
            if current_round > self.TOTAL_ROUNDS:
                await self._end_game(group_id)
                return
            
            # 为每个玩家生成随机事件
            round_results = []
            for player in game['players']:
                old_rank = player['rank']
                
                # 计算新排名
                new_rank = self._calculate_new_rank(player, {}, current_round)
                
                # 第5轮根据排名变化选择合适的事件
                if current_round == 5:
                    event = self._select_event_by_rank_change(old_rank, new_rank)
                else:
                    event = random.choice(self.random_events)
                
                player['rank'] = new_rank
                
                round_results.append({
                    'player': player,
                    'event': event,
                    'old_rank': old_rank,
                    'new_rank': new_rank
                })
            
            # 确保排名唯一性
            self._ensure_unique_ranks(game['players'])
            
            # 更新 round_results 中的 new_rank 值以反映唯一性调整后的排名
            for result in round_results:
                result['new_rank'] = result['player']['rank']
            
            # 排序玩家（按排名）
            game['players'].sort(key=lambda x: x['rank'])
            
            # 生成回合结果消息
            message = self._generate_round_message(game, current_round, round_results)
            
            # 存储回合结果到游戏状态中
            if 'round_results' not in game:
                game['round_results'] = []
            game['round_results'].append({
                'round': current_round,
                'message': message,
                'results': round_results,
                'timestamp': time.time()
            })
            
            # 记录消息（实际应用中需要发送到群组）
            self.log_unified("INFO", f"第{current_round}轮结果: {message}", group_id=group_id)
            
            # 自动广播回合结果
            await self._broadcast_status(game['group_id'], message)
            
            # 准备下一轮
            game['round'] += 1
            
            if current_round < self.TOTAL_ROUNDS:
                # 等待后进行下一轮
                await asyncio.sleep(self.ROUND_DELAY)
                await self._run_round(group_id)
            else:
                # 游戏结束
                await self._end_game(group_id)
            
        except Exception as e:
            self.log_unified("ERROR", f"执行回合失败: {e}", group_id=group_id)
    
    def _calculate_new_rank(self, player: Dict[str, Any], event: Dict[str, Any], round_num: int) -> int:
        """
        基于目标追踪算法计算新排名
        前两轮：从2000名快速上升到100-300名
        第3-4轮：在100-300名之间随机变动
        第5轮：直接到达最终目标排名
        
        Args:
            player (Dict[str, Any]): 玩家信息
            event (Dict[str, Any]): 随机事件
            round_num (int): 当前回合数
            
        Returns:
            int: 新排名
        """
        try:
            current_rank = player['rank']
            target_rank = player.get('target_rank', current_rank)
            
            # 第5轮直接到达最终排名
            if round_num == 5:
                return target_rank
            
            # 前两轮：从2000名快速上升到100-300名
            if round_num <= 2:
                # 目标区间：100-300名
                target_zone_min = 100
                target_zone_max = 300
                
                if current_rank > target_zone_max:
                    # 需要大幅提升到目标区间
                    if round_num == 1:
                        # 第1轮：从2000名左右快速提升到500-800名
                        improvement = random.randint(1200, 1500)
                        new_rank = current_rank - improvement
                        new_rank = max(500, min(new_rank, 800))
                    else:
                        # 第2轮：从500-800名提升到100-300名
                        improvement = random.randint(200, 500)
                        new_rank = current_rank - improvement
                        new_rank = max(target_zone_min, min(new_rank, target_zone_max))
                else:
                    # 已经在合理范围内，小幅调整
                    change = random.randint(-50, 50)
                    new_rank = current_rank + change
                    new_rank = max(target_zone_min, min(new_rank, target_zone_max))
            
            # 第3-4轮：在100-300名之间随机变动
            elif round_num <= 4:
                target_zone_min = 100
                target_zone_max = 300
                
                # 在目标区间内随机变动
                change = random.randint(-80, 80)
                new_rank = current_rank + change
                
                # 确保在目标区间内
                new_rank = max(target_zone_min, min(new_rank, target_zone_max))
            
            return new_rank
            
        except Exception as e:
            self.logger.error(f"计算新排名失败: {e}")
            return player['rank']
    
    def _select_event_by_rank_change(self, old_rank: int, new_rank: int) -> Dict[str, Any]:
        """
        根据排名变化选择合适的事件
        
        Args:
            old_rank (int): 旧排名
            new_rank (int): 新排名
            
        Returns:
            Dict[str, Any]: 选择的事件
        """
        try:
            rank_change = old_rank - new_rank  # 正数表示上升，负数表示下降
            
            # 根据排名变化幅度选择事件类型
            if rank_change >= 200:  # 大幅上升
                suitable_events = [
                    {"description": "在演唱会上完美演出，获得大量粉丝支持！", "rank_change": 0},
                    {"description": "发布的新歌在各大平台爆红，人气飙升！", "rank_change": 0},
                    {"description": "参加重要音乐节目，表现惊艳全场！", "rank_change": 0},
                    {"description": "与知名艺人合作，知名度大幅提升！", "rank_change": 0}
                ]
            elif rank_change >= 50:  # 中等上升
                suitable_events = [
                    {"description": "在街头演出中表现出色，吸引了不少观众！", "rank_change": 0},
                    {"description": "新发布的歌曲获得好评，粉丝数量稳步增长！", "rank_change": 0},
                    {"description": "参加小型音乐活动，获得积极反响！", "rank_change": 0},
                    {"description": "通过社交媒体互动，增加了不少人气！", "rank_change": 0}
                ]
            elif rank_change >= 10:  # 小幅上升
                suitable_events = [
                    {"description": "日常练习有所进步，实力稳步提升！", "rank_change": 0},
                    {"description": "发布了新的练习视频，获得粉丝支持！", "rank_change": 0},
                    {"description": "参加小规模演出，表现不错！", "rank_change": 0}
                ]
            elif rank_change >= -10:  # 排名基本不变
                suitable_events = [
                    {"description": "继续努力练习，保持当前状态！", "rank_change": 0},
                    {"description": "与其他成员一起训练，相互鼓励！", "rank_change": 0},
                    {"description": "专注于提升技能，为下次突破做准备！", "rank_change": 0}
                ]
            elif rank_change >= -50:  # 小幅下降
                suitable_events = [
                    {"description": "遇到一些小挫折，但很快调整状态！", "rank_change": 0},
                    {"description": "在竞争中暂时落后，但没有放弃！", "rank_change": 0},
                    {"description": "面临挑战，正在努力克服困难！", "rank_change": 0}
                ]
            else:  # 大幅下降
                suitable_events = [
                    {"description": "遭遇重大挫折，但决心重新振作！", "rank_change": 0},
                    {"description": "面临严峻挑战，需要更加努力！", "rank_change": 0},
                    {"description": "经历低谷期，但相信会重新崛起！", "rank_change": 0}
                ]
            
            return random.choice(suitable_events)
            
        except Exception as e:
            self.log_unified("ERROR", f"选择事件失败: {e}", group_id="system", user_id="system")
            return random.choice(self.random_events)
    
    def _ensure_unique_ranks(self, players: List[Dict[str, Any]]):
        """
        确保所有玩家排名唯一
        
        Args:
            players (List[Dict[str, Any]]): 玩家列表
        """
        try:
            # 使用集合来跟踪已使用的排名
            used_ranks = set()
            
            # 处理每个玩家的排名
            for player in players:
                original_rank = player['rank']
                current_rank = original_rank
                
                # 如果排名已被使用，寻找最近的可用排名
                while current_rank in used_ranks:
                    # 随机选择向上或向下调整
                    if random.choice([True, False]):
                        current_rank += 1
                    else:
                        current_rank = max(1, current_rank - 1)
                    
                    # 防止无限循环，如果调整幅度过大则重新随机生成
                    if abs(current_rank - original_rank) > 50:
                        current_rank = random.randint(1, 2000)
                        while current_rank in used_ranks:
                            current_rank = random.randint(1, 2000)
                        break
                
                # 更新玩家排名并记录已使用的排名
                player['rank'] = current_rank
                used_ranks.add(current_rank)
            
        except Exception as e:
            self.logger.error(f"确保排名唯一性失败: {e}")
    
    def _generate_round_message(self, game: Dict[str, Any], round_num: int, results: List[Dict[str, Any]]) -> str:
        """
        生成回合结果消息
        
        Args:
            game (Dict[str, Any]): 游戏状态
            round_num (int): 回合数
            results (List[Dict[str, Any]]): 回合结果
            
        Returns:
            str: 格式化的消息
        """
        try:
            message = f"📊 第{round_num}轮冲榜结果\n\n"
            
            # 显示每个玩家的事件和排名变化
            for result in results:
                player = result['player']
                event = result['event']
                old_rank = result['old_rank']
                new_rank = result['new_rank']
                
                npc_tag = "(NPC)" if player['is_npc'] else ""
                message += f"{player['name']}{npc_tag}:\n"
                message += f"  {event['description']}\n"
                
                # 第一轮不显示排名变化，直接显示当前排名
                if round_num == 1:
                    message += f"  当前排名：第{new_rank}名\n\n"
                else:
                    rank_change = old_rank - new_rank
                    if rank_change > 0:
                        change_text = f"↗️ 上升{rank_change}名"
                    elif rank_change < 0:
                        change_text = f"↘️ 下降{abs(rank_change)}名"
                    else:
                        change_text = "➡️ 排名不变"
                    message += f"  排名：{old_rank} → {new_rank} ({change_text})\n\n"
            
            return message
            
        except Exception as e:
            self.log_unified("ERROR", f"生成回合消息失败: {e}", group_id=group_id)
            return f"第{round_num}轮结果生成失败"
    
    async def _end_game(self, group_id: str):
        """
        结束游戏
        
        Args:
            group_id (str): 群组ID
        """
        try:
            if group_id not in self.active_games:
                return
            
            game = self.active_games[group_id]
            
            # 不再调用最终排名规则，因为第5轮已经设置了正确的排名
            # self._apply_final_ranking_rules(game)
            
            # 生成最终结果消息
            message = self._generate_final_message(game)
            
            # 标记游戏为已完成
            game['status'] = 'finished'
            game['end_time'] = time.time()
            game['final_message'] = message
            
            # 保存到历史记录
            if group_id not in self.finished_games:
                self.finished_games[group_id] = []
            self.finished_games[group_id].append(game.copy())
            
            # 只保留最近3场游戏的历史记录
            if len(self.finished_games[group_id]) > 3:
                self.finished_games[group_id] = self.finished_games[group_id][-3:]
            
            # 记录消息（实际应用中需要发送到群组）
            self.log_unified("INFO", f"游戏结束消息: {message}", group_id=group_id)
            
            # 自动广播游戏结束和最终结果
            await self._broadcast_status(game['group_id'], message)
            
            # 清理活跃游戏状态
            del self.active_games[group_id]
            
        except Exception as e:
            self.log_unified("ERROR", f"结束游戏失败: {e}", group_id=group_id)
    
    def _apply_final_ranking_rules(self, game: Dict[str, Any]):
        """
        应用最终排名规则 - 将玩家排名调整到预设的目标排名附近
        确保最终结果与预设目标基本一致
        
        Args:
            game (Dict[str, Any]): 游戏状态
        """
        try:
            players = game['players']
            assigned_ranks = set()
            
            # 为每个玩家分配最终排名（基于预设目标，但加入少量随机性）
            for player in players:
                target_rank = player.get('target_rank', player['rank'])
                
                # 在目标排名附近添加少量随机性（±10名）
                final_rank = target_rank + random.randint(-10, 10)
                
                # 确保排名在合理范围内
                final_rank = max(1, min(final_rank, 1000))
                
                # 确保排名唯一
                attempts = 0
                while final_rank in assigned_ranks and attempts < 50:
                    final_rank = target_rank + random.randint(-20, 20)
                    final_rank = max(1, min(final_rank, 1000))
                    attempts += 1
                
                # 如果仍然冲突，寻找最近的可用排名
                if final_rank in assigned_ranks:
                    for offset in range(1, 100):
                        for direction in [-1, 1]:
                            candidate = final_rank + (offset * direction)
                            if candidate >= 1 and candidate <= 1000 and candidate not in assigned_ranks:
                                final_rank = candidate
                                break
                        if final_rank not in assigned_ranks:
                            break
                
                player['rank'] = final_rank
                assigned_ranks.add(final_rank)
            
            # 最终排序
            players.sort(key=lambda x: x['rank'])
            
            self.logger.info("已应用最终排名规则，玩家排名已调整到预设目标附近")
            
        except Exception as e:
            self.logger.error(f"应用最终排名规则失败: {e}")
    
    def _generate_final_message(self, game: Dict[str, Any]) -> str:
        """
        生成最终结果消息
        
        Args:
            game (Dict[str, Any]): 游戏状态
            
        Returns:
            str: 最终结果消息
        """
        try:
            message = f"🎊 {game['event_name']} 冲榜挑战结束！\n\n"
            message += "🏆 最终排名：\n"
            
            sorted_players = sorted(game['players'], key=lambda x: x['rank'])
            winner = sorted_players[0]
            
            for i, player in enumerate(sorted_players, 1):
                npc_tag = "(NPC)" if player['is_npc'] else ""
                if i == 1:
                    message += f"👑 {player['name']}{npc_tag} - 第{player['rank']}名 (冠军!)\n"
                elif i <= 3:
                    message += f"🥉 {player['name']}{npc_tag} - 第{player['rank']}名\n"
                else:
                    message += f"{i}. {player['name']}{npc_tag} - 第{player['rank']}名\n"
            
            # 如果获胜者是NPC，添加获胜感言
            if winner['is_npc']:
                character_name = winner['name']
                if character_name in self.victory_speeches:
                    speech = random.choice(self.victory_speeches[character_name])
                else:
                    # 如果没有找到对应角色的感言，使用通用感言
                    default_speeches = [
                        "谢谢大家的支持！这次的胜利属于所有喜爱音乐的人！",
                        "音乐的力量真是不可思议呢～下次也要一起加油！",
                        "能和大家一起参与这样的活动真是太开心了！"
                    ]
                    speech = random.choice(default_speeches)
                message += f"\n💬 {winner['name']}的获胜感言：\n\"{speech}\""
            
            message += "\n\n感谢大家参与本次冲榜挑战！"
            
            return message
            
        except Exception as e:
            self.logger.error(f"生成最终消息失败: {e}")
            return "游戏结束，结果生成失败"
    
    def _get_game_status(self, group_id: str) -> str:
        """
        获取游戏状态
        
        Args:
            group_id (str): 群组ID
            
        Returns:
            str: 状态消息
        """
        try:
            if group_id not in self.active_games:
                return "当前没有进行中的冲榜游戏。\n使用 /报名 玩家姓名 开始新的挑战！"
            
            game = self.active_games[group_id]
            
            if game['status'] == 'registering':
                message = f"📝 {game['event_name']} 报名中\n\n"
                message += f"当前报名人数：{len(game['players'])}/{self.MAX_PLAYERS}\n\n"
                message += "已报名玩家：\n"
                for i, player in enumerate(game['players'], 1):
                    message += f"{i}. {player['name']}\n"
                
                return message
            
            elif game['status'] == 'running':
                message = f"🎮 {game['event_name']} 进行中\n\n"
                
                # 显示最新一轮的结果（如果有）
                if 'round_results' in game and game['round_results']:
                    latest_round = game['round_results'][-1]
                    completed_rounds = len(game['round_results'])
                    message += f"当前回合：{completed_rounds}/{self.TOTAL_ROUNDS}\n\n"
                    message += f"📊 最新回合结果（第{latest_round['round']}轮）：\n"
                    message += latest_round['message'] + "\n"
                else:
                    # 游戏刚开始，等待第一轮结果
                    message += f"当前回合：0/{self.TOTAL_ROUNDS}\n\n"
                    message += "游戏进行中，等待第一轮结果...\n"
                
                return message
            
            return "游戏状态异常"
            
        except Exception as e:
            self.logger.error(f"获取游戏状态失败: {e}")
            return "获取游戏状态失败"
    
    def _get_game_history(self, group_id: str) -> str:
        """
        获取游戏历史记录
        
        Args:
            group_id (str): 群组ID
            
        Returns:
            str: 游戏历史消息
        """
        try:
            # 首先检查是否有进行中的游戏
            if group_id in self.active_games:
                game = self.active_games[group_id]
                
                if game['status'] == 'registering':
                    return "游戏还在报名阶段，暂无历史记录。"
                
                if 'round_results' not in game or not game['round_results']:
                    return "暂无游戏历史记录。"
                
                message = f"📚 {game['event_name']} 完整历史记录\n\n"
                
                # 显示所有轮次的详细结果
                for round_result in game['round_results']:
                    message += f"=== 第{round_result['round']}轮 ===\n"
                    message += round_result['message'] + "\n"
                    message += "-" * 30 + "\n\n"
                
                return message
            
            # 检查是否有已完成的游戏历史
            if group_id in self.finished_games and self.finished_games[group_id]:
                latest_game = self.finished_games[group_id][-1]  # 获取最新的已完成游戏
                
                message = f"📚 {latest_game['event_name']} 完整历史记录\n\n"
                
                # 显示所有轮次的详细结果
                if 'round_results' in latest_game:
                    for round_result in latest_game['round_results']:
                        message += f"=== 第{round_result['round']}轮 ===\n"
                        message += round_result['message'] + "\n"
                        message += "-" * 30 + "\n\n"
                
                # 显示最终总结
                message += "🎊 最终总结\n"
                if 'final_message' in latest_game:
                    message += latest_game['final_message'] + "\n"
                else:
                    sorted_players = sorted(latest_game['players'], key=lambda x: x['rank'])
                    for i, player in enumerate(sorted_players, 1):
                        npc_tag = "(NPC)" if player['is_npc'] else ""
                        message += f"{i}. {player['name']}{npc_tag} - 第{player['rank']}名\n"
                
                return message
            
            return "暂无游戏历史记录。使用 /报名 玩家姓名 开始新的挑战！"
            
        except Exception as e:
            self.log_unified("ERROR", f"获取游戏历史失败: {e}", group_id=group_id)
            return "获取游戏历史记录失败"
    
    def _cancel_game(self, group_id: str, user_id: str) -> str:
        """
        取消游戏
        
        Args:
            group_id (str): 群组ID
            user_id (str): 用户ID
            
        Returns:
            str: 取消结果消息
        """
        try:
            if group_id not in self.active_games:
                return "当前没有进行中的冲榜游戏。"
            
            game = self.active_games[group_id]
            
            # 只有游戏创建者可以取消
            if game['creator'] != user_id:
                return "只有游戏发起者可以取消游戏。"
            
            # 删除游戏
            del self.active_games[group_id]
            
            return f"❌ {game['event_name']} 冲榜挑战已被取消。"
            
        except Exception as e:
            self.logger.error(f"取消游戏失败: {e}")
            return "取消游戏失败"
    
    def get_help_text(self) -> Dict[str, Any]:
        """
        获取帮助文本
        
        Returns:
            Dict[str, Any]: 帮助信息字典
        """
        return {
            "title": "🎵 Project SEKAI 冲榜大作战",
            "description": "模拟Project SEKAI活动冲榜的多人竞技小游戏",
            "commands": [
                "/冲榜 - 快速开始冲榜挑战（自动生成玩家名）",
                "/冲榜游戏 - 开始冲榜挑战",
                "/报名 玩家姓名 - 自定义玩家名报名参与",
                "/游戏状态 - 查看当前游戏状态",
                "/游戏历史 - 查看游戏完整历史记录",
                "/停止游戏 - 取消当前游戏（仅发起者）"
            ],
            "rules": [
                "最多10人参与，最少5人开始",
                "报名时间15秒，不足5人自动填充虚拟歌手NPC",
                "共5轮随机事件，影响排名变化",
                "最终排名越低越好，目标冲进前1000名",
                "获胜的虚拟歌手会发表个性化感言"
            ]
        }