# gacha_service.py
# 抽卡服务模块，处理模拟抽卡功能

import random
import json
import os
import logging
import pandas as pd
from typing import Dict, List, Any, Optional

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 服务类，用于处理抽卡功能
class GachaService:
    """抽卡服务类，处理模拟抽卡功能
    
    设计说明：
    1. 使用config.json中的配置管理UP角色和概率，而非硬编码
    2. 支持多地区卡池配置（日服/国服）
    3. 卡牌数据与配置分离，便于维护
    """
    
    def __init__(self, data_path: str, config: Dict[str, Any] = None):
        """初始化抽卡服务
        
        Args:
            data_path (str): 卡牌数据文件路径
            config (Dict[str, Any]): 从config.json加载的gacha配置
        """
        self.logger = logger
        self.data_path = data_path
        self.config = config or {}
        
        # 加载数据并初始化索引
        self.data = self.load_data()
        self.cards_by_id = {}
        self.cards_by_star = {}
        self.cards_by_team = {}
        self.cards_by_element = {}

        
        # 初始化索引
        self._initialize_indexes()

    def load_data(self) -> Dict[str, Any]:
        """加载卡牌数据
        
        Returns:
            Dict[str, Any]: 完整的数据结构
        """
        try:
            # 首先尝试加载JSON配置文件（包含元数据、团队信息等）
            json_data = {}
            if os.path.exists(self.data_path):
                with open(self.data_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
            
            # 尝试加载多个CSV卡牌数据文件
            data_dir = os.path.dirname(self.data_path)
            
            csv_files = {
                '2': 'cards_2star.csv',
                '3': 'cards_3star.csv', 
                '4': 'cards_4star.csv'
            }
            
            # 首先尝试加载原始的cards.csv（包含1星卡牌）
            cards_list = []
            cards_by_id = {}
            cards_by_star = {}
            cards_by_team = {}
            cards_by_element = {}
            
            # 加载1星卡牌（从原JSON或单独文件）
            original_csv = os.path.join(data_dir, 'cards.csv')
            if os.path.exists(original_csv):
                try:
                    df = pd.read_csv(original_csv, encoding='utf-8')
                    # 只取1星卡牌
                    one_star_cards = df[df['star'] == 1].to_dict('records')
                    cards_list.extend(one_star_cards)
                except Exception:
                    pass
            
            # 加载各星级卡牌文件
            for star, filename in csv_files.items():
                csv_path = os.path.join(data_dir, filename)
                if os.path.exists(csv_path):
                    try:
                        df = pd.read_csv(csv_path, encoding='utf-8')
                        star_cards = df.to_dict('records')
                        cards_list.extend(star_cards)
                    except Exception:
                        pass
            
            if cards_list:
                # 将CSV数据转换为原有的数据结构
                for card in cards_list:
                    card_id = str(card['id'])
                    cards_by_id[card_id] = card
                    
                    # 按星级分组
                    star = str(card['star'])
                    if star not in cards_by_star:
                        cards_by_star[star] = []
                    cards_by_star[star].append(int(card['id']))
                    
                    # 按团队分组
                    team = card['team']
                    if team not in cards_by_team:
                        cards_by_team[team] = []
                    cards_by_team[team].append(int(card['id']))
                    
                    # 按元素分组（如果有element字段）
                    if 'element' in card:
                        element = card['element']
                        if element not in cards_by_element:
                            cards_by_element[element] = []
                        cards_by_element[element].append(int(card['id']))
                
                # 更新JSON数据结构
                if 'cards' not in json_data:
                    json_data['cards'] = {}
                json_data['cards']['by_id'] = cards_by_id
                json_data['cards']['by_star'] = cards_by_star
                json_data['cards']['by_team'] = cards_by_team
                json_data['cards']['by_element'] = cards_by_element
                json_data['legacy_cards'] = cards_list
                
                pass
            else:
                # 尝试从JSON文件中的cards数据作为备用
                if 'cards' in json_data and json_data['cards']:
                    pass
                else:
                    # 创建默认的卡牌数据以防止系统崩溃
                    default_cards = [
                        {'id': 1, 'name': '初音未来', 'cardname': 'miku', 'star': 2, 'team': 'vs', 'element': 'cute'},
                        {'id': 2, 'name': '镜音铃', 'cardname': 'rin', 'star': 2, 'team': 'vs', 'element': 'pure'},
                        {'id': 3, 'name': '镜音连', 'cardname': 'len', 'star': 2, 'team': 'vs', 'element': 'cool'},
                        {'id': 4, 'name': '初音未来', 'cardname': 'miku', 'star': 3, 'team': 'vs', 'element': 'happy'},
                        {'id': 5, 'name': '初音未来', 'cardname': 'miku', 'star': 4, 'team': 'vs', 'element': 'mysterious'}
                    ]
                    
                    # 构建默认数据结构
                    default_by_id = {}
                    default_by_star = {}
                    default_by_team = {}
                    default_by_element = {}
                    
                    for card in default_cards:
                        card_id = str(card['id'])
                        default_by_id[card_id] = card
                        
                        star = str(card['star'])
                        if star not in default_by_star:
                            default_by_star[star] = []
                        default_by_star[star].append(int(card['id']))
                        
                        team = card['team']
                        if team not in default_by_team:
                            default_by_team[team] = []
                        default_by_team[team].append(int(card['id']))
                        
                        element = card['element']
                        if element not in default_by_element:
                            default_by_element[element] = []
                        default_by_element[element].append(int(card['id']))
                    
                    json_data['cards'] = {
                        'by_id': default_by_id,
                        'by_star': default_by_star,
                        'by_team': default_by_team,
                        'by_element': default_by_element
                    }
                    json_data['legacy_cards'] = default_cards
            
            return json_data
        except Exception:
            return {}
    
    def load_cards(self) -> List[Dict[str, Any]]:
        """兼容方法：加载卡牌数据（旧格式）
        
        Returns:
            List[Dict[str, Any]]: 卡牌列表
        """
        return self.data.get('legacy_cards', [])
    
    def _initialize_indexes(self):
        """初始化卡牌索引"""
        try:
            cards_data = self.data.get('cards', {})
            
            # 如果有新格式的索引数据，直接使用
            if 'by_id' in cards_data:
                self.cards_by_id = cards_data.get('by_id', {})
                
                # 将ID数组转换为卡牌对象数组
                by_star_ids = cards_data.get('by_star', {})
                self.cards_by_star = {}
                for star, card_ids in by_star_ids.items():
                    self.cards_by_star[star] = [self.cards_by_id[str(card_id)] for card_id in card_ids if str(card_id) in self.cards_by_id]
                
                by_team_ids = cards_data.get('by_team', {})
                self.cards_by_team = {}
                for team, card_ids in by_team_ids.items():
                    self.cards_by_team[team] = [self.cards_by_id[str(card_id)] for card_id in card_ids if str(card_id) in self.cards_by_id]
                
                by_element_ids = cards_data.get('by_element', {})
                self.cards_by_element = {}
                for element, card_ids in by_element_ids.items():
                    self.cards_by_element[element] = [self.cards_by_id[str(card_id)] for card_id in card_ids if str(card_id) in self.cards_by_id]
                
                pass
            else:
                # 从legacy_cards构建索引
                self._build_indexes_from_legacy()
                
        except Exception:
            pass
    
    def _build_indexes_from_legacy(self):
        """从旧格式数据构建索引"""
        legacy_cards = self.data.get('legacy_cards', [])
        
        for card in legacy_cards:
            card_id = str(card.get('id', ''))
            star = str(card.get('star', 1))
            team = card.get('team', '未知')
            element = card.get('element', '无')
            
            # 按ID索引
            self.cards_by_id[card_id] = card
            
            # 按星级索引
            if star not in self.cards_by_star:
                self.cards_by_star[star] = []
            self.cards_by_star[star].append(card)
            
            # 按团队索引
            if team not in self.cards_by_team:
                self.cards_by_team[team] = []
            self.cards_by_team[team].append(card)
            
            # 按元素索引
            if element not in self.cards_by_element:
                self.cards_by_element[element] = []
            self.cards_by_element[element].append(card)
    

    

    

    
    def get_card_by_id(self, card_id: str) -> Optional[Dict[str, Any]]:
        """根据ID获取卡牌"""
        return self.cards_by_id.get(str(card_id))
    
    def get_cards_by_star(self, star: int) -> List[Dict[str, Any]]:
        """根据星级获取卡牌列表"""
        # 尝试整数键和字符串键
        return self.cards_by_star.get(star, self.cards_by_star.get(str(star), []))
    
    def get_cards_by_team(self, team: str) -> List[Dict[str, Any]]:
        """根据团队获取卡牌列表"""
        return self.cards_by_team.get(team, [])
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取卡牌统计信息"""
        total_cards = sum(len(cards) for cards in self.cards_by_star.values())
        star_distribution = {star: len(cards) for star, cards in self.cards_by_star.items()}
        team_distribution = {team: len(cards) for team, cards in self.cards_by_team.items()}
        
        return {
            'total_cards': total_cards,
            'star_distribution': star_distribution,
            'team_distribution': team_distribution,
            'data_version': self.data.get('metadata', {}).get('version', '未知')
        }

    def _get_region_config(self, region: str) -> Dict[str, Any]:
        """获取指定地区的配置
        
        Args:
            region (str): 地区代码 ('jp' 或 'cn')
            
        Returns:
            Dict[str, Any]: 地区配置
        """
        return self.config.get('regions', {}).get(region, {})

    def _is_up_character(self, card: Dict[str, Any], region: str) -> bool:
        """检查卡牌是否为指定地区的UP角色
        
        Args:
            card (Dict[str, Any]): 卡牌信息字典，包含cardname字段
            region (str): 地区代码
            
        Returns:
            bool: 是否为UP角色
        """
        region_config = self._get_region_config(region)
        up_characters = region_config.get('up_characters', [])
        card_cardname = card.get('cardname', '')
        
        # 从UP角色字符串中提取卡面名称部分进行匹配
        for up_char in up_characters:
            if '[' in up_char and ']' in up_char:
                # 格式："角色名[卡面名称]"
                start_idx = up_char.find('[') + 1
                end_idx = up_char.find(']')
                up_card_name = up_char[start_idx:end_idx]
                # 使用卡面名称匹配数据库中的cardname字段
                if up_card_name == card_cardname:
                    return True
        return False

    def _draw_single_card(self, region: str = 'jp') -> Dict[str, Any]:
        """
        抽取单张卡牌 - 简化版本
        概率设置：4星3%，3星8.5%，2星88.5%
        4星卡牌有75%概率为UP角色（提高UP角色概率）
            
        Args:
            region (str): 地区代码，默认为'jp'
            
        Returns:
            Dict[str, Any]: 抽到的卡牌
        """
        # 简化的概率设置
        star_probabilities = {
            4: 0.03,    # 4星 3%
            3: 0.085,   # 3星 8.5%
            2: 0.885    # 2星 88.5%
        }
        
        # 首先根据概率决定星级
        rand = random.random()
        selected_star = 2  # 默认2星
        
        if rand < star_probabilities[4]:
            selected_star = 4
        elif rand < star_probabilities[4] + star_probabilities[3]:
            selected_star = 3
        else:
            selected_star = 2
        
        # 获取对应星级的卡牌
        star_cards = self.get_cards_by_star(selected_star)
        
        # 如果没有对应星级的卡牌，尝试获取所有卡牌
        if not star_cards:
            all_cards = []
            for star_level in self.cards_by_star:
                all_cards.extend(self.cards_by_star[star_level])
            
            if not all_cards:
                all_cards = self.data.get('legacy_cards', [])
            
            if not all_cards:
                return {
                    'id': 1,
                    'name': '初音未来',
                    'cardname': 'miku',
                    'star': 1,
                    'team': 'vs',
                    'element': 'cute'
                }
            
            # 从所有卡牌中随机选择
            selected_card = random.choice(all_cards)
        else:
            # 如果是4星，需要考虑UP概率
            if selected_star == 4:
                # 分离UP角色和非UP角色
                up_cards = [card for card in star_cards if self._is_up_character(card, region)]
                non_up_cards = [card for card in star_cards if not self._is_up_character(card, region)]
                
                # 75%概率选择UP角色（提高UP角色概率）
                if up_cards and random.random() < 0.75:
                    selected_card = random.choice(up_cards)
                else:
                    # 选择非UP角色，如果没有非UP角色则从所有4星中选择
                    if non_up_cards:
                        selected_card = random.choice(non_up_cards)
                    else:
                        selected_card = random.choice(star_cards)
            else:
                # 非4星直接随机选择
                selected_card = random.choice(star_cards)
        
        return selected_card

    def draw_card(self, region: str = 'jp') -> List[Dict[str, Any]]:
        """执行10连抽卡
        
        Args:
            region (str): 地区参数，默认为'jp'
            
        Returns:
            List[Dict[str, Any]]: 抽到的10张卡牌列表
        """
        drawn_cards = []
        has_three_star = False
        
        # 前9次独立抽卡
        for i in range(9):
            card = self._draw_single_card(region)
            drawn_cards.append(card)
            # 确保card是字典类型
            if isinstance(card, dict) and card.get('star', 0) >= 3:
                has_three_star = True
        
        # 第10次抽卡，保底机制
        if not has_three_star:
            # 保底机制：确保至少一张3星卡牌
            three_star_cards = self.get_cards_by_star(3) + self.get_cards_by_star(4)
            if three_star_cards:
                card = random.choice(three_star_cards)
            else:
                card = self._draw_single_card(region)
        else:
            # 正常抽卡
            card = self._draw_single_card(region)
        
        drawn_cards.append(card)
        return drawn_cards

    def _is_gacha_related(self, message: str) -> bool:
        """检查消息是否与抽卡相关
        
        Args:
            message (str): 消息内容
            
        Returns:
            bool: 是否与抽卡相关
        """
        return message.startswith('/模拟抽卡') or message.startswith('/jp模拟抽卡') or message.startswith('/cn模拟抽卡')

    def _format_gacha_result(self, drawn_cards: List[Dict[str, Any]], region: str = 'jp') -> str:
        """格式化抽卡结果
        
        Args:
            drawn_cards (List[Dict[str, Any]]): 抽到的卡牌列表
            region (str): 地区参数
            
        Returns:
            str: 格式化的抽卡结果文本
        """
        if not drawn_cards:
            return "抽卡失败，没有获得任何卡牌。"
        
        # 获取地区配置
        region_config = self._get_region_config(region)
        pool_name = region_config.get('pool_name', '未知卡池')
        up_characters = region_config.get('up_characters', [])
        
        result_lines = [f"卡池：{pool_name}"]
        if up_characters:
            result_lines.append(f"本期UP角色：{', '.join(up_characters)}")
        result_lines.append("本次模拟抽卡结果为：")
        
        for card in drawn_cards:
            star = card.get('star', 1)
            name = card.get('name', '未知卡牌')
            cardname = card.get('cardname', '未知卡牌')
            
            # 星级显示
            star_display = "⭐" * star
            
            # 检查是否为UP角色，如果是则添加美化的UP标识
            up_suffix = ""
            if self._is_up_character(card, region):
                up_suffix = "✨[UP角色]✨"
            
            result_lines.append(f"{star_display} {name} [{cardname}]{up_suffix}")
        
        return "\n".join(result_lines)

    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """处理抽卡消息
        
        Args:
            message (str): 消息内容
            user_id (str): 用户ID
            **kwargs: 额外参数
            
        Returns:
            Optional[Dict[str, Any]]: 处理结果，如果不相关则返回None
        """
        try:
            message = message.strip()
            
            # 检查是否是抽卡相关的消息
            if not self._is_gacha_related(message):
                return None
            
            # 获取地区参数
            region = 'jp' if message.startswith('/jp模拟抽卡') else 'cn' if message.startswith('/cn模拟抽卡') else 'jp'
            
            # 抽取卡牌
            drawn_cards = self.draw_card(region)
            
            # 格式化结果并返回字典格式
            result_text = self._format_gacha_result(drawn_cards, region)
            return {
                "content": result_text,
                "image_path": None
            }
            
        except Exception:
            return {
                "content": "……抽卡出问题了。稍后再试吧",
                "image_path": None
            }