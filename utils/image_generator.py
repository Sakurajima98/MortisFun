#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mortisfun项目 - 图片生成工具

本模块负责生成各种图片，包括车队甘特图、统计图表等。
主要功能：
- 车队时间甘特图生成
- 图片样式配置和主题管理
- 文字渲染和布局优化

作者: Trae Builder
创建时间: 2024
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
from PIL import Image, ImageDraw, ImageFont
import json

class ImageGenerator:
    """
    图片生成器类
    
    负责生成各种类型的图片，包括甘特图、统计图表等。
    支持自定义主题、字体和布局配置。
    """
    
    def __init__(self, config_path: str = None):
        """
        初始化图片生成器
        
        Args:
            config_path: 配置文件路径
        """
        self.logger = logging.getLogger(__name__)
        
        # 简洁清晰配置 - 解决乱码和视觉混乱问题
        self.config = {
            'width': 1200,
            'height': 600,
            'margin': 40,
            'header_height': 80,
            'row_height': 45,
            'time_column_width': 100,
            'colors': {
                'background': '#FFFFFF',  # 纯白背景
                'header_bg': '#4A90E2',   # 清爽蓝色
                'header_text': '#FFFFFF',
                'grid_line': '#E5E5E5',   # 浅灰网格
                'captain_bar': '#FF6B6B', # 红色队长条
                'member_bar': '#4ECDC4',  # 青色成员条
                'push_bar': '#45B7D1',    # 蓝色推车条
                'run_bar': '#96CEB4',     # 绿色共跑条
                'paotui_bar': '#8A7CF5',  # 靛紫跑推条
                'substitute_push_bar': '#FFB347',  # 橙色替补推车条
                'substitute_run_bar': '#DDA0DD',   # 紫色替补共跑条
                'substitute_paotui_bar': '#7E6BC4',# 深紫替补跑推条
                'text_primary': '#333333', # 深灰文字
                'text_secondary': '#666666', # 中灰文字
                'time_bg': '#F8F9FA',     # 浅灰时间背景
                'border': '#DDDDDD',      # 浅灰边框
                'time_axis_bg': '#F0F0F0' # 时间轴背景
            },
            'fonts': {
                'title': 20,
                'header': 14,
                'content': 12,
                'small': 10
            },
            'effects': {
                'shadow_offset': 0,       # 取消阴影
                'border_width': 1,        # 细边框
                'corner_radius': 0,       # 取消圆角
                'gradient': False         # 取消渐变
            }
        }
        
        # 加载自定义配置
        if config_path and os.path.exists(config_path):
            self._load_config(config_path)
        
        # 尝试加载字体
        self._load_fonts()
    
    def _load_config(self, config_path: str):
        """
        加载配置文件
        
        Args:
            config_path: 配置文件路径
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                custom_config = json.load(f)
                self.config.update(custom_config)
        except Exception as e:
            # 记录配置加载失败日志
            try:
                log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                os.makedirs(log_dir, exist_ok=True)
                log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                log_filepath = os.path.join(log_dir, log_filename)
                self._log_unified("WARNING", f"加载配置文件失败: {e}，使用默认配置", "system", "system", log_filepath)
            except Exception:
                pass
    
    def _log_unified(self, level: str, message: str, group_id: str = "system", user_id: str = "system", log_file_path: str = None):
        """
        统一日志记录方法
        
        Args:
            level: 日志级别 (INFO, WARNING, ERROR, DEBUG)
            message: 日志消息
            group_id: 群组ID
            user_id: 用户ID
            log_file_path: 日志文件路径
        """
        try:
            current_time = datetime.now()
            timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            log_msg = f"[{timestamp}][{level}][G:{group_id}][U:{user_id}]: {message}"
            
            # 打印到控制台
            print(log_msg)
            
            # 写入日志文件
            if log_file_path:
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(log_msg + "\n")
            
            # 使用logging模块记录
            logger = logging.getLogger(__name__)
            if level == "INFO":
                logger.info(message)
            elif level == "WARNING":
                logger.warning(message)
            elif level == "ERROR":
                logger.error(message)
            elif level == "DEBUG":
                logger.debug(message)
        except Exception:
            pass

    def _load_fonts(self):
        """
        加载字体文件
        
        尝试加载系统字体，支持Windows和Linux系统，如果失败则使用默认字体
        """
        self.fonts = {}
        
        # 跨平台字体路径配置（优先选择支持中文的字体）
        import platform
        system = platform.system()
        
        if system == "Windows":
            # Windows系统字体路径
            font_paths = [
                'C:/Windows/Fonts/msyh.ttc',  # 微软雅黑
                'C:/Windows/Fonts/msyhbd.ttc',  # 微软雅黑粗体
                'C:/Windows/Fonts/simhei.ttf',  # 黑体
                'C:/Windows/Fonts/simsun.ttc',  # 宋体
                'C:/Windows/Fonts/simkai.ttf',  # 楷体
                'C:/Windows/Fonts/simfang.ttf',  # 仿宋
            ]
        elif system == "Linux":
            # Linux系统字体路径（Ubuntu/Debian）
            font_paths = [
                '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',  # 文泉驿微米黑
                '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',    # 文泉驿正黑
                '/usr/share/fonts/truetype/arphic/uming.ttc',      # AR PL UMing
                '/usr/share/fonts/truetype/arphic/ukai.ttc',       # AR PL UKai
                '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', # DejaVu Sans
                '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf', # Liberation Sans
                '/System/Library/Fonts/PingFang.ttc',             # macOS PingFang（如果存在）
                '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc', # Noto Sans CJK
                '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',  # Noto Sans CJK
            ]
        elif system == "Darwin":  # macOS
            # macOS系统字体路径
            font_paths = [
                '/System/Library/Fonts/PingFang.ttc',             # 苹方
                '/System/Library/Fonts/Helvetica.ttc',            # Helvetica
                '/Library/Fonts/Arial Unicode MS.ttf',            # Arial Unicode MS
                '/System/Library/Fonts/STHeiti Light.ttc',        # 华文黑体
            ]
        else:
            # 其他系统的通用路径
            font_paths = [
                '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
                '/usr/share/fonts/TTF/DejaVuSans.ttf',
            ]
        
        font_loaded = False
        selected_font_path = None
        
        # 尝试加载系统字体
        for font_path in font_paths:
            if os.path.exists(font_path):
                try:
                    # 测试字体是否能正确加载中文字符
                    test_font = ImageFont.truetype(font_path, 16)
                    # 尝试渲染中文字符测试
                    test_img = Image.new('RGB', (100, 50), 'white')
                    test_draw = ImageDraw.Draw(test_img)
                    test_draw.text((10, 10), "测试中文", font=test_font, fill='black')
                    
                    # 如果测试成功，记录字体路径
                    selected_font_path = font_path
                    font_loaded = True
                    # 记录字体加载成功日志
                    try:
                        log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                        os.makedirs(log_dir, exist_ok=True)
                        log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                        log_filepath = os.path.join(log_dir, log_filename)
                        self._log_unified("INFO", f"成功找到中文字体: {font_path}", log_file_path=log_filepath)
                    except Exception:
                        pass
                    break
                except Exception as e:
                    # 记录字体测试失败日志
                    try:
                        log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                        os.makedirs(log_dir, exist_ok=True)
                        log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                        log_filepath = os.path.join(log_dir, log_filename)
                        self._log_unified("WARNING", f"测试字体失败 {font_path}: {e}", log_file_path=log_filepath)
                    except Exception:
                        pass
        
        # 如果找到了可用字体，加载所有尺寸
        if font_loaded and selected_font_path:
            try:
                for size_name, size in self.config['fonts'].items():
                    self.fonts[size_name] = ImageFont.truetype(selected_font_path, size)
                # 记录成功加载字体日志
                try:
                    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                    os.makedirs(log_dir, exist_ok=True)
                    log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                    log_filepath = os.path.join(log_dir, log_filename)
                    self._log_unified("INFO", f"成功加载所有尺寸的字体: {selected_font_path}", log_file_path=log_filepath)
                except Exception:
                    pass
            except Exception as e:
                # 记录字体加载失败日志
                try:
                    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                    os.makedirs(log_dir, exist_ok=True)
                    log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                    log_filepath = os.path.join(log_dir, log_filename)
                    self._log_unified("ERROR", f"加载字体尺寸失败: {e}", log_file_path=log_filepath)
                except Exception:
                    pass
                font_loaded = False
        
        # 如果系统字体加载失败，尝试使用PIL默认字体
        if not font_loaded:
            try:
                # 在Linux系统上，尝试使用更好的默认字体处理
                if system == "Linux":
                    # 尝试使用fc-list命令查找可用的中文字体
                    try:
                        import subprocess
                        result = subprocess.run(['fc-list', ':lang=zh'], 
                                              capture_output=True, text=True, timeout=5)
                        if result.returncode == 0 and result.stdout:
                            # 解析fc-list输出，查找第一个可用的中文字体
                            lines = result.stdout.strip().split('\n')
                            for line in lines:
                                if ':' in line:
                                    font_file = line.split(':')[0].strip()
                                    if os.path.exists(font_file) and (font_file.endswith('.ttf') or font_file.endswith('.ttc')):
                                        try:
                                            # 测试这个字体
                                            test_font = ImageFont.truetype(font_file, 16)
                                            for size_name, size in self.config['fonts'].items():
                                                self.fonts[size_name] = ImageFont.truetype(font_file, size)
                                            font_loaded = True
                                            # 记录fc-list字体发现日志
                                            try:
                                                log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                                                os.makedirs(log_dir, exist_ok=True)
                                                log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                                                log_filepath = os.path.join(log_dir, log_filename)
                                                self._log_unified("INFO", f"通过fc-list找到中文字体: {font_file}", log_file_path=log_filepath)
                                            except Exception:
                                                pass
                                            break
                                        except Exception:
                                            continue
                            if font_loaded:
                                return
                    except Exception as e:
                        # 记录fc-list查找失败日志
                        try:
                            log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                            os.makedirs(log_dir, exist_ok=True)
                            log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                            log_filepath = os.path.join(log_dir, log_filename)
                            self._log_unified("WARNING", f"fc-list查找字体失败: {e}", log_file_path=log_filepath)
                        except Exception:
                            pass
                
                # 使用PIL默认字体作为最后的回退
                for size_name, size in self.config['fonts'].items():
                    try:
                        # 尝试使用TrueType默认字体
                        self.fonts[size_name] = ImageFont.truetype("DejaVuSans.ttf", size)
                    except:
                        try:
                            # 回退到PIL默认字体
                            self.fonts[size_name] = ImageFont.load_default()
                        except:
                            # 最终回退
                            self.fonts[size_name] = None
                
                # 记录默认字体使用日志
                try:
                    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                    os.makedirs(log_dir, exist_ok=True)
                    log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                    log_filepath = os.path.join(log_dir, log_filename)
                    self._log_unified("WARNING", f"使用默认字体，在{system}系统上可能不支持中文显示", log_file_path=log_filepath)
                    self._log_unified("INFO", "建议在Ubuntu系统上安装中文字体: sudo apt-get install fonts-wqy-microhei fonts-wqy-zenhei", log_file_path=log_filepath)
                except Exception:
                    pass
                
            except Exception as e:
                # 最后的回退方案
                # 记录字体加载完全失败日志
                try:
                    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                    os.makedirs(log_dir, exist_ok=True)
                    log_filename = f"image_generator_{datetime.now().strftime('%Y%m%d')}.log"
                    log_filepath = os.path.join(log_dir, log_filename)
                    self._log_unified("ERROR", f"字体加载完全失败: {e}", log_file_path=log_filepath)
                except Exception:
                    pass
                for size_name in self.config['fonts'].keys():
                    self.fonts[size_name] = None
    
    def generate_team_gantt_chart(self, team_data: Dict[str, Any]) -> str:
        """
        生成车队甘特图
        
        Args:
            team_data: 车队数据，包含队长和成员信息
            
        Returns:
            生成的图片文件路径
        """
        try:
            # 解析时间数据
            time_slots = self._parse_team_time_data(team_data)
            if not time_slots:
                raise ValueError("无法解析车队时间数据")
            
            # 计算图片尺寸 - 需要包含所有人员（队长、成员、替补）
            # 先解析时间数据来获取实际的人员数量
            person_slots = {}
            for slot in time_slots:
                person_name = slot['name']
                if person_name not in person_slots:
                    person_slots[person_name] = []
                person_slots[person_name].append(slot)
            
            # 实际人员数量（包括队长、成员和所有替补）
            actual_members_count = len(person_slots)
            chart_height = (self.config['header_height'] + 
                          actual_members_count * self.config['row_height'] + 
                          self.config['margin'] * 2)
            
            # 创建图片
            img = Image.new('RGB', (self.config['width'], chart_height), 
                          self.config['colors']['background'])
            draw = ImageDraw.Draw(img)
            
            # 绘制标题
            self._draw_title(draw, team_data)
            
            # 绘制时间轴
            self._draw_time_axis(draw, time_slots)
            
            # 绘制甘特条
            self._draw_gantt_bars(draw, team_data, time_slots)
            
            # 绘制网格线（使用实际人员数量）
            self._draw_grid_lines(draw, actual_members_count)
            
            # 保存图片
            output_path = self._save_image(img, team_data['team_number'])
            
            self.logger.info(f"成功生成车队甘特图: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"生成车队甘特图失败: {e}")
            raise
    
    def _parse_team_time_data(self, team_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        解析车队时间数据，包括队长、成员和替补成员
        
        Args:
            team_data: 车队数据，包含队长、成员和替补成员信息
            
        Returns:
            解析后的时间段列表，包含所有人员的时间信息
        """
        time_slots = []
        
        try:
            # 添加队长时间段
            for start_ts, end_ts in team_data['timestamp_ranges']:
                time_slots.append({
                    'name': f"队长 {team_data['captain']}",
                    'start_time': datetime.fromtimestamp(start_ts),
                    'end_time': datetime.fromtimestamp(end_ts),
                    'type': 'captain'
                })
            
            # 添加成员时间段
            for member in team_data['members']:
                for start_ts, end_ts in member['timestamp_ranges']:
                    time_slots.append({
                        'name': f"成员 {member['name']}",
                        'start_time': datetime.fromtimestamp(start_ts),
                        'end_time': datetime.fromtimestamp(end_ts),
                        'type': 'member',
                        'join_type': member.get('join_type', '')
                    })
            
            # 添加替补成员时间段
            for substitute in team_data.get('substitutes', []):
                for start_ts, end_ts in substitute['timestamp_ranges']:
                    # 根据替补类型确定join_type
                    t = substitute.get('type')
                    if t == '推车替补':
                        substitute_join_type = '推车'
                    elif t == '共跑替补':
                        substitute_join_type = '共跑'
                    elif t == '跑推替补':
                        substitute_join_type = '跑推'
                    else:
                        substitute_join_type = '推车'
                    time_slots.append({
                        'name': f"替补 {substitute['name']}",
                        'start_time': datetime.fromtimestamp(start_ts),
                        'end_time': datetime.fromtimestamp(end_ts),
                        'type': 'substitute',
                        'join_type': substitute_join_type,
                        'substitute_type': substitute.get('type', '推车替补')
                    })
            
            return time_slots
            
        except Exception as e:
            self.logger.error(f"解析车队时间数据失败: {e}")
            return []
    
    def _get_time_range(self, time_slots: List[Dict[str, Any]]) -> Tuple[datetime, datetime]:
        """
        获取时间范围
        
        Args:
            time_slots: 时间段列表
            
        Returns:
            (开始时间, 结束时间)
        """
        if not time_slots:
            now = datetime.now()
            return now, now + timedelta(hours=1)
        
        start_times = [slot['start_time'] for slot in time_slots]
        end_times = [slot['end_time'] for slot in time_slots]
        
        return min(start_times), max(end_times)
    
    def _draw_title(self, draw: ImageDraw.Draw, team_data: Dict[str, Any]):
        """
        绘制简洁清晰的标题
        
        Args:
            draw: 绘图对象
            team_data: 车队数据
        """
        try:
            # 使用纯文本避免乱码
            title = f"车队{team_data['team_number']} - {team_data.get('song', '未知歌曲')}"
            # 构建副标题，包含日期、队长、要求周回和要求倍率（若有）
            subtitle_parts = [
                team_data.get('date_display', ''),
                f"队长: {team_data.get('captain', '未知')}"
            ]
            laps = team_data.get('laps')
            multiplier = team_data.get('multiplier')
            if laps is not None:
                subtitle_parts.append(f"要求周回: {laps}")
            if multiplier is not None:
                subtitle_parts.append(f"要求倍率: {multiplier}")
            # 综合力：优先显示显式字段；若缺失则以 要求周回*要求倍率 计算
            comp = team_data.get('comprehensive_power')
            if comp is not None and str(comp).strip():
                subtitle_parts.append(f"车主综合: {comp}")
            else:
                if laps is not None and multiplier is not None:
                    try:
                        comp_calc = float(laps) * float(multiplier)
                        comp_str = str(int(comp_calc)) if comp_calc.is_integer() else f"{comp_calc:.2f}".rstrip('0').rstrip('.')
                        subtitle_parts.append(f"车主综合: {comp_str}")
                    except Exception:
                        pass
            subtitle = " | ".join([p for p in subtitle_parts if p])
            
            header_height = self.config['header_height']
            
            # 绘制简洁的标题背景
            draw.rectangle(
                [0, 0, self.config['width'], header_height],
                fill=self.config['colors']['header_bg']
            )
            
            # 绘制边框
            draw.rectangle(
                [0, 0, self.config['width'] - 1, header_height - 1],
                outline=self.config['colors']['border'],
                width=self.config['effects']['border_width']
            )
            
            # 绘制主标题
            try:
                title_bbox = draw.textbbox((0, 0), title, font=self.fonts['title'])
                title_width = title_bbox[2] - title_bbox[0]
                title_x = (self.config['width'] - title_width) // 2
                
                draw.text(
                    (title_x, 15),
                    title,
                    fill=self.config['colors']['header_text'],
                    font=self.fonts['title']
                )
            except Exception as e:
                # 如果字体渲染失败，使用默认字体
                self.logger.warning(f"标题字体渲染失败，使用默认字体: {e}")
                draw.text(
                    (20, 15),
                    title,
                    fill=self.config['colors']['header_text']
                )
            
            # 绘制副标题
            try:
                subtitle_bbox = draw.textbbox((0, 0), subtitle, font=self.fonts['content'])
                subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
                subtitle_x = (self.config['width'] - subtitle_width) // 2
                
                draw.text(
                    (subtitle_x, 45),
                    subtitle,
                    fill=self.config['colors']['header_text'],
                    font=self.fonts['content']
                )
            except Exception as e:
                # 如果字体渲染失败，使用默认字体
                self.logger.warning(f"副标题字体渲染失败，使用默认字体: {e}")
                draw.text(
                    (20, 45),
                    subtitle,
                    fill=self.config['colors']['header_text']
                )
            
        except Exception as e:
            self.logger.error(f"绘制标题失败: {e}")
    
    def _blend_colors(self, color1: str, color2: str, ratio: float) -> str:
        """
        混合两种颜色
        
        Args:
            color1: 第一种颜色（十六进制）
            color2: 第二种颜色（十六进制）
            ratio: 混合比例（0-1）
            
        Returns:
            混合后的颜色
        """
        try:
            # 转换为RGB
            r1, g1, b1 = int(color1[1:3], 16), int(color1[3:5], 16), int(color1[5:7], 16)
            r2, g2, b2 = int(color2[1:3], 16), int(color2[3:5], 16), int(color2[5:7], 16)
            
            # 混合
            r = int(r1 * ratio + r2 * (1 - ratio))
            g = int(g1 * ratio + g2 * (1 - ratio))
            b = int(b1 * ratio + b2 * (1 - ratio))
            
            return f"#{r:02x}{g:02x}{b:02x}"
        except:
            return color1
    
    def _draw_decorative_elements(self, draw: ImageDraw.Draw, header_height: int):
        """
        绘制装饰性元素
        
        Args:
            draw: 绘图对象
            header_height: 标题高度
        """
        try:
            # 绘制左侧装饰线
            margin = self.config['margin']
            line_color = self.config['colors']['accent']
            
            # 垂直装饰线
            for i in range(3):
                x = margin + i * 8
                draw.line(
                    [(x, header_height // 4), (x, header_height * 3 // 4)],
                    fill=line_color,
                    width=3
                )
            
            # 右侧装饰线
            for i in range(3):
                x = self.config['width'] - margin - i * 8
                draw.line(
                    [(x, header_height // 4), (x, header_height * 3 // 4)],
                    fill=line_color,
                    width=3
                )
                
        except Exception as e:
            self.logger.error(f"绘制装饰元素失败: {e}")
    
    def generate_conversation_summary_image(self, title: str, lines: List[Dict[str, Any] or str]) -> str:
        """
        生成会话统计或总结图片
        
        Args:
            title: 图片标题，例如“最近 7 天会话统计”或“会话总结”
            lines: 要渲染到图片中的文本行列表
            
        Returns:
            生成的图片文件路径
        """
        try:
            # 将任意类型的行统一转换为字符串
            text_lines: List[str] = []
            for line in lines:
                if isinstance(line, str):
                    text_lines.append(line)
                else:
                    text_lines.append(str(line))
            
            # 估算图片高度：标题区域 + 行高 * 行数 + 底部边距
            header_height = self.config['header_height']
            margin = self.config['margin']
            line_height = 24
            min_height = self.config.get('height', 600)
            content_height = header_height + margin + line_height * (len(text_lines) + 2) + margin
            img_height = max(min_height, content_height)
            
            # 创建图片
            img = Image.new(
                'RGB',
                (self.config['width'], img_height),
                self.config['colors']['background']
            )
            draw = ImageDraw.Draw(img)
            
            # 绘制标题背景
            draw.rectangle(
                [0, 0, self.config['width'], header_height],
                fill=self.config['colors']['header_bg']
            )
            
            # 绘制标题文本
            try:
                title_font = self.fonts.get('title')
                title_bbox = draw.textbbox((0, 0), title, font=title_font)
                title_width = title_bbox[2] - title_bbox[0]
                title_x = (self.config['width'] - title_width) // 2
                draw.text(
                    (title_x, 15),
                    title,
                    fill=self.config['colors']['header_text'],
                    font=title_font
                )
            except Exception:
                draw.text(
                    (margin, 15),
                    title,
                    fill=self.config['colors']['header_text']
                )
            
            # 绘制正文文本
            y = header_height + margin
            content_font = self.fonts.get('content')
            for line in text_lines:
                if not line:
                    y += line_height
                    continue
                try:
                    draw.text(
                        (margin, y),
                        line,
                        fill=self.config['colors']['text_primary'],
                        font=content_font
                    )
                except Exception:
                    draw.text(
                        (margin, y),
                        line,
                        fill=self.config['colors']['text_primary']
                    )
                y += line_height
            
            # 保存图片
            output_path = self._save_image(img, "conversation")
            self.logger.info(f"成功生成会话统计图片: {output_path}")
            return output_path
        
        except Exception as e:
            self.logger.error(f"生成会话统计图片失败: {e}")
            raise
    
    def _draw_time_axis(self, draw: ImageDraw.Draw, time_slots: List[Dict[str, Any]]):
        """
        绘制智能时间轴 - 根据成员报名时间进行划分
        
        Args:
            draw: 绘图对象
            time_slots: 时间段列表
        """
        if not time_slots:
            return
            
        # 计算时间轴区域
        axis_y = self.config['header_height'] + 10
        axis_start_x = self.config['margin'] + self.config['time_column_width']
        axis_end_x = self.config['width'] - self.config['margin']
        axis_width = axis_end_x - axis_start_x
        
        # 获取所有关键时间点
        time_points = set()
        for slot in time_slots:
            time_points.add(slot['start_time'])
            time_points.add(slot['end_time'])
        
        sorted_times = sorted(time_points)
        if len(sorted_times) < 2:
            return
            
        start_time = sorted_times[0]
        end_time = sorted_times[-1]
        total_duration = (end_time - start_time).total_seconds()
        
        # 绘制时间轴背景
        draw.rectangle(
            [axis_start_x, axis_y, axis_end_x, axis_y + 30],
            fill=self.config['colors']['time_axis_bg'],
            outline=self.config['colors']['border']
        )
        
        # 绘制关键时间点
        for time_point in sorted_times:
            # 计算时间点在图表中的x坐标
            if total_duration > 0:
                time_progress = (time_point - start_time).total_seconds() / total_duration
            else:
                time_progress = 0
            tick_x = axis_start_x + time_progress * axis_width
            
            # 绘制刻度线
            draw.line(
                [tick_x, axis_y, tick_x, axis_y + 30],
                fill=self.config['colors']['grid_line'],
                width=1
            )
            
            # 绘制时间标签
            time_text = time_point.strftime('%H:%M')
            
            try:
                text_bbox = draw.textbbox((0, 0), time_text, font=self.fonts['small'])
                text_width = text_bbox[2] - text_bbox[0]
                text_x = tick_x - text_width // 2
            except:
                # 如果字体计算失败，使用估算宽度
                text_width = len(time_text) * 6
                text_x = tick_x - text_width // 2
            
            # 确保文字不超出边界
            text_x = max(axis_start_x, min(text_x, axis_end_x - text_width))
            
            try:
                draw.text(
                    (text_x, axis_y + 5),
                    time_text,
                    fill=self.config['colors']['text_secondary'],
                    font=self.fonts['small']
                )
            except:
                # 如果字体渲染失败，使用默认字体
                draw.text(
                    (text_x, axis_y + 5),
                    time_text,
                    fill=self.config['colors']['text_secondary']
                )
    
    def _draw_gantt_bars(self, draw: ImageDraw.Draw, team_data: Dict[str, Any], 
                        time_slots: List[Dict[str, Any]]):
        """
        绘制简洁清晰的甘特条
        
        Args:
            draw: 绘图对象
            team_data: 车队数据
            time_slots: 时间段列表
        """
        if not time_slots:
            return
            
        # 获取时间范围
        all_times = []
        for slot in time_slots:
            all_times.extend([slot['start_time'], slot['end_time']])
        
        if not all_times:
            return
            
        start_time = min(all_times)
        end_time = max(all_times)
        total_duration = (end_time - start_time).total_seconds()
        
        # 计算绘图区域
        chart_start_x = self.config['margin'] + self.config['time_column_width']
        chart_width = self.config['width'] - self.config['margin'] * 2 - self.config['time_column_width']
        chart_start_y = self.config['header_height'] + 50
        
        # 按人员分组时间段
        person_slots = {}
        for slot in time_slots:
            person_name = slot['name']
            if person_name not in person_slots:
                person_slots[person_name] = []
            person_slots[person_name].append(slot)
        
        # 按每个人员的最早开始时间排序
        def get_earliest_start_time(person_data):
            person_name, slots = person_data
            if not slots:
                return datetime.max
            return min(slot['start_time'] for slot in slots)
        
        sorted_person_slots = sorted(person_slots.items(), key=get_earliest_start_time)
        
        # 绘制每个人员的甘特条
        row_index = 0
        for person_name, slots in sorted_person_slots:
            row_y = chart_start_y + row_index * self.config['row_height']
            
            # 绘制人员名称背景
            name_bg_rect = [
                self.config['margin'], row_y, 
                self.config['margin'] + self.config['time_column_width'], 
                row_y + self.config['row_height'] - 2
            ]
            
            # 绘制名称背景
            draw.rectangle(
                name_bg_rect,
                fill=self.config['colors']['time_bg'],
                outline=self.config['colors']['border'],
                width=self.config['effects']['border_width']
            )
            
            # 绘制人员名称（去除前缀）
            name_text = person_name.replace('队长 ', '').replace('成员 ', '')
            name_text = name_text[:12] + '...' if len(name_text) > 12 else name_text
            
            try:
                draw.text(
                    (self.config['margin'] + 5, row_y + 15),
                    name_text,
                    fill=self.config['colors']['text_primary'],
                    font=self.fonts['content']
                )
            except:
                # 字体渲染失败时使用默认字体
                draw.text(
                    (self.config['margin'] + 5, row_y + 15),
                    name_text,
                    fill=self.config['colors']['text_primary']
                )
            
            # 绘制时间条
            for slot in slots:
                slot_start = slot['start_time']
                slot_end = slot['end_time']
                
                # 计算条形位置
                if total_duration > 0:
                    start_progress = (slot_start - start_time).total_seconds() / total_duration
                    end_progress = (slot_end - start_time).total_seconds() / total_duration
                else:
                    start_progress = 0
                    end_progress = 1
                
                bar_start_x = chart_start_x + start_progress * chart_width
                bar_end_x = chart_start_x + end_progress * chart_width
                bar_width = max(bar_end_x - bar_start_x, 3)  # 最小宽度3像素
                
                # 根据类型确定颜色
                if slot['type'] == 'captain':
                    bar_color = self.config['colors']['captain_bar']
                elif slot['type'] == 'substitute':
                    # 替补成员使用专用颜色
                    if slot.get('join_type') == '推车':
                        bar_color = self.config['colors']['substitute_push_bar']
                    elif slot.get('join_type') == '跑推':
                        bar_color = self.config['colors']['substitute_paotui_bar']
                    else:  # 共跑替补
                        bar_color = self.config['colors']['substitute_run_bar']
                elif slot.get('join_type') == '推车':
                    bar_color = self.config['colors']['push_bar']
                elif slot.get('join_type') == '共跑':
                    bar_color = self.config['colors']['run_bar']
                elif slot.get('join_type') == '跑推':
                    bar_color = self.config['colors']['paotui_bar']
                else:
                    bar_color = self.config['colors']['member_bar']
                
                # 绘制简洁的时间条
                bar_rect = [bar_start_x, row_y + 8, bar_start_x + bar_width, row_y + self.config['row_height'] - 8]
                
                draw.rectangle(
                    bar_rect,
                    fill=bar_color,
                    outline=self.config['colors']['border'],
                    width=1
                )
                
                # 在条形上显示时间信息（如果空间足够）
                if bar_width > 50:
                    time_text = f"{slot_start.strftime('%H:%M')}-{slot_end.strftime('%H:%M')}"
                    
                    try:
                        text_bbox = draw.textbbox((0, 0), time_text, font=self.fonts['small'])
                        text_width = text_bbox[2] - text_bbox[0]
                        
                        if text_width < bar_width - 6:
                            draw.text(
                                (bar_start_x + (bar_width - text_width) // 2, row_y + 18),
                                time_text,
                                fill='#FFFFFF',
                                font=self.fonts['small']
                            )
                    except:
                        # 字体渲染失败时跳过文字绘制
                        pass
            
            row_index += 1
    
    def _draw_grid_lines(self, draw: ImageDraw.Draw, members_count: int):
        """
        绘制简洁的网格线
        
        Args:
            draw: 绘图对象
            members_count: 成员数量
        """
        chart_start_y = self.config['header_height'] + 50
        chart_end_y = chart_start_y + members_count * self.config['row_height']
        chart_start_x = self.config['margin'] + self.config['time_column_width']
        chart_end_x = self.config['width'] - self.config['margin']
        
        # 绘制水平网格线
        for i in range(members_count + 1):
            y = chart_start_y + i * self.config['row_height']
            draw.line(
                [self.config['margin'], y, chart_end_x, y],
                fill=self.config['colors']['grid_line'],
                width=1
            )
        
        # 绘制垂直网格线
        draw.line(
            [chart_start_x, chart_start_y, chart_start_x, chart_end_y],
            fill=self.config['colors']['grid_line'],
            width=1
        )
        
        draw.line(
            [chart_end_x, chart_start_y, chart_end_x, chart_end_y],
            fill=self.config['colors']['grid_line'],
            width=1
        )
    
    def _save_image(self, img: Image.Image, team_number: str) -> str:
        """
        保存图片
        
        Args:
            img: 图片对象
            team_number: 车队号
            
        Returns:
            保存的文件路径
        """
        # 确保输出目录存在
        output_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'data', 'images'))
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"team_{team_number}_{timestamp}.png"
        output_path = os.path.realpath(os.path.join(output_dir, filename))
        
        # 保存图片
        img.save(output_path, 'PNG', quality=95)
        
        return output_path
