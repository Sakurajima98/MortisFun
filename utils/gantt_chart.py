#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
甘特图生成模块

该模块负责为车队信息生成甘特图，展示成员活动时间线。
主要功能包括：
- 时间轴处理和格式化
- 成员智能分段排布
- 甘特图图片生成
- 颜色主题管理

Author: Trae Builder
Date: 2024
"""

import logging
import os
from typing import Dict, List, Any, Tuple
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle

class GanttChartGenerator:
    """
    甘特图生成器类
    
    负责根据车队数据生成甘特图，展示成员活动时间线。
    支持智能分段排布和自定义样式。
    """
    
    def __init__(self):
        """
        初始化甘特图生成器
        """
        self.logger = logging.getLogger(__name__)
        
        # 颜色主题配置
        self.colors = {
            'captain': '#FF6B6B',      # 队长 - 红色
            'tuiche': '#4ECDC4',       # 推车成员 - 青色
            'gonggpao': '#95E1D3',     # 共跑成员 - 浅绿色
            'substitute_tuiche': '#FFB347',  # 替补推车 - 橙色
            'substitute_gonggpao': '#DDA0DD',  # 替补共跑 - 紫色
            'background': '#F8F9FA',   # 背景色
            'grid': '#E9ECEF',         # 网格线
            'text': '#343A40',         # 文字颜色
            'border': '#DEE2E6'        # 边框颜色
        }
        
        # 图表配置
        self.config = {
            'figure_width': 16,        # 图表宽度
            'figure_height': 10,       # 图表高度
            'dpi': 150,               # 分辨率
            'font_size': 10,          # 字体大小
            'title_font_size': 14,    # 标题字体大小
            'margin': 0.1,            # 边距
            'bar_height': 0.6,        # 条形图高度
            'segment_gap': 0.1        # 分段间隔
        }
        
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
    
    def generate_gantt_chart(self, team_data: Dict[str, Any], output_path: str = None) -> str:
        """
        生成车队甘特图
        
        Args:
            team_data: 车队数据
            output_path: 输出路径，如果为None则自动生成
            
        Returns:
            生成的图片文件路径
        """
        try:
            # 准备数据
            chart_data = self._prepare_chart_data(team_data)
            
            # 创建图表
            fig, ax = self._create_chart(chart_data)
            
            # 设置输出路径
            if output_path is None:
                team_number = team_data.get('team_number', 'unknown')
                # 保存图片到data/images目录
                output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'images')
                os.makedirs(output_dir, exist_ok=True)
                
                # 生成文件名
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"team_{team_number}_{timestamp}.png"
                output_path = os.path.join(output_dir, filename)
            
            # 保存图片
            plt.savefig(output_path, 
                       dpi=self.config['dpi'], 
                       bbox_inches='tight',
                       facecolor=self.colors['background'],
                       edgecolor='none')
            plt.close()
            
            self.logger.info(f"甘特图已生成: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"生成甘特图失败: {e}")
            raise
    
    def _prepare_chart_data(self, team_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备图表数据
        
        Args:
            team_data: 原始车队数据
            
        Returns:
            处理后的图表数据
        """
        chart_data = {
            'team_number': team_data.get('team_number', '未知车队'),
            'song': team_data.get('song', '未知歌曲'),
            'captain': team_data.get('captain', '未知队长'),
            'date': team_data.get('date_display', '未知日期'),
            'members': []
        }

        # 处理成员数据，排除队长
        captain_name = team_data.get('captain', '队长')
        for member in team_data.get('members', []):
            member_name = member.get('name', '未知成员')
            join_type = member.get('join_type', '推车')
            
            # 跳过队长，避免重复显示（按名字或join_type判断）
            if member_name == captain_name or join_type == 'captain':
                continue
                
            member_segments = self._process_time_segments(
                member.get('timestamp_ranges', []),
                member_name
            )

            if member_segments:
                # 根据join_type确定成员类型和图标
                if join_type == '共跑':
                    icon = '🏃'
                    role = 'gonggpao'
                else:  # 默认为推车
                    icon = '👤'
                    role = 'tuiche'
                
                chart_data['members'].append({
                    'name': f"{icon} {member_name}",
                    'role': role,
                    'segments': member_segments
                })

        # 处理替补成员数据
        for substitute in team_data.get('substitutes', []):
            substitute_name = substitute.get('name', '未知替补')
            substitute_type = substitute.get('type', '推车替补')
            
            substitute_segments = self._process_time_segments(
                substitute.get('timestamp_ranges', []),
                substitute_name
            )

            if substitute_segments:
                # 根据替补类型确定图标和角色
                if substitute_type == '共跑替补':
                    icon = '🔄🏃'
                    role = 'substitute_gonggpao'
                else:  # 默认为推车替补
                    icon = '🔄👤'
                    role = 'substitute_tuiche'
                
                chart_data['members'].append({
                    'name': f"{icon} {substitute_name}",
                    'role': role,
                    'segments': substitute_segments
                })

        # 处理队长数据（放在最后，确保队长显示在顶部）
        captain_segments = self._process_time_segments(
            team_data.get('timestamp_ranges', []),
            team_data.get('captain', '队长')
        )

        if captain_segments:
            # 将队长插入到成员列表的开头
            chart_data['members'].insert(0, {
                'name': f"👑 {team_data.get('captain', '队长')}",
                'role': 'captain',
                'segments': captain_segments
            })

        # 智能排布
        chart_data['members'] = self._smart_layout(chart_data['members'])

        return chart_data
    
    def _process_time_segments(self, timestamp_ranges: List[List[int]], name: str) -> List[Dict[str, Any]]:
        """
        处理时间段数据
        
        Args:
            timestamp_ranges: 时间戳范围列表
            name: 成员名称
            
        Returns:
            处理后的时间段列表
        """
        segments = []
        
        if not timestamp_ranges:
            return segments
            
        for time_range in timestamp_ranges:
            try:
                # 验证时间范围数据格式
                if not isinstance(time_range, (list, tuple)):
                    self.logger.warning(f"时间范围格式错误 {name}: {time_range}")
                    continue
                    
                if len(time_range) != 2:
                    self.logger.warning(f"时间范围长度错误 {name}: 期望2个元素，实际{len(time_range)}个")
                    continue
                
                start_ts, end_ts = time_range
                
                # 验证时间戳是否为数字
                if not isinstance(start_ts, (int, float)) or not isinstance(end_ts, (int, float)):
                    self.logger.warning(f"时间戳格式错误 {name}: start={start_ts}, end={end_ts}")
                    continue
                
                start_time = datetime.fromtimestamp(start_ts)
                end_time = datetime.fromtimestamp(end_ts)
                duration = (end_time - start_time).total_seconds() / 3600  # 转换为小时
                
                segments.append({
                    'start': start_time,
                    'end': end_time,
                    'duration': duration,
                    'start_ts': start_ts,
                    'end_ts': end_ts
                })
            except Exception as e:
                self.logger.warning(f"处理时间段失败 {name}: {e}")
                continue
        
        # 按开始时间排序
        segments.sort(key=lambda x: x['start'])
        
        return segments
    
    def _smart_layout(self, members: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        智能分段排布成员
        
        Args:
            members: 成员列表
            
        Returns:
            排布后的成员列表
        """
        try:
            # 按活动时间总长度排序（队长优先）
            def sort_key(member):
                if member['role'] == 'captain':
                    return (0, -sum(seg['duration'] for seg in member['segments']))
                else:
                    return (1, -sum(seg['duration'] for seg in member['segments']))
            
            members.sort(key=sort_key)
            
            # 计算每个成员的活动时间范围
            for member in members:
                if member['segments']:
                    member['earliest_start'] = min(seg['start'] for seg in member['segments'])
                    member['latest_end'] = max(seg['end'] for seg in member['segments'])
                    member['total_duration'] = sum(seg['duration'] for seg in member['segments'])
                else:
                    member['earliest_start'] = None
                    member['latest_end'] = None
                    member['total_duration'] = 0
            
            return members
            
        except Exception as e:
            self.logger.error(f"智能排布失败: {e}")
            return members
    
    def _create_chart(self, chart_data: Dict[str, Any]) -> Tuple[plt.Figure, plt.Axes]:
        """
        创建甘特图
        
        Args:
            chart_data: 图表数据
            
        Returns:
            图表对象和坐标轴对象
        """
        # 创建图表
        fig, ax = plt.subplots(figsize=(self.config['figure_width'], self.config['figure_height']))
        fig.patch.set_facecolor(self.colors['background'])
        
        # 设置标题
        title = f"车队 {chart_data['team_number']} - {chart_data['song']}\n{chart_data['date']}"
        ax.set_title(title, fontsize=self.config['title_font_size'], 
                    fontweight='bold', color=self.colors['text'], pad=20)
        
        members = chart_data['members']
        if not members:
            ax.text(0.5, 0.5, '暂无活动数据', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=16, color=self.colors['text'])
            return fig, ax
        
        # 计算时间范围
        all_times = []
        for member in members:
            for segment in member['segments']:
                all_times.extend([segment['start'], segment['end']])
        
        if not all_times:
            ax.text(0.5, 0.5, '暂无有效时间数据', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=16, color=self.colors['text'])
            return fig, ax
        
        min_time = min(all_times)
        max_time = max(all_times)
        
        # 添加时间缓冲
        time_buffer = (max_time - min_time) * 0.05
        min_time -= time_buffer
        max_time += time_buffer
        
        # 绘制甘特条
        y_positions = []
        y_labels = []
        
        for i, member in enumerate(members):
            y_pos = len(members) - i - 1
            y_positions.append(y_pos)
            y_labels.append(member['name'])
            
            # 选择颜色
            if member['role'] == 'captain':
                color = self.colors['captain']
            elif member['role'] == 'gonggpao':
                color = self.colors['gonggpao']
            elif member['role'] == 'substitute_tuiche':
                color = self.colors['substitute_tuiche']
            elif member['role'] == 'substitute_gonggpao':
                color = self.colors['substitute_gonggpao']
            else:  # tuiche
                color = self.colors['tuiche']
            
            # 绘制时间段
            for segment in member['segments']:
                duration = segment['end'] - segment['start']
                
                # 绘制主条形
                rect = Rectangle((mdates.date2num(segment['start']), y_pos - self.config['bar_height']/2),
                               mdates.date2num(segment['end']) - mdates.date2num(segment['start']),
                               self.config['bar_height'],
                               facecolor=color, edgecolor=self.colors['border'], 
                               linewidth=1, alpha=0.8)
                ax.add_patch(rect)
                
                # 添加时长标签
                if segment['duration'] > 0.5:  # 只在较长的时间段上显示标签
                    mid_time = segment['start'] + duration / 2
                    duration_text = f"{segment['duration']:.1f}h"
                    ax.text(mdates.date2num(mid_time), y_pos, duration_text,
                           ha='center', va='center', fontsize=8, 
                           color='white', fontweight='bold')
        
        # 设置Y轴
        ax.set_yticks(y_positions)
        ax.set_yticklabels(y_labels, fontsize=self.config['font_size'])
        ax.set_ylim(-0.5, len(members) - 0.5)
        
        # 设置X轴（时间轴）
        ax.set_xlim(mdates.date2num(min_time), mdates.date2num(max_time))
        
        # 格式化时间轴
        time_span = max_time - min_time
        if time_span.total_seconds() <= 3600:  # 1小时内
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
        elif time_span.total_seconds() <= 86400:  # 1天内
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        else:  # 多天
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        
        # 旋转X轴标签
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 设置网格
        ax.grid(True, alpha=0.3, color=self.colors['grid'])
        ax.set_axisbelow(True)
        
        # 设置样式
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color(self.colors['border'])
        ax.spines['bottom'].set_color(self.colors['border'])
        
        # 设置标签
        ax.set_xlabel('时间', fontsize=self.config['font_size'], color=self.colors['text'])
        ax.set_ylabel('成员', fontsize=self.config['font_size'], color=self.colors['text'])
        
        # 添加图例
        legend_elements = [
            Rectangle((0, 0), 1, 1, facecolor=self.colors['captain'], label='队长'),
            Rectangle((0, 0), 1, 1, facecolor=self.colors['tuiche'], label='推车'),
            Rectangle((0, 0), 1, 1, facecolor=self.colors['gonggpao'], label='共跑'),
            Rectangle((0, 0), 1, 1, facecolor=self.colors['substitute_tuiche'], label='替补推车'),
            Rectangle((0, 0), 1, 1, facecolor=self.colors['substitute_gonggpao'], label='替补共跑')
        ]
        ax.legend(handles=legend_elements, loc='upper right', framealpha=0.9)
        
        plt.tight_layout()
        
        return fig, ax