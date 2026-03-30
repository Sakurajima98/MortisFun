#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySekai 地图生成器

该模块负责生成MySekai收获地图的可视化图表，展示材料分布和采集点位置。
主要功能包括：
- 材料位置分析和统计
- 收获地图可视化生成
- 站点平面图绘制
- 材料分布报告生成
- 自定义背景图片支持

作者: AI Assistant
创建时间: 2025-10-11
"""

import json
import logging
import os
from collections import defaultdict, Counter
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.image as mpimg
import numpy as np

class MysekaiMapGenerator:
    """
    MySekai 地图生成器类
    用于分析收获地图数据并生成可视化图表和报告
    """
    
    def __init__(self, file_prefix: str = ""):
        """
        初始化地图生成器
        
        Args:
            file_prefix: 文件名前缀
        """
        self.logger = logging.getLogger(__name__)
        self.file_prefix = file_prefix
        
        # 站点名称映射
        self.site_name_mapping = {
            5: "图一初始空地",
            6: "图三心愿海滩", 
            7: "图二烂漫花田",
            8: "图四忘却之所"
        }
        
        # 背景图片路径映射
        self.background_images = {
            8: "D:\\123MortisFun\\data\\mysekai\\images\\site\\tu_8.jpg"
        }
        
        # 材料映射
        self.material_mapping = self._load_material_mapping()
        
        # 输出目录
        self.output_dir = self._get_output_directory()
        
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 图表配置
        self.chart_config = {
            'figsize': (12, 8),
            'dpi': 100,
            'title_fontsize': 16,
            'label_fontsize': 12,
            'legend_fontsize': 10,
            'annotation_fontsize': 8,
            'grid_alpha': 0.3
        }
        
        # 颜色配置
        self.colors = {
            'fixture_spawned': '#4CAF50',      # 绿色 - 已生成装置
            'fixture_harvested': '#FF9800',    # 橙色 - 已收获装置
            'material_colors': [
                '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
                '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9'
            ],
            'background': '#F8F9FA',           # 背景色
            'grid': '#E9ECEF'                  # 网格线
        }
        
        # 设置中文字体支持
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
    
    def _get_output_directory(self) -> str:
        """
        获取输出目录路径
        
        Returns:
            输出目录路径
        """
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'mysekai')
    
    def get_site_name(self, site_id: int) -> str:
        """
        获取站点名称
        
        Args:
            site_id: 站点ID
            
        Returns:
            站点名称，如果未找到映射则返回默认格式
        """
        return self.site_name_mapping.get(site_id, f"站点 {site_id}")
    
    def _load_material_mapping(self) -> Dict[int, Dict[str, Any]]:
        """
        加载材料映射数据
        
        Returns:
            材料映射字典
        """
        try:
            # 获取项目根目录
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            mapping_file = os.path.join(project_root, 'data', 'mysekai', 'material_mapping.json')
            
            if os.path.exists(mapping_file):
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 从 JSON 结构中提取材料映射
                    materials_data = data.get('materials', {})
                    mapping = {}
                    for material_id_str, material_info in materials_data.items():
                        material_id = int(material_id_str)
                        mapping[material_id] = material_info
                    self.logger.info(f"成功加载 {len(mapping)} 个材料映射")
                    return mapping
            else:
                self.logger.warning(f"材料映射文件不存在: {mapping_file}")
                return {}
        except Exception as e:
            self.logger.error(f"加载材料映射数据失败: {e}")
            return {}
    
    def get_material_name(self, material_id: int) -> str:
        """
        获取材料名称
        
        Args:
            material_id: 材料ID
            
        Returns:
            材料名称，如果找不到则返回 "材料{ID}"
        """
        if material_id in self.material_mapping:
            return self.material_mapping[material_id]['name']
        else:
            return f"材料{material_id}"
    
    def get_material_category(self, material_id: int) -> str:
        """
        获取材料类别
        
        Args:
            material_id: 材料ID
            
        Returns:
            材料类别
        """
        if material_id in self.material_mapping:
            return self.material_mapping[material_id].get('category', '未知')
        else:
            return '未知'
    
    def analyze_harvest_maps(self, file_path: str) -> Dict[str, Any]:
        """
        分析收获地图数据
        
        Args:
            file_path: JSON文件路径
            
        Returns:
            分析结果字典
        """
        self.logger.info("🔍 开始分析MySekai收获地图数据...")
        
        try:
            # 读取JSON文件
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 查找userMysekaiHarvestMaps数据
            harvest_maps = self._extract_harvest_maps(data)
            
            if not harvest_maps:
                self.logger.error("❌ 未找到userMysekaiHarvestMaps数据")
                return {}
            
            # 分析数据
            analysis_result = self._analyze_maps_data(harvest_maps)
            
            self.logger.info(f"✅ 分析完成，发现 {len(analysis_result['sites_data'])} 个站点")
            return analysis_result
            
        except FileNotFoundError:
            self.logger.error(f"❌ 文件未找到: {file_path}")
            return {}
        except Exception as e:
            self.logger.error(f"❌ 分析过程中发生错误: {e}")
            return {}
    
    def _extract_harvest_maps(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        从JSON数据中提取收获地图数据
        
        Args:
            data: JSON数据
            
        Returns:
            收获地图数据列表
        """
        # 首先在updatedResources中查找
        if 'updatedResources' in data:
            harvest_maps = data['updatedResources'].get('userMysekaiHarvestMaps', [])
            if harvest_maps:
                return harvest_maps
        
        # 然后在顶层查找
        return data.get('userMysekaiHarvestMaps', [])
    
    def _analyze_maps_data(self, harvest_maps: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        分析收获地图数据
        
        Args:
            harvest_maps: 收获地图数据列表
            
        Returns:
            分析结果
        """
        analysis_result = {
            'sites_data': {},
            'material_summary': defaultdict(lambda: {
                'total_quantity': 0,
                'locations': [],
                'sites': set()
            })
        }
        
        # 按站点分析
        for site in harvest_maps:
            site_id = site['mysekaiSiteId']
            fixtures = site.get('userMysekaiSiteHarvestFixtures', [])
            resource_drops = site.get('userMysekaiSiteHarvestResourceDrops', [])
            
            # 分析站点数据
            site_data = self._analyze_site_data(site_id, fixtures, resource_drops)
            analysis_result['sites_data'][site_id] = site_data
            
            # 更新材料汇总
            self._update_material_summary(
                analysis_result['material_summary'], 
                site_id, 
                resource_drops
            )
        
        # 转换set为list以便JSON序列化
        for material_id in analysis_result['material_summary']:
            analysis_result['material_summary'][material_id]['sites'] = list(
                analysis_result['material_summary'][material_id]['sites']
            )
        
        return analysis_result
    
    def _analyze_site_data(self, site_id: int, fixtures: List[Dict], 
                          resource_drops: List[Dict]) -> Dict[str, Any]:
        """
        分析单个站点数据
        
        Args:
            site_id: 站点ID
            fixtures: 装置列表
            resource_drops: 资源掉落列表
            
        Returns:
            站点分析数据
        """
        site_data = {
            'fixtures': [],
            'materials': [],
            'coordinate_bounds': {
                'x_min': float('inf'), 'x_max': float('-inf'),
                'z_min': float('inf'), 'z_max': float('-inf')
            }
        }
        
        # 分析装置
        for fixture in fixtures:
            fixture_info = {
                'id': fixture['mysekaiSiteHarvestFixtureId'],
                'x': fixture['positionX'],
                'z': fixture['positionZ'],
                'hp': fixture['hp'],
                'status': fixture['userMysekaiSiteHarvestFixtureStatus']
            }
            site_data['fixtures'].append(fixture_info)
            
            # 更新坐标边界
            self._update_coordinate_bounds(site_data['coordinate_bounds'], 
                                         fixture_info['x'], fixture_info['z'])
        
        # 分析资源掉落
        for drop in resource_drops:
            if drop['resourceType'] == 'mysekai_material':
                material_info = {
                    'material_id': drop['resourceId'],
                    'x': drop['positionX'],
                    'z': drop['positionZ'],
                    'quantity': drop.get('quantity', 1),
                    'hp_trigger': drop['hp'],
                    'seq': drop['seq'],
                    'status': drop['mysekaiSiteHarvestResourceDropStatus']
                }
                site_data['materials'].append(material_info)
                
                # 更新坐标边界
                self._update_coordinate_bounds(site_data['coordinate_bounds'], 
                                             material_info['x'], material_info['z'])
        
        return site_data
    
    def _update_coordinate_bounds(self, bounds: Dict[str, float], x: float, z: float):
        """
        更新坐标边界
        
        Args:
            bounds: 边界字典
            x: X坐标
            z: Z坐标
        """
        bounds['x_min'] = min(bounds['x_min'], x)
        bounds['x_max'] = max(bounds['x_max'], x)
        bounds['z_min'] = min(bounds['z_min'], z)
        bounds['z_max'] = max(bounds['z_max'], z)
    
    def _update_material_summary(self, material_summary: Dict, site_id: int, 
                               resource_drops: List[Dict]):
        """
        更新材料汇总信息
        
        Args:
            material_summary: 材料汇总字典
            site_id: 站点ID
            resource_drops: 资源掉落列表
        """
        for drop in resource_drops:
            if drop['resourceType'] == 'mysekai_material':
                material_id = drop['resourceId']
                quantity = drop.get('quantity', 1)
                
                material_info = {
                    'site_id': site_id,
                    'x': drop['positionX'],
                    'z': drop['positionZ'],
                    'quantity': quantity,
                    'hp_trigger': drop['hp'],
                    'seq': drop['seq'],
                    'status': drop['mysekaiSiteHarvestResourceDropStatus']
                }
                
                material_summary[material_id]['total_quantity'] += quantity
                material_summary[material_id]['locations'].append(material_info)
                material_summary[material_id]['sites'].add(site_id)
    
    def generate_site_map(self, site_id: int, site_data: Dict[str, Any], 
                         material_summary: Dict[str, Any], 
                         output_dir: str = "") -> str:
        """
        生成单个站点的平面图
        
        Args:
            site_id: 站点ID
            site_data: 站点数据
            material_summary: 材料汇总数据
            output_dir: 输出目录
            
        Returns:
            生成的图片文件路径
        """
        self.logger.info(f"🗺️ 正在绘制站点 {site_id} 的平面图...")
        
        # 创建图形
        fig, ax = plt.subplots(1, 1, figsize=self.chart_config['figsize'])
        
        # 设置坐标范围
        self._setup_coordinate_range(ax, site_data['coordinate_bounds'])
        
        # 绘制装置
        self._draw_fixtures(ax, site_data['fixtures'])
        
        # 绘制材料位置
        material_color_map = self._draw_materials(ax, site_data['materials'])
        
        # 设置图表样式
        self._setup_chart_style(ax, site_id, site_data)
        
        # 创建图例
        self._create_legend(ax, material_color_map, site_data['materials'])
        
        # 保存图片
        site_name = self.get_site_name(site_id)
        # 将站点名称转换为适合文件名的格式（去除特殊字符）
        safe_site_name = site_name.replace(" ", "_").replace("：", "_").replace(":", "_")
        filename = f"{self.file_prefix}{safe_site_name}_harvest_map" if self.file_prefix else f"{safe_site_name}_harvest_map"
        output_file = self._save_chart(fig, filename, output_dir)
        
        plt.close(fig)
        return output_file
    
    def _setup_coordinate_range(self, ax: plt.Axes, bounds: Dict[str, float]):
        """
        设置坐标范围
        
        Args:
            ax: matplotlib轴对象
            bounds: 坐标边界
        """
        x_range = bounds['x_max'] - bounds['x_min']
        z_range = bounds['z_max'] - bounds['z_min']
        margin = max(x_range, z_range) * 0.1
        
        ax.set_xlim(bounds['x_min'] - margin, bounds['x_max'] + margin)
        ax.set_ylim(bounds['z_min'] - margin, bounds['z_max'] + margin)
    
    def _draw_fixtures(self, ax: plt.Axes, fixtures: List[Dict[str, Any]]):
        """
        绘制装置
        
        Args:
            ax: matplotlib轴对象
            fixtures: 装置列表
        """
        fixture_colors = {
            'spawned': self.colors['fixture_spawned'],
            'harvested': self.colors['fixture_harvested']
        }
        
        for fixture in fixtures:
            color = fixture_colors.get(fixture['status'], 'gray')
            # 根据HP调整透明度
            alpha = 0.3 + (fixture['hp'] / 90.0) * 0.7 if fixture['hp'] > 0 else 0.2
            
            circle = patches.Circle(
                (fixture['x'], fixture['z']), 
                radius=0.8, 
                facecolor=color, 
                edgecolor='black',
                alpha=alpha,
                linewidth=1
            )
            ax.add_patch(circle)
            
            # 标注装置ID和HP
            ax.annotate(
                f"{fixture['id']}\nHP:{fixture['hp']}", 
                (fixture['x'], fixture['z']),
                ha='center', va='center', 
                fontsize=self.chart_config['annotation_fontsize'], 
                weight='bold'
            )
    
    def _draw_materials(self, ax: plt.Axes, materials: List[Dict[str, Any]]) -> Dict[int, Any]:
        """
        绘制材料位置
        
        Args:
            ax: matplotlib轴对象
            materials: 材料列表
            
        Returns:
            材料颜色映射
        """
        material_color_map = {}
        color_index = 0
        
        # 统计每个位置的材料，按材料ID合并数量
        position_materials = defaultdict(lambda: defaultdict(int))
        for material in materials:
            pos_key = (material['x'], material['z'])
            material_id = material['material_id']
            position_materials[pos_key][material_id] += material['quantity']
        
        # 绘制材料点
        for pos, materials_dict in position_materials.items():
            x, z = pos
            
            # 将材料按ID排序，确保显示一致性
            sorted_materials = sorted(materials_dict.items())
            
            # 为每种材料分配颜色并绘制
            for i, (material_id, total_quantity) in enumerate(sorted_materials):
                if material_id not in material_color_map:
                    material_color_map[material_id] = self.colors['material_colors'][
                        color_index % len(self.colors['material_colors'])
                    ]
                    color_index += 1
                
                # 绘制材料点（稍微偏移以避免重叠）
                offset_x = x + (i - len(sorted_materials)/2) * 0.3
                offset_z = z + (i - len(sorted_materials)/2) * 0.3
                
                ax.scatter(
                    offset_x, offset_z,
                    c=[material_color_map[material_id]], 
                    s=total_quantity * 15 + 40,  # 大小表示总数量
                    alpha=0.8,
                    edgecolors='black',
                    linewidth=1,
                    marker='s'  # 方形标记
                )
            
            # 标注位置的材料信息（整合显示）
            material_text = []
            for material_id, total_quantity in sorted_materials:
                material_name = self.get_material_name(material_id)
                material_text.append(f"{material_name}×{total_quantity}")
            
            ax.annotate(
                '\n'.join(material_text),
                (x, z - 1.2),
                ha='center', va='top',
                fontsize=self.chart_config['annotation_fontsize'] - 1,
                bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8)
            )
        
        return material_color_map
    
    def _setup_chart_style(self, ax: plt.Axes, site_id: int, site_data: Dict[str, Any]):
        """
        设置图表样式
        
        Args:
            ax: matplotlib轴对象
            site_id: 站点ID
            site_data: 站点数据
        """
        site_name = self.get_site_name(site_id)
        ax.set_title(
            f'MySekai 收获地图 - {site_name}\n'
            f'装置数: {len(site_data["fixtures"])}, 材料点: {len(site_data["materials"])}', 
            fontsize=self.chart_config['title_fontsize'], 
            weight='bold', 
            pad=20
        )
        ax.set_xlabel('X 坐标', fontsize=self.chart_config['label_fontsize'])
        ax.set_ylabel('Z 坐标', fontsize=self.chart_config['label_fontsize'])
        
        # 添加网格
        ax.grid(True, alpha=0.3, color=self.colors['grid'])
        ax.set_aspect('equal')
        
        # 设置背景图片或背景色
        self._set_background(ax, site_id, site_data)
    
    def _set_background(self, ax: plt.Axes, site_id: int, site_data: Dict[str, Any]):
        """
        设置图表背景（图片或颜色）
        
        Args:
            ax: matplotlib轴对象
            site_id: 站点ID
            site_data: 站点数据
        """
        # 检查是否有指定的背景图片
        if site_id in self.background_images:
            background_path = self.background_images[site_id]
            if os.path.exists(background_path):
                try:
                    # 加载背景图片
                    img = mpimg.imread(background_path)
                    
                    # 获取坐标范围
                    bounds = site_data['coordinate_bounds']
                    x_range = bounds['x_max'] - bounds['x_min']
                    z_range = bounds['z_max'] - bounds['z_min']
                    margin = max(x_range, z_range) * 0.1
                    
                    # 设置图片显示范围（与坐标轴范围一致）
                    extent = [
                        bounds['x_min'] - margin, 
                        bounds['x_max'] + margin,
                        bounds['z_min'] - margin, 
                        bounds['z_max'] + margin
                    ]
                    
                    # 显示背景图片
                    ax.imshow(img, extent=extent, aspect='auto', alpha=0.6, zorder=0)
                    self.logger.info(f"✅ 已为站点 {site_id} 设置背景图片: {background_path}")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ 加载背景图片失败 {background_path}: {e}")
                    # 如果图片加载失败，使用默认背景色
                    ax.set_facecolor(self.colors['background'])
            else:
                self.logger.warning(f"⚠️ 背景图片文件不存在: {background_path}")
                ax.set_facecolor(self.colors['background'])
        else:
            # 使用默认背景色
            ax.set_facecolor(self.colors['background'])
    
    def _create_legend(self, ax: plt.Axes, material_color_map: Dict[int, Any], 
                      materials: List[Dict[str, Any]]):
        """
        创建图例
        
        Args:
            ax: matplotlib轴对象
            material_color_map: 材料颜色映射
            materials: 材料列表
        """
        legend_elements = []
        
        # 装置状态图例
        fixture_colors = {
            'spawned': self.colors['fixture_spawned'],
            'harvested': self.colors['fixture_harvested']
        }
        
        for status, color in fixture_colors.items():
            legend_elements.append(
                patches.Patch(color=color, label=f'装置-{status}')
            )
        
        # 材料类型图例（显示前10种最常见的材料）
        material_counts = Counter()
        for material in materials:
            material_counts[material['material_id']] += material['quantity']
        
        top_materials = material_counts.most_common(10)
        for material_id, count in top_materials:
            if material_id in material_color_map:
                material_name = self.get_material_name(material_id)
                legend_elements.append(
                    patches.Patch(
                        color=material_color_map[material_id], 
                        label=f'{material_name} (×{count})'
                    )
                )
        
        ax.legend(
            handles=legend_elements, 
            loc='upper left', 
            bbox_to_anchor=(1.02, 1),
            fontsize=self.chart_config['legend_fontsize']
        )
    
    def _save_chart(self, fig: plt.Figure, filename: str, output_dir: str = "") -> str:
        """
        保存图表
        
        Args:
            fig: matplotlib图形对象
            filename: 文件名
            output_dir: 输出目录
            
        Returns:
            保存的文件路径
        """
        # 调整布局
        plt.tight_layout()
        
        # 生成输出文件路径
        output_file = os.path.join(output_dir, f"{filename}.png")
        
        # 保存图片
        fig.savefig(output_file, dpi=self.chart_config['dpi'], bbox_inches='tight')
        
        self.logger.info(f"✅ 图表已保存: {output_file}")
        return output_file
    
    def generate_material_summary_report(self, material_summary: Dict[str, Any], 
                                       output_file: str):
        """
        生成材料获取汇总报告
        
        Args:
            material_summary: 材料汇总数据
            output_file: 输出文件路径
        """
        self.logger.info("📊 生成材料获取汇总报告...")
        
        # 按总数量排序
        sorted_materials = sorted(
            material_summary.items(), 
            key=lambda x: x[1]['total_quantity'], 
            reverse=True
        )
        
        # 生成报告内容
        report_content = self._generate_report_content(sorted_materials, material_summary)
        
        # 保存报告
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        self.logger.info(f"✅ 材料汇总报告已保存: {output_file}")
    
    def _generate_report_content(self, sorted_materials: List[Tuple], 
                               material_summary: Dict[str, Any]) -> str:
        """
        生成报告内容
        
        Args:
            sorted_materials: 排序后的材料列表
            material_summary: 材料汇总数据
            
        Returns:
            报告内容字符串
        """
        report_content = f"""# MySekai 材料获取详细报告

生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 材料获取汇总

总材料种类: {len(material_summary)}
总材料数量: {sum(data['total_quantity'] for data in material_summary.values())}

## 📋 详细材料列表

| 材料名称 | 材料ID | 总数量 | 分布站点 | 位置数量 | 主要获取位置 |
|----------|--------|--------|----------|----------|--------------|
"""
        
        # 添加材料详细信息
        for material_id, data in sorted_materials:
            material_name = self.get_material_name(material_id)
            sites_str = ', '.join(map(str, sorted(data['sites'])))
            location_count = len(data['locations'])
            
            # 找出数量最多的位置
            best_location = max(data['locations'], key=lambda x: x['quantity'])
            best_pos = f"站点{best_location['site_id']}({best_location['x']},{best_location['z']})"
            
            report_content += f"| {material_name} | {material_id} | {data['total_quantity']} | {sites_str} | {location_count} | {best_pos} |\n"
        
        # 添加站点分布分析
        report_content += self._generate_site_analysis(material_summary)
        
        # 添加详细位置信息
        report_content += self._generate_detailed_locations(sorted_materials[:10])
        
        return report_content
    
    def _generate_site_analysis(self, material_summary: Dict[str, Any]) -> str:
        """
        生成站点分析内容
        
        Args:
            material_summary: 材料汇总数据
            
        Returns:
            站点分析内容
        """
        content = "\n## 🗺️ 站点分布分析\n\n"
        
        # 按站点统计
        site_stats = defaultdict(lambda: {'materials': 0, 'types': set(), 'locations': 0})
        
        for material_id, data in material_summary.items():
            for location in data['locations']:
                site_id = location['site_id']
                site_stats[site_id]['materials'] += location['quantity']
                site_stats[site_id]['types'].add(material_id)
                site_stats[site_id]['locations'] += 1
        
        for site_id in sorted(site_stats.keys()):
            stats = site_stats[site_id]
            site_name = self.get_site_name(site_id)
            # 生成材料类型名称列表
            material_names = [self.get_material_name(mid) for mid in sorted(stats['types'])]
            content += f"""
### {site_name}
- 材料总数: {stats['materials']}
- 材料种类: {len(stats['types'])}
- 采集点数: {stats['locations']}
- 材料类型: {', '.join(material_names)}
"""
        
        return content
    
    def _generate_detailed_locations(self, top_materials: List[Tuple]) -> str:
        """
        生成详细位置信息
        
        Args:
            top_materials: 前几种材料
            
        Returns:
            详细位置信息内容
        """
        content = "\n## 📍 详细位置信息\n\n"
        
        for material_id, data in top_materials:
            material_name = self.get_material_name(material_id)
            content += f"""
### {material_name} (ID: {material_id}, 总计 {data['total_quantity']} 个)

| 站点 | X坐标 | Z坐标 | 数量 | HP触发 | 序列 | 状态 |
|------|-------|-------|------|--------|------|------|
"""
            
            # 按数量排序位置
            sorted_locations = sorted(data['locations'], key=lambda x: x['quantity'], reverse=True)
            
            for loc in sorted_locations:
                content += f"| {loc['site_id']} | {loc['x']} | {loc['z']} | {loc['quantity']} | {loc['hp_trigger']} | {loc['seq']} | {loc['status']} |\n"
        
        return content
    
    def generate_all_maps(self, file_path: str, output_dir: str = "") -> Dict[str, Any]:
        """
        生成所有站点地图和报告
        
        Args:
            file_path: JSON文件路径
            output_dir: 输出目录
            
        Returns:
            生成结果信息
        """
        self.logger.info("🚀 开始生成MySekai收获地图...")
        
        try:
            # 分析数据
            analysis = self.analyze_harvest_maps(file_path)
            
            if not analysis:
                self.logger.error("❌ 数据分析失败")
                return {}
            
            # 保存分析结果
            analysis_file = os.path.join(output_dir, "material_location_analysis.json")
            with open(analysis_file, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, ensure_ascii=False, indent=2)
            self.logger.info(f"💾 分析结果已保存: {analysis_file}")
            
            # 生成站点地图
            map_files = []
            for site_id in sorted(analysis['sites_data'].keys()):
                site_data = analysis['sites_data'][site_id]
                map_file = self.generate_site_map(site_id, site_data, 
                                                analysis['material_summary'], output_dir)
                map_files.append(map_file)
            
            # 生成材料汇总报告
            report_file = os.path.join(output_dir, "material_harvest_summary.md")
            self.generate_material_summary_report(analysis['material_summary'], report_file)
            
            result = {
                'success': True,
                'analysis_file': analysis_file,
                'report_file': report_file,
                'map_files': map_files,
                'sites_count': len(analysis['sites_data']),
                'materials_count': len(analysis['material_summary']),
                'generated_files': [
                    f"分析文件: {analysis_file}",
                    f"汇总报告: {report_file}",
                    *[f"站点地图: {map_file}" for map_file in map_files]
                ]
            }
            
            self.logger.info("✅ MySekai收获地图生成完成!")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 生成过程中发生错误: {e}")
            return {}