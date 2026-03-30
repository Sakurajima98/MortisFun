#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件说明:
- PJSK 排名查询服务(pjskranking)
- 根据用户输入 `wcnsk100` / `wcnsk 100` / `wcnsk10-20` / `wcnsk 10-20` 查询指定排名用户信息
- 信息包含: score、name、rank、word；并结合历史记录计算过去一小时时速、近10次变化pt平均值、已停车时长
- 同时计算与阶段排名(Top100: 10/20/30/40/50/100；超过100时使用 border 的 200 等)的分数差距
- 返回一张整合图片(PNG)，并附带简要文本说明

跨平台说明:
- 路径与文件操作使用 `os.path`，兼容 Windows/Linux
- 字体加载优先使用系统中文字体(Windows: 微软雅黑；Linux: 文泉驿/Noto)，失败则回退到 PIL 默认字体
"""

import os
import asyncio
import re
import json
import math
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from PIL import Image, ImageDraw, ImageFont
import requests

from service.base_service import BaseService


class PJSKRankingService(BaseService):
    """类说明:
    - PJSK 排名查询服务
    - 负责解析用户消息、读取排名数据与历史、计算统计指标，并生成结果图片
    """

    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """函数说明:
        - 初始化服务实例，设置数据目录与常量
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.service_name = 'pjskranking'
        self.base_dir = os.path.join('data', 'pjsk', 'ranking')
        self.top100_path = os.path.join(self.base_dir, 'top100.json')
        self.border_path = os.path.join(self.base_dir, 'border.json')
        self.history_dir = os.path.join(self.base_dir, 'history')
        self.config_path = os.path.join(self.base_dir, 'config.json')
        self.output_dir = os.path.join('data', 'images', 'pjsk')
        os.makedirs(self.output_dir, exist_ok=True)
        self.user_dir = os.path.join('data', 'pjsk', 'user')
        os.makedirs(self.user_dir, exist_ok=True)
        self.profile_dir = os.path.join('data', 'pjsk', 'profile')
        os.makedirs(self.profile_dir, exist_ok=True)

        # Top100里用于阶段比较的里程碑排名
        self.top100_milestones = [10, 20, 30, 40, 50, 100]

        # 预加载字体(跨平台)
        self.fonts = self._load_fonts()
        # 最近截图节流记录（用户+URL -> 时间）
        self._recent_capture: Dict[str, datetime] = {}

    def get_help_text(self) -> Dict[str, Any]:
        """函数说明:
        - 返回服务帮助信息
        """
        tip_dir = self.base_dir
        return {
            'content': (
                "📊 PJSK排名查询服务\n"
                "可用指令：\n"
                "• wcnsk100 或 wcnsk 100  — 查询第100名的选手\n"
                "• wcnsk10-20 或 wcnsk 10-20 — 查询10到20名区间\n"
                f"数据来源：{tip_dir} (top100/border/history)\n"
            )
        }

    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """函数说明:
        - 处理用户输入并返回结果
        - 支持单排名与范围查询；单排名返回图片，范围返回文本列表
        """
        try:
            msg_raw = (message or '').strip()
            msg = msg_raw.lower().replace('\u3000', ' ')

            # SekaiRanking 接口：活动列表
            if re.match(r"^wcn\s*活动列表$", msg):
                data = self._sr_get('/events')
                if not data:
                    return {'content': '❌ 接口错误或无数据'}
                text = self._sr_format_events(data)
                return {'content': text}

            # SekaiRanking 接口：预测（最新）
            m_pred_latest = re.match(r"^wcn\s*预测$", msg)
            if m_pred_latest:
                data = self._sr_get('/predictions')
                if not data:
                    return {'content': '❌ 接口错误或无数据'}
                text = self._sr_format_json_brief(data, title='最新预测')
                return {'content': text}

            # SekaiRanking 接口：预测（指定活动ID）
            m_pred_eid = re.match(r"^wcn\s*预测\s*(\d+)$", msg)
            if m_pred_eid:
                eid = int(m_pred_eid.group(1))
                data = self._sr_get(f'/predictions/{eid}')
                if not data:
                    return {'content': f'❌ 接口错误或无数据: event_id={eid}'}
                text = self._sr_format_json_brief(data, title=f'预测 event {eid}')
                return {'content': text}

            # SekaiRanking 接口：预测历史（指定活动ID与rank）
            m_pred_hist = re.match(r"^wcn\s*预测历史\s*(\d+)\s*(\d+)$", msg)
            if m_pred_hist:
                eid = int(m_pred_hist.group(1))
                rank = int(m_pred_hist.group(2))
                data = self._sr_get(f'/predictions/{eid}/history', params={'rank': str(rank)})
                if not data:
                    return {'content': f'❌ 接口错误或无数据: event_id={eid}, rank={rank}'}
                text = self._sr_format_json_brief(data, title=f'预测历史 event {eid} rank {rank}')
                return {'content': text}

            # SekaiRanking 接口：指数（最新）
            m_index_latest = re.match(r"^wcn\s*指数$", msg)
            if m_index_latest:
                data = self._sr_get('/index')
                if not data:
                    return {'content': '❌ 接口错误或无数据'}
                text = self._sr_format_json_brief(data, title='最新指数')
                return {'content': text}

            # SekaiRanking 接口：指数（指定活动ID）
            m_index_eid = re.match(r"^wcn\s*指数\s*(\d+)$", msg)
            if m_index_eid:
                eid = int(m_index_eid.group(1))
                data = self._sr_get(f'/index/{eid}')
                if not data:
                    return {'content': f'❌ 接口错误或无数据: event_id={eid}'}
                text = self._sr_format_json_brief(data, title=f'指数 event {eid}')
                return {'content': text}

            # SekaiRanking 接口：K线（指定活动ID）
            m_kline_eid = re.match(r"^wcn\s*k线\s*(\d+)$", msg)
            if m_kline_eid:
                eid = int(m_kline_eid.group(1))
                data = self._sr_get(f'/kline/{eid}')
                if not data:
                    return {'content': f'❌ 接口错误或无数据: event_id={eid}'}
                text = self._sr_format_json_brief(data, title=f'K线 event {eid}')
                return {'content': text}

            # 榜线预测简：整合“预测+指数”文本摘要（可选 event_id）
            m_pred_brief = re.match(r"^wcn\s*榜线预测简(?:\s*(\d+))?$", msg)
            if m_pred_brief:
                eid = None
                if m_pred_brief.group(1):
                    eid = int(m_pred_brief.group(1))
                    pred = self._sr_get(f'/predictions/{eid}')
                else:
                    pred = self._sr_get('/predictions')
                if not pred:
                    return {'content': '❌ 预测接口错误或无数据'}
                # 解析预测事件ID
                pd = pred.get('data') if isinstance(pred, dict) else None
                event_id = (pd or {}).get('event_id') or eid
                # 指数：用预测的 event_id 对应指数
                idx = self._sr_get(f'/index/{event_id}') if event_id else self._sr_get('/index')
                text = self._sr_format_predict_summary(pred, idx)
                return {'content': text}

            # 榜线预测：自动进行首页网页截图
            if re.match(r"^wcn\s*榜线预测$", msg):
                url = "https://snowyviewer.exmeaning.com/prediction/"
                key = f"{user_id}:{url}"
                now_ts = datetime.now()
                last_ts = self._recent_capture.get(key)
                if last_ts and (now_ts - last_ts).total_seconds() < 10:
                    return None
                self._recent_capture[key] = now_ts
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._background_capture_and_send(url, kwargs.get('context', {}), emulate_mobile=False))
                except RuntimeError:
                    asyncio.create_task(self._background_capture_and_send(url, kwargs.get('context', {}), emulate_mobile=False))
                return None

            # 挖矿：根据绑定ID获取个人profile并计算剩余水晶/金币/心愿结晶，并输出图片
            if re.match(r"^/挖矿$", msg_raw) or re.match(r"^挖矿$", msg_raw):
                binding = self._load_binding(user_id)
                if not binding:
                    return {'content': "❌ 未绑定PJSK用户ID，请先使用 'wcn绑定 <id>'"}
                uid = int(binding.get('pjsk_user_id'))
                cfg = self._read_json(self.config_path)
                server = str(cfg.get('server', 'CN') or 'CN').upper()
                token = str(cfg.get('token', '')).strip()
                if not token:
                    return {'content': '❌ 未配置Haruki Sekai Token（data/pjsk/ranking/config.json 的 token）'}
                try:
                    prof = self._fetch_user_profile(uid, server, token)
                except Exception as e:
                    return {'content': f"❌ 获取个人信息失败: {e}"}
                stats = self._compute_mining_stats(prof)
                img_path = self._render_mining_image(stats)
                return {'image_path': img_path}

            # 网页截图命令: 支持 "网页截图 [url]"；未提供URL时使用绑定ID生成默认链接
            cap_m = re.match(r"^(?:网页截图)(?:\s+(https?://\S+))?$", msg_raw)
            if cap_m:
                if cap_m.group(1):
                    url = cap_m.group(1)
                else:
                    binding = self._load_binding(user_id)
                    if not binding:
                        return {'content': "❌ 未绑定PJSK用户ID，请先使用 'wcn绑定 <id>'"}
                    uid = int(binding.get('pjsk_user_id'))
                    url = (
                        f"https://pjsk-zh.mid.red/#"
                    )
                # 节流：同一用户+URL 在10秒内只发送一次
                key = f"{user_id}:{url}"
                now_ts = datetime.now()
                last_ts = self._recent_capture.get(key)
                if last_ts and (now_ts - last_ts).total_seconds() < 10:
                    return None
                self._recent_capture[key] = now_ts
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._background_capture_and_send(url, kwargs.get('context', {}), emulate_mobile=False))
                except RuntimeError:
                    asyncio.create_task(self._background_capture_and_send(url, kwargs.get('context', {}), emulate_mobile=False))
                return None

            # 网页下载命令: 支持 "网页下载 [url]"；未提供URL时默认下载 SekaiRanking 首页
            dl_m = re.match(r"^(?:网页下载)(?:\s+(https?://\S+))?$", msg_raw)
            if dl_m:
                url = dl_m.group(1) or "https://snowyviewer.exmeaning.com/prediction/"
                saved = self._download_webpage(url)
                if not saved:
                    return {'content': "❌ 下载失败或解析错误"}
                html_path = saved.get('html_path')
                assets_count = int(saved.get('assets_count', 0))
                return {'content': f"✅ 已下载网页到本地\nHTML: {html_path}\n资源文件: {assets_count} 个"}

            # 绑定命令: wcn绑定 123 或 wcn绑定123
            bind_m = re.match(r"^wcn绑定\s*(\d+)$", msg) or re.match(r"^wcn绑定(\d+)$", msg)
            if bind_m:
                pjsk_uid = int(bind_m.group(1))
                self._save_binding(user_id, pjsk_uid)
                return {'content': f"✅ 已绑定PJSK用户ID: {pjsk_uid}"}

            # 个人查询命令: wcnsk
            if msg == 'wcnsk':
                binding = self._load_binding(user_id)
                if not binding:
                    return {'content': "❌ 未绑定PJSK用户ID，请先使用 'wcn绑定 <id>'"}
                top100 = self._read_json(self.top100_path)
                border = self._read_json(self.border_path)
                cfg = self._read_json(self.config_path)
                event_id = int(cfg.get('event_id', 0))
                profile, dataset = self._get_profile_by_user_id(top100, border, int(binding.get('pjsk_user_id')))
                if not profile:
                    return {'content': "❌ 该用户不在Top100内"}
                now = datetime.now()
                latest_ts = self._extract_latest_snapshot_ts()
                uid = profile.get('userId')
                cur_score = int(profile.get('score', 0))
                history = self._collect_user_history(uid, event_id)
                stats = self._compute_extended_stats(history, cur_score, now)
                stage_info = self._compute_stage_diff(int(profile.get('rank', 0)), cur_score, top100, border)
                image_path = self._render_image(profile, stats, stage_info, latest_ts)
                return {'image_path': image_path}

            # 分数线信息: wcnsk线
            if re.match(r"^wcnsk\s*线$", msg):
                cfg = self._read_json(self.config_path)
                event_name = str(cfg.get('event_name', '未知活动'))
                end_time = self._parse_end_time(cfg.get('event_end_time'))
                now = datetime.now()
                top100 = self._read_json(self.top100_path)
                border = self._read_json(self.border_path)
                ts_top = self._latest_snapshot_ts('top100')
                ts_border = self._latest_snapshot_ts('border')
                img_path = self._render_line_image(event_name, end_time, now, top100, border, ts_top, ts_border)
                return {'image_path': img_path}

            # 档位时速: wcn时速
            if re.match(r"^wcn\s*时速$", msg):
                cfg = self._read_json(self.config_path)
                event_name = str(cfg.get('event_name', '未知活动'))
                end_time = self._parse_end_time(cfg.get('event_end_time'))
                now = datetime.now()
                top100 = self._read_json(self.top100_path)
                border = self._read_json(self.border_path)
                ts_top = self._latest_snapshot_ts('top100')
                ts_border = self._latest_snapshot_ts('border')
                img_path = self._render_speed_line_image(event_name, end_time, now, top100, border, ts_top, ts_border, int(cfg.get('event_id', 0)))
                return {'image_path': img_path}


            # 个人信息查询: wcn个人信息 [颜色/主题]（根据绑定ID抓取网页截图并返回，支持颜色覆盖）
            m_person = re.match(r"^wcn\s*个人信息(?:\s+(.+))?$", msg_raw)
            if m_person:
                binding = self._load_binding(user_id)
                if not binding:
                    return {'content': "❌ 未绑定PJSK用户ID，请先使用 'wcn绑定 <id>'"}
                uid = int(binding.get('pjsk_user_id'))
                url = (
                    f"https://sekaiprofile.exmeaning.com/profile/{uid}?token="
                    f"7ecfbda6567475312e012251ea3cef7ef96fa31758f3b24f9720a1dfc6ff744e"
                )
                key = f"{user_id}:{url}"
                now_ts = datetime.now()
                last_ts = self._recent_capture.get(key)
                if last_ts and (now_ts - last_ts).total_seconds() < 10:
                    return None
                self._recent_capture[key] = now_ts
                # 颜色/主题覆盖（可选）
                theme_token = (m_person.group(1) or '').strip()
                css = self._build_profile_css(theme_token)
                js = self._build_profile_js(theme_token)
                ctx = dict(kwargs.get('context', {}))
                if css:
                    ctx['css_overrides'] = css
                if js:
                    ctx['js_overrides'] = js
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._background_capture_and_send(url, ctx, emulate_mobile=True))
                except RuntimeError:
                    asyncio.create_task(self._background_capture_and_send(url, ctx, emulate_mobile=True))
                return None

            # 网页下载个人信息: 使用绑定的PJSK用户ID生成个人信息页地址并下载到本地
            if re.match(r"^网页下载个人信息$", msg_raw):
                binding = self._load_binding(user_id)
                if not binding:
                    return {'content': "❌ 未绑定PJSK用户ID，请先使用 'wcn绑定 <id>'"}
                uid = int(binding.get('pjsk_user_id'))
                url = (
                    f"https://sekaiprofile.exmeaning.com/profile/{uid}?token="
                    f"7ecfbda6567475312e012251ea3cef7ef96fa31758f3b24f9720a1dfc6ff744e"
                )
                saved = self._download_webpage(url)
                if not saved:
                    return {'content': "❌ 个人信息页面下载失败"}
                html_path = saved.get('html_path')
                assets_count = int(saved.get('assets_count', 0))
                return {'content': f"✅ 已下载个人信息页面\nHTML: {html_path}\n资源文件: {assets_count} 个"}

            if not self._is_rank_query(msg):
                return None

            # 记录服务使用
            self.log_service_usage(user_id, self.service_name, 'query')

            mode, a, b = self._parse_query(msg)
            top100 = self._read_json(self.top100_path)
            border = self._read_json(self.border_path)
            cfg = self._read_json(self.config_path)
            event_id = int(cfg.get('event_id', 0))

            if mode == 'single':
                rank = a
                profile, dataset = self._get_profile_by_rank(top100, border, rank)
                if not profile:
                    return {'content': f"❌ 未找到排名 {rank} 的数据，当前仅支持Top100与部分边界排名"}

                now = datetime.now()
                latest_ts = self._extract_latest_snapshot_ts()
                uid = profile.get('userId')
                cur_score = int(profile.get('score', 0))

                # 历史统计(24h范围内)
                history = self._collect_user_history(uid, event_id)
                stats = self._compute_extended_stats(history, cur_score, now)

                # 阶段差距
                stage_info = self._compute_stage_diff(rank, cur_score, top100, border)

                # 生成图片（仅返回图片，不附带文字消息）
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._background_send_single(profile, stats, stage_info, latest_ts, kwargs.get('context', {})))
                except RuntimeError:
                    asyncio.create_task(self._background_send_single(profile, stats, stage_info, latest_ts, kwargs.get('context', {})))
                return None

            else:
                start, end = a, b
                if start > end:
                    start, end = end, start
                profiles: List[Dict[str, Any]] = []
                ranks: List[int] = []
                for r in range(start, end + 1):
                    p, _ = self._get_profile_by_rank(top100, border, r)
                    if p:
                        profiles.append(p)
                        ranks.append(r)
                if not profiles:
                    return {'content': f"❌ 未找到区间 {start}-{end} 的任何数据"}

                # 为每个排名计算统计并绘制为纵向拼图
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._background_send_range(profiles, ranks, event_id, top100, border, kwargs.get('context', {})))
                except RuntimeError:
                    asyncio.create_task(self._background_send_range(profiles, ranks, event_id, top100, border, kwargs.get('context', {})))
                return None

        except Exception as e:
            return self.handle_error(e, context='pjskranking.process_message')

    # ======================== 内部工具函数 ========================

    def _is_rank_query(self, msg: str) -> bool:
        """函数说明: 判断是否为 wcnsk 排名查询指令"""
        return bool(re.match(r"^wcnsk\s*\d+(\s*-\s*\d+)?$", msg)) or bool(re.match(r"^wcnsk\d+(\-\d+)?$", msg))

    def _sr_base_url(self) -> str:
        """函数说明:
        - 返回 SekaiRanking API 基础 URL
        - 优先读取配置中的 `sekairanking_api_url`，否则使用默认
        """
        try:
            cfg = self._read_json(self.config_path)
            u = str(cfg.get('sekairanking_api_url', '')).strip()
            return u or 'https://sekairanking.exmeaning.com/api/v1'
        except Exception:
            return 'https://sekairanking.exmeaning.com/api/v1'

    def _sr_token(self) -> Optional[str]:
        """函数说明:
        - 返回 SekaiRanking API Token（仅读取配置文件 `sekairanking_token`）
        """
        try:
            cfg = self._read_json(self.config_path)
            tk = str(cfg.get('sekairanking_token', '')).strip()
            return tk or None
        except Exception:
            return None

    def _sr_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """函数说明:
        - 调用 SekaiRanking API GET 接口并返回 JSON
        - 自动附加鉴权头 `X-API-Token`
        """
        try:
            base = self._sr_base_url().rstrip('/')
            url = base + (path if path.startswith('/') else ('/' + path))
            tk = self._sr_token()
            headers = {'X-API-Token': tk} if tk else {}
            resp = requests.get(url, headers=headers, params=(params or {}), timeout=10)
            if resp.status_code != 200:
                return None
            return resp.json()
        except Exception:
            return None

    def _sr_ms_to_dt(self, ms: Optional[int]) -> Optional[datetime]:
        """函数说明:
        - 将毫秒时间戳(ms)转换为本地时间(datetime)
        """
        try:
            if ms is None:
                return None
            return datetime.fromtimestamp(int(ms) / 1000)
        except Exception:
            return None

    def _sr_format_predict_summary(self, pred_json: Dict[str, Any], index_json: Optional[Dict[str, Any]]) -> str:
        """函数说明:
        - 整合预测与指数为简要文本摘要，便于用户阅读
        - 包括：活动名称/ID/时间、是否当期、不同档线当前与预测分数、PGAI与最活跃/最不活跃档线
        """
        try:
            pd = pred_json.get('data') if isinstance(pred_json, dict) else None
            if not isinstance(pd, dict):
                return '❌ 预测数据格式错误'
            event_id = pd.get('event_id')
            event_name = pd.get('event_name', '')
            start_at = self._sr_ms_to_dt(pd.get('start_at'))
            end_at = self._sr_ms_to_dt(pd.get('end_at'))
            now = datetime.now()
            is_active = False
            try:
                if start_at and end_at:
                    is_active = (start_at <= now <= end_at)
                elif pd.get('progress') is not None:
                    prog = float(pd.get('progress') or 0.0)
                    is_active = (0.0 <= prog <= 100.0)
            except Exception:
                pass

            # 档线摘要
            lines = []
            lines.append(f"活动: {event_name} (ID: {event_id})")
            sa = start_at.strftime('%Y-%m-%d %H:%M:%S') if start_at else '未知'
            ea = end_at.strftime('%Y-%m-%d %H:%M:%S') if end_at else '未知'
            lines.append(f"时间: {sa} ~ {ea}")
            lines.append(f"当期活动: {'是' if is_active else '否'}")
            lines.append("")
            lines.append("档线（当前/预测，单位: 万）")
            ranks = pd.get('rankings') if isinstance(pd.get('rankings'), list) else []
            # 选取最多前12条，按 rank 升序
            try:
                ranks = sorted(ranks, key=lambda x: int(x.get('rank', 0)))[:12]
            except Exception:
                ranks = ranks[:12]
            for r in ranks:
                rk = r.get('rank')
                cur_w = self._to_w(r.get('current_score', 0))
                pred_w = self._to_w(r.get('predicted_score', 0))
                lines.append(f"- {rk}名: 当前 {cur_w:.2f}W / 预测 {pred_w:.2f}W")

            # 指数摘要
            lines.append("")
            lines.append("PJSK 全服实时积极指数 (PGAI)")
            idx_data = index_json.get('data') if isinstance(index_json, dict) else None
            if isinstance(idx_data, dict):
                pgai = idx_data.get('global_index')
                change = idx_data.get('global_change_pct')
                lines.append(f"- PGAI: {pgai}  (Δ {change:+.2f}%)" if isinstance(change, (int, float)) else f"- PGAI: {pgai}")
                tiers = idx_data.get('tier_indices') if isinstance(idx_data.get('tier_indices'), list) else []
                # 排序：最活跃按 ChangePct 降序，最不活跃按升序
                active_top = []
                inactive_bottom = []
                try:
                    tiers_sorted = sorted([t for t in tiers if isinstance(t.get('ChangePct'), (int, float))], key=lambda x: float(x.get('ChangePct')), reverse=True)
                    active_top = tiers_sorted[:3]
                    inactive_bottom = list(reversed(tiers_sorted[-3:])) if len(tiers_sorted) >= 3 else tiers_sorted[-3:]
                except Exception:
                    pass
                lines.append("- 最活跃档线 TOP3:")
                if active_top:
                    for t in active_top:
                        lines.append(f"  · Rank {t.get('Rank')}: Index {t.get('Index')}  (Δ {float(t.get('ChangePct')):+.2f}%)")
                else:
                    lines.append("  · 暂无数据")
                lines.append("- 最不活跃档线 BOTTOM3:")
                if inactive_bottom:
                    for t in inactive_bottom:
                        lines.append(f"  · Rank {t.get('Rank')}: Index {t.get('Index')}  (Δ {float(t.get('ChangePct')):+.2f}%)")
                else:
                    lines.append("  · 暂无数据")
            else:
                lines.append("- PGAI: 暂无数据")

            return '\n'.join(lines)
        except Exception:
            return '❌ 摘要生成失败'

    def _build_profile_css(self, token: Optional[str]) -> Optional[str]:
        """函数说明:"""
        try:
            t = (token or '').strip()
            if not t:
                cfg = self._read_json(self.config_path)
                t = str(cfg.get('profile_screenshot_css_default', '')).strip()
            if not t:
                return None
            def _hex_to_rgb(h: str) -> Optional[Tuple[int,int,int]]:
                s = h.strip()
                if not s:
                    return None
                if s.startswith('#'):
                    s = s[1:]
                if len(s) == 3:
                    s = ''.join([c*2 for c in s])
                if len(s) != 6:
                    return None
                try:
                    n = int(s, 16)
                    r = (n >> 16) & 255
                    g = (n >> 8) & 255
                    b = n & 255
                    return r, g, b
                except Exception:
                    return None
            def _rgb_to_hex(r: int, g: int, b: int) -> str:
                return '#{0:02x}{1:02x}{2:02x}'.format(max(0,min(255,r)), max(0,min(255,g)), max(0,min(255,b)))
            def _rgb_to_hsl(r: int, g: int, b: int) -> Tuple[float,float,float]:
                rf, gf, bf = r/255.0, g/255.0, b/255.0
                mx, mn = max(rf,gf,bf), min(rf,gf,bf)
                l = (mx + mn) / 2.0
                if mx == mn:
                    return 0.0, 0.0, l
                d = mx - mn
                s = d / (2.0 - mx - mn) if l > 0.5 else d / (mx + mn)
                if mx == rf:
                    h = (gf - bf) / d + (6 if gf < bf else 0)
                elif mx == gf:
                    h = (bf - rf) / d + 2
                else:
                    h = (rf - gf) / d + 4
                h /= 6.0
                return h, s, l
            def _hsl_to_rgb(h: float, s: float, l: float) -> Tuple[int,int,int]:
                def _hue2rgb(p: float, q: float, t: float) -> float:
                    if t < 0: t += 1
                    if t > 1: t -= 1
                    if t < 1/6: return p + (q - p) * 6 * t
                    if t < 1/2: return q
                    if t < 2/3: return p + (q - p) * (2/3 - t) * 6
                    return p
                if s == 0:
                    r = g = b = l
                else:
                    q = l * (1 + s) if l < 0.5 else (l + s - l * s)
                    p = 2 * l - q
                    r = _hue2rgb(p, q, h + 1/3)
                    g = _hue2rgb(p, q, h)
                    b = _hue2rgb(p, q, h - 1/3)
                return int(round(r*255)), int(round(g*255)), int(round(b*255))
            def _derive_dark(hex_color: str) -> str:
                rgb = _hex_to_rgb(hex_color)
                if not rgb:
                    return hex_color
                h, s, l = _rgb_to_hsl(*rgb)
                if l >= 0.7:
                    l = max(0.0, l - 0.25)
                    s = min(1.0, s + 0.1)
                else:
                    l = max(0.0, l - 0.18)
                r, g, b = _hsl_to_rgb(h, s, l)
                return _rgb_to_hex(r, g, b)
            def _derive_light_rgba(hex_color: str, alpha: float = 0.15) -> str:
                rgb = _hex_to_rgb(hex_color)
                if not rgb:
                    return 'rgba(51,204,187,{0})'.format(alpha)
                return 'rgba({0},{1},{2},{3})'.format(rgb[0], rgb[1], rgb[2], alpha)
            parts = re.split(r"[,\s]+", t)
            theme_in = parts[0] if parts else t
            accent_in = parts[1] if len(parts) > 1 else None
            preset = {
                '紫色': '#a366ff', 'blue': '#33a0ff', '蓝色': '#33a0ff', '粉色': '#ff8ab4',
                'pink': '#ff8ab4', 'teal': '#33ccbb', '青色': '#33ccbb', '绿色': '#22c55e',
                'green': '#22c55e', '橙色': '#f59e0b', 'orange': '#f59e0b'
            }
            def _normalize_color(x: str) -> Optional[str]:
                if not x:
                    return None
                x = x.strip().lower()
                if x in preset:
                    return preset[x]
                if x.startswith('#') and len(x) in (4,7):
                    return x
                m = re.match(r"^rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$", x)
                if m:
                    r, g, b = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    return _rgb_to_hex(r, g, b)
                return None
            base = _normalize_color(theme_in) or '#33ccbb'
            accent = _normalize_color(accent_in) or base
            dark = _derive_dark(base)
            light = _derive_light_rgba(base, 0.15)
            acc_dark = _derive_dark(accent)
            acc_light = _derive_light_rgba(accent, 0.6)
            acc_lighter = _derive_light_rgba(accent, 0.35)
            css = f"""
                :root {{ --theme-color: {base}; --theme-dark: {dark}; --theme-light: {light}; --p-teal: {base}; }}
                .top-deco {{ background: linear-gradient(135deg, var(--theme-color), var(--theme-dark)) !important; }}
                .avatar-frame {{ border-color: var(--theme-color) !important; }}
                .user-word {{ background: linear-gradient(90deg, var(--theme-light), transparent) !important; border-left-color: var(--theme-color) !important; }}
                .user-badges .badge.rank-badge {{ background: linear-gradient(135deg, var(--theme-color), var(--theme-dark)) !important; color: #fff !important; }}
                .user-badges .badge.power-badge {{ background: var(--theme-light) !important; color: var(--theme-color) !important; }}
                .challenge-capsule .capsule-value, #challengeScore {{ color: var(--theme-color) !important; }}
                .section-header {{ border-bottom-color: var(--theme-light) !important; }}
                .deck-name {{ color: var(--theme-color) !important; background: var(--theme-light) !important; }}
                .deck-card.is-leader {{ border-color: var(--theme-color) !important; }}
                .leader-ribbon {{ background: var(--theme-color) !important; color: #fff !important; }}
                .mr-badge {{ border-color: var(--theme-color) !important; background: var(--theme-light) !important; }}
                .stars .star-icon, .ribbon-icon {{ color: var(--theme-color) !important; }}
                .honor-lv {{ background: var(--theme-color) !important; }}
                .music-stat-val.fc {{ color: var(--theme-color) !important; }}
                .unit-tab.active {{ border-color: var(--theme-color) !important; }}
                .announcement-card {{ background: linear-gradient(135deg, {acc_light} 0%, {acc_lighter} 100%) !important; border: 1px solid {acc_dark} !important; }}
                .announcement-content {{ color: {acc_dark} !important; }}
                .announcement-content a {{ color: {accent} !important; }}
            """
            return css
        except Exception:
            return None

    def _build_profile_js(self, token: Optional[str]) -> Optional[str]:
        try:
            t = (token or '').strip()
            if not t:
                cfg = self._read_json(self.config_path)
                t = str(cfg.get('profile_screenshot_css_default', '')).strip()
            if not t:
                return None
            # 解析同 _build_profile_css 逻辑，提取主题色与派生色
            preset = {
                '紫色': '#a366ff', 'blue': '#33a0ff', '蓝色': '#33a0ff', '粉色': '#ff8ab4',
                'pink': '#ff8ab4', 'teal': '#33ccbb', '青色': '#33ccbb', '绿色': '#22c55e',
                'green': '#22c55e', '橙色': '#f59e0b', 'orange': '#f59e0b'
            }
            import re as _re
            def _hex_to_rgb(h: str):
                s = h.strip()
                if s.startswith('#'):
                    s = s[1:]
                if len(s) == 3:
                    s = ''.join([c*2 for c in s])
                if len(s) != 6:
                    return None
                try:
                    n = int(s, 16)
                    return (n>>16)&255, (n>>8)&255, n&255
                except Exception:
                    return None
            def _rgb_to_hex(r: int, g: int, b: int) -> str:
                return '#{0:02x}{1:02x}{2:02x}'.format(max(0,min(255,r)), max(0,min(255,g)), max(0,min(255,b)))
            def _rgb_to_hsl(r: int, g: int, b: int):
                rf, gf, bf = r/255.0, g/255.0, b/255.0
                mx, mn = max(rf,gf,bf), min(rf,gf,bf)
                l = (mx + mn) / 2.0
                if mx == mn:
                    return 0.0, 0.0, l
                d = mx - mn
                s = d / (2.0 - mx - mn) if l > 0.5 else d / (mx + mn)
                if mx == rf:
                    h = (gf - bf) / d + (6 if gf < bf else 0)
                elif mx == gf:
                    h = (bf - rf) / d + 2
                else:
                    h = (rf - gf) / d + 4
                return h/6.0, s, l
            def _hsl_to_rgb(h: float, s: float, l: float):
                def _hue2rgb(p, q, t):
                    if t < 0: t += 1
                    if t > 1: t -= 1
                    if t < 1/6: return p + (q - p) * 6 * t
                    if t < 1/2: return q
                    if t < 2/3: return p + (q - p) * (2/3 - t) * 6
                    return p
                if s == 0:
                    r = g = b = l
                else:
                    q = l * (1 + s) if l < 0.5 else (l + s - l * s)
                    p = 2 * l - q
                    r = _hue2rgb(p, q, h + 1/3)
                    g = _hue2rgb(p, q, h)
                    b = _hue2rgb(p, q, h - 1/3)
                return int(round(r*255)), int(round(g*255)), int(round(b*255))
            def _derive_dark(hex_color: str) -> str:
                rgb = _hex_to_rgb(hex_color)
                if not rgb:
                    return hex_color
                h, s, l = _rgb_to_hsl(*rgb)
                if l >= 0.7:
                    l = max(0.0, l - 0.25)
                    s = min(1.0, s + 0.1)
                else:
                    l = max(0.0, l - 0.18)
                r, g, b = _hsl_to_rgb(h, s, l)
                return _rgb_to_hex(r, g, b)
            def _derive_light_rgba(hex_color: str, alpha: float = 0.15) -> str:
                rgb = _hex_to_rgb(hex_color)
                if not rgb:
                    return 'rgba(51,204,187,{0})'.format(alpha)
                return 'rgba({0},{1},{2},{3})'.format(rgb[0], rgb[1], rgb[2], alpha)
            parts = _re.split(r"[,\s]+", t)
            theme_in = parts[0] if parts else t
            base = preset.get(theme_in.lower(), None) if theme_in else None
            if not base:
                if theme_in and theme_in.startswith('#') and len(theme_in) in (4,7):
                    base = theme_in
                else:
                    m = _re.match(r"^rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$", theme_in or '')
                    if m:
                        base = _rgb_to_hex(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            base = base or '#33ccbb'
            dark = _derive_dark(base)
            light = _derive_light_rgba(base, 0.15)
            js = (
                "(function(){"
                "try{var root=document.documentElement.style;"
                f"root.setProperty('--theme-color','{base}');"
                f"root.setProperty('--theme-dark','{dark}');"
                f"root.setProperty('--theme-light','{light}');"
                f"root.setProperty('--p-teal','{base}');"
                "var td=document.querySelector('.top-deco');"
                f"if(td) td.style.background='linear-gradient(135deg,{base},{dark})';"
                "var cs=document.getElementById('challengeScore');"
                f"if(cs) cs.style.color='{base}';"
                "}catch(e){}" 
                "})();"
            )
            return js
        except Exception:
            return None

    def _sr_format_json_brief(self, data: Dict[str, Any], title: str = '') -> str:
        """函数说明:
        - 将 JSON 数据简要格式化为文本，控制长度，适合群消息
        """
        try:
            s = json.dumps(data, ensure_ascii=False, indent=2)
            if len(s) > 1500:
                s = s[:1500] + '\n... (已截断)'
            return (title + '\n' + s) if title else s
        except Exception:
            return title or '数据格式化失败'

    def _sr_format_events(self, data: Dict[str, Any]) -> str:
        """函数说明:
        - 将活动列表数据格式化为多行文本
        """
        try:
            arr = []
            items = data.get('events') if isinstance(data, dict) else None
            if isinstance(items, list):
                for x in items[:30]:
                    eid = x.get('event_id') or x.get('id') or x.get('eventId')
                    name = x.get('name') or x.get('event_name') or ''
                    st = x.get('start_time') or x.get('start') or ''
                    et = x.get('end_time') or x.get('end') or ''
                    arr.append(f"- event_id={eid}  {name}  {st} ~ {et}")
            else:
                # 兜底：直接输出 JSON 简要
                return self._sr_format_json_brief(data, title='活动列表')
            if not arr:
                return '暂无活动数据'
            return '活动列表\n' + '\n'.join(arr)
        except Exception:
            return '活动数据解析失败'

    def _format_crystal_remaining(self, profile_json: Dict[str, Any]) -> str:
        try:
            cfg = self._read_json(self.config_path)
            total_music_count = int(cfg.get('total_music_count', 412) or 412)
            append_music_count = int(cfg.get('append_music_count', 77) or 77)
            fc_rewards = {
                'hard':   {'crystal': 50},
                'expert': {'crystal': 70},
                'master': {'crystal': 70},
            }
            coin_rewards = {
                'easy': 8500,
                'normal': 17000,
                'hard': 10500,
                'expert': 6000,
                'master': 9000,
                'append': 9000,
            }
            grade_rewards = {'C': 10, 'B': 20, 'A': 30, 'S': 50}
            total_base_songs = total_music_count
            dlist = profile_json.get('userMusicDifficultyClearCount') or []
            counts: Dict[str, Dict[str,int]] = {}
            for item in dlist:
                d = str(item.get('musicDifficultyType','')).lower()
                counts[d] = {
                    'liveClear': int(item.get('liveClear', 0) or 0),
                    'fullCombo': int(item.get('fullCombo', 0) or 0),
                    'allPerfect': int(item.get('allPerfect', 0) or 0),
                }
            def _get(d: str, k: str) -> int:
                return int(((counts.get(d) or {}).get(k) or 0))
            # 估计“已清谱的歌曲数”：取 base 难度中的最大 clear 计数
            cleared_est = max(_get('easy','liveClear'), _get('normal','liveClear'), _get('hard','liveClear'), _get('expert','liveClear'), _get('master','liveClear'))
            cleared_est = max(0, min(cleared_est, total_base_songs))
            grade_sum_per_song = grade_rewards['C'] + grade_rewards['B'] + grade_rewards['A'] + grade_rewards['S']
            grade_remaining_songs = max(0, total_base_songs - cleared_est)
            grade_remaining_total = grade_remaining_songs * grade_sum_per_song
            # FC剩余（水晶仅 hard/expert/master），按“已清但未FC/AP”的数量估计
            fc_remaining_total = 0
            fc_obtained_total = 0
            fc_detail: List[str] = []
            for d in ['hard','expert','master']:
                clear = _get(d,'liveClear')
                fc_ap = _get(d,'fullCombo') + _get(d,'allPerfect')
                rem = max(0, clear - fc_ap)
                per = int((fc_rewards.get(d) or {}).get('crystal') or 0)
                fc_remaining_total += rem * per
                fc_obtained_total += fc_ap * per
                fc_detail.append(f"{d}: 清谱 {clear}，FC/AP {fc_ap}，剩余 {rem} × {per}")
            remaining_total = grade_remaining_total + fc_remaining_total

            # 金币剩余（单位: 万）— 基于“每难度歌曲总数 - 已FC/AP数”
            coin_remaining_total = 0
            coin_detail: List[str] = []
            for d in ['easy','normal','hard','expert','master']:
                total = total_base_songs
                got = _get(d,'fullCombo') + _get(d,'allPerfect')
                rem = max(0, total - got)
                per = int(coin_rewards.get(d, 0))
                coin_remaining_total += rem * per
                coin_detail.append(f"{d}: 总 {total}，FC/AP {got}，剩余 {rem} × {per}")
            # append 难度
            got_app = _get('append','fullCombo') + _get('append','allPerfect')
            rem_app = max(0, append_music_count - got_app)
            coin_remaining_total += rem_app * int(coin_rewards.get('append', 0))
            coin_detail.append(f"append: 总 {append_music_count}，FC/AP {got_app}，剩余 {rem_app} × {coin_rewards.get('append', 0)}")
            coin_remaining_wan = coin_remaining_total / 10000.0
            lines = []
            lines.append("💎 剩余水晶估算（简化规则）")
            lines.append(f"- 基础曲目总数: {total_base_songs}")
            lines.append(f"- 已清谱(估计): {cleared_est}")
            lines.append(f"- 评分奖励剩余(未清谱×每曲{grade_sum_per_song}): {grade_remaining_songs} × {grade_sum_per_song} = {grade_remaining_total}")
            lines.append(f"- FC奖励剩余(硬/特/宗): {fc_remaining_total}")
            lines.append("  · 详情: " + "; ".join(fc_detail))
            lines.append(f"- 剩余水晶合计: {remaining_total}")
            lines.append(f"- 剩余金币合计: {coin_remaining_wan:.2f} 万")
            lines.append("  · 金币详情: " + "; ".join(coin_detail))
            lines.append("")
            lines.append("说明：")
            lines.append("- 评分奖励简化：清谱视为已达成S（累计获得S/A/B/C），未清视为未获得")
            lines.append("- FC奖励：已清但未FC/AP的曲目计入剩余；未清谱曲目因数据缺失未计入")
            lines.append("- 金币：每难度的剩余=总歌曲数-已FC/AP；append 难度使用配置的数量")
            return "\n".join(lines)
        except Exception:
            return "❌ 剩余水晶计算失败"

    def _compute_mining_stats(self, profile_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        文件级注释: 本函数用于从 Haruki Sekai 的用户 `profile` JSON 中提取曲目完成数据，
        并在“简化规则”下汇总矿产收益统计。统计包括：水晶（评分与FC）、金币（各难度）、心愿结晶（APPEND）。

        类级注释: 归属于 `PJSKRankingService`，作为 `/挖矿` 指令的数据准备层，供图片渲染函数使用。

        函数级注释:
        - 输入: `profile_json` 为 Haruki Sekai 的 `profile` 响应。
        - 处理: 读取 `userMusicDifficultyClearCount`，估算清谱数、FC/AP数，并结合配置与固定奖励表计算总量/已获得/剩余。
        - 输出: 结构化字典，包含水晶/金币/心愿结晶的统计，以及用于渲染的分项细节。
        详细说明:
        - 评分奖励简化: 若一首歌在任意基础难度下已 `liveClear`，则视为已获得 S/A/B/C 四档累计奖励；未清谱则四档均未获得。
        - FC奖励: 仅统计 `hard/expert/master` 三个基础难度；“已清但未FC/AP”的曲目计为剩余。
        - 金币: 以“每难度的总歌曲数 - 已FC/AP数”为剩余；`append` 使用配置的曲目数量。
        - 心愿结晶: 基于 `append` 的 FC/AP 统计，剩余 = `append_total - append_fc_ap`，每曲 15。
        """
        # 读取基础配置（总歌曲数与 append 数量）
        cfg = self._read_json(self.config_path)
        total_music_count = int(cfg.get('total_music_count', 412) or 412)
        append_music_count = int(cfg.get('append_music_count', 77) or 77)

        # 固定奖励配置（若后续需要可迁移到 config.json）
        fc_rewards = {
            'hard':   {'crystal': 50},
            'expert': {'crystal': 70},
            'master': {'crystal': 70},
        }
        coin_rewards = {
            'easy': 8500,
            'normal': 17000,
            'hard': 10500,
            'expert': 6000,
            'master': 9000,
            'append': 9000,
        }
        grade_rewards = {'C': 10, 'B': 20, 'A': 30, 'S': 50}

        # 解析难度统计
        dlist = profile_json.get('userMusicDifficultyClearCount') or []
        counts: Dict[str, Dict[str, int]] = {}
        for item in dlist:
            d = str(item.get('musicDifficultyType', '')).lower()
            counts[d] = {
                'liveClear': int(item.get('liveClear', 0) or 0),
                'fullCombo': int(item.get('fullCombo', 0) or 0),
                'allPerfect': int(item.get('allPerfect', 0) or 0),
            }
        def _get(d: str, k: str) -> int:
            return int(((counts.get(d) or {}).get(k) or 0))

        # 估算清谱歌曲数：取基础难度的最大 liveClear 计数（避免重复累计）
        base_diffs = ['easy', 'normal', 'hard', 'expert', 'master']
        cleared_est = max(_get('easy', 'liveClear'), _get('normal', 'liveClear'), _get('hard', 'liveClear'), _get('expert', 'liveClear'), _get('master', 'liveClear'))
        cleared_est = max(0, min(cleared_est, total_music_count))

        # 评分奖励统计：按“最大 clear 数”计算（总曲目 - max(各难度 clear)）× 每曲评分奖励
        grade_sum_per_song = grade_rewards['C'] + grade_rewards['B'] + grade_rewards['A'] + grade_rewards['S']
        grade_total = total_music_count * grade_sum_per_song
        base_clear_max = max(_get('easy', 'liveClear'), _get('normal', 'liveClear'), _get('hard', 'liveClear'), _get('expert', 'liveClear'), _get('master', 'liveClear'))
        grade_obtained = base_clear_max * grade_sum_per_song
        grade_remaining_songs = max(0, total_music_count - base_clear_max)
        grade_remaining = grade_remaining_songs * grade_sum_per_song

        # FC奖励统计（hard/expert/master）：剩余=总歌曲数 - FC 数；AP ⊆ FC，不重复累计
        fc_obtained_total = 0
        fc_remaining_total = 0
        fc_remaining_by_diff: Dict[str, int] = {}
        for d in ['hard', 'expert', 'master']:
            fc_only = _get(d, 'fullCombo')
            rem = max(0, total_music_count - fc_only)
            per = int((fc_rewards.get(d) or {}).get('crystal') or 0)
            fc_obtained_total += fc_only * per
            fc_remaining_total += rem * per
            fc_remaining_by_diff[d] = rem * per

        # 心愿结晶（APPEND）：按 FC/AP 统计
        # 心愿结晶（APPEND）：按“至少 FC”的曲目计数；AP ⊆ FC
        append_fc_ap = max(_get('append', 'fullCombo'), _get('append', 'allPerfect'))
        wish_per_song = 15
        wish_total = append_music_count * wish_per_song
        wish_obtained = append_fc_ap * wish_per_song
        wish_remaining = max(0, append_music_count - append_fc_ap) * wish_per_song

        # 金币统计：各难度剩余=总歌曲数-已FC；append 用配置数量
        coin_total = 0
        coin_obtained = 0
        coin_remaining = 0
        coin_remaining_by_diff: Dict[str, int] = {}
        for d in ['easy', 'normal', 'hard', 'expert', 'master']:
            total = total_music_count
            # 金币计算与水晶判断一致：仅按 FC 统计，AP 作为 FC 子集不重复累计
            got = _get(d, 'fullCombo')
            rem = max(0, total - got)
            per = int(coin_rewards.get(d, 0))
            coin_total += total * per
            coin_obtained += got * per
            coin_remaining += rem * per
            coin_remaining_by_diff[d] = rem * per
        # append 难度
        per_app = int(coin_rewards.get('append', 0))
        append_fc_only = _get('append', 'fullCombo')
        rem_app = max(0, append_music_count - append_fc_only)
        coin_total += append_music_count * per_app
        coin_obtained += append_fc_only * per_app
        coin_remaining += rem_app * per_app
        coin_remaining_by_diff['append'] = rem_app * per_app

        # 总水晶（评分+FC）的已获得/剩余
        crystal_total = grade_total + sum(total_music_count * int((fc_rewards.get(d) or {}).get('crystal') or 0) for d in ['hard', 'expert', 'master'])
        crystal_obtained = grade_obtained + fc_obtained_total
        crystal_remaining = grade_remaining + fc_remaining_total

        # 汇总输出结构供图片渲染
        now_iso = datetime.now().isoformat(timespec='seconds')
        return {
            'generated_at': now_iso,
            'summary': {
                'total_music_count': total_music_count,
                'append_music_count': append_music_count
            },
            'crystal': {
                'total': crystal_total,
                'obtained_total': crystal_obtained,
                'remaining_total': crystal_remaining,
                'grade_sum_per_song': grade_sum_per_song,
                'grade_total': grade_total,
                'grade_obtained': grade_obtained,
                'grade_remaining': grade_remaining,
                'fc_obtained_total': fc_obtained_total,
                'fc_remaining_total': fc_remaining_total,
                'fc_remaining_by_diff': fc_remaining_by_diff
            },
            'coin': {
                'total': coin_total,
                'obtained_total': coin_obtained,
                'remaining_total': coin_remaining,
                'remaining_by_diff': coin_remaining_by_diff
            },
            'wish': {
                'total': wish_total,
                'obtained_total': wish_obtained,
                'remaining_total': wish_remaining
            }
        }

    def _render_mining_image(self, stats: Dict[str, Any]) -> str:
        """
        文件级注释: 将 `_compute_mining_stats` 的结构化统计结果以双栏卡片形式渲染为 PNG 图片。

        类级注释: 归属于 `PJSKRankingService` 的展示层，生成可直接发送的图片文件。

        函数级注释:
        - 输入: `stats` 字典包含水晶/金币/心愿结晶统计与生成时间。
        - 输出: 本地图片路径字符串。
        渲染说明:
        - 画布采用白底，顶部标题居中，右上角显示“数据更新时间”。
        - 左卡片：水晶统计；右卡片：金币统计；底部备注。
        - 字体复用服务内统一中文字体加载逻辑。
        """
        # 画布与字体
        width = 1100
        height = 860
        margin = 30
        col_gap = 30
        img = Image.new('RGB', (width, height), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)
        font_big = self.fonts.get('big') or ImageFont.load_default()
        font_mid = self.fonts.get('mid') or ImageFont.load_default()
        font_small = self.fonts.get('small') or ImageFont.load_default()

        # 标题与时间
        title = '挖矿收益概览'
        tb = draw.textbbox((0, 0), title, font=font_big)
        title_x = (width - (tb[2] - tb[0])) // 2
        draw.text((title_x, margin), title, fill=(0, 0, 0), font=font_big)
        ts_text = f"数据更新时间：{stats.get('generated_at', '')}"
        ts_bbox = draw.textbbox((0, 0), ts_text, font=font_small)
        draw.text((width - margin - (ts_bbox[2] - ts_bbox[0]), margin + 4), ts_text, fill=(120, 120, 120), font=font_small)

        # 卡片区域计算
        card_top = margin + 60
        card_w = (width - 2 * margin - col_gap) // 2
        card_h = height - card_top - margin - 60
        left_x = margin
        right_x = margin + card_w + col_gap

        # 绘制卡片边框
        def draw_card(x: int, y: int, w: int, h: int):
            draw.rounded_rectangle([x, y, x + w, y + h], radius=12, outline=(200, 200, 200), width=2, fill=(245, 248, 250))

        draw_card(left_x, card_top, card_w, card_h)
        draw_card(right_x, card_top, card_w, card_h)

        # 左卡片：水晶
        cx = left_x + 20
        cy = card_top + 16
        draw.text((cx, cy), '水晶', fill=(0, 0, 0), font=font_mid)
        cy += 36
        crystal = stats.get('crystal', {})
        wish = stats.get('wish', {})
        # 行内容
        draw.text((cx, cy), f"剩余可获得总水晶数：{int(crystal.get('remaining_total', 0))}", fill=(0, 0, 0), font=font_small); cy += 28
        fc_by_diff = crystal.get('fc_remaining_by_diff') or {}
        draw.text((cx, cy), f"Hard：{int(fc_by_diff.get('hard', 0))}", fill=(0, 0, 0), font=font_small); cy += 24
        draw.text((cx, cy), f"EXPERT：{int(fc_by_diff.get('expert', 0))}", fill=(0, 0, 0), font=font_small); cy += 24
        draw.text((cx, cy), f"MASTER：{int(fc_by_diff.get('master', 0))}", fill=(0, 0, 0), font=font_small); cy += 28
        # 评分奖励合并为一条
        draw.text((cx, cy), f"评分奖励：{int(crystal.get('grade_remaining', 0))}", fill=(0, 0, 0), font=font_small); cy += 28
        # FC 总剩余
        draw.text((cx, cy), f"FC奖励（剩余）：{int(crystal.get('fc_remaining_total', 0))}", fill=(0, 0, 0), font=font_small); cy += 28
        # 已获得水晶
        draw.text((cx, cy), f"已获得水晶数：{int(crystal.get('obtained_total', 0))}", fill=(0, 0, 0), font=font_small); cy += 28
        # 心愿结晶
        draw.text((cx, cy), f"心愿结晶（碎片）剩余可获得：{int(wish.get('remaining_total', 0))}", fill=(0, 0, 0), font=font_small); cy += 24

        # 右卡片：金币
        gx = right_x + 20
        gy = card_top + 16
        draw.text((gx, gy), '金币', fill=(0, 0, 0), font=font_mid)
        gy += 36
        coin = stats.get('coin', {})
        coin_by = coin.get('remaining_by_diff') or {}
        rem_w = (round(float(coin.get('remaining_total', 0)), -2) / 10000.0)
        draw.text((gx, gy), f"剩余可获取总金币数：{rem_w:.2f} w", fill=(0, 0, 0), font=font_small); gy += 28
        for k in ['EASY', 'NORMAL', 'HARD', 'EXPERT', 'MASTER', 'APPEND']:
            v = (round(float(coin_by.get(k.lower(), 0)), -2) / 10000.0)
            draw.text((gx, gy), f"{k}：{v:.2f} w", fill=(0, 0, 0), font=font_small)
            gy += 24
        gy += 8
        obt_w = (round(float(coin.get('obtained_total', 0)), -2) / 10000.0)
        draw.text((gx, gy), f"已获得金币数：{obt_w:.2f} w", fill=(0, 0, 0), font=font_small)

        # 底部说明（包含误差与公式）
        foot_lines = [
            "计算说明（存在误差）",
            "评分奖励：剩余 = 总曲目 - max(基础难度clear) × 110",
            "FC水晶：剩余 = 总曲目 - FC × 每曲（Hard50 / Expert70 / Master70）",
            "金币：剩余 = 总曲目 - FC × 每曲（各难度金币值；append 9000/曲）",
            "心愿碎片：append 至少FC × 15；append 不产生水晶",
            "误差来源：缺少曲目级评级与clear明细，仅以聚合计数推断"
        ]
        # 计算说明区域高度
        line_h = 22
        explain_h = len(foot_lines) * line_h
        ex_y = height - margin - explain_h
        ex_x = margin
        for i, text in enumerate(foot_lines):
            draw.text((ex_x, ex_y + i * line_h), text, fill=(120, 120, 120), font=font_small)

        # 保存到输出目录
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        fname = f"pjsk_mining_{ts}.png"
        path = os.path.join(self.output_dir, fname)
        img.save(path, 'PNG', quality=95)
        return path

    def _parse_query(self, msg: str) -> Tuple[str, int, Optional[int]]:
        """函数说明: 解析排名查询指令，返回 (mode, a, b)"""
        s = msg.replace(' ', '')
        m = re.match(r"^wcnsk(\d+)-(\d+)$", s)
        if m:
            return 'range', int(m.group(1)), int(m.group(2))
        m = re.match(r"^wcnsk(\d+)$", s)
        if m:
            return 'single', int(m.group(1)), None
        # 宽松匹配带空格
        m = re.match(r"^wcnsk\s*(\d+)\s*-\s*(\d+)$", msg)
        if m:
            return 'range', int(m.group(1)), int(m.group(2))
        m = re.match(r"^wcnsk\s*(\d+)$", msg)
        if m:
            return 'single', int(m.group(1)), None
        return 'single', 0, None

    async def _background_send_single(self, profile: Dict[str, Any], stats: Dict[str, Any], stage_info: Dict[str, Any], latest_ts: datetime, context: Dict[str, Any]) -> None:
        try:
            path = await asyncio.to_thread(self._render_image, profile, stats, stage_info, latest_ts)
            await self._send_image_path(path, context)
        except Exception:
            pass

    async def _background_send_range(self, profiles: List[Dict[str, Any]], ranks: List[int], event_id: int, top100: Dict[str, Any], border: Dict[str, Any], context: Dict[str, Any]) -> None:
        try:
            path = await asyncio.to_thread(self._render_range_image, profiles, ranks, event_id, top100, border)
            await self._send_image_path(path, context)
        except Exception:
            pass


    async def _send_image_path(self, path: str, context: Dict[str, Any]) -> None:
        try:
            if not path:
                return
            abs_path = os.path.abspath(path)
            mt = context.get('message_type')
            if mt == 'private':
                target_id = str(context.get('user_id', ''))
                if not target_id:
                    return
                payload = {
                    "action": "send_private_msg",
                    "params": {
                        "user_id": target_id,
                        "message": [{"type": "image", "data": {"file": f"file://{abs_path}"}}]
                    }
                }
            else:
                group_id = str(context.get('group_id', ''))
                if not group_id:
                    return
                payload = {
                    "action": "send_group_msg",
                    "params": {
                        "group_id": group_id,
                        "message": [{"type": "image", "data": {"file": f"file://{abs_path}"}}]
                    }
                }
            if hasattr(self, 'server') and self.server:
                await self.server.send_response_to_napcat(payload)
        except Exception:
            pass

    async def _background_capture_and_send(self, url: str, context: Dict[str, Any], emulate_mobile: bool = False) -> None:
        """
        函数说明:
        - 后台抓取指定网页截图并通过 napcat 推送图片
        - 支持通过 `emulate_mobile=True` 启用手机端浏览器模拟以进行截图
        - 优先尝试 Playwright(Chromium)；缺失或失败则回退到 Selenium(Chrome)
        - 截图保存至 `data/web_capture/screenshots`
        """
        try:
            css_overrides = None
            try:
                css_overrides = (context or {}).get('css_overrides')
            except Exception:
                css_overrides = None
            js_overrides = None
            try:
                js_overrides = (context or {}).get('js_overrides')
            except Exception:
                js_overrides = None
            path = await asyncio.to_thread(self._capture_screenshot, url, emulate_mobile, css_overrides, js_overrides)
            await self._send_image_path(path, context)
        except Exception:
            pass

    def _capture_screenshot(self, url: str, emulate_mobile: bool = False, css_overrides: Optional[str] = None, js_overrides: Optional[str] = None) -> Optional[str]:
        """
        函数说明:
        - 抓取网页截图并返回本地文件路径
        - 当 `emulate_mobile=True` 时，模拟手机端浏览器环境后再进行截图

        实现细节：
        - 优先使用 Playwright 的 Chromium 无头浏览器进行全页截图；
        - 如 Playwright 不可用，尝试 Selenium Chrome 无头截图；
        - 任一成功则返回截图的绝对路径，否则返回 None。
        """
        try:
            # 输出目录：data/web_capture/screenshots
            out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'web_capture', 'screenshots'))
            os.makedirs(out_dir, exist_ok=True)
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            out_path = os.path.join(out_dir, f"screenshot_{ts}.png")

            # 优先 Playwright（同步API，避免事件循环冲突）
            try:
                from playwright.sync_api import sync_playwright
                def _do_playwright():
                    with sync_playwright() as p:
                        browser = p.chromium.launch(headless=True)
                        if emulate_mobile:
                            device = p.devices.get('Pixel 5') or p.devices.get('iPhone 12') or {}
                            context = browser.new_context(**device)
                        else:
                            context = browser.new_context(device_scale_factor=2)
                        page = context.new_page()
                        page.goto(url, wait_until='networkidle')
                        if css_overrides:
                            try:
                                page.add_style_tag(content=css_overrides)
                            except Exception:
                                pass
                        if js_overrides:
                            try:
                                page.add_script_tag(content=js_overrides)
                            except Exception:
                                pass
                        if not emulate_mobile:
                            page.set_viewport_size({"width": 980, "height": 1200})
                        page.screenshot(path=out_path, full_page=True)
                        browser.close()
                _do_playwright()
                return out_path if os.path.exists(out_path) else None
            except Exception as e:
                self.logger.warning(f"Playwright 截图失败或不可用，将尝试 Selenium：{e}")

            # 回退 Selenium（适配 Linux 云环境）
            try:
                from selenium import webdriver
                from selenium.webdriver.chrome.options import Options
                options = Options()
                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                options.add_argument('--hide-scrollbars')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                if emulate_mobile:
                    options.add_experimental_option('mobileEmulation', {'deviceName': 'Pixel 5'})
                    options.add_argument('--window-size=390,844')
                else:
                    options.add_argument('--window-size=980,1200')
                driver = webdriver.Chrome(options=options)
                driver.get(url)
                if css_overrides:
                    try:
                        driver.execute_script("""
                            var style = document.createElement('style');
                            style.type = 'text/css';
                            style.appendChild(document.createTextNode(arguments[0]));
                            document.head.appendChild(style);
                        """, css_overrides)
                    except Exception:
                        pass
                if js_overrides:
                    try:
                        driver.execute_script(js_overrides)
                    except Exception:
                        pass
                driver.save_screenshot(out_path)
                driver.quit()
                return out_path if os.path.exists(out_path) else None
            except Exception as e2:
                self.logger.error(f"Selenium 截图失败：{e2}")
                return None
        except Exception as e:
            self.logger.error(f"网页截图异常：{e}")
            return None

    def _download_webpage(self, url: str) -> Optional[Dict[str, Any]]:
        """函数说明:
        - 下载指定网页及其主要静态资源到本地目录，便于后续修改与离线预览
        - 仅处理同源下的 CSS/JS/图片等常见资源，第三方绝对链接保留原样
        - 输出目录: data/web_capture/site/<host>/，主页保存为 index.html
        """
        try:
            from urllib.parse import urlparse, urljoin
            base_url = url.strip()
            if not re.match(r"^https?://", base_url):
                return None
            resp = requests.get(base_url, timeout=10)
            if resp.status_code != 200:
                return None
            html = resp.text or ""
            pr = urlparse(base_url)
            host = pr.netloc.replace(':', '_')
            out_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'web_capture', 'site', host))
            os.makedirs(out_root, exist_ok=True)
            # 收集资源链接
            assets: List[Tuple[str, str]] = []
            def add_asset(link: str) -> None:
                try:
                    full = urljoin(base_url, link)
                    p = urlparse(full)
                    if p.netloc != pr.netloc:
                        return
                    rel = p.path
                    if rel.startswith('/'):
                        rel = rel[1:]
                    local_path = os.path.join(out_root, rel.replace('/', os.sep))
                    assets.append((full, local_path))
                except Exception:
                    pass
            # link href / script src / img src
            for m in re.finditer(r"<(?:link|script|img)[^>]+?(?:href|src)=[\"']([^\"']+)[\"']", html, flags=re.IGNORECASE):
                add_asset(m.group(1))
            # 简单处理 CSS 中的 url(...) 引用（仅主页内联样式）
            for m in re.finditer(r"url\(\s*['\"]?([^'\"\)]+)['\"]?\s*\)", html, flags=re.IGNORECASE):
                add_asset(m.group(1))
            # 下载资源
            for full, local in assets:
                try:
                    os.makedirs(os.path.dirname(local), exist_ok=True)
                    r = requests.get(full, timeout=10)
                    if r.status_code == 200:
                        mode = 'wb'
                        data = r.content
                        with open(local, mode) as f:
                            f.write(data)
                        # 重写主页 HTML 中的引用为相对路径
                        rel_path = os.path.relpath(local, out_root).replace('\\', '/')
                        html = re.sub(re.escape(full), rel_path, html)
                    else:
                        continue
                except Exception:
                    continue
            # 保存主页
            html_path = os.path.join(out_root, 'index.html')
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html)
            return {
                'html_path': html_path,
                'assets_count': len(assets),
                'out_root': out_root
            }
        except Exception:
            return None

    def _read_json(self, path: str) -> Dict[str, Any]:
        """函数说明: 读取JSON文件，失败返回空字典"""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_binding(self, qq: str, pjsk_user_id: int) -> None:
        """函数说明: 保存QQ到PJSK用户ID的绑定"""
        try:
            os.makedirs(self.user_dir, exist_ok=True)
            path = os.path.join(self.user_dir, f"{qq}.json")
            obj = {
                'qq': str(qq),
                'pjsk_user_id': int(pjsk_user_id),
                'bound_at': datetime.now().isoformat()
            }
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _load_binding(self, qq: str) -> Optional[Dict[str, Any]]:
        """函数说明: 读取QQ绑定信息"""
        try:
            path = os.path.join(self.user_dir, f"{qq}.json")
            if not os.path.isfile(path):
                return None
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None

    def _get_profile_by_rank(self, top100: Dict[str, Any], border: Dict[str, Any], rank: int) -> Tuple[Optional[Dict[str, Any]], str]:
        """函数说明:
        - 根据排名在 top100 或 border 中获取对应用户信息
        - 返回 (profile, dataset_tag)
        """
        try:
            if rank <= 100:
                arr = (top100 or {}).get('rankings', [])
                for x in arr:
                    if int(x.get('rank', -1)) == rank:
                        return x, 'top100'
            # 查找边界数据(如200等)
            barr = (border or {}).get('borderRankings', [])
            for x in barr:
                if int(x.get('rank', -1)) == rank:
                    return x, 'border'
            return None, ''
        except Exception:
            return None, ''

    def _get_profile_by_user_id(self, top100: Dict[str, Any], border: Dict[str, Any], user_id: int) -> Tuple[Optional[Dict[str, Any]], str]:
        """函数说明: 根据 userId 在 top100 或 border 中查找用户"""
        try:
            for x in (top100 or {}).get('rankings', []):
                if int(x.get('userId', -1)) == int(user_id):
                    return x, 'top100'
            for x in (border or {}).get('borderRankings', []):
                if int(x.get('userId', -1)) == int(user_id):
                    return x, 'border'
            return None, ''
        except Exception:
            return None, ''

    def _latest_snapshot_ts(self, kind: str) -> Optional[datetime]:
        """函数说明: 获取最新快照时间戳(kind: 'top100'|'border')"""
        try:
            if not os.path.isdir(self.history_dir):
                return None
            ts_list: List[datetime] = []
            for name in os.listdir(self.history_dir):
                if kind == 'top100' and name.endswith('_top100.json'):
                    m = re.match(r"^(\d+)_(\d{8}_\d{6})_top100\.json$", name)
                    if m:
                        ts_list.append(datetime.strptime(m.group(2), '%Y%m%d_%H%M%S'))
                elif kind == 'border' and name.endswith('_border.json'):
                    m = re.match(r"^(\d+)_(\d{8}_\d{6})_border\.json$", name)
                    if m:
                        ts_list.append(datetime.strptime(m.group(2), '%Y%m%d_%H%M%S'))
            return max(ts_list) if ts_list else None
        except Exception:
            return None

    def _parse_end_time(self, value: Any) -> Optional[datetime]:
        """函数说明: 解析结束时间字符串为datetime"""
        try:
            if not value:
                return None
            s = str(value).strip().replace('Z', '')
            for fmt in [
                '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M',
                '%Y/%m/%d %H:%M:%S', '%Y/%m/%d %H:%M',
                '%Y%m%d_%H%M%S', '%Y%m%d%H%M%S'
            ]:
                try:
                    return datetime.strptime(s, fmt)
                except Exception:
                    continue
            try:
                return datetime.fromisoformat(s)
            except Exception:
                return None
        except Exception:
            return None

    def _format_remaining(self, end_time: Optional[datetime], now: datetime) -> str:
        """函数说明: 计算活动剩余时间文本"""
        if not end_time:
            return '未知'
        delta = end_time - now
        if delta.total_seconds() < 0:
            delta = -delta
        return self._format_timedelta(delta)

    def _get_rank_score(self, data: Dict[str, Any], rank: int, kind: str) -> Optional[int]:
        """函数说明: 从数据中获取指定排名的分数"""
        try:
            key = 'rankings' if kind == 'top100' else 'borderRankings'
            for x in (data or {}).get(key, []):
                if int(x.get('rank', -1)) == int(rank):
                    return int(x.get('score', 0))
            return None
        except Exception:
            return None

    def _collect_rank_history(self, event_id: int, rank: int, kind: str) -> List[Tuple[datetime, int]]:
        """函数说明: 收集指定排名在最近24小时的分数历史(kind: 'top100'|'border')"""
        out: List[Tuple[datetime, int]] = []
        try:
            if not os.path.isdir(self.history_dir):
                return out
            cutoff = datetime.now() - timedelta(hours=24)
            for name in sorted(os.listdir(self.history_dir)):
                if kind == 'top100' and not name.endswith('_top100.json'):
                    continue
                if kind == 'border' and not name.endswith('_border.json'):
                    continue
                m = re.match(r"^(\d+)_(\d{8}_\d{6})_(top100|border)\.json$", name)
                if not m:
                    continue
                eid = int(m.group(1))
                if eid != event_id:
                    continue
                ts = datetime.strptime(m.group(2), '%Y%m%d_%H%M%S')
                if ts < cutoff:
                    continue
                with open(os.path.join(self.history_dir, name), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                score = self._get_rank_score(data, rank, 'top100' if name.endswith('_top100.json') else 'border')
                if score is not None:
                    out.append((ts, score))
        except Exception:
            return out
        out.sort(key=lambda z: z[0])
        return out

    def _rank_speed_1h_w(self, event_id: int, rank: int, kind: str, current_score: Optional[int]) -> float:
        """函数说明: 计算指定排名的近1小时增量(单位W)"""
        try:
            history = self._collect_rank_history(event_id, rank, kind)
            one_hour_ago = datetime.now() - timedelta(hours=1)
            past_score = None
            for ts, sc in history:
                if ts <= one_hour_ago:
                    past_score = sc
                else:
                    break
            if past_score is None and history:
                past_score = history[0][1]
            if past_score is None or current_score is None:
                return 0.0
            return self._to_w(current_score - past_score)
        except Exception:
            return 0.0

    def _rank_speed_24h_w(self, event_id: int, rank: int, kind: str, current_score: Optional[int]) -> float:
        try:
            history = self._collect_rank_history(event_id, rank, kind)
            if not history:
                return 0.0
            start_ts, start_sc = history[0]
            end_sc = current_score if current_score is not None else history[-1][1]
            hours = max(1e-6, (history[-1][0] - start_ts).total_seconds() / 3600.0)
            return self._to_w(end_sc - start_sc) / hours
        except Exception:
            return 0.0

    def _rank_speed_ema_w(self, event_id: int, rank: int, kind: str, alpha: float = 0.4) -> float:
        """函数说明: 使用指数加权对最近24小时的分数斜率(W/h)进行平滑，得到更稳健的速度估计"""
        try:
            hist = self._collect_rank_history(event_id, rank, kind)
            if len(hist) < 2:
                return 0.0
            ema = None
            prev_ts, prev_sc = hist[0]
            for ts, sc in hist[1:]:
                dt_h = max(1e-6, (ts - prev_ts).total_seconds() / 3600.0)
                slope = self._to_w(sc - prev_sc) / dt_h
                ema = slope if ema is None else (alpha * slope + (1 - alpha) * ema)
                prev_ts, prev_sc = ts, sc
            return float(ema or 0.0)
        except Exception:
            return 0.0



    # 删除HTML渲染，不再生成网页


    def _render_line_image(self, event_name: str, end_time: Optional[datetime], now: datetime, top100: Dict[str, Any], border: Dict[str, Any], ts_top: Optional[datetime], ts_border: Optional[datetime]) -> str:
        """函数说明: 绘制分数线信息图片"""
        width_base = 720
        margin = 20
        font_big = self.fonts.get('big') or ImageFont.load_default()
        font_mid = self.fonts.get('mid') or ImageFont.load_default()
        tmp_img = Image.new('RGB', (width_base, 100), (255, 255, 255))
        tmp_draw = ImageDraw.Draw(tmp_img)

        lines: List[str] = []
        lines.append(f"当前活动为: {event_name}")
        lines.append(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")
        if end_time:
            lines.append(f"结活时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")
            lines.append(f"活动还剩{self._format_remaining(end_time, now)}")

        def add_rank_line(rank: int, kind: str) -> None:
            sc = self._get_rank_score(top100 if kind == 'top100' else border, rank, kind)
            ts = ts_top if kind == 'top100' else ts_border
            if sc is not None:
                lines.append(f"{rank}名: {self._to_w(sc):.4f}万 (记录时间: {(ts.strftime('%m-%d %H:%M:%S') if ts else '未知')})")

        for r in [10, 20, 30, 40, 50, 100]:
            add_rank_line(r, 'top100')
        for r in [200, 300, 400, 500, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 10000, 20000, 30000, 40000, 50000, 100000, 200000]:
            add_rank_line(r, 'border')

        lines.append("注意：由于服务器缓存，分数线有最大300s的延迟，非实时，请留出足够裕度")

        # 动态尺寸
        def measure_w(text: str, big=False) -> int:
            bbox = tmp_draw.textbbox((0, 0), text, font=(font_big if big else font_mid))
            return bbox[2] - bbox[0]
        max_w = 0
        for i, t in enumerate(lines):
            max_w = max(max_w, measure_w(t, big=(i == 0)))
        width = max(480, min(width_base, max_w + 2 * margin))
        height = margin + 36 * len(lines) + margin
        img = Image.new('RGB', (width, height), (255, 255, 255))
        draw = ImageDraw.Draw(img)

        y = margin
        for i, t in enumerate(lines):
            draw.text((margin, y), t, fill=(0, 0, 0), font=(font_big if i == 0 else font_mid))
            y += 36

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = os.path.join(self.output_dir, f"pjsk_line_{ts}.png")
        img.save(path, 'PNG', quality=95)
        return path

    def _render_speed_line_image(self, event_name: str, end_time: Optional[datetime], now: datetime, top100: Dict[str, Any], border: Dict[str, Any], ts_top: Optional[datetime], ts_border: Optional[datetime], event_id: int) -> str:
        """函数说明: 绘制分数线时速信息图片(近1小时增量，单位W)"""
        width_base = 720
        margin = 20
        font_big = self.fonts.get('big') or ImageFont.load_default()
        font_mid = self.fonts.get('mid') or ImageFont.load_default()
        tmp_img = Image.new('RGB', (width_base, 100), (255, 255, 255))
        tmp_draw = ImageDraw.Draw(tmp_img)

        lines: List[str] = []
        lines.append(f"当前活动为: {event_name}")
        lines.append(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")
        if end_time:
            lines.append(f"结活时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")
            lines.append(f"活动还剩{self._format_remaining(end_time, now)}")

        def add_speed_line(rank: int, kind: str) -> None:
            cur = self._get_rank_score(top100 if kind == 'top100' else border, rank, kind)
            sp = self._rank_speed_1h_w(event_id, rank, kind, cur)
            ts = ts_top if kind == 'top100' else ts_border
            lines.append(f"{rank}名: {sp:.4f}万/h (记录时间: {(ts.strftime('%m-%d %H:%M:%S') if ts else '未知')})")

        for r in [10, 20, 30, 40, 50, 100]:
            add_speed_line(r, 'top100')
        for r in [200, 300, 400, 500, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 10000, 20000, 30000, 40000, 50000, 100000, 200000]:
            add_speed_line(r, 'border')

        lines.append("注意：由于服务器缓存，分数线有最大300s的延迟，非实时，请留出足够裕度")

        # 动态尺寸
        def measure_w(text: str, big=False) -> int:
            bbox = tmp_draw.textbbox((0, 0), text, font=(font_big if big else font_mid))
            return bbox[2] - bbox[0]
        max_w = 0
        for i, t in enumerate(lines):
            max_w = max(max_w, measure_w(t, big=(i == 0)))
        width = max(480, min(width_base, max_w + 2 * margin))
        height = margin + 36 * len(lines) + margin
        img = Image.new('RGB', (width, height), (255, 255, 255))
        draw = ImageDraw.Draw(img)

        y = margin
        for i, t in enumerate(lines):
            draw.text((margin, y), t, fill=(0, 0, 0), font=(font_big if i == 0 else font_mid))
            y += 36

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = os.path.join(self.output_dir, f"pjsk_speed_line_{ts}.png")
        img.save(path, 'PNG', quality=95)
        return path

    def _extract_latest_snapshot_ts(self) -> Optional[datetime]:
        """函数说明:
        - 从 history 目录中提取最新快照时间戳(Top100文件)
        """
        try:
            if not os.path.isdir(self.history_dir):
                return None
            ts_list: List[datetime] = []
            for name in os.listdir(self.history_dir):
                if name.endswith('_top100.json'):
                    m = re.match(r"^(\d+)_(\d{8}_\d{6})_top100\.json$", name)
                    if not m:
                        continue
                    ts = datetime.strptime(m.group(2), '%Y%m%d_%H%M%S')
                    ts_list.append(ts)
            if not ts_list:
                return None
            return max(ts_list)
        except Exception:
            return None

    def _collect_user_history(self, user_id: int, event_id: int) -> List[Tuple[datetime, int]]:
        """函数说明:
        - 收集指定用户在最近24小时的分数历史(来自 Top100 快照)
        - 若某快照中该用户不在Top100，则跳过
        """
        out: List[Tuple[datetime, int]] = []
        try:
            if not os.path.isdir(self.history_dir):
                return out
            cutoff = datetime.now() - timedelta(hours=24)
            names = sorted(os.listdir(self.history_dir))
            for name in names:
                if not name.endswith('_top100.json'):
                    continue
                m = re.match(r"^(\d+)_(\d{8}_\d{6})_top100\.json$", name)
                if not m:
                    continue
                eid = int(m.group(1))
                if eid != event_id:
                    continue
                ts = datetime.strptime(m.group(2), '%Y%m%d_%H%M%S')
                if ts < cutoff:
                    continue
                try:
                    with open(os.path.join(self.history_dir, name), 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    arr = (data or {}).get('rankings', [])
                    for x in arr:
                        if int(x.get('userId', -1)) == int(user_id):
                            out.append((ts, int(x.get('score', 0))))
                            break
                except Exception:
                    continue
        except Exception:
            return out
        # 按时间升序
        out.sort(key=lambda z: z[0])
        return out

    def _compute_stats(self, history: List[Tuple[datetime, int]], cur_score: int, now: datetime) -> Tuple[float, float, str]:
        """函数说明:
        - 计算过去一小时时速(单位W)、近10次变化pt的平均值(单位W)、已停车时长文本
        """
        # 时速: 当前分数 - 1小时前分数(若存在)
        speed_w = 0.0
        one_hour_ago = now - timedelta(hours=1)
        # 找到最接近一小时前且时间<= one_hour_ago 的分数(若不存在则用列表最早值)
        past_score = None
        for ts, sc in history:
            if ts <= one_hour_ago:
                past_score = sc
            else:
                break
        if past_score is None and history:
            past_score = history[0][1]
        if past_score is not None:
            speed_w = self._to_w(cur_score - past_score)

        # 近10次变化平均: 取连续快照中分数发生变化的差值，取最后10次求均值
        diffs: List[int] = []
        prev = None
        prev_change_ts: Optional[datetime] = None
        for ts, sc in history:
            if prev is None:
                prev = sc
                continue
            if sc != prev:
                diffs.append(sc - prev)
                prev = sc
                prev_change_ts = ts
        # 保证包含当前到最后一次变化的差值(若当前分数不同于历史最后值)
        if history:
            last_sc = history[-1][1]
            last_ts = history[-1][0]
            if cur_score != last_sc:
                diffs.append(cur_score - last_sc)
                prev_change_ts = now

        avg_pt_w = self._to_w(sum(diffs[-10:]) / max(1, min(10, len(diffs[-10:]))) ) if diffs else 0.0

        # 停车时长: 从上一次分数变化到现在的时间间隔
        park_text = '未知'
        try:
            last_change_time = None
            # 若当前与历史最后一个不同，则认为刚刚变化
            if history:
                if cur_score != history[-1][1]:
                    last_change_time = now
                else:
                    # 找到最近一次变化时间
                    last_change_time = prev_change_ts or history[0][0]
            if last_change_time:
                delta = now - last_change_time
                park_text = self._format_timedelta(delta)
        except Exception:
            pass

        return speed_w, avg_pt_w, park_text

    def _compute_extended_stats(self, history: List[Tuple[datetime, int]], cur_score: int, now: datetime) -> Dict[str, Any]:
        """函数说明:
        - 扩展统计，包含：
          近10次平均Pt、最近一次Pt、近1小时时速(W)、20min×3时速(W)、近1小时周回次数、20min×3周回、停车时长、连续周回时间
        """
        from datetime import timedelta

        # 构建变化事件列表 (时间, 增量)
        change_events: List[Tuple[datetime, int]] = []
        prev = None
        for ts, sc in history:
            if prev is None:
                prev = sc
                continue
            if sc != prev:
                change_events.append((ts, sc - prev))
                prev = sc
        if history:
            last_sc = history[-1][1]
            if cur_score != last_sc:
                change_events.append((now, cur_score - last_sc))

        # 近10次平均Pt与最近一次Pt
        last_pt = change_events[-1][1] if change_events else 0
        recent_pts = [d for (_, d) in change_events[-10:]]
        avg_pt = (sum(recent_pts) / max(1, len(recent_pts))) if recent_pts else 0.0

        # 近1小时时速 (W) - 使用差值法
        one_hour_ago = now - timedelta(hours=1)
        past_score = None
        for ts, sc in history:
            if ts <= one_hour_ago:
                past_score = sc
            else:
                break
        if past_score is None and history:
            past_score = history[0][1]
        speed_1h_w = self._to_w(cur_score - past_score) if past_score is not None else 0.0

        # 20min×3时速 (W)
        twenty_ago = now - timedelta(minutes=20)
        ref_20_sc = None
        for ts, sc in history:
            if ts <= twenty_ago:
                ref_20_sc = sc
            else:
                break
        if ref_20_sc is None and history:
            ref_20_sc = history[0][1]
        speed_20x3_w = (self._to_w(cur_score - ref_20_sc) * 3.0) if ref_20_sc is not None else 0.0

        # 周回次数统计
        count_1h = sum(1 for (ts, _) in change_events if ts >= one_hour_ago)
        count_20 = sum(1 for (ts, _) in change_events if ts >= twenty_ago)
        count_20x3 = count_20 * 3

        # 停车时长（与旧实现保持一致）
        park_text = '未知'
        try:
            last_change_time = change_events[-1][0] if change_events else (history[-1][0] if history else None)
            if last_change_time:
                delta = now - last_change_time
                park_text = self._format_timedelta(delta)
        except Exception:
            pass

        # 连续周回时间：4分钟内有变化视为连续
        continuous_text = '未知'
        try:
            change_times = [ts for (ts, _) in change_events]
            if not change_times:
                continuous_text = '0秒'
                is_continuous = False
            else:
                last_ts = change_times[-1]
                is_continuous = (now - last_ts) <= timedelta(minutes=4)
                end_time = now if is_continuous else last_ts
                # 向前寻找连续段起点
                start = last_ts
                for i in range(len(change_times) - 2, -1, -1):
                    if (start - change_times[i]) <= timedelta(minutes=4):
                        start = change_times[i]
                    else:
                        break
                continuous_text = self._format_timedelta(end_time - start)
        except Exception:
            pass

        return {
            'avg_pt': float(avg_pt),
            'last_pt': int(last_pt),
            'speed_1h_w': float(speed_1h_w),
            'speed_20x3_w': float(speed_20x3_w),
            'count_1h': int(count_1h),
            'count_20x3': int(count_20x3),
            'park_text': park_text,
            'continuous_text': continuous_text,
            'is_continuous': True if 'is_continuous' in locals() and is_continuous else False,
        }

    def _compute_stage_diff(self, rank: int, cur_score: int, top100: Dict[str, Any], border: Dict[str, Any]) -> Dict[str, Any]:
        """函数说明:
        - 计算与阶段排名的差距(两个方向: 上一阶段与下一阶段)
        - 返回: { 'prev': (stage_rank, stage_score, diff_w, arrow), 'next': (...) }
        """
        def find_score_for_rank(r: int) -> Optional[int]:
            if r <= 100:
                for x in (top100 or {}).get('rankings', []):
                    if int(x.get('rank', -1)) == r:
                        return int(x.get('score', 0))
            for x in (border or {}).get('borderRankings', []):
                if int(x.get('rank', -1)) == r:
                    return int(x.get('score', 0))
            return None

        prev_stage = None
        next_stage = None

        if rank <= 100:
            lower = [m for m in self.top100_milestones if m < rank]
            higher = [m for m in self.top100_milestones if m > rank]
            prev_stage = max(lower) if lower else None
            next_stage = min(higher) if higher else 200  # 100之后使用200作为下一阶段
        else:
            # 对于>100，尝试用border相邻常见阶段: 200, 300, 500, 1000...
            common = [200, 300, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000]
            lower = [m for m in common if m < rank]
            higher = [m for m in common if m > rank]
            prev_stage = max(lower) if lower else None
            next_stage = min(higher) if higher else None

        res = {
            'prev': None,
            'next': None
        }
        if prev_stage is not None:
            s = find_score_for_rank(prev_stage)
            if s is not None:
                diff_w = self._to_w(s - cur_score)
                res['prev'] = (prev_stage, s, diff_w, '↑' if s > cur_score else '↓')
        if next_stage is not None:
            s = find_score_for_rank(next_stage)
            if s is not None:
                diff_w = self._to_w(cur_score - s)
                res['next'] = (next_stage, s, diff_w, '↓' if s < cur_score else '↑')

        return res

    def _wrap_text(self, draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_width: int) -> List[str]:
        """函数说明: 基于宽度自动换行"""
        lines: List[str] = []
        if not text:
            return lines
        cur = ""
        for ch in str(text):
            tmp = cur + ch
            bbox = draw.textbbox((0, 0), tmp, font=font)
            w = bbox[2] - bbox[0]
            if w > max_width and cur:
                lines.append(cur)
                cur = ch
            else:
                cur = tmp
        if cur:
            lines.append(cur)
        return lines

    def _fetch_user_profile(self, user_id: int, server: str, token: str) -> Dict[str, Any]:
        """函数说明:
        - 通过 Haruki Sekai API 获取个人档案(JSON)
        - 路径: {base}/api/{server}/{user_id}/profile
        """
        base = "https://public-api.haruki.seiunx.com/sekai-api/v5".rstrip('/')
        server = (server or 'CN').upper()
        sess = requests.Session()
        sess.headers.update({
            "X-Haruki-Sekai-Token": token,
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "User-Agent": "walnutmortis-pjskranking-service/1.0"
        })

        def try_json(u: str) -> Dict[str, Any]:
            r = sess.get(u, timeout=30)
            # 若401，抛出具体说明，便于上层回退
            if r.status_code == 401:
                raise RuntimeError("401 未认证: 请确认令牌与服务器(CN)匹配，并避免使用JP/EN")
            r.raise_for_status()
            try:
                return r.json()
            except json.JSONDecodeError:
                raise RuntimeError("响应非JSON")

        # 先探测 system，确认令牌有效
        try:
            _sys = try_json(f"{base}/api/{server}/system")
        except Exception as e:
            # 若 system 都失败，直接返回401提示
            raise RuntimeError(f"令牌或服务器访问失败: {e}")

        # 主路径
        url1 = f"{base}/api/{server}/{user_id}/profile"
        # 备选路径(部分部署可能挂载为 /user/{id}/profile)
        url2 = f"{base}/api/{server}/user/{user_id}/profile"

        try:
            return try_json(url1)
        except Exception as e1:
            # 回退到备选路径
            try:
                return try_json(url2)
            except Exception as e2:
                raise RuntimeError(f"获取个人信息失败: {e1}；备选路径失败: {e2}")

    def _format_user_profile_text(self, profile: Dict[str, Any], user_id: int) -> str:
        """函数说明:
        - 将个人档案JSON格式化为文本摘要
        - 优先显示常见字段: 昵称、个签、用户ID、最近活跃信息(如可用)
        """
        name = ''
        word = ''
        try:
            name = str((profile or {}).get('name') or (profile or {}).get('userName') or '')
        except Exception:
            name = ''
        try:
            word = str(((profile or {}).get('userProfile') or {}).get('word') or (profile or {}).get('word') or '')
        except Exception:
            word = ''
        lines = []
        lines.append(f"用户ID: {user_id}")
        if name:
            lines.append(f"昵称: {name}")
        if word:
            lines.append(f"个签: {word}")
        # 附带若干可见字段(健壮处理)
        for key in ['rank', 'title', 'team', 'region']:
            try:
                val = (profile or {}).get(key)
                if val is not None:
                    lines.append(f"{key}: {val}")
            except Exception:
                continue
        # 若存在嵌套 userProfile 的其他信息
        up = (profile or {}).get('userProfile') or {}
        for key in ['introduction', 'twitterId']:
            try:
                val = up.get(key)
                if val:
                    lines.append(f"{key}: {val}")
            except Exception:
                continue
        lines.append(f"拉取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")
        return "\n".join(lines)

    def _render_image(self, profile: Dict[str, Any], stats: Dict[str, Any], stage_info: Dict[str, Any], latest_ts: Optional[datetime]) -> str:
        """函数说明:
        - 根据数据生成结果图片，返回图片文件路径
        """
        # 画布(自适应大小：先测量后绘制)
        base_width = 600
        margin = 20
        tmp_img = Image.new('RGB', (base_width, 100), color=(255, 255, 255))
        tmp_draw = ImageDraw.Draw(tmp_img)

        # 字体
        font_big = self.fonts.get('big') or ImageFont.load_default()
        font_mid = self.fonts.get('mid') or ImageFont.load_default()
        font_small = self.fonts.get('small') or ImageFont.load_default()

        name = profile.get('name', '')
        word = (profile.get('userProfile') or {}).get('word', '')
        rank = int(profile.get('rank', 0))
        score_w = self._to_w(profile.get('score', 0))

        # 预计算换行与行宽
        name_lines = self._wrap_text(tmp_draw, name, font_mid, base_width - 2 * margin)
        word_lines = self._wrap_text(tmp_draw, word, font_mid, base_width - 2 * margin)
        prev_lines: List[str] = []
        next_lines: List[str] = []
        if stage_info.get('prev'):
            pr, ps, pd, arrow = stage_info['prev']
            prev_lines = self._wrap_text(tmp_draw, f"{pr}名分数 {self._to_w(ps):.4f}W  {arrow}{abs(pd):.4f}W", font_mid, base_width - 2 * margin)
        if stage_info.get('next'):
            nr, ns, nd, arrow = stage_info['next']
            next_lines = self._wrap_text(tmp_draw, f"{nr}名分数 {self._to_w(ns):.4f}W  {arrow}{abs(nd):.4f}W", font_mid, base_width - 2 * margin)

        # 计算最大行宽
        def _measure(lines: List[str], font) -> int:
            maxw = 0
            for ln in lines:
                bbox = tmp_draw.textbbox((0, 0), ln, font=font)
                maxw = max(maxw, bbox[2] - bbox[0])
            return maxw
        max_line_w = 0
        max_line_w = max(max_line_w, _measure(name_lines, font_mid))
        max_line_w = max(max_line_w, _measure(word_lines, font_mid))
        max_line_w = max(max_line_w, _measure(prev_lines, font_mid))
        max_line_w = max(max_line_w, _measure(next_lines, font_mid))
        # 主信息各行文本宽度估算
        main_texts = [
            f"分数{score_w:.4f}W，排名{rank}",
            f"近10次平均Pt:  {stats.get('avg_pt', 0.0):.1f}",
            f"最近一次Pt:  {stats.get('last_pt', 0)}",
            f"近1小时pt增长数（时速）:  {stats.get('speed_1h_w', 0.0):.4f}W",
            f"20min×3时速:  {stats.get('speed_20x3_w', 0.0):.4f}W",
            f"近1小时pt增长次数（周回）:  {stats.get('count_1h', 0)}",
            f"20min×3周回:  {stats.get('count_20x3', 0)}",
            (f"连续周回时间: {stats.get('continuous_text', '未知')}" if stats.get('is_continuous', False) else f"已停车: {stats.get('park_text', '未知')}")
        ]
        # 第一行用大号字体
        if main_texts:
            bbox_big = tmp_draw.textbbox((0, 0), main_texts[0], font=font_big)
            max_line_w = max(max_line_w, bbox_big[2] - bbox_big[0])
            for t in main_texts[1:]:
                bbox_mid = tmp_draw.textbbox((0, 0), t, font=font_mid)
                max_line_w = max(max_line_w, bbox_mid[2] - bbox_mid[0])

        # 计算所需高度
        title_h = (len(name_lines) + len(word_lines)) * 32
        main_h = 316
        stage_h = (len(prev_lines) + len(next_lines)) * 32
        height = margin + title_h + main_h + stage_h + margin

        # 动态宽度（不扩大，只在内容更窄时缩小）
        width = max(480, min(base_width, max_line_w + 2 * margin))
        img = Image.new('RGB', (width, height), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)
        max_w = width - 2 * margin
        y = margin
        for ln in name_lines:
            draw.text((margin, y), ln, fill=(0, 0, 0), font=font_mid)
            y += 32
        for ln in word_lines:
            draw.text((margin, y), ln, fill=(0, 0, 0), font=font_mid)
            y += 32

        # 主信息
        draw.text((margin, y + 20), f"分数{score_w:.4f}W，排名{rank}", fill=(0, 0, 0), font=font_big)
        draw.text((margin, y + 60), f"近10次平均Pt:  {stats.get('avg_pt', 0.0):.1f}", fill=(0, 0, 0), font=font_mid)
        draw.text((margin, y + 92), f"最近一次Pt:  {stats.get('last_pt', 0)}", fill=(0, 0, 0), font=font_mid)
        draw.text((margin, y + 124), f"近1小时pt增长数（时速）:  {stats.get('speed_1h_w', 0.0):.4f}W", fill=(0, 0, 0), font=font_mid)
        draw.text((margin, y + 156), f"20min×3时速:  {stats.get('speed_20x3_w', 0.0):.4f}W", fill=(0, 0, 0), font=font_mid)
        draw.text((margin, y + 188), f"近1小时pt增长次数（周回）:  {stats.get('count_1h', 0)}", fill=(0, 0, 0), font=font_mid)
        draw.text((margin, y + 220), f"20min×3周回:  {stats.get('count_20x3', 0)}", fill=(0, 0, 0), font=font_mid)
        # 连续或停车二选一展示
        if stats.get('is_continuous', False):
            draw.text((margin, y + 252), f"连续周回时间: {stats.get('continuous_text', '未知')}", fill=(0, 0, 0), font=font_mid)
        else:
            draw.text((margin, y + 252), f"已停车: {stats.get('park_text', '未知')}", fill=(0, 0, 0), font=font_mid)

        # 阶段对比
        y = y + 316
        if stage_info.get('prev'):
            for ln in prev_lines:
                draw.text((margin, y), ln, fill=(0, 0, 0), font=font_mid)
                y += 32
        if stage_info.get('next'):
            for ln in next_lines:
                draw.text((margin, y), ln, fill=(0, 0, 0), font=font_mid)
                y += 32

        # 右下角不再显示水印或更新时间

        # 保存
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        uid = profile.get('userId', 0)
        fname = f"pjsk_{uid}_{rank}_{ts}.png"
        path = os.path.join(self.output_dir, fname)
        img.save(path, 'PNG', quality=95)
        return path

    def _render_range_image(self, profiles: List[Dict[str, Any]], ranks: List[int], event_id: int, top100: Dict[str, Any], border: Dict[str, Any]) -> str:
        """函数说明:
        - 将多名用户的统计按块纵向排列生成一张图片
        """
        width = 600
        margin = 20
        # 先测量每个块需要高度
        tmp_img = Image.new('RGB', (width, 100), color=(255, 255, 255))
        tmp_draw = ImageDraw.Draw(tmp_img)
        font_big = self.fonts.get('big') or ImageFont.load_default()
        font_mid = self.fonts.get('mid') or ImageFont.load_default()
        max_w = width - 2 * margin

        blocks: List[Dict[str, Any]] = []
        total_h = margin
        now = datetime.now()
        for i, p in enumerate(profiles):
            uid = p.get('userId')
            cur_score = int(p.get('score', 0))
            history = self._collect_user_history(uid, event_id)
            stats = self._compute_extended_stats(history, cur_score, now)
            rank = int(p.get('rank', ranks[i] if i < len(ranks) else 0))
            stage_info = self._compute_stage_diff(rank, cur_score, top100, border)
            name = p.get('name', '')
            word = (p.get('userProfile') or {}).get('word', '')
            # 估算行数
            name_lines = self._wrap_text(tmp_draw, name, font_mid, max_w)
            word_lines = self._wrap_text(tmp_draw, word, font_mid, max_w)
            stage_prev_lines = []
            stage_next_lines = []
            if stage_info.get('prev'):
                pr, ps, pd, arrow = stage_info['prev']
                stage_prev_lines = self._wrap_text(tmp_draw, f"{pr}名分数 {self._to_w(ps):.4f}W  {arrow}{abs(pd):.4f}W", font_mid, max_w)
            if stage_info.get('next'):
                nr, ns, nd, arrow = stage_info['next']
                stage_next_lines = self._wrap_text(tmp_draw, f"{nr}名分数 {self._to_w(ns):.4f}W  {arrow}{abs(nd):.4f}W", font_mid, max_w)
            # 主信息9行 + 标题与阶段行高度
            block_h = 20 + (len(name_lines) + len(word_lines)) * 32 + 316 + (len(stage_prev_lines) + len(stage_next_lines)) * 32 + 20
            blocks.append({
                'profile': p,
                'rank': rank,
                'stats': stats,
                'stage_info': stage_info,
                'name_lines': name_lines,
                'word_lines': word_lines,
                'stage_prev_lines': stage_prev_lines,
                'stage_next_lines': stage_next_lines,
                'block_h': block_h
            })
            total_h += block_h
        total_h += margin

        # 绘制大图
        img = Image.new('RGB', (width, total_h), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)
        y = margin
        for b in blocks:
            p = b['profile']
            rank = b['rank']
            stats = b['stats']
            stage_info = b['stage_info']
            score_w = self._to_w(p.get('score', 0))
            # 标题
            for ln in b['name_lines']:
                draw.text((20, y), ln, fill=(0, 0, 0), font=font_mid)
                y += 32
            for ln in b['word_lines']:
                draw.text((20, y), ln, fill=(0, 0, 0), font=font_mid)
                y += 32
            # 主信息
            draw.text((20, y + 20), f"分数{score_w:.4f}W，排名{rank}", fill=(0, 0, 0), font=font_big)
            draw.text((20, y + 60), f"近10次平均Pt:  {stats.get('avg_pt', 0.0):.1f}", fill=(0, 0, 0), font=font_mid)
            draw.text((20, y + 92), f"最近一次Pt:  {stats.get('last_pt', 0)}", fill=(0, 0, 0), font=font_mid)
            draw.text((20, y + 124), f"近1小时pt增长数（时速）:  {stats.get('speed_1h_w', 0.0):.4f}W", fill=(0, 0, 0), font=font_mid)
            draw.text((20, y + 156), f"20min×3时速:  {stats.get('speed_20x3_w', 0.0):.4f}W", fill=(0, 0, 0), font=font_mid)
            draw.text((20, y + 188), f"近1小时pt增长次数（周回）:  {stats.get('count_1h', 0)}", fill=(0, 0, 0), font=font_mid)
            draw.text((20, y + 220), f"20min×3周回:  {stats.get('count_20x3', 0)}", fill=(0, 0, 0), font=font_mid)
            if stats.get('is_continuous', False):
                draw.text((20, y + 252), f"连续周回时间: {stats.get('continuous_text', '未知')}", fill=(0, 0, 0), font=font_mid)
            else:
                draw.text((20, y + 252), f"已停车: {stats.get('park_text', '未知')}", fill=(0, 0, 0), font=font_mid)
            y = y + 316
            # 阶段
            for ln in b['stage_prev_lines']:
                draw.text((20, y), ln, fill=(0, 0, 0), font=font_mid)
                y += 32
            for ln in b['stage_next_lines']:
                draw.text((20, y), ln, fill=(0, 0, 0), font=font_mid)
                y += 32
            # 分隔线与块间距
            sep_y = y + 10
            draw.line([(20, sep_y), (width - 20, sep_y)], fill=(210, 210, 210), width=1)
            y += 20

        # 保存
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        fname = f"pjsk_range_{ranks[0]}-{ranks[-1]}_{ts}.png"
        path = os.path.join(self.output_dir, fname)
        img.save(path, 'PNG', quality=95)
        return path

    def _build_text_summary(self, profile: Dict[str, Any], speed_w: float, avg_pt_w: float, park_text: str, stage_info: Dict[str, Any]) -> str:
        """函数说明:
        - 构建文字摘要，作为图片的补充说明
        """
        name = profile.get('name', '')
        word = (profile.get('userProfile') or {}).get('word', '')
        rank = int(profile.get('rank', 0))
        score_w = self._to_w(profile.get('score', 0))
        lines = [
            f"{name}@{word}",
            f"分数{score_w:.4f}W，排名{rank}",
            f"近10次平均pt: {avg_pt_w:.5f}W",
            f"时速: {speed_w:.2f}W",
            f"已停车时长: {park_text}"
        ]
        if stage_info.get('prev'):
            pr, ps, pd, arrow = stage_info['prev']
            lines.append(f"{pr}名分数 {self._to_w(ps):.4f}W  {arrow}{abs(pd):.4f}W")
        if stage_info.get('next'):
            nr, ns, nd, arrow = stage_info['next']
            lines.append(f"{nr}名分数 {self._to_w(ns):.4f}W  {arrow}{abs(nd):.4f}W")
        return "\n".join(lines)

    def _to_w(self, score: Any) -> float:
        """函数说明: 分数转为W单位"""
        try:
            return float(score) / 10000.0
        except Exception:
            return 0.0

    def _format_timedelta(self, delta: timedelta) -> str:
        """函数说明: 将时间差格式化为中文字符串"""
        total = int(delta.total_seconds())
        days = total // 86400
        total %= 86400
        hours = total // 3600
        total %= 3600
        minutes = total // 60
        seconds = total % 60
        if days > 0:
            return f"{days}天{hours}小时{minutes}分{seconds}秒"
        return f"{hours}小时{minutes}分{seconds}秒"

    def _load_fonts(self) -> Dict[str, ImageFont.FreeTypeFont]:
        """函数说明:
        - 尝试加载中文字体(跨平台)，失败则回退到默认字体
        """
        fonts: Dict[str, ImageFont.FreeTypeFont] = {}
        candidates: List[str] = []
        try:
            import platform
            sysname = platform.system()
            if sysname == 'Windows':
                candidates = [
                    'C:/Windows/Fonts/msyh.ttc',
                    'C:/Windows/Fonts/msyhbd.ttc',
                    'C:/Windows/Fonts/simhei.ttf',
                    'C:/Windows/Fonts/simsun.ttc'
                ]
            else:
                candidates = [
                    '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
                    '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
                    '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
                    '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
                    '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
                ]
            font_path = None
            for p in candidates:
                if os.path.exists(p):
                    font_path = p
                    break
            if font_path:
                fonts['big'] = ImageFont.truetype(font_path, 36)
                fonts['mid'] = ImageFont.truetype(font_path, 26)
                fonts['small'] = ImageFont.truetype(font_path, 20)
            else:
                fonts['big'] = ImageFont.load_default()
                fonts['mid'] = ImageFont.load_default()
                fonts['small'] = ImageFont.load_default()
        except Exception:
            fonts['big'] = ImageFont.load_default()
            fonts['mid'] = ImageFont.load_default()
            fonts['small'] = ImageFont.load_default()
        return fonts

