#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件说明:
- PJSK 抽卡记录服务 (pjskgachashistory)
- 提供以下功能：
  1) 查询指定用户的抽卡历史记录（分页/limit）
  2) 查询指定用户的“未知卡池”记录
  3) 提交卡池映射（修复未知卡池）
  4) 查询指定时间戳可能存在的已知卡池

基础配置:
- 默认 API 基址: https://pjskgachaadmin.exmeaning.com
- 默认 Token: 读取配置 services.pjskgachashistory.token，若缺省则回退到用户提供的令牌

指令示例:
- pjskgacha 123456 10          # 查询用户 123456 最新10条抽卡记录
- pjsk未知卡池 123456           # 查询用户 123456 的未知卡池列表
- pjsk修复卡池 123456 1764560850 123  # 为用户 123456 提交卡池映射(detail_time, gacha_id)
- pjsk可能卡池 1764560850       # 查询该时间戳可能的卡池
"""

import os
import re
import json
import time
from typing import Any, Dict, Optional, List

import requests

from service.base_service import BaseService


class PJSKGachaHistoryService(BaseService):
    """
    类说明:
    - PJSK 抽卡历史服务
    - 解析用户消息并调用后端管理 API，返回结构化文本结果
    """
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        """
        函数说明:
        - 初始化服务实例，读取基础配置与令牌
        """
        super().__init__(config, data_manager, text_formatter, server)
        self.service_name = "pjskgachashistory"
        svc_conf = self.config.get("services", {}).get(self.service_name, {})
        # API 基址与令牌（令牌默认读取配置，缺省时使用用户提供的默认令牌）
        self.api_base: str = svc_conf.get("api_base", "https://pjskgachaadmin.exmeaning.com").rstrip("/")
        self.token: str = svc_conf.get("token", "7ecfbda6567475312e012251ea3cef7ef96fa31758f3b24f9720a1dfc6ff744e")
        self.timeout: int = int(svc_conf.get("timeout", 15))
        # 统一请求头
        self._headers: Dict[str, str] = {"Authorization": f"Bearer {self.token}"}
        # 历史记录目录
        self.gacha_dir = os.path.join("data", "pjsk", "gachas")
        os.makedirs(self.gacha_dir, exist_ok=True)
        # 图片输出目录
        self.image_dir = os.path.join("data", "images", "pjsk_gacha")
        os.makedirs(self.image_dir, exist_ok=True)

    def get_help_text(self) -> Dict[str, Any]:
        """
        函数说明:
        - 返回空或简短提示，详细帮助统一在 help_service / 网站帮助页
        """
        return {"content": ""}

    def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        函数说明:
        - 解析用户输入并触发相应的接口调用
        - 返回字典结构：{'content': '...'} 或更复杂消息
        """
        try:
            msg = (message or "").strip()
            # 统一半角空格并转换中文指令关键字
            msg_norm = re.sub(r"\s+", " ", msg)

            # A) 基于 wcn 绑定的抽卡记录查询：wcn抽卡记录 [limit]
            m_wcn_hist = re.match(r"^wcn抽卡记录(?:\s+(\d+))?$", msg_norm)
            if m_wcn_hist:
                limit = int(m_wcn_hist.group(1)) if m_wcn_hist and m_wcn_hist.group(1) else 10
                bound_uid = self._get_bound_pjsk_user_id(str(user_id))
                if not bound_uid:
                    return {"content": "❌ 未找到绑定的 PJSK 用户ID，请先使用 wcn 绑定"}
                # 首次使用获取全部，否则按limit
                if self._is_first_time(bound_uid):
                    limit = 1000
                data = self._get_gacha_history(bound_uid, limit=limit)
                # 保存历史记录（去重合并）
                try:
                    added = self._save_and_merge_history(bound_uid, data)
                except Exception:
                    pass
                # 图片渲染（将本次抓取的数据分组，未知单独块）
                records = self._normalize_list(data)
                if not records:
                    local_records = self._load_history_records(bound_uid)
                    if not local_records:
                        return {"content": "没有找到用户绑定id的抽卡历史记录，该抽卡历史记录需要上传文件。请访问https://walnutmortis.top/help/packet_capture.html获取抓包文件。"}
                    records = local_records
                groups = self._group_by_pool(records)
                unknown = [r for r in records if bool(r.get("is_unknown_pool", False))]
                images = self._render_group_images(bound_uid, groups, unknown)
                if images:
                    return {"content": "", "image_path": images}
                return {"content": self._format_gacha_history(bound_uid, data)}

            # 强制获取全部：支持多种写法
            if re.match(r"^wcn抽卡记录\s*全$", msg_norm) or re.match(r"^wcn抽卡记录全$", msg_norm) or re.match(r"^wcn抽卡记录全治$", msg_norm):
                bound_uid = self._get_bound_pjsk_user_id(str(user_id))
                if not bound_uid:
                    return {"content": "❌ 未找到绑定的 PJSK 用户ID，请先使用 wcn 绑定"}
                data = self._get_gacha_history(bound_uid, limit=1000)
                try:
                    self._save_and_merge_history(bound_uid, data)
                except Exception:
                    pass
                # 优先使用合并后的本地完整历史
                records = self._load_history_records(bound_uid)
                if not records:
                    records = self._normalize_list(data)
                if not records:
                    return {"content": "没有找到用户绑定id的抽卡历史记录，该抽卡历史记录需要上传文件。请联系BOT主获取。"}
                groups = self._group_by_pool(records)
                unknown = [r for r in records if bool(r.get("is_unknown_pool", False))]
                images = self._render_group_images(bound_uid, groups, unknown)
                if images:
                    return {"content": "", "image_path": images}
                return {"content": self._format_gacha_history(bound_uid, data)}

            # B) 基于 wcn 绑定的未知卡池查询：wcn抽卡修复
            m_wcn_fix = re.match(r"^wcn抽卡修复$", msg_norm)
            if m_wcn_fix:
                bound_uid = self._get_bound_pjsk_user_id(str(user_id))
                if not bound_uid:
                    return {"content": "❌ 未找到绑定的 PJSK 用户ID，请先使用 wcn 绑定"}
                data = self._get_unknown_pools(bound_uid)
                # 渲染未知卡池为图片块
                records = self._normalize_list(data)
                images = self._render_unknown_images(bound_uid, records)
                if images:
                    return {"content": "", "image_path": images}
                return {"content": self._format_unknown_pools(bound_uid, data)}

            # 1) 抽卡记录查询：pjskgacha <user_id> [limit]
            m_hist = re.match(r"^pjskgacha\s+(\d+)(?:\s+(\d+))?$", msg_norm, re.IGNORECASE)
            if m_hist:
                target_user_id = int(m_hist.group(1))
                limit = int(m_hist.group(2)) if m_hist.group(2) else 10
                data = self._get_gacha_history(target_user_id, limit=limit)
                return {"content": self._format_gacha_history(target_user_id, data)}

            # 2) 未知卡池查询：pjsk未知卡池 <user_id>
            m_unknown = re.match(r"^pjsk未知卡池\s+(\d+)$", msg_norm)
            if m_unknown:
                target_user_id = int(m_unknown.group(1))
                data = self._get_unknown_pools(target_user_id)
                return {"content": self._format_unknown_pools(target_user_id, data)}

            # 3) 卡池映射修复：pjsk修复卡池 <user_id> <detail_time> <gacha_id>
            m_fix = re.match(r"^pjsk修复卡池\s+(\d+)\s+(\d+)\s+(\d+)$", msg_norm)
            if m_fix:
                target_user_id = int(m_fix.group(1))
                detail_time = int(m_fix.group(2))
                gacha_id = int(m_fix.group(3))
                ok, resp_text = self._post_pool_mapping(target_user_id, detail_time, gacha_id)
                if ok:
                    return {"content": f"✅ 卡池映射已提交: user_id={target_user_id}, detail_time={detail_time}, gacha_id={gacha_id}"}
                else:
                    return {"content": f"❌ 卡池映射提交失败: {resp_text}"}

            # 4) 可能卡池查询：pjsk可能卡池 <timestamp>
            m_possible = re.match(r"^pjsk可能卡池\s+(\d+)$", msg_norm)
            if m_possible:
                ts = int(m_possible.group(1))
                data = self._get_possible_gachas(ts)
                return {"content": self._format_possible_gachas(ts, data)}

            return None
        except Exception as e:
            self.log_unified("ERROR", f"处理消息异常: {e}", group_id=kwargs.get("group_id"), user_id=user_id)
            return {"content": f"❌ 发生错误: {e}"}

    def _build_url(self, path: str) -> str:
        """
        函数说明:
        - 构建完整URL，确保基址拼接正确
        """
        return self.api_base + path

    def _get_bound_pjsk_user_id(self, qq: str) -> Optional[int]:
        """
        函数说明:
        - 从绑定文件中读取该 QQ 用户对应的 PJSK 用户ID
        - 文件路径: data/pjsk/user/{qq}.json
        """
        try:
            base = os.path.join("data", "pjsk", "user")
            path = os.path.join(base, f"{qq}.json")
            if not os.path.exists(path):
                return None
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            uid = data.get("pjsk_user_id")
            if uid is None:
                return None
            return int(uid)
        except Exception:
            return None
    
    def _is_first_time(self, target_user_id: int) -> bool:
        """
        函数说明:
        - 判断是否首次使用（历史文件不存在）
        """
        path = os.path.join(self.gacha_dir, f"{target_user_id}.json")
        return not os.path.exists(path)

    def _get_gacha_history(self, target_user_id: int, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
        """
        函数说明:
        - 调用抽卡记录查询接口
        - GET /api/v1/users/:user_id/gacha?limit=10
        """
        try:
            url = self._build_url(f"/api/v1/users/{target_user_id}/gacha")
            params = {"limit": str(limit)}
            resp = requests.get(url, headers=self._headers, params=params, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception:
            return None

    def _get_unknown_pools(self, target_user_id: int) -> Optional[List[Dict[str, Any]]]:
        """
        函数说明:
        - 调用未知卡池查询接口
        - GET /api/v1/users/:user_id/unknown-pools
        """
        try:
            url = self._build_url(f"/api/v1/users/{target_user_id}/unknown-pools")
            resp = requests.get(url, headers=self._headers, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception:
            return None

    def _post_pool_mapping(self, target_user_id: int, detail_time: int, gacha_id: int) -> (bool, str):
        """
        函数说明:
        - 提交卡池映射修复
        - POST /api/v1/users/:user_id/pool-mapping
        - Body: {"user_id": "...", "detail_time": 1764560850, "gacha_id": 123}
        """
        try:
            url = self._build_url(f"/api/v1/users/{target_user_id}/pool-mapping")
            body = {"user_id": str(target_user_id), "detail_time": detail_time, "gacha_id": gacha_id}
            resp = requests.post(url, headers={**self._headers, "Content-Type": "application/json"}, data=json.dumps(body), timeout=self.timeout)
            if resp.status_code in (200, 201):
                return True, resp.text
            return False, resp.text
        except Exception as e:
            return False, str(e)

    def _get_possible_gachas(self, ts: int) -> Optional[List[Dict[str, Any]]]:
        """
        函数说明:
        - 查询指定时间戳可能存在的已知卡池
        - GET /api/v1/gachas/possible?timestamp={ts}
        """
        try:
            url = self._build_url("/api/v1/gachas/possible")
            params = {"timestamp": str(ts)}
            resp = requests.get(url, headers=self._headers, params=params, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception:
            return None

    def _fmt_ts(self, ts: int) -> str:
        """
        函数说明:
        - 将秒级时间戳格式化为人类可读字符串（到分钟）
        """
        try:
            return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))
        except Exception:
            return str(ts)

    def _format_gacha_history(self, target_user_id: int, data: Optional[List[Dict[str, Any]]]) -> str:
        """
        函数说明:
        - 将抽卡记录格式化为文本列表（支持示例结构：顶层含 records/total_records）
        """
        if not data:
            return f"❌ 未获取到抽卡记录: user_id={target_user_id}"
        # 处理顶层对象（如 {records: [...], total_records: N}）
        total_records = None
        if isinstance(data, dict):
            if isinstance(data.get("total_records"), int):
                total_records = data.get("total_records")
        records = self._normalize_list(data)
        if not records:
            return f"❌ 抽卡记录返回为空或格式异常: user_id={target_user_id}"
        count_text = total_records if total_records is not None else len(records)
        lines = [f"📜 抽卡记录 (user_id={target_user_id}) 共 {count_text} 条"]
        for i, item in enumerate(records, 1):
            # 兼容字段名（假设后端返回包含 detail_time/gacha_id/pool_name/rarity 等）
            ts = int(item.get("detail_time", item.get("ts", 0)) or 0)
            rid = item.get("id", "")
            gid = item.get("gacha_id", item.get("mapped_gacha_id", ""))
            pool_id = item.get("pool_id", "")
            matched_gacha_name = item.get("matched_gacha_name", "")
            pull_type = item.get("pull_type", "")
            jewel_consume = item.get("jewel_consume", 0)
            is_unknown_pool = bool(item.get("is_unknown_pool", False))
            # 卡列表美化（若存在 cards 数组）
            cards_brief = self._format_cards_brief(item.get("cards"))
            pool_brief = matched_gacha_name or pool_id or "未知卡池"
            unknown_tag = "未知" if is_unknown_pool else "已识别"
            consume_brief = f"{pull_type}/{jewel_consume}"
            lines.append(
                f"{i}. 时间: {self._fmt_ts(ts)} | 记录ID: {rid} | 卡池: {pool_brief} | 状态: {unknown_tag} | "
                f"抽卡: {consume_brief} | gacha_id: {gid}\n    {cards_brief}"
            )
        return "\n".join(lines)

    def _format_unknown_pools(self, target_user_id: int, data: Optional[List[Dict[str, Any]]]) -> str:
        """
        函数说明:
        - 将未知卡池列表格式化为文本
        """
        if not data:
            return f"❌ 未获取到未知卡池: user_id={target_user_id}"
        records = self._normalize_list(data)
        if not records:
            return f"❌ 未知卡池返回为空或格式异常: user_id={target_user_id}"
        lines = [f"❓ 未知卡池 (user_id={target_user_id}) 共 {len(records)} 条"]
        for i, item in enumerate(records, 1):
            ts = int(item.get("detail_time", item.get("ts", 0)) or 0)
            gid = item.get("gacha_id", item.get("id", ""))
            hint = item.get("hint", "")
            lines.append(f"{i}. 时间: {self._fmt_ts(ts)} | gacha_id: {gid} | hint: {hint}")
        lines.append("📝 可使用: pjsk修复卡池 <user_id> <detail_time> <gacha_id> 来提交修复")
        return "\n".join(lines)

    def _format_possible_gachas(self, ts: int, data: Optional[List[Dict[str, Any]]]) -> str:
        """
        函数说明:
        - 将可能卡池查询结果格式化为文本
        """
        if not data:
            return f"❌ 未获取到可能卡池: ts={ts}"
        records = self._normalize_list(data)
        if not records:
            return f"❌ 可能卡池返回为空或格式异常: ts={ts}"
        lines = [f"🔎 可能卡池 (timestamp={ts}, {self._fmt_ts(ts)}) 共 {len(records)} 条"]
        for i, item in enumerate(records, 1):
            gid = item.get("gacha_id", item.get("id", ""))
            name = item.get("name", item.get("pool_name", ""))
            start_ts = int(item.get("start_time", 0))
            end_ts = int(item.get("end_time", 0))
            lines.append(f"{i}. 卡池: {name} | gacha_id: {gid} | 开始: {self._fmt_ts(start_ts)} | 结束: {self._fmt_ts(end_ts)}")
        return "\n".join(lines)

    def _normalize_list(self, data: Any) -> List[Dict[str, Any]]:
        """
        函数说明:
        - 规范化接口返回，提取记录列表
        - 支持以下常见结构：
          * 直接为 list
          * dict 包含 'data'/'items'/'list'/'records'/'result' 等列表字段
        """
        try:
            if isinstance(data, list):
                return [d for d in data if isinstance(d, dict)]
            if isinstance(data, dict):
                for key in ("data", "items", "list", "records", "result"):
                    val = data.get(key)
                    if isinstance(val, list):
                        return [d for d in val if isinstance(d, dict)]
            return []
        except Exception:
            return []

    def _format_cards_brief(self, cards: Any) -> str:
        """
        函数说明:
        - 美化单条记录的卡片列表摘要，避免对字符串调用字典接口
        """
        try:
            if not isinstance(cards, list) or len(cards) == 0:
                return "卡片: 无解析明细"
            parts: List[str] = []
            # 最多展示前5张，避免消息过长
            for idx, c in enumerate(cards[:5]):
                if not isinstance(c, dict):
                    continue
                name = c.get("raw_card_name") or f"{c.get('parsed_prefix', '')}{c.get('parsed_char_name', '')}".strip()
                rarity = c.get("parsed_rarity")
                if rarity is not None:
                    parts.append(f"{name}★{rarity}")
                else:
                    parts.append(name or "未知卡")
            more = "" if len(cards) <= 5 else f" 等{len(cards)}张"
            return "卡片: " + "，".join(parts) + more
        except Exception:
            return "卡片: 解析失败"

    def _save_and_merge_history(self, target_user_id: int, data: Any) -> int:
        """
        函数说明:
        - 将最新抓取的数据合并到历史文件中，仅追加新记录
        - 返回新增记录数
        """
        try:
            records = self._normalize_list(data)
            if not records:
                return 0
            path = os.path.join(self.gacha_dir, f"{target_user_id}.json")
            old = {"user_id": str(target_user_id), "records": []}
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        old = json.load(f) or old
                except Exception:
                    pass
            old_records = old.get("records") or []
            # 使用记录ID做去重；若无ID则用(detail_time,pull_type,raw_card_list)作为近似键
            def key_of(rec: Dict[str, Any]) -> str:
                rid = rec.get("id")
                if rid is not None:
                    return f"id:{rid}"
                dt = rec.get("detail_time")
                pt = rec.get("pull_type")
                rcl = rec.get("raw_card_list", "")
                return f"k:{dt}:{pt}:{hash(rcl)}"
            existing_keys = set()
            for r in old_records:
                if isinstance(r, dict):
                    existing_keys.add(key_of(r))
            new_items: List[Dict[str, Any]] = []
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                k = key_of(rec)
                if k in existing_keys:
                    continue
                new_items.append(rec)
                existing_keys.add(k)
            merged = old_records + new_items
            # 可按时间降序排序
            try:
                merged.sort(key=lambda x: int(x.get("detail_time", 0)), reverse=True)
            except Exception:
                pass
            out = {
                "user_id": str(target_user_id),
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "total": len(merged),
                "records": merged
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            return len(new_items)
        except Exception:
            return 0
    
    def _load_history_records(self, target_user_id: int) -> List[Dict[str, Any]]:
        """
        函数说明:
        - 读取本地完整历史记录
        """
        try:
            path = os.path.join(self.gacha_dir, f"{target_user_id}.json")
            if not os.path.exists(path):
                return []
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
            recs = obj.get("records") or []
            return [r for r in recs if isinstance(r, dict)]
        except Exception:
            return []
    
    def _group_by_pool(self, records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        函数说明:
        - 按卡池名称分组：优先 matched_gacha_name，其次 pool_id，最后“未知卡池”
        - 过滤掉 is_unknown_pool=True 的记录（它们由未知区域单独展示）
        """
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in records:
            if not isinstance(r, dict):
                continue
            if bool(r.get("is_unknown_pool", False)):
                # 未知卡池不进入分组
                continue
            pool = (r.get("matched_gacha_name") or r.get("pool_id") or "未知卡池").strip()
            groups.setdefault(pool, []).append(r)
        # 每组按时间降序
        for k in list(groups.keys()):
            try:
                groups[k].sort(key=lambda x: int(x.get("detail_time", 0)), reverse=True)
            except Exception:
                pass
        return groups
    
    def _pull_type_of(self, rec: Dict[str, Any]) -> str:
        """
        函数说明:
        - 判断抽卡类型：单抽或十连
        - 依据：cards 长度/ raw_card_list中逗号数量 / pull_type 字段
        """
        try:
            cards = rec.get("cards") or []
            if isinstance(cards, list) and len(cards) >= 10:
                return "十连"
            rcl = rec.get("raw_card_list") or ""
            if isinstance(rcl, str):
                # 通过逗号分隔项数量判断
                items = [s for s in rcl.split(",") if s.strip()]
                if len(items) >= 10:
                    return "十连"
            pt = str(rec.get("pull_type", "")).lower()
            if pt == "multi":
                return "十连"
            return "单抽"
        except Exception:
            return "单抽"
    
    def _format_record_line(self, rec: Dict[str, Any]) -> str:
        """
        函数说明:
        - 格式化单条记录为一行文本：
          [YYYY-MM-DD HH:MM] 单抽:[前缀]角色★稀有度
          或 十连:[卡1]，[卡2]，...（完整10张）
        """
        dt = self._fmt_ts(int(rec.get("detail_time", 0)))
        typ = self._pull_type_of(rec)
        cards = rec.get("cards") or []
        parts: List[str] = []
        if isinstance(cards, list) and len(cards) > 0:
            for c in cards:
                if not isinstance(c, dict):
                    continue
                prefix = c.get("parsed_prefix") or ""
                char = c.get("parsed_char_name") or ""
                rarity = c.get("parsed_rarity")
                name = c.get("raw_card_name") or f"[{prefix}]{char}".strip() if prefix or char else (c.get("raw_card_name") or "")
                if rarity is not None:
                    parts.append(f"{name}★{rarity}")
                else:
                    parts.append(name or "未知卡")
        else:
            # 回退：raw_card_list（逗号分隔）
            rcl = rec.get("raw_card_list") or ""
            if isinstance(rcl, str) and rcl.strip():
                items = [s.strip() for s in rcl.split(",") if s.strip()]
                parts.extend(items)
        joined = ",".join(parts)
        return f"[{dt}] {typ}:{joined}"
    
    def _render_group_images(self, user_id: int, groups: Dict[str, List[Dict[str, Any]]], unknown: List[Dict[str, Any]]) -> List[str]:
        """
        函数说明:
        - 将分组后的记录渲染为若干图片
        - 每张图片包含一个或多个卡池块；控制每张图片的行数以避免过长
        """
        try:
            from PIL import Image, ImageDraw, ImageFont
        except Exception:
            return []
        images: List[str] = []
        # 字体加载（跨平台尽量使用系统字体，失败回退默认）
        font_path_candidates = [
            "C:\\Windows\\Fonts\\msyh.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"
        ]
        font = None
        for p in font_path_candidates:
            try:
                if os.path.exists(p):
                    font = ImageFont.truetype(p, 28)
                    break
            except Exception:
                pass
        if font is None:
            try:
                font = ImageFont.load_default()
            except Exception:
                return []
        # 渲染参数
        width = 1200
        padding = 24
        line_h = 40
        token_spacing = 24
        # 预创建测量器
        tmp_img = Image.new("RGB", (10, 10), (255, 255, 255))
        measurer = ImageDraw.Draw(tmp_img)
        max_text_width = width - padding * 2
        # 预计算高度：遍历并测量所有行（按卡池块）
        total_lines = 0
        def measure_record(rec: Dict[str, Any]) -> int:
            prefix, tokens = self._tokenize_record(rec)
            return self._measure_wrapped_tokens(prefix, tokens, font, measurer, max_text_width, token_spacing)
        for pool_name, recs in groups.items():
            total_lines += 2  # 标题+“记录：”
            for rec in recs:
                total_lines += measure_record(rec)
            total_lines += 1  # 空行
        if unknown:
            # 未知池以普通池形式渲染
            total_lines += 2
            for rec in unknown:
                total_lines += measure_record(rec)
            total_lines += 1
        height = padding * 2 + total_lines * line_h
        img = Image.new("RGB", (width, height), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        y = padding
        # 绘制普通池
        for pool_name, recs in groups.items():
            draw.text((padding, y), f"卡池：{pool_name}", fill=(0, 0, 0), font=font)
            y += line_h
            draw.text((padding, y), "记录：", fill=(0, 0, 0), font=font)
            y += line_h
            for rec in recs:
                prefix, tokens = self._tokenize_record(rec)
                y = self._draw_wrapped_tokens(draw, padding, y, max_text_width, prefix, tokens, font, token_spacing)
            y += line_h
        # 绘制未知池
        if unknown:
            draw.text((padding, y), "卡池：未知", fill=(0, 0, 0), font=font)
            y += line_h
            draw.text((padding, y), "记录：", fill=(0, 0, 0), font=font)
            y += line_h
            for rec in unknown:
                prefix, tokens = self._tokenize_record(rec)
                y = self._draw_wrapped_tokens(draw, padding, y, max_text_width, prefix, tokens, font, token_spacing)
            y += line_h
        # 保存单张长图
        fname = f"gacha_{user_id}_{int(time.time())}_1.png"
        fpath = os.path.join(self.image_dir, fname)
        try:
            img.save(fpath, format="PNG")
            images.append(os.path.abspath(fpath))
        except Exception:
            pass
        return images
    
    def _wrap_record_line(self, line: str, font, measurer, max_width: int) -> List[str]:
        """
        函数说明:
        - 对长行进行按逗号友好换行，避免超宽导致信息被截断
        - 保留前缀（例如 '[时间] 十连:'），后续内容按逗号分隔分行
        """
        try:
            if measurer.textlength(line, font=font) <= max_width:
                return [line]
            # 拆分前缀与内容
            sep_idx = line.find(":")
            if sep_idx <= 0:
                # 无法识别前缀，用空格分词简单换行
                return self._wrap_by_words(line, font, measurer, max_width)
            prefix = line[:sep_idx+1]
            content = line[sep_idx+1:]
            tokens = [t.strip() for t in content.split(",") if t.strip()]
            out_lines: List[str] = []
            cur = prefix
            for t in tokens:
                candidate = (cur + t + ",").strip()
                if measurer.textlength(candidate, font=font) <= max_width:
                    cur = candidate
                else:
                    if cur.endswith(","):
                        cur = cur[:-1]
                    out_lines.append(cur)
                    cur = ("  " + t + ",").strip()
            if cur:
                if cur.endswith(","):
                    cur = cur[:-1]
                out_lines.append(cur)
            return out_lines
        except Exception:
            return [line]
    
    def _wrap_by_words(self, line: str, font, measurer, max_width: int) -> List[str]:
        """
        函数说明:
        - 退化的按空格换行，用于无法识别前缀的情况
        """
        try:
            words = line.split(" ")
            out_lines: List[str] = []
            cur = ""
            for w in words:
                candidate = (cur + " " + w).strip() if cur else w
                if measurer.textlength(candidate, font=font) <= max_width:
                    cur = candidate
                else:
                    if cur:
                        out_lines.append(cur)
                    cur = w
            if cur:
                out_lines.append(cur)
            return out_lines
        except Exception:
            return [line]

    def _tokenize_record(self, rec: Dict[str, Any]) -> (str, List[Dict[str, Any]]):
        """
        函数说明:
        - 将记录拆分为前缀和卡片token数组，每个token包含文本与稀有度
        """
        dt = self._fmt_ts(int(rec.get("detail_time", 0)))
        typ = self._pull_type_of(rec)
        prefix = f"[{dt}] {typ}:"
        tokens: List[Dict[str, Any]] = []
        cards = rec.get("cards") or []
        if isinstance(cards, list) and len(cards) > 0:
            for c in cards:
                if not isinstance(c, dict):
                    continue
                prefix_txt = c.get("parsed_prefix") or ""
                char = c.get("parsed_char_name") or ""
                rarity = c.get("parsed_rarity")
                name = c.get("raw_card_name") or (f"[{prefix_txt}]{char}".strip() if prefix_txt or char else "")
                tokens.append({"text": name, "rarity": rarity})
        else:
            rcl = rec.get("raw_card_list") or ""
            if isinstance(rcl, str) and rcl.strip():
                items = [s.strip() for s in rcl.split(",") if s.strip()]
                for it in items:
                    # 尝试解析尾部★N
                    rarity = None
                    m = re.search(r"★\s*([234])", it)
                    if m:
                        rarity = int(m.group(1))
                    tokens.append({"text": it, "rarity": rarity})
        return prefix, tokens

    def _measure_wrapped_tokens(self, prefix: str, tokens: List[Dict[str, Any]], font, measurer, max_width: int, token_spacing: int) -> int:
        """
        函数说明:
        - 计算按token绘制时的行数（包含前缀）
        """
        lines = 1
        cur_w = measurer.textlength(prefix, font=font)
        if cur_w > max_width:
            lines += 1
            cur_w = 0
        for tok in tokens:
            text = tok.get("text") or ""
            w = measurer.textlength(text, font=font)
            # 加间隔
            add_w = (token_spacing if cur_w > 0 else 0) + w
            if cur_w + add_w <= max_width:
                cur_w += add_w
            else:
                lines += 1
                cur_w = w
        return lines

    def _draw_wrapped_tokens(self, draw, x: int, y: int, max_width: int, prefix: str, tokens: List[Dict[str, Any]], font, token_spacing: int) -> int:
        """
        函数说明:
        - 绘制带有稀有度颜色的token，支持换行与较大间隔
        - 稀有度颜色：
          ★4 彩色（彩虹），★3 金色(255,215,0)，其他黑色
        """
        # 绘制前缀
        from PIL import ImageFont
        draw.text((x, y), prefix, fill=(0, 0, 0), font=font)
        cur_w = draw.textlength(prefix, font=font)
        # 如前缀已超宽，换到下一行
        if cur_w > max_width:
            y += 40
            cur_w = 0
        for tok in tokens:
            text = tok.get("text") or ""
            rarity = tok.get("rarity")
            w = draw.textlength(text, font=font)
            need_w = (token_spacing if cur_w > 0 else 0) + w
            if cur_w + need_w > max_width:
                y += 40
                cur_w = 0
            if cur_w > 0:
                # 间隔
                draw.text((x + cur_w, y), " " * 2, fill=(0, 0, 0), font=font)
                cur_w += token_spacing
            # 颜色绘制
            if rarity == 4:
                self._draw_rainbow_text(draw, x + cur_w, y, text, font)
            elif rarity == 3:
                draw.text((x + cur_w, y), text, fill=(255, 215, 0), font=font)
            else:
                draw.text((x + cur_w, y), text, fill=(0, 0, 0), font=font)
            cur_w += w
        # 完成后下移一行
        return y + 40

    def _draw_rainbow_text(self, draw, x: int, y: int, text: str, font) -> None:
        """
        函数说明:
        - 使用彩虹色逐字绘制文本
        """
        palette = [(255, 0, 0), (255, 128, 0), (255, 215, 0), (0, 180, 0), (0, 128, 255), (160, 32, 240)]
        cur_x = x
        for i, ch in enumerate(text):
            color = palette[i % len(palette)]
            draw.text((cur_x, y), ch, fill=color, font=font)
            w = draw.textlength(ch, font=font)
            cur_x += w
    
    def _render_unknown_images(self, user_id: int, unknown: List[Dict[str, Any]]) -> List[str]:
        """
        函数说明:
        - 单独渲染未知卡池为图片
        """
        return self._render_group_images(user_id, {}, unknown)

