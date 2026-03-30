#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件说明:
- 在 Windows/Linux 环境下按分钟调度(每分钟第 2 秒)获取 Haruki Sekai API 的活动排行榜 Top100 与分数线(border)数据
- 参考 `python-client-guide.md` 中的路由与鉴权说明进行实现
- 将获取的信息存入 `data/pjsk/ranking` 目录，并支持历史留存与实时配置 `event_id`

使用说明:
- 配置文件: `data/pjsk/ranking/config.json`，字段例如: {"event_id": 150, "server": "CN", "token": "<你的令牌>"}
- 运行方式: `python utils/pjsk_ranking_fetcher.py`

安全与合规:
- 控制请求频率(62 秒)，避免触发限流
- 令牌仅授权国服 CN；其他服请求将被拒绝
"""

import os
import json
import time
import re
from typing import Any, Dict, Optional

import requests


class HarukiSekaiClient:
    """类说明:
    - 封装 Haruki Sekai API 的调用方法(数据端点)
    - 自动附加鉴权头部
    """

    def __init__(self, base_url: str, token: str, server: str = "CN", rate_limit_sec: float = 0.0) -> None:
        """函数说明:
        - 初始化客户端, 设置基础 URL、令牌、服务器与简单的频率控制

        参数:
        - base_url: 公共端点, 例如 https://public-api.haruki.seiunx.com/sekai-api/v5
        - token: X-Haruki-Sekai-Token (JWT)
        - server: 服务器标识, 建议使用 "CN"
        - rate_limit_sec: 每次请求后的休眠秒数, 用于简单限速
        """
        self.base_url = base_url.rstrip("/")
        self.server = server
        self.session = requests.Session()
        self.session.headers.update({
            "X-Haruki-Sekai-Token": token,
            "Accept": "application/json",
            "User-Agent": "walnutmortis-pjsk-ranking-fetcher/1.0"
        })
        self.rate_limit_sec = rate_limit_sec

    def _sleep_if_needed(self) -> None:
        """函数说明: 简单频控"""
        if self.rate_limit_sec > 0:
            time.sleep(self.rate_limit_sec)

    def _get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """函数说明:
        - 以 GET 请求方式获取 JSON 数据, 拼接 REST 路径: {base_url}/api/{server}{path}

        参数:
        - path: 游戏接口相对路径, 形如 "/event/150/ranking-border"
        - params: 可选的查询参数字典

        返回:
        - 解析后的 JSON 字典; 若响应非 JSON, 抛出异常
        """
        url = f"{self.base_url}/api/{self.server}{path}"
        resp = self.session.get(url, params=params, timeout=30)
        self._sleep_if_needed()
        resp.raise_for_status()
        try:
            return resp.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(f"响应非 JSON: {e}")

    def get_event_ranking_top100(self, event_id: int) -> Dict[str, Any]:
        """函数说明:
        - 查询活动排行榜 Top100; 路由: "/api/CN/event/{event_id}/ranking-top100"

        参数:
        - event_id: 纯数字活动 ID
        """
        if not isinstance(event_id, int) or event_id <= 0:
            raise ValueError("event_id 必须为正整数")
        return self._get_json(f"/event/{event_id}/ranking-top100")

    def get_event_ranking_border(self, event_id: int) -> Dict[str, Any]:
        """函数说明:
        - 查询活动分数线(边界); 路由: "/api/CN/event/{event_id}/ranking-border"

        参数:
        - event_id: 纯数字活动 ID
        """
        if not isinstance(event_id, int) or event_id <= 0:
            raise ValueError("event_id 必须为正整数")
        return self._get_json(f"/event/{event_id}/ranking-border")


class RankingFetcher:
    """类说明:
    - 负责定时从 Haruki Sekai API 拉取 Top100 与 Border 数据
    - 将数据写入 `data/pjsk/ranking` 目录，并兼容历史留存
    - 支持从配置文件中动态读取 `event_id`
    """

    BASE_URL = "https://public-api.haruki.seiunx.com/sekai-api/v5"
    RANKING_DIR = os.path.join("data", "pjsk", "ranking")
    CONFIG_PATH = os.path.join(RANKING_DIR, "config.json")
    HISTORY_DIR = os.path.join(RANKING_DIR, "history")

    def __init__(self, interval_sec: int = 62) -> None:
        """函数说明:
        - 初始化定时器与客户端, 准备数据目录

        参数:
        - interval_sec: 轮询间隔秒数; 默认 62 秒
        """
        self.interval_sec = max(1, int(interval_sec))
        self._ensure_directories()
        cfg = self._load_config()
        token = str(cfg.get("token", "")).strip()
        if not token:
            raise RuntimeError("配置文件缺少 token 字段或为空")
        server = cfg.get("server", "CN")
        self.client = HarukiSekaiClient(base_url=self.BASE_URL, token=token, server=server, rate_limit_sec=0.0)

    def _ensure_directories(self) -> None:
        """函数说明: 确保数据与历史目录存在"""
        os.makedirs(self.RANKING_DIR, exist_ok=True)
        os.makedirs(self.HISTORY_DIR, exist_ok=True)

    def _load_config(self) -> Dict[str, Any]:
        """函数说明:
        - 加载配置文件 `config.json`; 若不存在则创建默认配置

        返回:
        - 配置字典, 至少包含 `event_id` 与 `server`
        """
        if not os.path.isfile(self.CONFIG_PATH):
            default_cfg = {
                "event_id": 150,
                "server": "CN",
                "token": "",
                "start_time": "",
                "end_time": "",
                "event_end_time": "",
                "event_name": "",
                "event_type": "混活",
                "history_retention_days": 8
            }
            with open(self.CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(default_cfg, f, ensure_ascii=False, indent=2)
            return default_cfg
        with open(self.CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_json(self, path: str, data: Dict[str, Any]) -> None:
        """函数说明: 以 UTF-8 保存 JSON 文件"""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _timestamp(self) -> str:
        """函数说明: 生成简洁时间戳字符串"""
        import datetime
        return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    def _sleep_until_next_minute(self, at_second: int = 2) -> None:
        """函数说明:
        - 休眠至下一分钟的指定秒数(默认第 2 秒)
        - 例如当前 15:43:xx，则休眠到 15:44:02；若当前 <15:43:02，则休眠到 15:43:02
        """
        import datetime
        now = datetime.datetime.now()
        target = now.replace(second=at_second, microsecond=0)
        if now.second >= at_second:
            target = (now + datetime.timedelta(minutes=1)).replace(second=at_second, microsecond=0)
        sleep_seconds = (target - now).total_seconds()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    def _cleanup_history(self, max_age_hours: int = 24) -> None:
        """函数说明:
        - 清理 `history` 目录中过期文件，仅保留最近 `max_age_hours` 小时内的记录
        - 匹配文件名格式: `{event_id}_{YYYYMMDD_HHMMSS}_{top100|border}.json`
        - 默认保留 24 小时；可根据配置 `history_retention_days` 调整
        """
        import datetime
        now = datetime.datetime.now()
        max_age = datetime.timedelta(hours=max_age_hours)
        try:
            for name in os.listdir(self.HISTORY_DIR):
                path = os.path.join(self.HISTORY_DIR, name)
                if not os.path.isfile(path):
                    continue
                m = re.match(r"^(\d+)_(\d{8}_\d{6})_(top100|border)\.json$", name)
                if not m:
                    continue
                ts_str = m.group(2)
                try:
                    ts = datetime.datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                except Exception:
                    continue
                if (now - ts) > max_age:
                    try:
                        os.remove(path)
                    except Exception:
                        # 删除失败不影响后续流程
                        pass
        except Exception:
            # 清理过程异常不影响主流程
            pass

    def _post_event_summary(self, event_id: int, event_end_time: Optional["datetime.datetime"], event_type: str, retention_days: int) -> None:
        """函数说明:
        - 在活动结束10分钟后，基于历史数据生成事后总结文件，辅助未来预测
        - 输出路径: `data/pjsk/ranking/summary/{event_id}_summary.json`
        - 指标包含: 各档位最终分数(万)、全程OLS速度(W/h)、最后6小时速度(W/h)、冲刺系数(last6h/OLS)
        """
        import datetime
        import math as _math
        try:
            if not event_end_time:
                return
            now = datetime.datetime.now()
            # 仅在结活后10分钟生成
            if (now - event_end_time).total_seconds() < 600:
                return
            summary_dir = os.path.join(self.RANKING_DIR, 'summary')
            os.makedirs(summary_dir, exist_ok=True)
            out_path = os.path.join(summary_dir, f"{event_id}_summary.json")
            if os.path.isfile(out_path):
                return

            # 收集时间序列
            def collect_series(kind: str, rank: int) -> list:
                pts = []
                for name in sorted(os.listdir(self.HISTORY_DIR)):
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
                    ts = datetime.datetime.strptime(m.group(2), "%Y%m%d_%H%M%S")
                    try:
                        with open(os.path.join(self.HISTORY_DIR, name), 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        key = 'rankings' if kind == 'top100' else 'borderRankings'
                        arr = (data or {}).get(key, [])
                        for x in arr:
                            if int(x.get('rank', -1)) == int(rank):
                                pts.append((ts, int(x.get('score', 0))))
                                break
                    except Exception:
                        continue
                pts.sort(key=lambda z: z[0])
                return pts

            def to_w(v: int) -> float:
                try:
                    return float(v) / 10000.0
                except Exception:
                    return 0.0

            def ols_speed(series: list) -> float:
                if len(series) < 2:
                    return 0.0
                t0 = series[0][0]
                xs = [(ts - t0).total_seconds() / 3600.0 for ts, _ in series]
                ys = [to_w(sc) for _, sc in series]
                n = len(xs)
                sumx = sum(xs)
                sumy = sum(ys)
                sumxx = sum(x*x for x in xs)
                sumxy = sum(x*y for x, y in zip(xs, ys))
                denom = (n * sumxx - sumx * sumx)
                if abs(denom) < 1e-9:
                    return 0.0
                return (n * sumxy - sumx * sumy) / denom

            ranks_top = [10, 20, 30, 40, 50, 100]
            ranks_border = [200, 300, 400, 500, 1000, 2000, 5000, 10000, 20000]
            summary = {
                "event_id": event_id,
                "event_type": event_type,
                "event_end_time": event_end_time.strftime('%Y-%m-%d %H:%M:%S'),
                "generated_at": now.strftime('%Y-%m-%d %H:%M:%S'),
                "history_retention_days": int(retention_days),
                "ranks": {}
            }

            def summarize(kind: str, rank: int) -> None:
                series = collect_series(kind, rank)
                if not series:
                    return
                final_w = to_w(series[-1][1])
                full_ols_w = ols_speed(series)
                # 最后6小时速度
                cutoff = series[-1][0] - datetime.timedelta(hours=6)
                last = [(ts, sc) for ts, sc in series if ts >= cutoff]
                last_ols_w = ols_speed(last) if len(last) >= 2 else full_ols_w
                sprint_factor = (last_ols_w / full_ols_w) if full_ols_w > 1e-6 else 1.0
                summary["ranks"][str(rank)] = {
                    "kind": kind,
                    "final_w": round(final_w, 4),
                    "ols_w": round(full_ols_w, 4),
                    "last6h_w": round(last_ols_w, 4),
                    "sprint_factor": round(sprint_factor, 4)
                }

            for r in ranks_top:
                summarize('top100', r)
            for r in ranks_border:
                summarize('border', r)

            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            self._log("INFO", f"已生成活动 {event_id} 事后总结: {out_path}")
        except Exception as e:
            self._log("ERROR", f"生成事后总结失败: {e}")

    def _parse_end_time(self, value: Any) -> Optional["datetime.datetime"]:
        """函数说明:
        - 解析配置中的结束时间字段，支持多种格式；返回 `datetime` 或 `None`
        - 支持: ISO 格式、`YYYY-MM-DD HH:MM[:SS]`、`YYYY/MM/DD HH:MM[:SS]`、`YYYYMMDD_HHMMSS`、`YYYYMMDDHHMMSS`
        """
        import datetime
        try:
            if value is None:
                return None
            s = str(value).strip()
            if not s:
                return None
            try:
                # 优先尝试 ISO 格式
                return datetime.datetime.fromisoformat(s.replace("Z", ""))
            except Exception:
                pass
            for fmt in [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M",
                "%Y%m%d_%H%M%S",
                "%Y%m%d%H%M%S",
            ]:
                try:
                    return datetime.datetime.strptime(s, fmt)
                except Exception:
                    continue
            return None
        except Exception:
            return None

    def _log(self, level: str, msg: str) -> None:
        """函数说明: 控制台统一日志格式"""
        import datetime
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}][{level}] {msg}")

    def run_forever(self) -> None:
        """函数说明:
        - 无限循环: 每分钟第 2 秒执行一次；每次读取配置中的 `event_id` 与 `token`
        - 拉取 Top100 与 Border，保存最新与历史，并按配置清理历史(默认 8 天)
        - 在活动结束 10 分钟后生成事后总结文件
        """
        self._log("INFO", "PJSK 排行抓取任务启动: 每分钟第 2 秒执行")
        while True:
            try:
                cfg = self._load_config()
                # 动态读取 token 与 event_id，支持运行时更新
                token = str(cfg.get("token", "")).strip()
                event_id = int(cfg.get("event_id", 0))
                # 结束时间控制，避免远端压力
                start_time = self._parse_end_time(cfg.get("start_time"))
                end_time = self._parse_end_time(cfg.get("end_time"))
                event_end_time = self._parse_end_time(cfg.get("event_end_time"))
                retention_days = int(cfg.get("history_retention_days", 8))
                retention_hours = max(24, retention_days * 24)
                event_type = str(cfg.get("event_type", "混活"))
                import datetime as _dt
                now = _dt.datetime.now()
                should_fetch = True
                # 时间窗口判定：只有在 start_time 之后且 end_time 之前才启动抓取
                # 若 start_time 在 end_time 之后，则认为冲突，不启动抓取
                if start_time and end_time and start_time > end_time:
                    self._log("ERROR", f"时间窗口冲突: start_time({start_time.strftime('%Y-%m-%d %H:%M:%S')}) > end_time({end_time.strftime('%Y-%m-%d %H:%M:%S')})，不启动抓取")
                    should_fetch = False
                elif start_time and now < start_time:
                    self._log("INFO", f"未到启动时间 {start_time.strftime('%Y-%m-%d %H:%M:%S')}，等待中")
                    should_fetch = False
                elif end_time and now >= end_time:
                    self._log("INFO", f"已到达结束时间 {end_time.strftime('%Y-%m-%d %H:%M:%S')}，暂停抓取，等待配置更新")
                    # 事后总结(若符合条件)
                    try:
                        self._post_event_summary(event_id, event_end_time, event_type, retention_days)
                    except Exception:
                        pass
                    should_fetch = False
                elif event_id <= 0:
                    self._log("ERROR", "配置中的 event_id 非法(<=0), 跳过本轮")
                    should_fetch = False

                if should_fetch:
                    if token:
                        # 若 token 更新，刷新客户端头部
                        self.client.session.headers.update({"X-Haruki-Sekai-Token": token})
                    # 拉取 Top100
                    top100 = self.client.get_event_ranking_top100(event_id)
                    # 拉取 Border
                    border = self.client.get_event_ranking_border(event_id)

                    # 保存最新快照
                    self._save_json(os.path.join(self.RANKING_DIR, "top100.json"), top100)
                    self._save_json(os.path.join(self.RANKING_DIR, "border.json"), border)

                    # 保存历史留存
                    ts = self._timestamp()
                    self._save_json(os.path.join(self.HISTORY_DIR, f"{event_id}_{ts}_top100.json"), top100)
                    self._save_json(os.path.join(self.HISTORY_DIR, f"{event_id}_{ts}_border.json"), border)

                    # 清理超期历史(按配置保留天数)
                    self._cleanup_history(max_age_hours=retention_hours)

                    self._log("INFO", f"已更新 event_id={event_id} 的 Top100+Border")
                else:
                    # 在不抓取的周期仍执行历史清理，以控制磁盘占用
                    try:
                        self._cleanup_history(max_age_hours=retention_hours)
                    except Exception:
                        pass
            except requests.HTTPError as he:
                self._log("ERROR", f"HTTP 请求失败: {he}")
            except Exception as e:
                self._log("ERROR", f"抓取异常: {e}")
            finally:
                # 进入下一分钟的第 2 秒
                self._sleep_until_next_minute(at_second=2)


def main() -> None:
    """函数说明:
    - 命令行入口: 启动定时抓取任务
    """
    fetcher = RankingFetcher()
    fetcher.run_forever()


if __name__ == "__main__":
    main()
