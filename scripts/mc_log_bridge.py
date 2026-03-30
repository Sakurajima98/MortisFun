#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件说明:
- Minecraft 日志桥接服务 (FastAPI)
- 读取远程服务器的 Minecraft 日志文件 (latest.log)，解析玩家加入/离开、成就/目标/挑战、死亡等事件
- 将解析到的事件通过 HTTP 接口 `/query` 供外部机器人轮询
- 绑定到 0.0.0.0:8123，适配云安全组开放的公网端口，便于本地 `mc_service` 访问

使用示例:
- 前台运行:
  python3 mc_log_bridge.py --host 0.0.0.0 --port 8123 --log "/opt/mcsmanager/daemon/data/InstanceData/<ID>/logs/latest.log"
- 后台运行:
  nohup python3 mc_log_bridge.py --host 0.0.0.0 --port 8123 --log "/opt/mcsmanager/daemon/data/InstanceData/<ID>/logs/latest.log" >/var/log/mc_log_bridge.out 2>&1 &

接口规范:
- GET /query
  返回尚未传输过的事件列表（全局一次性传输，不再区分客户端）:
  [
    {"id": "...", "ts": 1730000000, "type": "join|leave|advancement|goal|challenge|death|info", "data": {...}}
  ]
"""

import asyncio
import os
import re
import time
import json
import uuid
import argparse
from typing import List, Dict, Any, Optional, Set
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


class MessageEvent:
    """
    类说明:
    - 表示解析到的一条 Minecraft 事件, 并记录已通知的客户端ID集合
    字段:
      id: 事件唯一ID (uuid4)
      ts: 事件时间戳 (秒)
      type: 事件类型 (join/leave/advancement/goal/challenge/death/info)
      data: 事件数据 (玩家名、成就标题、击杀者等)
      notified_client_ids: 已通知的客户端ID集合
    """
    def __init__(self, typ: str, data: Dict[str, Any]):
        """
        函数说明:
        - 初始化消息事件并生成唯一ID和时间戳
        """
        self.id: str = uuid.uuid4().hex
        self.ts: int = int(time.time())
        self.type: str = typ
        self.data: Dict[str, Any] = data
        self.notified_client_ids: Set[str] = set()


class LogBridgeServer:
    """
    类说明:
    - 日志桥接服务核心逻辑: 负责文件尾读、行解析、事件缓存、HTTP接口对接
    """
    def __init__(self, log_path: str, retention_seconds: int = 120):
        """
        函数说明:
        - 初始化日志桥接服务
        参数:
          log_path: Minecraft 日志文件的绝对路径或包含 latest.log 的目录
          retention_seconds: 事件保留时间，超时将被清理
        """
        self.log_path = log_path
        self.retention_seconds = retention_seconds
        self.events: List[MessageEvent] = []
        self.server_started: bool = False
        self._last_pos: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    def resolve_log_path(self, p: str) -> str:
        """
        函数说明:
        - 解析实际日志文件路径
        规则:
          - 若 p 为目录，优先返回 p/latest.log；否则返回目录中最近修改的 *.log
          - 若 p 为文件但不存在，则尝试使用其父目录中的 latest.log 或最近的 *.log
        """
        if not p:
            return p
        try:
            if os.path.isdir(p):
                latest = os.path.join(p, "latest.log")
                if os.path.exists(latest):
                    return latest
                candidates = []
                for name in os.listdir(p):
                    full = os.path.join(p, name)
                    if os.path.isfile(full) and name.lower().endswith(".log"):
                        candidates.append((full, os.path.getmtime(full)))
                if candidates:
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    return candidates[0][0]
                return p
            if not os.path.exists(p):
                parent = os.path.dirname(p)
                if parent and os.path.isdir(parent):
                    latest = os.path.join(parent, "latest.log")
                    if os.path.exists(latest):
                        return latest
                    candidates = []
                    for name in os.listdir(parent):
                        full = os.path.join(parent, name)
                        if os.path.isfile(full) and name.lower().endswith(".log"):
                            candidates.append((full, os.path.getmtime(full)))
                    if candidates:
                        candidates.sort(key=lambda x: x[1], reverse=True)
                        return candidates[0][0]
            return p
        except Exception:
            return p

    def parse_line(self, line: str) -> List[MessageEvent]:
        """
        函数说明:
        - 解析一行日志文本，返回 0~N 条事件
        - 事件类型对齐 `mc_service.py:498-546` 的映射逻辑 (join/leave/advancement/goal/challenge/death)
        - death 事件返回更丰富的数据:
          data = { "player": ..., "killer": 可选, "cause": 可选, "message": 原始行 }
        """
        out: List[MessageEvent] = []
        if not line:
            return out
        raw = line.strip()

        # 玩家加入
        m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+joined the game", raw)
        if m:
            out.append(MessageEvent("join", {"player": m.group(1), "message": raw}))
            return out

        # 玩家离开
        m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+left the game", raw)
        if m:
            out.append(MessageEvent("leave", {"player": m.group(1), "message": raw}))
            return out

        # 成就/挑战/目标
        m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+has made the advancement\s+\[([^\]]+)\]", raw)
        if m:
            out.append(MessageEvent("advancement", {"player": m.group(1), "title": m.group(2), "message": raw}))
            return out
        m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+has completed the challenge\s+\[([^\]]+)\]", raw)
        if m:
            out.append(MessageEvent("challenge", {"player": m.group(1), "title": m.group(2), "message": raw}))
            return out
        m = re.search(r":\s*([A-Za-z0-9_]{3,16})\s+has reached the goal\s+\[([^\]]+)\]", raw)
        if m:
            out.append(MessageEvent("goal", {"player": m.group(1), "title": m.group(2), "message": raw}))
            return out

        # 玩家死亡（详细模板）
        # 1) 被某个实体击杀
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+was\s+slain\s+by\s+(.+)$", raw, re.IGNORECASE)
        if m:
            victim, killer = m.group(1), m.group(2).strip()
            out.append(MessageEvent("death", {"player": victim, "killer": killer, "cause": "击杀", "message": raw}))
            return out
        # 2) 被射杀
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+was\s+shot\s+by\s+(.+)$", raw, re.IGNORECASE)
        if m:
            victim, killer = m.group(1), m.group(2).strip()
            out.append(MessageEvent("death", {"player": victim, "killer": killer, "cause": "射杀", "message": raw}))
            return out
        # 3) 溺亡
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+drowned\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "溺亡", "message": raw}))
            return out
        # 4) 高处坠落
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+fell\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "高处坠落", "message": raw}))
            return out
        # 5) 爆炸
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+was\s+blown\s+up\s+by\s+(.+)$", raw, re.IGNORECASE)
        if m:
            victim, killer = m.group(1), m.group(2).strip()
            out.append(MessageEvent("death", {"player": victim, "killer": killer, "cause": "爆炸致死", "message": raw}))
            return out
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+was\s+blown\s+up\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "爆炸致死", "message": raw}))
            return out
        # 6) 烧伤 / 饥饿 / 窒息
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+burned\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "烧伤致死", "message": raw}))
            return out
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+starved\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "饥饿致死", "message": raw}))
            return out
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+suffocated\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "窒息致死", "message": raw}))
            return out
        # 7) 岩浆
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+tried\s+to\s+swim\s+in\s+lava\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "岩浆致死", "message": raw}))
            return out
        # 8) 被扎死/刺穿/压死
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+was\s+pricked\s+to\s+death\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "扎死", "message": raw}))
            return out
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+was\s+impaled\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "刺穿致死", "message": raw}))
            return out
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+was\s+crushed\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "被压死", "message": raw}))
            return out
        # 9) 方括号插件格式： ...]: [Killer: Killed Victim]
        m = re.search(r"]:\s*\[([A-Za-z0-9_]{3,32}):\s*Killed\s+([A-Za-z0-9_]{3,32})\]", raw, re.IGNORECASE)
        if m:
            killer, victim = m.group(1), m.group(2)
            out.append(MessageEvent("death", {"player": victim, "killer": killer, "cause": "击杀", "message": raw}))
            return out
        # 10) 通用死亡
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+was\s+killed\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "cause": "被击杀", "message": raw}))
            return out
        m = re.search(r"]:\s*([A-Za-z0-9_]{3,32})\s+died\b", raw, re.IGNORECASE)
        if m:
            victim = m.group(1)
            out.append(MessageEvent("death", {"player": victim, "message": raw}))
            return out

        return out

    async def tail_loop(self):
        """
        函数说明:
        - 异步循环读取日志新增内容，并解析生成事件
        - 自动处理日志文件轮转/截断，维护读取偏移
        """
        self.server_started = True
        while True:
            try:
                real_path = self.resolve_log_path(self.log_path)
                if not real_path or not os.path.exists(real_path):
                    await asyncio.sleep(2.0)
                    continue
                size = os.path.getsize(real_path)
                if size < self._last_pos:
                    self._last_pos = 0
                with open(real_path, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(self._last_pos)
                    lines = f.readlines()
                    self._last_pos = f.tell()
                async with self._lock:
                    for raw in lines:
                        line = raw.strip()
                        events = self.parse_line(line)
                        if events:
                            self.events.extend(events)
                    # 清理过期事件
                    now = int(time.time())
                    valid_idx = 0
                    for i, ev in enumerate(self.events):
                        if (now - ev.ts) <= self.retention_seconds:
                            valid_idx = i + 1
                    if valid_idx > 0 and valid_idx < len(self.events):
                        self.events = self.events[valid_idx:]
            except Exception:
                pass
            await asyncio.sleep(1.0)


def create_app(server: LogBridgeServer) -> FastAPI:
    """
    函数说明:
    - 创建 FastAPI 应用并注册生命周期、CORS、中间件与路由
    """
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        asyncio.create_task(server.tail_loop())
        yield

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    @app.get("/query")
    async def query(client_id: Optional[str] = None):
        """
        函数说明:
        - 查询当前缓存的事件并一次性返回；事件在传输后即清空缓存
        - 不再区分客户端链接；参数 client_id 仅为兼容保留
        返回:
          List[Dict]: 事件数组（全局只传一次）
        """
        if not server.server_started:
            return []
        async with server._lock:
            ret: List[Dict[str, Any]] = [
                {"id": ev.id, "ts": ev.ts, "type": ev.type, "data": ev.data}
                for ev in server.events
            ]
            server.events.clear()
        return ret

    return app


def main():
    """
    函数说明:
    - 解析命令行参数并启动 uvicorn HTTP 服务
    """
    parser = argparse.ArgumentParser("Minecraft日志桥接服务")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址 (建议0.0.0.0)")
    parser.add_argument("--port", type=int, default=8123, help="监听端口")
    parser.add_argument("--log", required=True, help="Minecraft日志文件路径或包含latest.log的目录")
    args = parser.parse_args()

    server = LogBridgeServer(log_path=args.log)
    app = create_app(server)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
