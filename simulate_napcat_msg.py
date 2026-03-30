#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件说明:
- NapCat 消息模拟客户端: 连接到 MortisfunServer 的 WebSocket, 发送 OneBot v11 群消息事件
- 用途: 在没有 NapCat 的情况下, 验证服务路由与 /send 指令触发 MC 广播逻辑

使用示例:
    python simulate_napcat_msg.py --text "/send 模拟消息到MC服务器" --group 721157770
"""

import asyncio
import json
import argparse
import websockets


async def simulate(text: str, group_id: str, server_url: str):
    """
    函数说明:
    - 连接到指定 WebSocket 服务器并发送 OneBot v11 群消息事件
    参数:
        text: 发送的纯文本内容, 例如 "/send Hello"
        group_id: 目标群号字符串
        server_url: WebSocket 服务器地址, 例如 "ws://localhost:8003/fun"
    """
    print(f"连接到服务器: {server_url}")
    async with websockets.connect(server_url) as ws:
        # 接收欢迎消息
        try:
            welcome = await asyncio.wait_for(ws.recv(), timeout=3.0)
            print("收到欢迎消息:", welcome)
        except Exception:
            print("未收到欢迎消息, 继续发送事件")
        # 构造 OneBot v11 群消息事件
        event = {
            "post_type": "message",
            "message_type": "group",
            "group_id": group_id,
            "user_id": "123456789",
            "message": [
                {
                    "type": "text",
                    "data": {
                        "text": text
                    }
                }
            ]
        }
        await ws.send(json.dumps(event, ensure_ascii=False))
        print("已发送事件:", event)
        # 尝试接收服务器响应(若有)
        try:
            resp = await asyncio.wait_for(ws.recv(), timeout=3.0)
            print("收到服务器响应:", resp)
        except Exception:
            print("在超时内未收到响应, 可能为服务端不回包或异步发送到NapCat")


def main():
    """
    函数说明:
    - 解析命令行参数并运行模拟
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--text", default="/send 模拟消息到MC服务器", help="要发送的文本内容")
    parser.add_argument("--group", default="721157770", help="目标群号")
    parser.add_argument("--url", default="ws://localhost:8003/fun", help="WebSocket服务器地址")
    args = parser.parse_args()
    asyncio.run(simulate(args.text, args.group, args.url))


if __name__ == "__main__":
    main()
