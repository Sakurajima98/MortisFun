import json
import os
from datetime import datetime
from typing import Any, Dict, Optional, List

from service.base_service import BaseService


class WorkOrderService(BaseService):
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        super().__init__(config, data_manager, text_formatter, server)
        self.service_name = "work_order"
        self.service_config = self._load_service_config()
        self.enabled = self.service_config.get("enabled", True)
        self.admin_users = [str(x) for x in (self.get_service_config("admin_users", []) or [])]
        self.notify_groups = [str(x) for x in (self.get_service_config("notify_groups", []) or [])]
        self.notify_users = [str(x) for x in (self.get_service_config("notify_users", []) or [])]
        self.data_dir = str(self.get_service_config("data_dir", os.path.join("data", "work")) or os.path.join("data", "work"))
        os.makedirs(self.data_dir, exist_ok=True)
        self.index_file = os.path.join(self.data_dir, "index.json")

    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        try:
            if not isinstance(message, str):
                return None
            text = message.strip()
            if not (text.startswith("/提交工单") or text.startswith("/查询工单") or text.startswith("/完成工单")):
                return None

            if text.startswith("/提交工单"):
                content = text[len("/提交工单"):].strip()
                if not content:
                    return {"content": "❌ 请输入工单内容，例如：/提交工单 问题描述"}
                order = self._create_work_order(content, str(user_id), str(kwargs.get("group_id", "") or ""))
                await self._notify_targets(order)
                return {"content": f"✅ 工单已提交，编号：{order['id']}"}

            if text.startswith("/查询工单"):
                order_id = text[len("/查询工单"):].strip()
                if not order_id:
                    return {"content": "❌ 请输入工单号，例如：/查询工单 1"}
                order = self._load_work_order(order_id)
                if not order:
                    return {"content": "❌ 未找到对应工单。"}
                requester_id = str(user_id)
                is_admin = requester_id in self.admin_users
                return {"content": self._format_order_status(order, requester_id, is_admin)}

            if str(user_id) not in self.admin_users:
                return {"content": "❌ 权限不足，仅管理员可使用该指令。"}

            rest = text[len("/完成工单"):].strip()
            if not rest:
                return {"content": "❌ 请输入工单号和批语，例如：/完成工单 1 已处理"}
            parts = rest.split(maxsplit=1)
            order_id = parts[0].strip()
            reply = parts[1].strip() if len(parts) > 1 else ""
            if not reply:
                return {"content": "❌ 请输入批语，例如：/完成工单 1 已处理"}
            order = self._load_work_order(order_id)
            if not order:
                return {"content": "❌ 未找到对应工单。"}
            order["status"] = "completed"
            order["reply"] = reply
            order["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            order["completed_by"] = str(user_id)
            self._save_work_order(order)
            return {"content": f"✅ 工单 {order_id} 已完成"}
        except Exception as e:
            self.log_unified("ERROR", f"工单处理失败: {e}", kwargs.get("group_id"), user_id)
            return {"content": "❌ 工单处理失败，请稍后重试。"}

    def get_help_text(self) -> Dict[str, Any]:
        return {
            "content": (
                "🧾 工单服务\n"
                "可用指令：\n"
                "• /提交工单 内容\n"
                "• /查询工单 工单号\n"
                "• /完成工单 工单号 批语（管理员）"
            )
        }

    def _create_work_order(self, content: str, user_id: str, group_id: str) -> Dict[str, Any]:
        order_id = str(self._next_order_id())
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order = {
            "id": order_id,
            "content": content,
            "status": "open",
            "reply": "",
            "created_at": now,
            "user_id": user_id,
            "group_id": group_id,
        }
        self._save_work_order(order)
        return order

    def _format_order_status(self, order: Dict[str, Any], requester_id: str, is_admin: bool) -> str:
        status = "已完成" if order.get("status") == "completed" else "处理中"
        owner_id = str(order.get("user_id") or "")
        is_owner = requester_id == owner_id
        lines = [
            f"工单号：{order.get('id')}",
            f"状态：{status}",
            f"内容：{order.get('content')}",
        ]
        if is_admin or is_owner:
            lines.append(f"提交时间：{order.get('created_at')}")
            lines.append(f"提交人：{order.get('user_id')}")
            group_id = str(order.get("group_id") or "")
            if group_id:
                lines.append(f"提交群聊：{group_id}")
            if order.get("status") == "completed":
                lines.append(f"完成时间：{order.get('completed_at')}")
        if order.get("status") == "completed":
            reply = str(order.get("reply") or "").strip()
            if reply:
                lines.append(f"批语：{reply}")
        return "\n".join(lines)

    def _order_file_path(self, order_id: str) -> str:
        safe_id = str(order_id).strip()
        return os.path.join(self.data_dir, f"work_{safe_id}.json")

    def _load_work_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        file_path = self._order_file_path(order_id)
        data = self._read_json(file_path)
        return data if isinstance(data, dict) else None

    def _save_work_order(self, order: Dict[str, Any]) -> None:
        file_path = self._order_file_path(order.get("id"))
        self._write_json(file_path, order)

    def _next_order_id(self) -> int:
        data = self._read_json(self.index_file) or {}
        last_id = int(data.get("last_id", 0)) if isinstance(data, dict) else 0
        new_id = last_id + 1
        self._write_json(self.index_file, {"last_id": new_id})
        return new_id

    def _read_json(self, file_path: str) -> Optional[Dict[str, Any]]:
        try:
            if not os.path.exists(file_path):
                return None
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _write_json(self, file_path: str, data: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    async def _notify_targets(self, order: Dict[str, Any]) -> None:
        if not self.server or not hasattr(self.server, "send_response_to_napcat"):
            return
        content = self._format_notify_text(order)
        segments = [{"type": "text", "data": {"text": content}}]
        for gid in self.notify_groups:
            payload = {
                "action": "send_group_msg",
                "params": {
                    "group_id": int(gid) if str(gid).isdigit() else gid,
                    "message": segments,
                },
            }
            try:
                await self.server.send_response_to_napcat(payload)
            except Exception:
                pass
        for uid in self.notify_users:
            payload = {
                "action": "send_private_msg",
                "params": {
                    "user_id": int(uid) if str(uid).isdigit() else uid,
                    "message": segments,
                },
            }
            try:
                await self.server.send_response_to_napcat(payload)
            except Exception:
                pass

    def _format_notify_text(self, order: Dict[str, Any]) -> str:
        group_id = str(order.get("group_id") or "")
        lines: List[str] = [
            "🧾 新工单",
            f"编号：{order.get('id')}",
            f"内容：{order.get('content')}",
            f"提交人：{order.get('user_id')}",
            f"提交时间：{order.get('created_at')}",
        ]
        if group_id:
            lines.append(f"提交群聊：{group_id}")
        return "\n".join(lines)
