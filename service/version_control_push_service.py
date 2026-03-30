import json
import os
from typing import Any, Dict, List, Optional

from service.base_service import BaseService


class VersionControlPushService(BaseService):
    def __init__(self, config: Dict[str, Any], data_manager, text_formatter, server=None) -> None:
        super().__init__(config, data_manager, text_formatter, server)
        self.service_name = "version_control_push"
        self.service_config = self._load_service_config()
        self.enabled = self.service_config.get("enabled", True)
        self.admin_users = [str(x) for x in (self.get_service_config("admin_users", []) or [])]
        self.excluded_groups = [str(x) for x in (self.get_service_config("excluded_groups", []) or [])]
        self.version_file = str(self.get_service_config("version_file", "version.json") or "version.json")

    async def process_message(self, message: str, user_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        try:
            if not isinstance(message, str):
                return None
            text = message.strip()
            if not (text.startswith("/更新推送") or text.startswith("/信息推送")):
                return None

            if str(user_id) not in self.admin_users:
                return {"content": "❌ 权限不足，仅管理员可使用该指令。"}

            if text.startswith("/信息推送"):
                content = text[len("/信息推送"):].strip()
                if not content:
                    return {"content": "❌ 请输入推送内容，例如：/信息推送 内容"}
                result = await self._broadcast_to_groups([{"type": "text", "data": {"text": content}}])
                return {"content": result}

            version_data = self._load_version_data()
            if not version_data:
                return {"content": "❌ 版本信息读取失败，请检查 version.json 配置。"}
            segments = self._build_version_segments(version_data)
            if not segments:
                return {"content": "❌ 版本信息为空，推送已取消。"}
            result = await self._broadcast_to_groups(segments)
            return {"content": result}
        except Exception as e:
            self.log_unified("ERROR", f"版本推送处理失败: {e}", kwargs.get("group_id"), user_id)
            return {"content": "❌ 推送过程中出现错误，请稍后重试。"}

    def get_help_text(self) -> Dict[str, Any]:
        return {
            "content": (
                "📌 版本控制与信息推送\n"
                "可用指令：\n"
                "• /更新推送  - 推送 version.json 中的版本信息\n"
                "• /信息推送 内容  - 推送自定义消息到所有群\n"
                "仅管理员可用"
            )
        }

    def _load_version_data(self) -> Optional[Dict[str, Any]]:
        try:
            with open(self.version_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
            return None
        except Exception as e:
            self.log_unified("ERROR", f"读取版本信息失败: {e}", "system", "system")
            return None

    def _build_version_segments(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        title = str(data.get("title") or "版本更新").strip()
        version = str(data.get("version") or "").strip()
        date = str(data.get("date") or "").strip()
        summary = str(data.get("summary") or "").strip()
        content = str(data.get("content") or "").strip()
        items = data.get("items") or []
        lines: List[str] = [f"📣 {title}"]
        if version:
            lines.append(f"版本号：{version}")
        if date:
            lines.append(f"更新时间：{date}")
        if summary:
            lines.append(summary)
        if isinstance(items, list) and items:
            lines.append("更新内容：")
        elif content:
            lines.append(content)
        segments: List[Dict[str, Any]] = [{"type": "text", "data": {"text": "\n".join(lines)}}]
        if isinstance(items, list) and items:
            for idx, item in enumerate(items, 1):
                if isinstance(item, dict):
                    item_text = str(item.get("text") or "").strip()
                    image_path = str(item.get("image") or item.get("image_path") or "").strip()
                else:
                    item_text = str(item).strip()
                    image_path = ""
                if item_text:
                    segments.append(
                        {"type": "text", "data": {"text": f"{idx}. {item_text}"}}
                    )
                image_segment = self._build_image_segment(image_path)
                if image_segment:
                    segments.append(image_segment)
        return segments

    def _build_image_segment(self, image_path: str) -> Optional[Dict[str, Any]]:
        if not image_path:
            return None
        if image_path.startswith("http://") or image_path.startswith("https://") or image_path.startswith("file://"):
            file_value = image_path
        else:
            abs_path = os.path.abspath(image_path)
            file_value = f"file://{abs_path}"
        return {"type": "image", "data": {"file": file_value}}

    async def _broadcast_to_groups(self, message_segments: List[Dict[str, Any]]) -> str:
        group_ids = await self._get_group_list()
        if not group_ids:
            return "❌ 未获取到群列表，推送已取消。"
        excluded_set = {str(x) for x in self.excluded_groups}
        total = 0
        sent = 0
        skipped = 0
        for gid in group_ids:
            gid_str = str(gid)
            if gid_str in excluded_set:
                skipped += 1
                continue
            total += 1
            payload = {
                "action": "send_group_msg",
                "params": {
                    "group_id": int(gid_str) if gid_str.isdigit() else gid_str,
                    "message": message_segments,
                },
            }
            ok = False
            if self.server and hasattr(self.server, "send_response_to_napcat"):
                try:
                    ok = await self.server.send_response_to_napcat(payload)
                except Exception:
                    ok = False
            if ok:
                sent += 1
        return f"✅ 推送完成：成功 {sent}/{total} 个群，排除 {skipped} 个群。"

    async def _get_group_list(self) -> List[str]:
        if not self.server or not hasattr(self.server, "call_napcat_api"):
            return []
        try:
            response = await self.server.call_napcat_api({"action": "get_group_list"})
            if not response or response.get("status") != "ok":
                return []
            data = response.get("data") or []
            group_ids = []
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        gid = item.get("group_id")
                    else:
                        gid = item
                    if gid is not None:
                        group_ids.append(str(gid))
            return group_ids
        except Exception as e:
            self.log_unified("ERROR", f"获取群列表失败: {e}", "system", "system")
            return []
