#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import websockets
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from typing import Dict, Any, Optional, Set, Union, List
from datetime import datetime
import traceback
from asyncio import Queue, Semaphore
from concurrent.futures import ThreadPoolExecutor
import time
import threading

# 导入服务模块
from service.calculator_service import CalculatorService

try:
    from service.help_service import HelpService
except Exception:
    HelpService = None
try:
    from service.team_service import TeamService
except Exception:
    TeamService = None
try:
    from service.team_reminder_service import TeamReminderService
except Exception:
    TeamReminderService = None
try:
    from service.mc_service import McService
except Exception:
    McService = None
try:
    from service.pjskranking_service import PJSKRankingService
except Exception:
    PJSKRankingService = None
try:
    from service.pjskgachashistory_service import PJSKGachaHistoryService
except Exception:
    PJSKGachaHistoryService = None
try:
    from service.user_management_service import UserManagementService
except Exception:
    UserManagementService = None
try:
    from service.version_control_push_service import VersionControlPushService
except Exception:
    VersionControlPushService = None
try:
    from service.work_order_service import WorkOrderService
except Exception:
    WorkOrderService = None
try:
    from service.audit_service import AuditService
except Exception:
    AuditService = None
try:
    from service.gallery_service import GalleryService
except Exception:
    GalleryService = None
try:
    from service.pan_search_service import PanSearchService
except Exception:
    PanSearchService = None
try:
    from service.music_service import MusicService
except Exception:
    MusicService = None
try:
    from service.voice_service import VoiceService
except Exception:
    VoiceService = None
try:
    from service.gacha_service import GachaService
except Exception:
    GachaService = None
try:
    from service.ranking_game_service import RankingGameService
except Exception:
    RankingGameService = None
try:
    from service.push_time_statistics_service import PushTimeStatisticsService
except Exception:
    PushTimeStatisticsService = None
try:
    from service.daily_team_statistics_service import DailyTeamStatisticsService
except Exception:
    DailyTeamStatisticsService = None
try:
    from service.daily_push_time_statistics_service import (
        DailyPushTimeStatisticsService,
    )
except Exception:
    DailyPushTimeStatisticsService = None
try:
    from service.daily_wife_service import DailyWifeService
except Exception:
    DailyWifeService = None
try:
    from service.mutsmi_service import MutsmiService
except Exception:
    MutsmiService = None
try:
    from service.spell_service import SpellService
except Exception:
    SpellService = None
try:
    from service.group_member_service import GroupMemberService
except Exception:
    GroupMemberService = None
try:
    from service.tarot_service import TarotService
except Exception:
    TarotService = None
try:
    from service.fortune_service import FortuneService
except Exception:
    FortuneService = None
try:
    from service.daily_fortune_service import DailyFortuneService
except Exception:
    DailyFortuneService = None
try:
    from service.choice_service import ChoiceService
except Exception:
    ChoiceService = None
try:
    from service.chat_service import ChatService
except Exception:
    ChatService = None
try:
    from service.conversation_statistics_service import ConversationStatisticsService
except Exception:
    ConversationStatisticsService = None
try:
    from service.napcat_integration import create_napcat_integration
except Exception:
    create_napcat_integration = None
try:
    from utils.pjsk_ranking_fetcher import RankingFetcher
except Exception:
    RankingFetcher = None

from data_manager import DataManager
from text_formatter import TextFormatter


class MortisfunServer:
    """
    Mortisfun WebSocket服务器主类

    负责管理WebSocket连接、消息路由、服务调度等核心功能。
    作为napcat客户端的服务端，处理来自QQ的消息并提供相应服务。
    """

    def __init__(self, config_path: str = "config.json"):
        """
        初始化Mortisfun服务器

        Args:
            config_path (str): 配置文件路径
        """
        # 加载配置
        self.config = self._load_config(config_path)
        # Validate and normalize configuration early
        try:
            self._validate_config()
            self._ensure_config_defaults()
        except Exception as conf_err:
            # 输出到标准输出，待日志系统就绪后再统一记录
            print(f"配置初始化失败: {conf_err}")
            raise

        # 初始化日志（必须在使用logger之前）
        self._setup_logging()

        # 初始化logger
        self.logger = logging.getLogger("MortisfunServer")

        # 初始化核心组件
        self.data_manager = DataManager()
        self.text_formatter = TextFormatter()

        # 初始化服务
        self.services = self._initialize_services()

        # WebSocket连接管理
        self.connected_clients: Set[websockets.WebSocketServerProtocol] = set()
        self.napcat_client: Optional[websockets.WebSocketServerProtocol] = None

        # 服务器状态
        self.is_running = False
        self.server = None

        # 消息处理统计
        self.message_count = 0
        self.start_time = datetime.now()

        # 并发处理相关
        self.message_queue = Queue(maxsize=1000)  # 消息队列，最大1000条消息
        self.processing_semaphore = Semaphore(10)  # 并发处理限制，最多同时处理10个消息
        self.thread_pool = ThreadPoolExecutor(
            max_workers=5
        )  # 线程池，用于CPU密集型任务

        # 初始化NapCat集成模块
        self.napcat_integration = None
        self.processing_tasks = set()  # 跟踪正在处理的任务
        self.is_processing = False  # 消息处理器状态

        # API请求响应管理
        self.pending_api_requests = {}  # 存储等待响应的API请求

        self.logger.info("Mortisfun服务器初始化完成")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        加载配置文件

        Args:
            config_path (str): 配置文件路径

        Returns:
            Dict[str, Any]: 配置字典
        """
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"配置文件未找到: {config_path}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"配置文件格式错误: {config_path} - {e}") from e
        except Exception as e:
            raise RuntimeError(f"加载配置文件失败: {config_path}，{e}")

    def _validate_config(self) -> None:
        """验证关键配置项是否存在且符合基本结构，避免启动时崩溃。"""
        if not isinstance(self.config, dict):
            raise ValueError("配置文件应返回字典对象")
        # websocket 配置至少需要一个容器来获取 host/port
        if "websocket" not in self.config or not isinstance(
            self.config.get("websocket"), dict
        ):
            raise ValueError("缺少 websocket 配置或格式不正确")
        # bot 名称/版本可选，若缺失后续会回落到默认值
        if "bot" in self.config and not isinstance(self.config.get("bot"), dict):
            raise ValueError("显式的 bot 配置应为字典对象")

    def _ensure_config_defaults(self) -> None:
        """为关键配置项填充默认值，确保在无配置时也能以合理方式启动。"""
        # websocket defaults
        ws = self.config.setdefault("websocket", {})
        ws.setdefault("host", "127.0.0.1")
        ws.setdefault("port", 8765)
        ws.setdefault("path", "/fun")
        ws.setdefault("heartbeat_interval", 30)
        ws.setdefault("max_message_size", 1024 * 1024)
        # logging defaults
        log = self.config.setdefault("logging", {})
        log.setdefault("level", "INFO")
        log.setdefault("file", "mortisfun.log")
        log.setdefault("max_size", "10MB")
        log.setdefault("backup_count", 5)
        # bot defaults
        bot = self.config.setdefault("bot", {})
        bot.setdefault("name", "MortisfunBot")
        bot.setdefault("version", "0.1")

    def _setup_logging(self):
        """
        设置日志配置（使用滚动文件处理器）

        说明：
        - 日志文件名从配置文件 `logging.file` 读取，默认 `mortisfun.log`。
        - 日志文件统一存放在 `logs/` 目录下，若不存在则自动创建。
        - 支持根据配置的 `max_size` 与 `backup_count` 进行日志滚动。
        - 同时输出到控制台，方便实时观察。
        """
        log_config = self.config.get("logging", {})
        log_level = getattr(logging, log_config.get("level", "INFO"))
        log_file_name = log_config.get("file", "mortisfun.log")
        max_size_str = log_config.get("max_size", "10MB")
        backup_count = int(log_config.get("backup_count", 5))

        # 解析 max_size（支持KB/MB/GB），默认MB
        size_multiplier = 1
        upper = str(max_size_str).upper()
        if upper.endswith("KB"):
            size_multiplier = 1024
            size_value = upper[:-2]
        elif upper.endswith("MB"):
            size_multiplier = 1024 * 1024
            size_value = upper[:-2]
        elif upper.endswith("GB"):
            size_multiplier = 1024 * 1024 * 1024
            size_value = upper[:-2]
        else:
            # 纯数字或未带单位，按字节处理
            size_value = upper
        try:
            max_bytes = int(float(size_value) * size_multiplier)
        except Exception:
            max_bytes = 10 * 1024 * 1024  # 解析失败回退到10MB

        # 统一日志目录（确保目录存在）
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, log_file_name)

        # 配置日志格式
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # 滚动文件处理器（避免日志无限增长）
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)

        # 控制台处理器（保留，但对统一日志进行抑制，避免与 print 重复）
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        class SuppressUnifiedFilter(logging.Filter):
            """过滤器：抑制由统一日志接口写入的控制台重复输出"""

            def filter(self, record: logging.LogRecord) -> bool:
                return not getattr(record, "is_unified", False)

        console_handler.addFilter(SuppressUnifiedFilter())

        # 配置根日志器（清理旧处理器防止重复）
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        for h in list(root_logger.handlers):
            try:
                root_logger.removeHandler(h)
            except Exception:
                pass
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

    def _get_configured_log_file_path(self) -> str:
        """
        获取配置的日志文件完整路径（位于 `logs/` 目录）

        Returns:
            str: 日志文件的绝对或相对路径（相对项目根目录）
        """
        log_config = self.config.get("logging", {})
        file_name = log_config.get("file", "mortisfun.log")
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, file_name)

    def format_unified_log(
        self, level: str, group_id: str = None, user_id: str = None, message: str = ""
    ) -> str:
        """
        格式化统一的日志消息

        Args:
            level: 日志级别 (INFO, WARNING, ERROR等)
            group_id: QQ群聊的群号 (可选)
            user_id: 用户QQ号 (可选)
            message: 日志消息内容

        Returns:
            格式化后的日志消息
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]  # 精确到毫秒

        # 构建日志前缀
        log_parts = [f"[{timestamp}]", f"[{level}]"]

        if group_id:
            log_parts.append(f"[G:{group_id}]")

        if user_id:
            log_parts.append(f"[U:{user_id}]")

        # 组合完整日志消息
        log_prefix = "".join(log_parts)
        return f"{log_prefix}:{message}"

    def _log_file_path(self, file_name: str = "unified.log") -> str:
        """
        Return the log file path under the logs directory, creating it if needed.
        """
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, file_name)

    def _write_log(self, text: str, file_name: str = "unified.log") -> None:
        """
        Safely append a log line to the specified log file.
        """
        try:
            path = self._log_file_path(file_name)
            with open(path, "a", encoding="utf-8") as f:
                f.write(text + "\n")
        except Exception:
            # Do not raise on logging failures to avoid impacting runtime
            pass

    async def _log_worker(self):
        """
        Background task to persist async logs from the log queue.
        """
        self.logger.info("Async log worker started")
        try:
            while self.is_running or not self.log_queue.empty():
                try:
                    item = await asyncio.wait_for(self.log_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                text = item.get("text", "")
                file_name = item.get("file_name", "async_unified.log")
                try:
                    path = self._log_file_path(file_name)
                    with open(path, "a", encoding="utf-8") as f:
                        f.write(text + "\n")
                except Exception:
                    pass
                finally:
                    self.log_queue.task_done()
        finally:
            self.logger.info("Async log worker stopped")

    def _enqueue_log(self, text: str, file_name: str = "async_unified.log") -> None:
        """Enqueue a log line to be persisted by the background log worker."""
        try:
            if hasattr(self, "log_queue"):
                self.log_queue.put_nowait({"text": text, "file_name": file_name})
        except Exception:
            # 若队列满或其他异常，静默回退，不影响主流程
            pass

    def _compose_log_message_from_segments(
        self,
        message_content: Union[str, List[Dict[str, Any]]],
        processed_result: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        将QQ消息的段落结构组合为可读的日志文本（兼容图片等非文本消息）。

        设计目的：
        - 之前仅提取 `text` 段，导致纯图片消息在日志中显示为空。本方法对 `image` 等段添加占位与关键字段摘要，确保日志不再为空。
        - 如存在 NapCat 解析结果（`processed_result`），融合其文本与图片信息，以提升日志可读性与完整性。

        Args:
            message_content: 原始消息内容，可能是字符串或由多段组成的列表。
            processed_result: NapCat 解析结果（可选），包含 `parsed.text_content` 与 `images`/`image_results`。

        Returns:
            组合后的日志文本字符串，尽量保留关键可读信息。
        """

        # 基础组合：处理原始消息段
        def _summarize_image_segment(data: Dict[str, Any]) -> str:
            keys = [
                "summary",
                "file",
                "sub_type",
                "url",
                "file_size",
                "appid",
                "fileid",
                "rkey",
            ]
            kvs = []
            for k in keys:
                v = data.get(k)
                if v is not None and v != "":
                    kvs.append(f"{k}={v}")
            return f"[CQ:image,{','.join(kvs)}]" if kvs else "[CQ:image]"

        composed = ""
        if isinstance(message_content, list):
            parts: List[str] = []
            for seg in message_content:
                seg_type = seg.get("type")
                seg_data = seg.get("data", {}) or {}
                if seg_type == "text":
                    parts.append(seg_data.get("text", ""))
                elif seg_type == "image":
                    parts.append(_summarize_image_segment(seg_data))
                else:
                    # 其他段落类型保留占位，避免丢失信息
                    parts.append(f"[{seg_type}]")
            composed = "".join(parts).strip()
        else:
            composed = str(message_content or "").strip()

        # 融合 NapCat 解析结果（如可用）
        try:
            if processed_result and processed_result.get("success"):
                parsed = processed_result.get("parsed", {}) or {}
                text_content = parsed.get("text_content") or ""
                if text_content:
                    if composed:
                        # 若已有内容，拼接解析文本以提高完整度
                        composed = f"{composed} {text_content}".strip()
                    else:
                        composed = text_content.strip()

                # 兼容 images / image_results
                images = (
                    processed_result.get("images")
                    or processed_result.get("image_results")
                    or []
                )
                if images:
                    img_summaries: List[str] = []
                    for img in images[:3]:  # 最多记录前三个，避免日志过长
                        kvs = []
                        for k in ["file_path", "url", "summary", "width", "height"]:
                            v = img.get(k)
                            if v:
                                kvs.append(f"{k}={v}")
                        img_summaries.append(
                            f"[NAPCAT_IMAGE,{','.join(kvs)}]"
                            if kvs
                            else "[NAPCAT_IMAGE]"
                        )
                    if img_summaries:
                        suffix = " ".join(img_summaries)
                        composed = f"{composed} {suffix}".strip()
        except Exception:
            # 安全保护：解析融合异常不影响基础日志
            pass

        # 若最终仍为空，给出非文本提示，避免日志空白
        return composed if composed else "[非文本消息]"

    def log_and_print(
        self, level: str, group_id: str = None, user_id: str = None, message: str = ""
    ):
        """
        统一的日志记录函数：同时输出到终端并通过标准 logging 写入配置的日志文件

        Args:
            level (str): 日志级别 (INFO, WARNING, ERROR等)
            group_id (str, optional): QQ群聊的群号
            user_id (str, optional): 用户QQ号
            message (str): 日志消息内容
        """
        # 统一格式化日志消息（包含毫秒与群组/用户维度）
        log_msg = self.format_unified_log(level, group_id, user_id, message)
        # 将日志异步写入日志队列（备份到异步日志文件）
        try:
            self._enqueue_log(log_msg, file_name="async_unified.log")
        except Exception:
            pass

        # 输出到终端（保持原有实时可读性）
        print(log_msg)

        # 使用标准 logging 输出到文件（由 _setup_logging 配置的处理器负责写入）
        try:
            level_upper = (level or "INFO").upper()
            extra = {"is_unified": True}
            if level_upper == "DEBUG":
                logging.debug(log_msg, extra=extra)
            elif level_upper == "INFO":
                logging.info(log_msg, extra=extra)
            elif level_upper == "WARNING" or level_upper == "WARN":
                logging.warning(log_msg, extra=extra)
            elif level_upper == "ERROR":
                logging.error(log_msg, extra=extra)
            elif level_upper == "CRITICAL" or level_upper == "FATAL":
                logging.critical(log_msg, extra=extra)
            else:
                # 未知级别，回退到INFO
                logging.info(log_msg)
        except Exception as e:
            # 若标准 logging 写入失败，提供降级提示但不影响终端输出
            fallback_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            print(f"[{fallback_ts}][ERROR][G:system][U:system]: logging写入失败: {e}")

    def _initialize_services(self) -> Dict[str, Any]:
        """
        初始化所有服务模块

        Returns:
            Dict[str, Any]: 服务实例字典
        """
        services = {}

        try:
            message_sender = self.send_response_to_napcat

            if HelpService:
                services["help"] = HelpService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if TeamReminderService:
                services["team_reminder"] = TeamReminderService(
                    self.config,
                    self.data_manager,
                    self.text_formatter,
                    message_sender=message_sender,
                    server=self,
                )

            if TeamService:
                services["team"] = TeamService(
                    self.config,
                    self.data_manager,
                    self.text_formatter,
                    message_sender=message_sender,
                    server=self,
                    reminder_service=services.get("team_reminder"),
                )

            if McService:
                services["mc"] = McService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if PJSKRankingService:
                services["pjskranking"] = PJSKRankingService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if PJSKGachaHistoryService:
                services["pjskgachashistory"] = PJSKGachaHistoryService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if UserManagementService:
                services["user_management"] = UserManagementService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if VersionControlPushService:
                services["version_control_push"] = VersionControlPushService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if WorkOrderService:
                services["work_order"] = WorkOrderService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if AuditService:
                services["audit"] = AuditService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if GalleryService:
                services["gallery"] = GalleryService(
                    self.config,
                    self.data_manager,
                    self.text_formatter,
                    self.call_napcat_api,
                    self,
                )

            if PanSearchService:
                services["pan_search"] = PanSearchService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if MusicService:
                services["music"] = MusicService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if VoiceService:
                services["voice"] = VoiceService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            services["calculator"] = CalculatorService(
                self.config, self.data_manager, self.text_formatter, self
            )
            # Initialize asynchronous logging queue (worker is started after event loop starts)
            self.log_queue = asyncio.Queue(maxsize=1000)
            self._log_worker_task = None

            if GachaService:
                gacha_data_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "cards.json"
                )
                services["gacha"] = GachaService(gacha_data_path, self.config)

            if RankingGameService:
                services["ranking_game"] = RankingGameService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if PushTimeStatisticsService:
                services["push_time_statistics"] = PushTimeStatisticsService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if DailyTeamStatisticsService:
                services["daily_team_statistics"] = DailyTeamStatisticsService(
                    self.config,
                    self.data_manager,
                    self.text_formatter,
                    message_sender=message_sender,
                    server=self,
                )

            if DailyPushTimeStatisticsService:
                services["daily_push_time_statistics"] = DailyPushTimeStatisticsService(
                    self.config,
                    self.data_manager,
                    self.text_formatter,
                    message_sender=message_sender,
                    server=self,
                )

            if DailyWifeService:
                services["daily_wife"] = DailyWifeService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if MutsmiService:
                services["mutsmi"] = MutsmiService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if SpellService:
                services["spell"] = SpellService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if GroupMemberService:
                services["group_member"] = GroupMemberService(
                    self.config,
                    self.data_manager,
                    self.text_formatter,
                    message_sender=self.call_napcat_api,
                    server=self,
                )

            if TarotService:
                services["tarot"] = TarotService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if FortuneService:
                services["fortune"] = FortuneService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if DailyFortuneService:
                services["daily_fortune"] = DailyFortuneService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if ChoiceService:
                services["choice"] = ChoiceService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            if ChatService:
                services["chat"] = ChatService(
                    self.config, self.data_manager, self.text_formatter, self
                )

            # 记录服务初始化成功信息 - 使用统一格式
            success_log = self.format_unified_log(
                "INFO", "system", "system", f" 成功初始化 {len(services)} 个服务模块"
            )
            print(success_log)
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(success_log + "\n")
            except Exception:
                pass
            return services

        except Exception as e:
            # 记录服务初始化失败信息 - 使用统一格式
            error_log = self.format_unified_log(
                "ERROR", "system", "system", f" 服务初始化失败: {e}"
            )
            print(error_log)
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log + "\n")
            except Exception:
                pass
            raise

    async def handle_client_connection(self, websocket, path):
        """
        处理客户端连接

        Args:
            websocket: WebSocket连接对象
            path: 连接路径
        """
        client_address = websocket.remote_address
        self.logger.info(f"新客户端连接: {client_address} -> {path}")

        # 添加到连接集合
        self.connected_clients.add(websocket)

        # 记录最新的客户端为napcat客户端（兼容不同路径和多端连接）
        self.napcat_client = websocket
        self.logger.info(
            f"🎉 客户端已连接: {client_address}, path={path}，已设置为NapCat API连接"
        )

        try:
            # 发送欢迎消息
            bot_conf = self.config.get("bot", {})
            welcome_msg = {
                "type": "system",
                "message": "连接成功",
                "server_info": {
                    "name": bot_conf.get("name", "MortisfunBot"),
                    "version": bot_conf.get("version", "0.1"),
                    "time": datetime.now().isoformat(),
                },
            }
            await websocket.send(json.dumps(welcome_msg, ensure_ascii=False))

            # 处理消息循环
            async for message in websocket:
                await self.handle_message(websocket, message)

        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"客户端断开连接: {client_address}")
        except Exception as e:
            self.logger.error(f"处理客户端连接时出错: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            # 清理连接
            self.connected_clients.discard(websocket)
            if websocket == self.napcat_client:
                self.napcat_client = None
                self.logger.warning("⚠️ Napcat客户端连接已断开！")
                self.logger.error("❌ QQ消息发送功能已停止，请重新连接napcat客户端")

    async def handle_message(self, websocket, message: str):
        """
        处理接收到的消息（异步队列版本）

        Args:
            websocket: WebSocket连接对象
            message (str): 接收到的消息
        """
        try:
            # 解析JSON消息
            data = json.loads(message)
            self.message_count += 1

            # 检查是否是API响应（包含echo字段）
            if "echo" in data and hasattr(self, "pending_api_requests"):
                echo_id = data["echo"]
                if echo_id in self.pending_api_requests:
                    # 这是一个API响应，将结果传递给等待的Future
                    future = self.pending_api_requests[echo_id]
                    if not future.done():
                        future.set_result(data)
                    self.logger.debug(f"✅ 处理API响应: {echo_id}")
                    return

            # 根据消息类型处理
            if data.get("post_type") == "message":
                # 使用新的NapCat消息处理系统
                if hasattr(self, "napcat_integration") and self.napcat_integration:
                    try:
                        # 使用NapCat集成模块处理消息
                        processed_result = (
                            await self.napcat_integration.process_napcat_message(
                                message, websocket
                            )
                        )

                        if processed_result and processed_result.get("success"):
                            # 将处理结果放入队列进行后续处理
                            message_item = {
                                "data": data,
                                "processed_result": processed_result,
                                "timestamp": time.time(),
                                "websocket": websocket,
                            }

                            try:
                                # 非阻塞方式放入队列
                                self.message_queue.put_nowait(message_item)
                                self.logger.debug(
                                    f"消息已加入处理队列（含NapCat处理结果），当前队列大小: {self.message_queue.qsize()}"
                                )
                            except asyncio.QueueFull:
                                # 队列满时发送繁忙响应
                                await self._send_busy_response(data, websocket)
                                self.logger.warning(f"消息队列已满，发送繁忙响应")
                        else:
                            self.logger.warning("NapCat消息处理失败，使用原始处理流程")
                            # 回退到原始处理流程
                            await self._handle_message_fallback(data, websocket)

                    except Exception as e:
                        self.logger.error(f"NapCat消息处理出错: {e}")
                        # 回退到原始处理流程
                        await self._handle_message_fallback(data, websocket)
                else:
                    # NapCat集成未初始化，使用原始处理流程
                    await self._handle_message_fallback(data, websocket)

            elif data.get("post_type") == "meta_event":
                # 元事件直接处理，不放入队列
                await self.handle_meta_event(data)
            elif data.get("post_type") == "notice":
                # 通知事件（如群成员减少）
                try:
                    await self.handle_notice_event(data)
                except Exception as e:
                    self.logger.error(f"处理通知事件出错: {e}")

        except json.JSONDecodeError:
            self.logger.error(f"❌ JSON解析失败: {message[:100]}...")
        except Exception as e:
            self.logger.error(f"❌ 处理消息时出错: {e}")
            self.logger.error(traceback.format_exc())

    async def _handle_message_fallback(self, data: dict, websocket):
        """
        原始消息处理流程（回退方案）

        Args:
            data (dict): 消息数据
            websocket: WebSocket连接对象
        """
        try:
            # 将消息放入队列进行异步处理
            message_item = {
                "data": data,
                "timestamp": time.time(),
                "websocket": websocket,
            }

            try:
                # 非阻塞方式放入队列
                self.message_queue.put_nowait(message_item)
                self.logger.debug(
                    f"消息已加入处理队列（原始流程），当前队列大小: {self.message_queue.qsize()}"
                )
            except asyncio.QueueFull:
                self.logger.warning("消息队列已满，丢弃消息")
                # 可以选择发送一个"服务器繁忙"的响应
                await self._send_busy_response(data, websocket)

        except Exception as e:
            self.logger.error(f"处理消息时出错: {e}")
            self.logger.error(traceback.format_exc())

    async def handle_notice_event(self, data: Dict[str, Any]) -> None:
        """
        函数说明:
        - 处理 OneBot v11 的通知事件（群成员进群/退群）
        - 进群: 若不存在群成员CSV，拉取全量并保存；若存在，延迟5分钟更新新成员
        - 退群: 优先从推时CSV反查CN，找不到则回退群成员CSV的群名称/昵称，并发送提示消息
        参数:
        - data: 通知事件原始字典
        返回:
        - None
        """
        try:
            notice_type = str(data.get("notice_type") or "").strip()
            group_id = str(data.get("group_id") or "").strip()
            user_id = str(data.get("user_id") or "").strip()
            if not group_id or not user_id:
                return

            notice_settings = self.config.get("notice_settings", {}) or {}
            ignored_groups = (
                notice_settings.get("ignore_groups", [])
                or self.config.get("notice_ignore_groups", [])
                or self.config.get("bot", {}).get("ignore_notice_groups", [])
                or []
            )
            ignored_groups_set = {
                str(x).strip() for x in ignored_groups if str(x).strip()
            }
            if ignored_groups_set and group_id in ignored_groups_set:
                return
            # 统一获取服务
            gm_service = self.services.get("group_member")
            pt_service = self.services.get("push_time_statistics")
            # 处理进群
            if notice_type == "group_increase":
                if gm_service:
                    try:
                        # 若已存在Excel，则只做单成员合并更新；否则复用成员获取的全量拉取并保存
                        if hasattr(
                            gm_service, "excel_exists"
                        ) and gm_service.excel_exists(group_id):
                            await gm_service.update_single_member_in_excel(
                                group_id, user_id
                            )
                        else:
                            await gm_service.get_group_members_and_save(
                                user_id="system", group_id=group_id, full_info=True
                            )
                    except Exception as e:
                        self.logger.error(
                            f"进群CSV更新失败: 群 {group_id}, 成员 {user_id}, 错误: {e}"
                        )
                return
            # 处理退群
            if notice_type == "group_decrease":
                # 1) 优先推时CSV反查CN
                cn_name = None
                try:
                    if pt_service and hasattr(pt_service, "_get_cn_by_qq"):
                        cn_name = pt_service._get_cn_by_qq(group_id, user_id)
                except Exception:
                    cn_name = None
                # 2) 回退群成员CSV的群名称/昵称
                if (not cn_name) and gm_service:
                    try:
                        cn_name = gm_service.lookup_name_by_qq_in_csv(group_id, user_id)
                    except Exception:
                        cn_name = None
                if not cn_name:
                    cn_name = user_id
                # 获取群聊名称
                group_name = "该群聊"
                try:
                    resp = await self.call_napcat_api(
                        {
                            "action": "get_group_info",
                            "params": {
                                "group_id": int(group_id)
                                if group_id.isdigit()
                                else group_id
                            },
                        }
                    )
                    if (
                        resp
                        and isinstance(resp, dict)
                        and resp.get("status") == "ok"
                        and resp.get("retcode") == 0
                    ):
                        gdata = resp.get("data", {}) or {}
                        gname = str(gdata.get("group_name") or "").strip()
                        if gname:
                            group_name = gname
                except Exception:
                    pass
                # 发送提示消息
                text = f"很遗憾，{cn_name}({user_id})离开了群聊{group_name}，祝他/她幸福♥。"
                payload = {
                    "action": "send_group_msg",
                    "params": {
                        "group_id": int(group_id) if group_id.isdigit() else group_id,
                        "message": [{"type": "text", "data": {"text": text}}],
                    },
                }
                try:
                    await self.send_response_to_napcat(payload)
                except Exception as e:
                    self.logger.error(
                        f"退群提示发送失败: 群 {group_id}, 成员 {user_id}, 错误: {e}"
                    )
        except Exception as e:
            self.logger.error(f"处理通知事件失败: {e}")

    async def _send_busy_response(self, data: Dict[str, Any], websocket=None):
        """
        发送服务器繁忙响应

        Args:
            data (Dict[str, Any]): 原始消息数据
            websocket: WebSocket连接对象（可选）
        """
        try:
            message_type = data.get("message_type")
            user_id = str(data.get("user_id", ""))

            busy_message = "服务器当前处理消息较多，请稍后重试。"

            if message_type == "private":
                response = {
                    "action": "send_private_msg",
                    "params": {
                        "user_id": user_id,
                        "message": [{"type": "text", "data": {"text": busy_message}}],
                    },
                }
            elif message_type == "group":
                group_id = str(data.get("group_id", ""))
                response = {
                    "action": "send_group_msg",
                    "params": {
                        "group_id": group_id,
                        "message": [{"type": "text", "data": {"text": busy_message}}],
                    },
                }
            else:
                return

            await self.send_response_to_napcat(response)

        except Exception as e:
            self.logger.error(f"发送繁忙响应时出错: {e}")

    async def start_message_processor(self):
        """
        启动消息处理器
        """
        if self.is_processing:
            return

        self.is_processing = True
        self.logger.info("启动消息处理器")

        # 启动多个消息处理协程
        for i in range(3):  # 启动3个处理协程
            task = asyncio.create_task(self._message_processor_worker(f"worker-{i}"))
            self.processing_tasks.add(task)
            task.add_done_callback(self.processing_tasks.discard)

    async def _message_processor_worker(self, worker_name: str):
        """
        消息处理工作协程

        Args:
            worker_name (str): 工作协程名称
        """
        self.logger.info(f"消息处理工作协程 {worker_name} 启动")

        while self.is_processing:
            try:
                # 从队列获取消息，设置超时避免无限等待
                try:
                    message_item = await asyncio.wait_for(
                        self.message_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # 使用信号量控制并发数量
                async with self.processing_semaphore:
                    await self._process_message_item(message_item, worker_name)

                # 标记任务完成
                self.message_queue.task_done()

            except Exception as e:
                self.logger.error(f"消息处理工作协程 {worker_name} 出错: {e}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(0.1)  # 短暂休息避免错误循环

        self.logger.info(f"消息处理工作协程 {worker_name} 停止")

    async def _process_message_item(
        self, message_item: Dict[str, Any], worker_name: str
    ):
        """
        处理单个消息项

        Args:
            message_item (Dict[str, Any]): 消息项
            worker_name (str): 处理的工作协程名称
        """
        try:
            data = message_item["data"]
            timestamp = message_item["timestamp"]
            processed_result = message_item.get(
                "processed_result"
            )  # 获取NapCat处理结果

            # 检查消息是否过期（超过30秒）
            if time.time() - timestamp > 30:
                self.logger.warning(f"丢弃过期消息: {data.get('user_id', 'unknown')}")
                return

            self.logger.debug(
                f"工作协程 {worker_name} 开始处理消息: {data.get('user_id', 'unknown')}"
            )

            group_id = (
                data.get("group_id") if data.get("message_type") == "group" else None
            )
            user_id = data.get("user_id")
            message_content = data.get("message", "")

            text_content = self._compose_log_message_from_segments(
                message_content, processed_result
            )
            self.log_and_print("INFO", group_id, user_id, text_content)

            if processed_result and processed_result.get("success"):
                self.logger.info("使用NapCat处理结果进行消息处理")
                responses = await self.process_qq_message(
                    data, napcat_processed=processed_result
                )
                img_list = processed_result.get("images") or processed_result.get(
                    "image_results"
                )
                if img_list:
                    self.logger.info(f"包含 {len(img_list)} 个图片处理结果")

            else:
                responses = await self.process_qq_message(data)

            if responses:
                self.logger.info(f"生成了响应，准备发送: {type(responses)}")
                try:
                    if isinstance(responses, list):
                        success_count = 0
                        for i, response in enumerate(responses):
                            success = await self.send_response_to_napcat(response)
                            if success:
                                success_count += 1
                                self.logger.debug(
                                    f"已发送响应 {i + 1}/{len(responses)}"
                                )
                            else:
                                self.logger.error(
                                    f"发送响应 {i + 1}/{len(responses)} 失败"
                                )
                                break  # 如果发送失败，停止发送后续消息
                            # 添加小延迟确保发送顺序
                            await asyncio.sleep(0.1)

                        if success_count == len(responses):
                            self.logger.info(f"✅ 成功发送所有 {len(responses)} 条响应")
                        else:
                            self.logger.warning(
                                f"⚠️ 仅成功发送 {success_count}/{len(responses)} 条响应"
                            )
                    else:
                        # 兼容旧的单个响应格式
                        success = await self.send_response_to_napcat(responses)
                        if success:
                            self.logger.debug("✅ 已发送单个响应")
                        else:
                            self.logger.error("❌ 发送单个响应失败")
                except Exception as send_error:
                    self.logger.error(f"发送响应时出错: {send_error}")
                    # 不抛出异常，继续处理后续消息

            self.logger.debug(
                f"工作协程 {worker_name} 完成处理消息: {data.get('user_id', 'unknown')}"
            )

        except Exception as e:
            self.logger.error(f"处理消息项时出错: {e}")
            self.logger.error(traceback.format_exc())

    async def process_qq_message(
        self, data: Dict[str, Any], napcat_processed: Optional[Dict[str, Any]] = None
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        处理QQ消息并生成响应（适配“图片+文本”复合消息）

        说明：
        - 原逻辑仅从 `data['message']` 中提取文本与图片；
        - 为适配 NapCatIntegration 的解析结果，这里新增 `napcat_processed` 参数；
          当该参数存在时，优先使用其中的 `parsed.text_content` 与 `images` 列表，
          从而兼容同时含有文本与图片的消息，并将图片路径透传给业务服务。

        Args:
            data (Dict[str, Any]): QQ消息数据（OneBot v11 事件）
            napcat_processed (Optional[Dict[str, Any]]): NapCatIntegration 返回的解析结果，
                包含 `parsed.text_content`、`images`（其中 `processed.file_path` 为图片缓存路径）等字段。

        Returns:
            Optional[Dict[str, Any]]: napcat API 格式的响应数据或响应列表；如无需回复则返回 None
        """
        try:
            message_type = data.get("message_type")
            user_id = str(data.get("user_id", ""))
            message_content = data.get("message", "")

            # 提取纯文本消息和图片信息（优先采用 napcat_processed 中的解析结果）
            text_content = ""
            image_paths: List[str] = []

            if napcat_processed:
                # 使用解析出的文本
                parsed = napcat_processed.get("parsed", {})
                text_content = (
                    napcat_processed.get("formatted_text")
                    or parsed.get("text_content", "")
                    or ""
                )
                # 使用解析出的图片缓存路径（processed.file_path）
                for img in napcat_processed.get("images", []) or []:
                    try:
                        processed_info = img.get("processed") or {}
                        fp = processed_info.get("file_path")
                        if fp:
                            image_paths.append(fp)
                    except Exception:
                        # 单个图片解析失败不影响整体流程
                        pass

                # 若解析结果未包含图片或文本，回退到从 data.message 提取
                if (not image_paths) or (not text_content.strip()):
                    message_content_fallback = message_content
                    if isinstance(message_content_fallback, list):
                        for msg in message_content_fallback:
                            if msg.get("type") == "text":
                                text_content += msg.get("data", {}).get("text", "")
                            elif msg.get("type") == "image":
                                image_data = msg.get("data", {})
                                image_file = image_data.get("file", "")
                                image_url = image_data.get("url", "")
                                if image_file:
                                    if image_file.startswith("file://"):
                                        image_paths.append(image_file[7:])
                                    elif image_file.startswith("base64://"):
                                        self.logger.info(
                                            "检测到base64格式图片，暂时跳过处理"
                                        )
                                    else:
                                        image_paths.append(image_file)
                                elif image_url:
                                    image_paths.append(image_url)
                    else:
                        text_content = str(message_content_fallback)
            else:
                # 旧逻辑：从 OneBot v11 事件的 message 段提取
                if isinstance(message_content, list):
                    for msg in message_content:
                        if msg.get("type") == "text":
                            text_content += msg.get("data", {}).get("text", "")
                        elif msg.get("type") == "image":
                            image_data = msg.get("data", {})
                            image_file = image_data.get("file", "")
                            image_url = image_data.get("url", "")
                            if image_file:
                                if image_file.startswith("file://"):
                                    image_paths.append(image_file[7:])
                                elif image_file.startswith("base64://"):
                                    self.logger.info(
                                        "检测到base64格式图片，暂时跳过处理"
                                    )
                                else:
                                    image_paths.append(image_file)
                            elif image_url:
                                image_paths.append(image_url)
                else:
                    text_content = str(message_content)

            # 如果没有文本内容且没有图片，不处理
            if not text_content.strip() and not image_paths:
                return None

            # 路由消息到相应服务，传递图片信息
            service_response = await self.route_message_to_service(
                text_content, user_id, data, image_paths
            )

            # 处理服务响应
            content = ""
            image_path = None
            voice_path = None
            file_path = None
            music_url = None
            is_mixed_message = False
            is_complex_message = False
            is_file_message = False
            message_segments = None

            if service_response:
                # 解析服务响应
                if isinstance(service_response, dict):
                    # 检查是否为文件消息格式
                    if service_response.get("type") == "file_message":
                        is_file_message = True
                        message_segments = service_response.get("message", [])
                    # 检查是否为复杂消息格式（包含message_segments）
                    elif (
                        service_response.get("is_complex_message", False)
                        and "message_segments" in service_response
                    ):
                        is_complex_message = True
                        message_segments = service_response.get("message_segments", [])
                    else:
                        # 支持多种字段名格式
                        content = service_response.get(
                            "content", ""
                        ) or service_response.get("text", "")
                        image_path = service_response.get(
                            "image_path"
                        ) or service_response.get("image_paths")
                        image_url = service_response.get("image_url")  # 支持网络图片URL
                        voice_path = service_response.get("voice_path")
                        file_path = service_response.get("file_path")
                        music_url = service_response.get("music_url")  # 支持音乐URL
                        voice_message = service_response.get(
                            "voice_message"
                        )  # 支持语音消息
                        is_mixed_message = service_response.get("mixed_message", False)

                        # 如果有语音消息，直接返回语音消息格式
                        if voice_message:
                            if message_type == "private":
                                return [
                                    {
                                        "action": "send_private_msg",
                                        "params": {
                                            "user_id": user_id,
                                            "message": [voice_message],
                                        },
                                    }
                                ]
                            elif message_type == "group":
                                group_id = str(data.get("group_id", ""))
                                return [
                                    {
                                        "action": "send_group_msg",
                                        "params": {
                                            "group_id": group_id,
                                            "message": [voice_message],
                                        },
                                    }
                                ]

                        # 如果有音乐URL，将其添加到内容中
                        if music_url:
                            content += f"\n🔗 播放链接: {music_url}"
                else:
                    content = str(service_response)
                    image_path = None
                    is_mixed_message = False
            else:
                # 没有服务处理消息时不做回应（适用于群聊场景）
                return None

            # 构建napcat API响应
            if message_type == "private":
                if is_file_message:
                    return [
                        self._build_file_response("private", user_id, message_segments)
                    ]
                elif is_complex_message:
                    return [
                        self._build_complex_response(
                            "private", user_id, message_segments
                        )
                    ]
                elif is_mixed_message:
                    return [
                        self._build_mixed_response(
                            "private", user_id, content, image_path, image_url
                        )
                    ]
                else:
                    return self._build_private_response(
                        user_id, content, image_path, voice_path, file_path
                    )
            elif message_type == "group":
                group_id = str(data.get("group_id", ""))
                if is_file_message:
                    return [
                        self._build_file_response("group", group_id, message_segments)
                    ]
                elif is_complex_message:
                    return [
                        self._build_complex_response(
                            "group", group_id, message_segments
                        )
                    ]
                elif is_mixed_message:
                    return [
                        self._build_mixed_response(
                            "group", group_id, content, image_path, image_url
                        )
                    ]
                else:
                    return self._build_group_response(
                        group_id, content, image_path, voice_path, file_path
                    )

            return None

        except Exception as e:
            self.logger.error(f"处理QQ消息时出错: {e}")
            return None

    async def route_message_to_service(
        self,
        message: str,
        user_id: str,
        context: Dict[str, Any],
        images: List[str] = None,
    ) -> Optional[str]:
        """
        将消息路由到相应的服务

        Args:
            message (str): 消息内容
            user_id (str): 用户ID
            context (Dict[str, Any]): 消息上下文
            images (List[str], optional): 图片路径列表

        Returns:
            Optional[str]: 服务响应
        """
        try:
            # 按优先级尝试各个服务
            # 调整路由顺序：确保若叶睦服务在 chat 之前处理（且在 daily_wife 之后）
            service_order = [
                "help",
                "team",
                "mc",
                "pjskranking",
                "pjskgachashistory",
                "user_management",
                "version_control_push",
                "work_order",
                "audit",
                "gallery",
                "pan_search",
                "music",
                "voice",
                "calculator",
                "gacha",
                "ranking_game",
                "push_time_statistics",
                "daily_team_statistics",
                "daily_push_time_statistics",
                "daily_wife",
                "mutsmi",
                "spell",
                "group_member",
                "tarot",
                "fortune",
                "daily_fortune",
                "choice",
                "conversation_statistics",
                "chat",
            ]

            for service_name in service_order:
                if service_name in self.services:
                    service = self.services[service_name]

                    # 检查服务是否启用
                    if not self._is_service_enabled(service_name):
                        continue

                    # 尝试处理消息
                    if hasattr(service, "process_message") and hasattr(
                        service.process_message, "__call__"
                    ):
                        # 检查服务是否支持额外参数
                        import inspect

                        sig = inspect.signature(service.process_message)
                        supports_kwargs = any(
                            param.kind == param.VAR_KEYWORD
                            for param in sig.parameters.values()
                        )
                        supports_images = "images" in sig.parameters

                        if supports_kwargs:
                            # 准备传递给服务的参数
                            kwargs = {"context": context}
                            if "group_id" in context:
                                kwargs["group_id"] = context["group_id"]
                            if "message_type" in context:
                                kwargs["message_type"] = context["message_type"]
                            # 若服务支持 **kwargs，则仍传递 images（由服务自行读取/忽略）
                            if images:
                                kwargs["images"] = images
                        else:
                            # 只传递基本参数
                            kwargs = {}
                            # 如果服务明确支持images参数，添加它
                            if images and supports_images:
                                kwargs["images"] = images

                        # 检查是否为异步方法
                        if asyncio.iscoroutinefunction(service.process_message):
                            response = await service.process_message(
                                message, user_id, **kwargs
                            )
                        else:
                            response = service.process_message(
                                message, user_id, **kwargs
                            )

                        if response:
                            # 使用统一日志格式记录服务处理
                            group_id = (
                                context.get("group_id")
                                if context.get("message_type") == "group"
                                else None
                            )
                            service_log = self.format_unified_log(
                                "INFO",
                                group_id,
                                user_id,
                                f"已被[{service_name}服务]处理:消息处理，成功发送响应",
                            )
                            print(service_log)  # 直接输出到控制台
                            return response

            # 如果没有服务处理，直接返回None（忽略消息）
            return None

        except Exception as e:
            # 记录错误日志但不返回错误消息给用户
            self.log_and_print("ERROR", "system", "system", f" 消息路由时出错: {e}")
            import traceback

            self.log_and_print(
                "ERROR", "system", "system", f" 错误堆栈: {traceback.format_exc()}"
            )
            # 直接返回None，忽略出错的消息
            return None

    def _is_service_enabled(self, service_name: str) -> bool:
        """
        检查服务是否启用

        Args:
            service_name (str): 服务名称

        Returns:
            bool: 服务是否启用
        """
        return (
            self.config.get("services", {}).get(service_name, {}).get("enabled", True)
        )

    def _build_private_response(
        self,
        user_id: str,
        content: str,
        image_path=None,
        voice_path=None,
        file_path=None,
    ) -> list:
        """
        构建私聊响应数据（分开发送文本、图片和语音）

        Args:
            user_id (str): 用户ID
            content (str): 响应内容
            image_path (str or list, optional): 图片文件路径或路径列表
            voice_path (str, optional): 语音文件路径

        Returns:
            list: napcat API格式的私聊响应列表
        """
        responses = []

        # 规范化ID类型为int（OneBot v11要求）
        try:
            normalized_user_id = (
                int(user_id)
                if isinstance(user_id, (str, int)) and str(user_id).isdigit()
                else user_id
            )
        except Exception:
            normalized_user_id = user_id

        # 如果有图片，先发送图片（按顺序）
        if image_path:
            import os

            # 处理单张图片或多张图片
            image_paths = image_path if isinstance(image_path, list) else [image_path]

            # 按顺序发送每张图片，添加序号确保顺序
            for idx, img_path in enumerate(image_paths):
                if img_path:  # 确保路径不为空
                    # 如果是相对路径，转换为绝对路径
                    abs_image_path = os.path.realpath(os.path.abspath(img_path))

                    response = {
                        "action": "send_private_msg",
                        "params": {
                            "user_id": normalized_user_id,
                            "message": [
                                {
                                    "type": "image",
                                    "data": {"file": f"file://{abs_image_path}"},
                                }
                            ],
                        },
                        "_order": idx,  # 添加顺序标识
                    }
                    responses.append(response)

        # 如果有语音文件，发送语音（放在图片之后）
        if voice_path:
            import os

            # 如果是相对路径，转换为绝对路径
            if not os.path.isabs(voice_path):
                abs_voice_path = os.path.abspath(voice_path)
            else:
                abs_voice_path = voice_path

            voice_response = {
                "action": "send_private_msg",
                "params": {
                    "user_id": normalized_user_id,
                    "message": [
                        {"type": "record", "data": {"file": f"file://{abs_voice_path}"}}
                    ],
                },
                "_order": len(responses),  # 语音消息在所有图片之后
            }
            responses.append(voice_response)

        # 如果有文件，发送文件（放在语音之后）
        if file_path:
            import os

            # 如果是相对路径，转换为绝对路径
            if not os.path.isabs(file_path):
                abs_file_path = os.path.abspath(file_path)
            else:
                abs_file_path = file_path

            file_response = {
                "action": "send_private_msg",
                "params": {
                    "user_id": normalized_user_id,
                    "message": [
                        {"type": "file", "data": {"file": f"file://{abs_file_path}"}}
                    ],
                },
                "_order": len(responses),  # 文件消息在所有其他消息之后
            }
            responses.append(file_response)

        # 如果有文本内容，发送文本（放在语音之后）
        if content:
            text_response = {
                "action": "send_private_msg",
                "params": {
                    "user_id": normalized_user_id,
                    "message": [{"type": "text", "data": {"text": content}}],
                },
                "_order": len(responses),  # 文本消息在所有语音和图片之后
            }
            responses.append(text_response)

        return responses

    def _build_group_response(
        self,
        group_id: str,
        content: str,
        image_path=None,
        voice_path=None,
        file_path=None,
    ) -> list:
        """
        构建群聊响应数据（分开发送文本、图片和语音）

        Args:
            group_id (str): 群组ID
            content (str): 响应内容
            image_path (str or list, optional): 图片文件路径或路径列表
            voice_path (str, optional): 语音文件路径

        Returns:
            list: napcat API格式的群聊响应列表
        """
        responses = []

        # 规范化ID类型为int（OneBot v11要求）
        try:
            normalized_group_id = (
                int(group_id)
                if isinstance(group_id, (str, int)) and str(group_id).isdigit()
                else group_id
            )
        except Exception:
            normalized_group_id = group_id

        # 如果有图片，先发送图片（按顺序）
        if image_path:
            import os

            # 处理单张图片或多张图片
            image_paths = image_path if isinstance(image_path, list) else [image_path]

            # 按顺序发送每张图片，添加序号确保顺序
            for idx, img_path in enumerate(image_paths):
                if img_path:  # 确保路径不为空
                    # 如果是相对路径，转换为绝对路径
                    abs_image_path = os.path.realpath(os.path.abspath(img_path))

                    response = {
                        "action": "send_group_msg",
                        "params": {
                            "group_id": normalized_group_id,
                            "message": [
                                {
                                    "type": "image",
                                    "data": {"file": f"file://{abs_image_path}"},
                                }
                            ],
                        },
                        "_order": idx,  # 添加顺序标识
                    }
                    responses.append(response)

        # 如果有语音文件，发送语音（放在图片之后）
        if voice_path:
            import os

            # 如果是相对路径，转换为绝对路径
            if not os.path.isabs(voice_path):
                abs_voice_path = os.path.abspath(voice_path)
            else:
                abs_voice_path = voice_path

            voice_response = {
                "action": "send_group_msg",
                "params": {
                    "group_id": normalized_group_id,
                    "message": [
                        {"type": "record", "data": {"file": f"file://{abs_voice_path}"}}
                    ],
                },
                "_order": len(responses),  # 语音消息在所有图片之后
            }
            responses.append(voice_response)

        # 如果有文件，发送文件（放在语音之后）
        if file_path:
            import os

            # 如果是相对路径，转换为绝对路径
            if not os.path.isabs(file_path):
                abs_file_path = os.path.abspath(file_path)
            else:
                abs_file_path = file_path

            file_response = {
                "action": "send_group_msg",
                "params": {
                    "group_id": normalized_group_id,
                    "message": [
                        {"type": "file", "data": {"file": f"file://{abs_file_path}"}}
                    ],
                },
                "_order": len(responses),  # 文件消息在所有其他消息之后
            }
            responses.append(file_response)

        # 如果有文本内容，发送文本（放在语音之后）
        if content:
            text_response = {
                "action": "send_group_msg",
                "params": {
                    "group_id": normalized_group_id,
                    "message": [{"type": "text", "data": {"text": content}}],
                },
                "_order": len(responses),  # 文本消息在所有语音和图片之后
            }
            responses.append(text_response)

        return responses

    def _build_mixed_response(
        self,
        target_type: str,
        target_id: str,
        content: str,
        image_path: str = None,
        image_url: str = None,
    ) -> Dict[str, Any]:
        """
        构建混合消息响应（文字和图片在同一条消息中）

        Args:
            target_type (str): 消息类型，'private' 或 'group'
            target_id (str): 目标ID（用户ID或群组ID）
            content (str): 文本内容
            image_path (str, optional): 本地图片路径
            image_url (str, optional): 网络图片URL

        Returns:
            Dict[str, Any]: napcat API格式的混合消息响应
        """
        import os

        # 构建消息段数组
        message_segments = []

        # 添加文本段
        if content:
            message_segments.append({"type": "text", "data": {"text": content}})

        # 添加图片段
        if image_url:
            # 优先使用网络图片URL
            message_segments.append({"type": "image", "data": {"file": image_url}})
        elif image_path:
            # 如果没有网络URL，使用本地路径
            # 如果是相对路径，转换为绝对路径
            abs_image_path = os.path.realpath(os.path.abspath(image_path))

            # 检查文件是否存在
            if os.path.exists(abs_image_path):
                message_segments.append(
                    {"type": "image", "data": {"file": f"file://{abs_image_path}"}}
                )
            else:
                self.logger.warning(f"图片文件不存在: {abs_image_path}")

        # 构建API请求
        if target_type == "private":
            action = "send_private_msg"
            params = {"user_id": target_id, "message": message_segments}
        else:  # group
            action = "send_group_msg"
            params = {"group_id": target_id, "message": message_segments}

        return {"action": action, "params": params}

    def _build_complex_response(
        self, target_type: str, target_id: str, message_segments: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        构建复杂消息响应（支持多种消息段类型，包括@功能）

        Args:
            target_type (str): 消息类型，'private' 或 'group'
            target_id (str): 目标ID（用户ID或群组ID）
            message_segments (List[Dict[str, Any]]): 消息段数组

        Returns:
            Dict[str, Any]: napcat API格式的复杂消息响应
        """
        # 构建API请求
        if target_type == "private":
            action = "send_private_msg"
            params = {"user_id": target_id, "message": message_segments}
        else:  # group
            action = "send_group_msg"
            params = {"group_id": target_id, "message": message_segments}

        self.logger.info(
            f"构建复杂消息响应: {action}, 目标: {target_id}, 消息段数量: {len(message_segments)}"
        )

        return {"action": action, "params": params}

    def _build_file_response(
        self, target_type: str, target_id: str, message_segments: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        构建文件消息响应（支持发送文件到QQ群聊或私聊）

        Args:
            target_type (str): 消息类型，'private' 或 'group'
            target_id (str): 目标ID（用户ID或群组ID）
            message_segments (List[Dict[str, Any]]): 消息段数组，包含文件信息

        Returns:
            Dict[str, Any]: napcat API格式的文件消息响应
        """
        # 构建API请求
        if target_type == "private":
            action = "send_private_msg"
            params = {"user_id": target_id, "message": message_segments}
        else:  # group
            action = "send_group_msg"
            params = {"group_id": target_id, "message": message_segments}

        self.logger.info(
            f"构建文件消息响应: {action}, 目标: {target_id}, 消息段数量: {len(message_segments)}"
        )

        return {"action": action, "params": params}

    async def send_response_to_napcat(self, response_data: Dict[str, Any]):
        """
        发送响应到napcat客户端

        Args:
            response_data (Dict[str, Any]): 响应数据
        """
        if not self.napcat_client:
            # 记录Napcat客户端未连接错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log = f"[{timestamp}][ERROR][G:system][U:system]: ❌ Napcat客户端未连接，无法发送响应！请确保napcat正确连接到服务器"
                print(error_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log + "\n")
            except Exception:
                pass
            return False

        try:
            # 检查连接是否仍然有效
            if hasattr(self.napcat_client, "closed") and self.napcat_client.closed:
                # 记录Napcat客户端连接已关闭错误日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    error_log = f"[{timestamp}][ERROR][G:system][U:system]: ⚠️ Napcat客户端连接已关闭，消息发送失败！请检查napcat连接状态"
                    print(error_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(error_log + "\n")
                except Exception:
                    pass
                self.napcat_client = None  # 清空无效连接
                return False

            # 尝试发送消息
            message = json.dumps(response_data, ensure_ascii=False)
            await self.napcat_client.send(message)
            # 记录成功发送响应调试日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                debug_log = f"[{timestamp}][DEBUG][G:system][U:system]: ✅ 成功发送响应到napcat: {response_data.get('action', 'unknown')}"
                print(debug_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(debug_log + "\n")
            except Exception:
                pass
            return True

        except websockets.exceptions.ConnectionClosed as e:
            # 记录Napcat连接断开错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log = f"[{timestamp}][ERROR][G:system][U:system]: ❌ Napcat连接已断开: {e} - 请重新连接napcat客户端"
                print(error_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log + "\n")
            except Exception:
                pass
            self.napcat_client = None  # 清空断开的连接
            return False
        except Exception as e:
            # 记录发送响应失败错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log1 = f"[{timestamp}][ERROR][G:system][U:system]: ❌ 发送响应到napcat失败: {e}"
                print(error_log1)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log1 + "\n")
                error_log2 = f"[{timestamp}][ERROR][G:system][U:system]: 错误详情: {traceback.format_exc()}"
                print(error_log2)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log2 + "\n")
                # 检查是否是连接相关的错误
                if "connection" in str(e).lower() or "closed" in str(e).lower():
                    error_log3 = f"[{timestamp}][ERROR][G:system][U:system]: ⚠️ 检测到napcat连接问题，请检查napcat客户端状态"
                    print(error_log3)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(error_log3 + "\n")
            except Exception:
                pass
                self.napcat_client = None
            return False

    async def call_napcat_api(
        self, api_request: Dict[str, Any], timeout: float = 10.0
    ) -> Optional[Dict[str, Any]]:
        """
        调用napcat API并等待响应

        Args:
            api_request (Dict[str, Any]): API请求数据
            timeout (float): 超时时间（秒）

        Returns:
            Optional[Dict[str, Any]]: API响应数据
        """
        if not self.napcat_client:
            # 记录Napcat客户端未连接错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log = f"[{timestamp}][ERROR][G:system][U:system]: ❌ Napcat客户端未连接，无法调用API！请确保napcat正确连接到服务器"
                print(error_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log + "\n")
            except Exception:
                pass
            return None

        try:
            # 检查连接是否仍然有效
            if hasattr(self.napcat_client, "closed") and self.napcat_client.closed:
                # 记录Napcat客户端连接已关闭错误日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    error_log = f"[{timestamp}][ERROR][G:system][U:system]: ⚠️ Napcat客户端连接已关闭，API调用失败！请检查napcat连接状态"
                    print(error_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(error_log + "\n")
                except Exception:
                    pass
                self.napcat_client = None
                return None

            # 生成唯一的请求ID
            import uuid

            request_id = str(uuid.uuid4())
            api_request["echo"] = request_id

            # 创建响应等待器
            response_future = asyncio.Future()

            # 将响应等待器存储到实例变量中
            if not hasattr(self, "pending_api_requests"):
                self.pending_api_requests = {}
            self.pending_api_requests[request_id] = response_future

            # 发送API请求
            message = json.dumps(api_request, ensure_ascii=False)
            await self.napcat_client.send(message)
            # 记录发送API请求调试日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                debug_log = f"[{timestamp}][DEBUG][G:system][U:system]: ✅ 发送napcat API请求: {api_request.get('action', 'unknown')} (ID: {request_id})"
                print(debug_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(debug_log + "\n")
            except Exception:
                pass

            # 等待响应
            try:
                response = await asyncio.wait_for(response_future, timeout=timeout)
                # 记录收到API响应调试日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    debug_log = f"[{timestamp}][DEBUG][G:system][U:system]: ✅ 收到napcat API响应: {response.get('status', 'unknown')} (ID: {request_id})"
                    print(debug_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(debug_log + "\n")
                except Exception:
                    pass
                return response
            except asyncio.TimeoutError:
                # 记录API调用超时错误日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    error_log = f"[{timestamp}][ERROR][G:system][U:system]: ❌ napcat API调用超时: {api_request.get('action', 'unknown')} (ID: {request_id})"
                    print(error_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(error_log + "\n")
                except Exception:
                    pass
                return None
            finally:
                # 清理等待器
                if (
                    hasattr(self, "pending_api_requests")
                    and request_id in self.pending_api_requests
                ):
                    del self.pending_api_requests[request_id]

        except websockets.exceptions.ConnectionClosed as e:
            # 记录Napcat连接断开错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log = f"[{timestamp}][ERROR][G:system][U:system]: ❌ Napcat连接已断开: {e} - 请重新连接napcat客户端"
                print(error_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log + "\n")
            except Exception:
                pass
            self.napcat_client = None
            return None
        except Exception as e:
            # 记录API调用失败错误日志
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log1 = f"[{timestamp}][ERROR][G:system][U:system]: ❌ napcat API调用失败: {e}"
                print(error_log1)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log1 + "\n")
                error_log2 = f"[{timestamp}][ERROR][G:system][U:system]: 错误详情: {traceback.format_exc()}"
                print(error_log2)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log2 + "\n")
            except Exception:
                pass
            return None

    async def _ranking_game_status_callback(self, group_id: str, message: str):
        """
        冲榜游戏状态回调函数
        用于自动发送游戏状态消息到指定群组

        Args:
            group_id (str): 群组ID
            message (str): 游戏状态消息
        """
        try:
            # 检查group_id是否为有效的数字
            if not group_id or not group_id.isdigit():
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    warning_log = f"[{timestamp}][WARNING][G:system][U:system]: ⚠️ 无效的群组ID: {group_id}，跳过消息发送"
                    print(warning_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(warning_log + "\n")
                except Exception:
                    pass
                return

            # 构造发送消息的数据
            response_data = {
                "action": "send_group_msg",
                "params": {"group_id": int(group_id), "message": message},
            }

            # 发送消息到napcat
            success = await self.send_response_to_napcat(response_data)
            if success:
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    info_log = f"[{timestamp}][INFO][G:system][U:system]: ✅ 冲榜游戏消息已发送到群组 {group_id}"
                    print(info_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(info_log + "\n")
                except Exception:
                    pass
            else:
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    error_log = f"[{timestamp}][ERROR][G:system][U:system]: ❌ 冲榜游戏消息发送失败，群组 {group_id}"
                    print(error_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(error_log + "\n")
                except Exception:
                    pass

        except Exception as e:
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log = f"[{timestamp}][ERROR][G:system][U:system]: ❌ 冲榜游戏状态回调失败: {e}"
                print(error_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log + "\n")
            except Exception:
                pass

    async def monitor_napcat_connection(self):
        """
        监控napcat连接状态
        定期检查连接是否有效，并在连接断开时发出警告
        """
        last_connected = False
        warning_count = 0
        ping_fail_count = 0

        while self.is_running:
            try:
                current_connected = self.napcat_client is not None and not (
                    hasattr(self.napcat_client, "closed") and self.napcat_client.closed
                )

                # 如果连接存在，尝试ping测试
                if current_connected:
                    try:
                        # 发送ping测试连接活性
                        await self.napcat_client.ping()
                        ping_fail_count = 0  # 重置ping失败计数

                        # 连接状态发生变化时记录
                        if current_connected != last_connected:
                            try:
                                log_dir = "logs"
                                if not os.path.exists(log_dir):
                                    os.makedirs(log_dir)
                                log_file_path = os.path.join(log_dir, "unified.log")
                                current_time = datetime.now()
                                timestamp = current_time.strftime(
                                    "%Y-%m-%d %H:%M:%S,%f"
                                )[:-3]
                                info_log = f"[{timestamp}][INFO][G:system][U:system]: ✅ Napcat客户端连接正常"
                                print(info_log)
                                with open(log_file_path, "a", encoding="utf-8") as f:
                                    f.write(info_log + "\n")
                            except Exception:
                                pass
                            warning_count = 0

                    except Exception as ping_error:
                        ping_fail_count += 1
                        try:
                            log_dir = "logs"
                            if not os.path.exists(log_dir):
                                os.makedirs(log_dir)
                            log_file_path = os.path.join(log_dir, "unified.log")
                            current_time = datetime.now()
                            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[
                                :-3
                            ]
                            debug_log = f"[{timestamp}][DEBUG][G:system][U:system]: Napcat连接ping失败 ({ping_fail_count}): {ping_error}"
                            print(debug_log)
                            with open(log_file_path, "a", encoding="utf-8") as f:
                                f.write(debug_log + "\n")
                        except Exception:
                            pass
                        # 不再因为ping失败而断开连接，保持连接稳定
                else:
                    ping_fail_count = 0

                # 连接状态发生变化时记录
                if current_connected != last_connected:
                    if current_connected:
                        # 记录Napcat客户端连接正常信息日志
                        try:
                            log_dir = "logs"
                            if not os.path.exists(log_dir):
                                os.makedirs(log_dir)
                            log_file_path = os.path.join(log_dir, "unified.log")
                            current_time = datetime.now()
                            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[
                                :-3
                            ]
                            info_log = f"[{timestamp}][INFO][G:system][U:system]: ✅ Napcat客户端连接正常"
                            print(info_log)
                            with open(log_file_path, "a", encoding="utf-8") as f:
                                f.write(info_log + "\n")
                        except Exception:
                            pass
                        warning_count = 0
                    else:
                        # 记录Napcat客户端连接断开警告日志
                        try:
                            log_dir = "logs"
                            if not os.path.exists(log_dir):
                                os.makedirs(log_dir)
                            log_file_path = os.path.join(log_dir, "unified.log")
                            current_time = datetime.now()
                            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[
                                :-3
                            ]
                            warning_log = f"[{timestamp}][WARNING][G:system][U:system]: ⚠️ Napcat客户端连接断开，消息将无法发送到QQ"
                            print(warning_log)
                            with open(log_file_path, "a", encoding="utf-8") as f:
                                f.write(warning_log + "\n")
                        except Exception:
                            pass

                # 如果连接断开，定期发出警告（每5分钟一次）
                elif not current_connected:
                    warning_count += 1
                    if warning_count % 10 == 0:  # 每10次检查（5分钟）警告一次
                        # 记录Napcat客户端仍未连接错误日志
                        try:
                            log_dir = "logs"
                            if not os.path.exists(log_dir):
                                os.makedirs(log_dir)
                            log_file_path = os.path.join(log_dir, "unified.log")
                            current_time = datetime.now()
                            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[
                                :-3
                            ]
                            error_log = f"[{timestamp}][ERROR][G:system][U:system]: ❌ Napcat客户端仍未连接！请检查napcat配置和网络连接"
                            print(error_log)
                            with open(log_file_path, "a", encoding="utf-8") as f:
                                f.write(error_log + "\n")
                        except Exception:
                            pass

                last_connected = current_connected

                # 每30秒检查一次
                await asyncio.sleep(30)

            except Exception as e:
                # 记录连接状态监控出错日志
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    error_log = f"[{timestamp}][ERROR][G:system][U:system]: 连接状态监控出错: {e}"
                    print(error_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(error_log + "\n")
                except Exception:
                    pass
                await asyncio.sleep(30)

    async def handle_meta_event(self, data: Dict[str, Any]):
        """
        处理元事件

        Args:
            data (Dict[str, Any]): 元事件数据
        """
        meta_event_type = data.get("meta_event_type")

        if meta_event_type == "heartbeat":
            # 心跳事件
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                debug_log = f"[{timestamp}][DEBUG][G:system][U:system]: 收到心跳事件"
                print(debug_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(debug_log + "\n")
            except Exception:
                pass
        elif meta_event_type == "lifecycle":
            # 生命周期事件
            sub_type = data.get("sub_type")
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                info_log = f"[{timestamp}][INFO][G:system][U:system]: 收到生命周期事件: {sub_type}"
                print(info_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(info_log + "\n")
            except Exception:
                pass
        else:
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                debug_log = f"[{timestamp}][DEBUG][G:system][U:system]: 收到元事件: {meta_event_type}"
                print(debug_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(debug_log + "\n")
            except Exception:
                pass

    async def _websocket_handler(self, websocket):
        """
        WebSocket连接处理器包装函数

        Args:
            websocket: WebSocket连接对象
        """
        # 从websocket对象获取路径信息
        path = websocket.path if hasattr(websocket, "path") else "/fun"
        await self.handle_client_connection(websocket, path)

    async def start_server(self):
        """
        启动WebSocket服务器
        """
        try:
            host = self.config["websocket"]["host"]
            port = self.config["websocket"]["port"]

            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                info_log1 = (
                    f"[{timestamp}][INFO][G:system][U:system]: 启动Mortisfun服务器..."
                )
                print(info_log1)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(info_log1 + "\n")
                info_log2 = (
                    f"[{timestamp}][INFO][G:system][U:system]: 监听地址: {host}:{port}"
                )
                print(info_log2)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(info_log2 + "\n")
                info_log3 = f"[{timestamp}][INFO][G:system][U:system]: WebSocket路径: {self.config['websocket']['path']}"
                print(info_log3)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(info_log3 + "\n")
            except Exception:
                pass

            if create_napcat_integration and self.napcat_integration is None:
                try:
                    self.napcat_integration = await create_napcat_integration(
                        self.config
                    )
                    self.logger.info("✅ NapCat集成模块初始化成功")
                except Exception as e:
                    self.logger.error(f"❌ NapCat集成模块初始化失败: {e}")
                    self.napcat_integration = None
            elif not create_napcat_integration:
                self.logger.warning(
                    "⚠️ NapCat集成模块导入失败，图片解析等增强能力不可用"
                )

            # 启动WebSocket服务器
            # 修复配置读取：使用heartbeat_interval作为ping_interval，从connection_manager读取ping_timeout
            ping_interval = self.config["websocket"].get("heartbeat_interval", 30)
            ping_timeout = self.config["connection_manager"].get("ping_timeout", 10)

            self.server = await websockets.serve(
                self._websocket_handler,
                host,
                port,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                max_size=self.config["websocket"].get("max_message_size", 1024 * 1024),
                compression=None,
            )

            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                info_log = f"[{timestamp}][INFO][G:system][U:system]: WebSocket配置: ping_interval={ping_interval}s, ping_timeout={ping_timeout}s"
                print(info_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(info_log + "\n")
            except Exception:
                pass

            self.is_running = True
            try:
                if (
                    hasattr(self, "log_queue")
                    and self.log_queue
                    and (not hasattr(self, "_log_worker_task") or not self._log_worker_task)
                ):
                    self._log_worker_task = asyncio.create_task(self._log_worker())
            except Exception:
                pass

            # 启动消息处理器
            await self.start_message_processor()

            # 启动 MC 远程/本地日志监听（无需任何触发条件，服务器启动后即开始轮询）
            try:
                if "mc" in self.services:
                    await self.services["mc"].start_log_watch()
                    try:
                        log_dir = "logs"
                        if not os.path.exists(log_dir):
                            os.makedirs(log_dir)
                        log_file_path = os.path.join(log_dir, "unified.log")
                        current_time = datetime.now()
                        timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                        info_log = f"[{timestamp}][INFO][G:system][U:system]: MC日志监听已主动启动"
                        print(info_log)
                        with open(log_file_path, "a", encoding="utf-8") as f:
                            f.write(info_log + "\n")
                    except Exception:
                        pass
            except Exception:
                pass

            # 启动连接状态监控
            asyncio.create_task(self.monitor_napcat_connection())

            # 启动定时提醒服务
            if "team_reminder" in self.services:
                await self.services["team_reminder"].start_reminder_service()
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    info_log = (
                        f"[{timestamp}][INFO][G:system][U:system]: 定时提醒服务已启动"
                    )
                    print(info_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(info_log + "\n")
                except Exception:
                    pass

            # 启动每日车队统计服务
            if "daily_team_statistics" in self.services:
                self.services["daily_team_statistics"].start_service()
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    info_log = f"[{timestamp}][INFO][G:system][U:system]: 每日车队统计服务已启动"
                    print(info_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(info_log + "\n")
                except Exception:
                    pass

            # 启动每日推时统计服务
            if "daily_push_time_statistics" in self.services:
                self.services["daily_push_time_statistics"].start_service()
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    info_log = f"[{timestamp}][INFO][G:system][U:system]: 每日推时统计服务已启动"
                    print(info_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(info_log + "\n")
                except Exception:
                    pass

            # 启动 PJSK 排行抓取任务（后台线程，Windows/Linux 通用）
            try:
                if RankingFetcher and (
                    not hasattr(self, "pjsk_fetcher_thread")
                    or self.pjsk_fetcher_thread is None
                ):

                    def _run_fetcher():
                        try:
                            fetcher = RankingFetcher(interval_sec=62)
                            # 记录启动日志
                            log_dir = "logs"
                            os.makedirs(log_dir, exist_ok=True)
                            log_file_path = os.path.join(log_dir, "unified.log")
                            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                            info_log = f"[{ts}][INFO][G:system][U:system]: PJSK 排行抓取任务已启动(每分钟第 2 秒执行)"
                            print(info_log)
                            try:
                                with open(log_file_path, "a", encoding="utf-8") as f:
                                    f.write(info_log + "\n")
                            except Exception:
                                pass
                            fetcher.run_forever()
                        except Exception as e:
                            # 记录错误日志
                            try:
                                log_dir = "logs"
                                os.makedirs(log_dir, exist_ok=True)
                                log_file_path = os.path.join(log_dir, "unified.log")
                                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[
                                    :-3
                                ]
                                error_log = f"[{ts}][ERROR][G:system][U:system]: PJSK 排行抓取任务启动失败: {e}"
                                print(error_log)
                                with open(log_file_path, "a", encoding="utf-8") as f:
                                    f.write(error_log + "\n")
                            except Exception:
                                pass

                    self.pjsk_fetcher_thread = threading.Thread(
                        target=_run_fetcher, daemon=True
                    )
                    self.pjsk_fetcher_thread.start()
            except Exception:
                pass

            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                info_log = f"[{timestamp}][INFO][G:system][U:system]: 服务器启动成功，等待连接..."
                print(info_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(info_log + "\n")
            except Exception:
                pass

        except Exception as e:
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log = (
                    f"[{timestamp}][ERROR][G:system][U:system]: 启动服务器失败: {e}"
                )
                print(error_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log + "\n")
            except Exception:
                pass
            raise

    async def run(self):
        """
        运行服务器主循环
        """
        try:
            await self.start_server()

            # 保持服务器运行
            while self.is_running:
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                info_log = f"[{timestamp}][INFO][G:system][U:system]: 收到中断信号，正在关闭服务器..."
                print(info_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(info_log + "\n")
            except Exception:
                pass
        except Exception as e:
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                error_log = (
                    f"[{timestamp}][ERROR][G:system][U:system]: 服务器运行时出错: {e}"
                )
                print(error_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(error_log + "\n")
            except Exception:
                pass
            raise
        finally:
            await self.cleanup()

    async def cleanup(self):
        """
        清理资源
        """
        try:
            # 优雅关闭异步日志消费进程
            if (
                hasattr(self, "log_queue")
                and hasattr(self, "_log_worker_task")
                and self._log_worker_task
            ):
                try:
                    await self.log_queue.join()
                except Exception:
                    pass
                self._log_worker_task.cancel()
                try:
                    await self._log_worker_task
                except Exception:
                    pass
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            log_file_path = os.path.join(log_dir, "unified.log")
            current_time = datetime.now()
            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            info_log = f"[{timestamp}][INFO][G:system][U:system]: 正在清理资源..."
            print(info_log)
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(info_log + "\n")
        except Exception:
            pass

        # 停止消息处理器
        self.is_processing = False

        # 等待所有处理任务完成
        if self.processing_tasks:
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                info_log = (
                    f"[{timestamp}][INFO][G:system][U:system]: 等待消息处理任务完成..."
                )
                print(info_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(info_log + "\n")
            except Exception:
                pass
            await asyncio.gather(*self.processing_tasks, return_exceptions=True)

        # 等待队列中剩余消息处理完成（最多等待5秒）
        try:
            await asyncio.wait_for(self.message_queue.join(), timeout=5.0)
        except asyncio.TimeoutError:
            try:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                log_file_path = os.path.join(log_dir, "unified.log")
                current_time = datetime.now()
                timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                warning_log = (
                    f"[{timestamp}][WARNING][G:system][U:system]: 消息队列清理超时"
                )
                print(warning_log)
                with open(log_file_path, "a", encoding="utf-8") as f:
                    f.write(warning_log + "\n")
            except Exception:
                pass

        # 关闭线程池
        if hasattr(self, "thread_pool"):
            self.thread_pool.shutdown(wait=True)

        # 关闭服务器
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        # 关闭所有WebSocket连接
        if self.connected_clients:
            await asyncio.gather(
                *[client.close() for client in self.connected_clients],
                return_exceptions=True,
            )

        # 停止定时提醒服务
        if "team_reminder" in self.services:
            try:
                await self.services["team_reminder"].stop_reminder_service()
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    info_log = (
                        f"[{timestamp}][INFO][G:system][U:system]: 定时提醒服务已停止"
                    )
                    print(info_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(info_log + "\n")
                except Exception:
                    pass
            except Exception as e:
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    error_log = f"[{timestamp}][ERROR][G:system][U:system]: 停止定时提醒服务失败: {e}"
                    print(error_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(error_log + "\n")
                except Exception:
                    pass

        # 清理服务
        for service_name, service in self.services.items():
            try:
                if hasattr(service, "cleanup"):
                    await service.cleanup()
            except Exception as e:
                try:
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    log_file_path = os.path.join(log_dir, "unified.log")
                    current_time = datetime.now()
                    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
                    error_log = f"[{timestamp}][ERROR][G:system][U:system]: 清理服务 {service_name} 时出错: {e}"
                    print(error_log)
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(error_log + "\n")
                except Exception:
                    pass

        self.is_running = False
        try:
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            log_file_path = os.path.join(log_dir, "unified.log")
            current_time = datetime.now()
            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            info_log = f"[{timestamp}][INFO][G:system][U:system]: 资源清理完成"
            print(info_log)
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(info_log + "\n")
        except Exception:
            pass

    def get_server_stats(self) -> Dict[str, Any]:
        """
        获取服务器统计信息

        Returns:
            Dict[str, Any]: 服务器统计信息
        """
        return {
            "connected_clients": len(self.connected_clients),
            "napcat_connected": self.napcat_client is not None,
            "message_count": self.message_count,
            "uptime": (datetime.now() - self.start_time).total_seconds(),
            "concurrent_processing": {
                "queue_size": self.message_queue.qsize(),
                "max_queue_size": self.message_queue.maxsize,
                "processing_workers": len(self.processing_tasks),
                "is_processing": self.is_processing,
                "semaphore_available": self.processing_semaphore._value,
                "semaphore_total": 10,  # 硬编码的信号量总数
            },
            "services": {
                name: {
                    "enabled": self._is_service_enabled(name),
                    "loaded": name in self.services,
                }
                for name in [
                    "help",
                    "calculator",
                    "gacha",
                    "tarot",
                    "fortune",
                    "daily_fortune",
                    "chat",
                ]
            },
        }


def main():
    """
    主函数
    """
    try:
        # 创建服务器实例
        server = MortisfunServer()

        # 运行服务器
        asyncio.run(server.run())

    except KeyboardInterrupt:
        # 使用logging模块记录日志
        logging.info("\n程序被用户中断")
        print("\n程序被用户中断")
    except Exception as e:
        # 使用logging模块记录日志
        logging.error(f"程序运行出错: {e}")
        print(f"程序运行出错: {e}")
        import traceback

        logging.error(f"错误堆栈: {traceback.format_exc()}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
