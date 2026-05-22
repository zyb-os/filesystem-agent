"""
orchestrator_client.py — WebSocket + HTTP client for the filesystem-agent.

Registers with the orchestrator, handles filesystem task_requests, and
applies settings pushed from the dashboard (allowed paths, size limits, etc.).
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import httpx
import websockets
import websockets.exceptions

from filesystem_client import FilesystemClient, PathViolation

logger = logging.getLogger(__name__)

# ── Stable agent identity ──────────────────────────────────────────────────────

_AGENT_ID_FILE = Path(".agent_id")


def _stable_agent_id() -> str:
    if _AGENT_ID_FILE.exists():
        return _AGENT_ID_FILE.read_text().strip()
    new_id = str(uuid.uuid4())
    _AGENT_ID_FILE.write_text(new_id)
    logger.info("Generated new stable agent ID: %s", new_id)
    return new_id


# ── Registration payload ──────────────────────────────────────────────────────

AGENT_NAME = "filesystem-agent"
AGENT_VERSION = "1.0.0"
AGENT_DESCRIPTION = (
    "Cross-platform filesystem agent (Linux, macOS, Windows). "
    "Provides read, write, search, and directory operations with "
    "configurable path sandboxing — access is restricted to allowed "
    "directories set via the orchestrator dashboard."
)

REGISTRATION_PAYLOAD: dict = {
    "name": AGENT_NAME,
    "description": AGENT_DESCRIPTION,
    "version": AGENT_VERSION,
    "tags": ["filesystem", "files", "storage", "search"],
    "capabilities": [
        {
            "name": "read_file",
            "description": "Read the contents of a file. Binary files are automatically base64-encoded.",
            "tags": ["filesystem", "read", "open", "load", "get", "retrieve", "content", "text", "view"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute path to the file to read.",
                    },
                    "encoding": {
                        "type": "string",
                        "default": "utf-8",
                        "description": "Text encoding (e.g. utf-8, latin-1) or 'base64' to force binary read.",
                    },
                },
                "required": ["path"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "content": {"type": "string"},
                    "encoding": {"type": "string"},
                    "size_bytes": {"type": "integer"},
                    "path": {"type": "string"},
                },
            },
        },
        {
            "name": "write_file",
            "description": "Create or overwrite a file with the given content. Parent directories are created automatically.",
            "tags": ["filesystem", "write", "save", "create", "store", "output", "persist", "overwrite"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path of the file to write."},
                    "content": {"type": "string", "description": "Content to write. Use encoding=base64 for binary data."},
                    "encoding": {"type": "string", "default": "utf-8", "description": "Text encoding or 'base64' for binary."},
                    "create_dirs": {"type": "boolean", "default": True, "description": "Create missing parent directories."},
                },
                "required": ["path", "content"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "size_bytes": {"type": "integer"},
                    "created": {"type": "boolean", "description": "True if the file was newly created."},
                },
            },
        },
        {
            "name": "append_file",
            "description": "Append text to the end of a file. Creates the file if it does not exist.",
            "tags": ["filesystem", "write", "append", "add", "insert", "update", "extend", "log"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the file."},
                    "content": {"type": "string", "description": "Text to append."},
                    "encoding": {"type": "string", "default": "utf-8"},
                },
                "required": ["path", "content"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "size_bytes": {"type": "integer"},
                },
            },
        },
        {
            "name": "delete_file",
            "description": "Delete a single file. Requires fs_allow_delete=true in agent settings.",
            "tags": ["filesystem", "delete", "remove", "erase", "cleanup", "unlink"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the file to delete."},
                },
                "required": ["path"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "deleted": {"type": "boolean"},
                },
            },
        },
        {
            "name": "move_file",
            "description": "Move or rename a file. Both source and destination must be within allowed paths.",
            "tags": ["filesystem", "write", "move", "rename", "relocate", "transfer"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string", "description": "Absolute path of the file to move."},
                    "destination": {"type": "string", "description": "Absolute destination path."},
                },
                "required": ["source", "destination"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "destination": {"type": "string"},
                },
            },
        },
        {
            "name": "copy_file",
            "description": "Copy a file to a new location. Both paths must be within allowed directories.",
            "tags": ["filesystem", "write", "copy", "duplicate", "clone", "backup"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string", "description": "Absolute path of the file to copy."},
                    "destination": {"type": "string", "description": "Absolute destination path."},
                },
                "required": ["source", "destination"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "destination": {"type": "string"},
                    "size_bytes": {"type": "integer"},
                },
            },
        },
        {
            "name": "list_directory",
            "description": "List files and subdirectories in a directory with metadata (size, type, modified time).",
            "tags": ["filesystem", "read", "list", "browse", "view", "explore", "find", "ls", "show", "directory"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the directory."},
                    "pattern": {"type": "string", "default": "*", "description": "Glob filter, e.g. '*.txt' or '*.py'."},
                    "include_hidden": {"type": "boolean", "default": False, "description": "Include hidden files (names starting with '.')."},
                },
                "required": ["path"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "entries": {"type": "array"},
                    "count": {"type": "integer"},
                    "directories": {"type": "integer"},
                    "files": {"type": "integer"},
                },
            },
        },
        {
            "name": "create_directory",
            "description": "Create a directory and all missing intermediate parents (equivalent to mkdir -p).",
            "tags": ["filesystem", "write", "create", "mkdir", "make", "folder", "new", "directory"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path of the directory to create."},
                },
                "required": ["path"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "created": {"type": "boolean"},
                },
            },
        },
        {
            "name": "delete_directory",
            "description": "Delete a directory. Set recursive=true to delete non-empty directories. Requires fs_allow_delete=true.",
            "tags": ["filesystem", "delete", "remove", "cleanup", "rmdir", "folder"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the directory to delete."},
                    "recursive": {"type": "boolean", "default": False, "description": "Delete contents recursively. Required for non-empty directories."},
                },
                "required": ["path"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "deleted": {"type": "boolean"},
                    "recursive": {"type": "boolean"},
                },
            },
        },
        {
            "name": "get_file_info",
            "description": "Return metadata for a file or directory: name, type, size, timestamps, symlink status.",
            "tags": ["filesystem", "read", "info", "stat", "metadata", "properties", "size", "details", "check"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to inspect."},
                },
                "required": ["path"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "name": {"type": "string"},
                    "extension": {"type": "string"},
                    "type": {"type": "string"},
                    "size_bytes": {"type": "integer"},
                    "created_at": {"type": "number"},
                    "modified_at": {"type": "number"},
                    "is_symlink": {"type": "boolean"},
                    "platform": {"type": "string"},
                },
            },
        },
        {
            "name": "search_files",
            "description": (
                "Recursively search for files matching a glob pattern under a directory. "
                "Optionally filter by content — only files containing search_content text are returned."
            ),
            "tags": ["filesystem", "search", "find", "locate", "grep", "lookup", "discover", "glob", "filter"],
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Root directory to search in."},
                    "pattern": {"type": "string", "description": "Glob pattern, e.g. '*.py', '*.log', 'config.*'."},
                    "search_content": {"type": "string", "default": "", "description": "If provided, only return files containing this text."},
                    "max_results": {"type": "integer", "default": 100, "description": "Maximum number of results (1–1000)."},
                },
                "required": ["path", "pattern"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "search_root": {"type": "string"},
                    "pattern": {"type": "string"},
                    "results": {"type": "array"},
                    "count": {"type": "integer"},
                    "truncated": {"type": "boolean"},
                },
            },
        },
    ],
    "required_settings": [
        {
            "key": "fs_allowed_paths",
            "label": "Allowed Paths",
            "description": (
                "Newline- or semicolon-separated list of directories this agent may access. "
                "All operations are sandboxed to these paths. "
                "Example (Linux/Mac): /home/user/documents;/tmp/workspace\n"
                "Example (Windows): C:\\Users\\user\\Documents;D:\\projects"
            ),
            "type": "string",
            "required": True,
            "default": "",
        },
        {
            "key": "fs_max_file_size_mb",
            "label": "Max File Size (MB)",
            "description": "Maximum file size in MB the agent will read. Files larger than this are rejected.",
            "type": "integer",
            "required": False,
            "default": 10,
            "min": 1,
            "max": 500,
        },
        {
            "key": "fs_allow_delete",
            "label": "Allow Delete Operations",
            "description": "Whether delete_file and delete_directory capabilities are permitted.",
            "type": "string",
            "required": False,
            "default": "true",
            "options": ["true", "false"],
        },
    ],
}

# ── Constants ─────────────────────────────────────────────────────────────────

HEARTBEAT_INTERVAL_S: int = 15
MAX_BACKOFF_S: int = 60
DRAIN_TIMEOUT_S: int = 30

# Map capability name → FilesystemClient method name
_CAPABILITY_MAP: dict[str, str] = {
    "read_file":        "read_file",
    "write_file":       "write_file",
    "append_file":      "append_file",
    "delete_file":      "delete_file",
    "move_file":        "move_file",
    "copy_file":        "copy_file",
    "list_directory":   "list_directory",
    "create_directory": "create_directory",
    "delete_directory": "delete_directory",
    "get_file_info":    "get_file_info",
    "search_files":     "search_files",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _envelope(
    sender_id: str,
    msg_type: str,
    payload: dict,
    recipient_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    msg_id: Optional[str] = None,
) -> str:
    return json.dumps({
        "id":             msg_id or str(uuid.uuid4()),
        "type":           msg_type,
        "sender_id":      sender_id,
        "recipient_id":   recipient_id,
        "payload":        payload,
        "timestamp":      _now_iso(),
        "correlation_id": correlation_id,
    })


# ── Main client ───────────────────────────────────────────────────────────────

class OrchestratorClient:
    """Registers the filesystem-agent and handles incoming task_requests."""

    def __init__(self, orchestrator_url: str = "http://localhost:8000") -> None:
        self._base = orchestrator_url.rstrip("/")
        self._http = httpx.AsyncClient(timeout=30)

        self._agent_id: str = ""
        self._ws_url: str = ""
        self._common_settings: dict[str, Any] = {}

        self._status: str = "starting"
        self._active_tasks: int = 0
        self._tasks_completed: int = 0
        self._tasks_failed: int = 0
        self._total_duration_ms: float = 0.0
        self._start_time: float = time.monotonic()

        self._shutting_down: bool = False
        self._current_ws: Any = None

        self._fs = FilesystemClient()

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._graceful_shutdown()))

        await self._register()
        await self._connect_loop()

    # ── Registration ──────────────────────────────────────────────────────

    async def _register(self) -> None:
        url = f"{self._base}/api/v1/agents/register"
        logger.info("Registering with orchestrator at %s …", url)
        payload = {**REGISTRATION_PAYLOAD, "id": _stable_agent_id()}
        resp = await self._http.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        self._agent_id = data["agent_id"]
        self._ws_url = data["ws_url"]
        self._common_settings = {
            **data.get("common_settings", {}),
            **data.get("agent_settings", {}),   # per-agent required_settings values
        }
        self._fs.update_settings(self._common_settings)
        logger.info("Registered — agent_id=%s  ws=%s", self._agent_id, self._ws_url)
        await self._sync_allowed_paths()

    # ── WebSocket loop ────────────────────────────────────────────────────

    async def _connect_loop(self) -> None:
        backoff = 1.0
        while not self._shutting_down:
            try:
                logger.info("Connecting to %s …", self._ws_url)
                async with websockets.connect(self._ws_url) as ws:
                    backoff = 1.0
                    await self._run_session(ws)

            except websockets.exceptions.ConnectionClosed as exc:
                code = exc.rcvd.code if exc.rcvd else None
                if code == 4004:
                    logger.warning("Unknown agent_id (4004) — re-registering …")
                    try:
                        await self._register()
                    except Exception as reg_exc:
                        logger.error("Re-registration failed: %s", reg_exc)
                elif code == 4003:
                    logger.info("Agent disabled (4003) — will retry")
                    backoff = max(backoff, 10.0)
                elif self._shutting_down:
                    break
                else:
                    logger.warning("WS closed (code=%s) — retry in %.0fs", code, backoff)

            except (OSError, Exception) as exc:
                if self._shutting_down:
                    break
                logger.warning("WS error (%s) — retry in %.0fs", exc, backoff)

            if not self._shutting_down:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)

    async def _run_session(self, ws) -> None:
        self._current_ws = ws
        self._status = "available"
        logger.info("WebSocket session active")
        try:
            await asyncio.gather(
                self._heartbeat_loop(ws),
                self._recv_loop(ws),
            )
        finally:
            self._current_ws = None
            self._status = "offline"

    # ── Heartbeat ─────────────────────────────────────────────────────────

    async def _heartbeat_loop(self, ws) -> None:
        while True:
            await self._ws_send(ws, self._msg(
                "heartbeat",
                {
                    "status":       self._status,
                    "current_load": min(self._active_tasks / 5.0, 1.0),
                    "active_tasks": self._active_tasks,
                    "metrics":      self._metrics(),
                },
            ))
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)

    # ── Receive loop ──────────────────────────────────────────────────────

    async def _recv_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Non-JSON frame ignored")
                continue
            mtype = msg.get("type", "?")
            _lvl = logging.DEBUG if mtype in ("agent_registered", "agent_offline", "heartbeat_ack", "settings_push") else logging.INFO
            logger.log(_lvl, "← [%s] from=%s", mtype, msg.get("sender_id", "?"))
            await self._dispatch(ws, msg)

    async def _dispatch(self, ws, msg: dict) -> None:
        mtype = msg.get("type", "")
        payload = msg.get("payload", {})

        if mtype == "task_request":
            asyncio.create_task(self._handle_task(ws, msg))

        elif mtype == "settings_push":
            pushed = payload.get("settings", {})
            self._common_settings.update(pushed)
            self._fs.update_settings(self._common_settings)
            logger.info("Settings updated via push: %s", list(pushed.keys()))
            asyncio.create_task(self._sync_allowed_paths())

        elif mtype in ("agent_registered", "agent_offline"):
            logger.debug("Peer event [%s]: %s", mtype, payload.get("agent_id"))

        elif mtype == "error":
            logger.error(
                "Orchestrator error [%s]: %s",
                payload.get("code"), payload.get("detail"),
            )

        else:
            logger.debug("Unhandled message type: %r", mtype)

    # ── Task handling ─────────────────────────────────────────────────────

    async def _handle_task(self, ws, msg: dict) -> None:
        req_id     = msg.get("id")
        sender_id  = msg.get("sender_id")
        payload    = msg.get("payload", {})
        capability = payload.get("capability", "")
        input_data = payload.get("input_data", {})

        self._active_tasks += 1
        self._status = "busy"
        t0 = time.monotonic()

        try:
            output, error = await self._dispatch_capability(capability, input_data)
            duration_ms = (time.monotonic() - t0) * 1000

            if error:
                self._tasks_failed += 1
                await self._ws_send(ws, self._msg(
                    "task_response",
                    {"success": False, "error": error, "duration_ms": round(duration_ms, 1)},
                    recipient_id=sender_id,
                    correlation_id=req_id,
                ))
            else:
                self._tasks_completed += 1
                self._total_duration_ms += duration_ms
                await self._ws_send(ws, self._msg(
                    "task_response",
                    {"success": True, "output_data": output, "duration_ms": round(duration_ms, 1)},
                    recipient_id=sender_id,
                    correlation_id=req_id,
                ))

        except Exception as exc:
            duration_ms = (time.monotonic() - t0) * 1000
            self._tasks_failed += 1
            logger.exception("Unhandled error in capability %r", capability)
            await self._ws_send(ws, self._msg(
                "task_response",
                {"success": False, "error": str(exc), "duration_ms": round(duration_ms, 1)},
                recipient_id=sender_id,
                correlation_id=req_id,
            ))

        finally:
            self._active_tasks = max(0, self._active_tasks - 1)
            self._status = "draining" if self._shutting_down else (
                "busy" if self._active_tasks else "available"
            )
            await self._send_status_update(ws)

    async def _dispatch_capability(
        self, capability: str, input_data: dict
    ) -> tuple[Optional[dict], Optional[str]]:
        """Route capability name → FilesystemClient method, run in thread."""
        method_name = _CAPABILITY_MAP.get(capability)
        if not method_name:
            return None, f"Unknown capability: {capability!r}"

        method = getattr(self._fs, method_name)
        # Strip internal orchestration keys (e.g. _reply_context injected by
        # the planner/executor) before forwarding to the capability method.
        clean_input = {k: v for k, v in input_data.items() if not k.startswith("_")}
        try:
            result = await asyncio.to_thread(method, **clean_input)
            return result, None
        except PathViolation as exc:
            return None, f"Access denied: {exc}"
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError, PermissionError, ValueError) as exc:
            return None, str(exc)
        except TypeError as exc:
            return None, f"Invalid input for {capability!r}: {exc}"
        except OSError as exc:
            return None, f"OS error: {exc}"

    # ── Status update ─────────────────────────────────────────────────────

    async def _send_status_update(self, ws) -> None:
        await self._ws_send(ws, self._msg(
            "status_update",
            {
                "status":       self._status,
                "current_load": min(self._active_tasks / 5.0, 1.0),
                "active_tasks": self._active_tasks,
                "metrics":      self._metrics(),
            },
        ))

    # ── User notification helper ───────────────────────────────────────────

    async def _notify_user(self, reply_context: dict, message: str) -> None:
        """Send a status message back to the originating user channel."""
        if not reply_context or not message:
            return
        try:
            await self._http.post(
                f"{self._base}/api/v1/notify",
                json={**reply_context, "message": message, "sender_agent_id": self._agent_id},
                timeout=10.0,
            )
        except Exception as exc:
            logger.warning("_notify_user failed: %s", exc)

    # ── Graceful shutdown ─────────────────────────────────────────────────

    async def _graceful_shutdown(self) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        logger.info("Shutdown signal received — draining …")
        self._status = "draining"

        deadline = time.monotonic() + DRAIN_TIMEOUT_S
        while self._active_tasks > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.5)

        if self._agent_id:
            try:
                await self._http.delete(f"{self._base}/api/v1/agents/{self._agent_id}")
                logger.info("Deregistered from orchestrator.")
            except Exception as exc:
                logger.warning("Deregister failed: %s", exc)

        await self._http.aclose()
        logger.info("Shutdown complete.")

    # ── Orchestrator metadata sync ────────────────────────────────────────

    async def _sync_allowed_paths(self) -> None:
        """Push the resolved allowed roots into the agent's orchestrator metadata.

        This lets the task-planner read the real allowed paths and inject them
        as a path constraint in the LLM planning prompt so it never produces
        paths outside the sandbox.
        """
        if not self._agent_id:
            return
        roots = self._fs.allowed_roots
        try:
            resp = await self._http.patch(
                f"{self._base}/api/v1/agents/{self._agent_id}",
                json={"metadata": {"fs_allowed_paths": roots}},
            )
            resp.raise_for_status()
            logger.info("Synced allowed paths to orchestrator: %s", roots)
        except Exception as exc:
            logger.warning("Failed to sync allowed paths metadata: %s", exc)

    # ── Helpers ───────────────────────────────────────────────────────────

    async def _ws_send(self, ws, msg_str: str) -> None:
        msg = json.loads(msg_str)
        mtype = msg.get("type", "?")
        noisy = mtype in ("heartbeat", "status_update")
        (logger.debug if noisy else logger.info)(
            "→ [%s] to=%s", mtype, msg.get("recipient_id") or "orchestrator"
        )
        try:
            await ws.send(msg_str)
        except websockets.exceptions.ConnectionClosed:
            raise  # propagate → heartbeat loop exits → asyncio.gather raises → reconnect
        except Exception as exc:
            logger.warning("WS send failed: %s", exc)

    def _msg(
        self,
        msg_type: str,
        payload: dict,
        recipient_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> str:
        return _envelope(self._agent_id, msg_type, payload, recipient_id, correlation_id)

    def _metrics(self) -> dict:
        n = self._tasks_completed + self._tasks_failed
        return {
            "tasks_completed":      self._tasks_completed,
            "tasks_failed":         self._tasks_failed,
            "avg_response_time_ms": round(self._total_duration_ms / n, 1) if n else 0.0,
            "uptime_seconds":       round(time.monotonic() - self._start_time, 1),
        }
