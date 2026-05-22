"""
filesystem_client.py — Core filesystem operations with sandboxed path access.

All operations resolve paths with Path.resolve() and verify the result sits
within one of the configured allowed roots before proceeding.  Supports
Linux, macOS and Windows via pathlib.
"""
from __future__ import annotations

import base64
import fnmatch
import logging
import os
import platform
import shutil
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class PathViolation(Exception):
    """Raised when a requested path falls outside every allowed root."""


class FilesystemClient:
    """
    Sandboxed filesystem operations.

    Call ``update_settings()`` on startup and whenever settings are pushed
    from the orchestrator dashboard to refresh allowed paths and limits.
    """

    def __init__(self) -> None:
        self._allowed_roots: list[Path] = []
        self._max_file_size_bytes: int = 10 * 1024 * 1024   # 10 MB
        self._allow_delete: bool = True

    # ── Settings ───────────────────────────────────────────────────────────

    def update_settings(self, settings: dict) -> None:
        """Apply settings received from the orchestrator."""
        raw = settings.get("fs_allowed_paths", "")
        roots: list[Path] = []
        if raw:
            # Accept newline, semicolon, or comma as separator
            parts = [raw]
            for sep in ("\n", ";", ","):
                parts = [chunk for p in parts for chunk in p.split(sep)]
            for part in parts:
                part = part.strip()
                if part:
                    try:
                        roots.append(Path(os.path.expandvars(part)).expanduser().resolve())
                    except Exception as exc:
                        logger.warning("Could not resolve allowed path %r: %s", part, exc)
        self._allowed_roots = roots
        logger.info("Allowed roots updated: %s", [str(r) for r in self._allowed_roots])

        try:
            self._max_file_size_bytes = int(float(settings.get("fs_max_file_size_mb", 10))) * 1024 * 1024
        except (TypeError, ValueError):
            self._max_file_size_bytes = 10 * 1024 * 1024

        self._allow_delete = str(settings.get("fs_allow_delete", "true")).lower() not in ("false", "0", "no")

    @property
    def allowed_roots(self) -> list[str]:
        """Resolved allowed root paths as strings (empty if none configured)."""
        return [str(r) for r in self._allowed_roots]

    # ── Path resolution + sandboxing ───────────────────────────────────────

    def _resolve(self, path_str: str) -> Path:
        """
        Expand env vars and user (~), resolve symlinks, then verify the
        result is inside at least one allowed root.
        """
        if not path_str or not path_str.strip():
            raise ValueError("path must not be empty")

        expanded = os.path.expandvars(path_str.strip())
        p = Path(expanded).expanduser().resolve()

        if not self._allowed_roots:
            raise PathViolation(
                "No allowed paths configured. "
                "Set fs_allowed_paths in agent settings on the orchestrator dashboard."
            )

        for root in self._allowed_roots:
            try:
                p.relative_to(root)
                return p          # within this root — OK
            except ValueError:
                continue

        raise PathViolation(
            f"Path '{p}' is outside all allowed directories: "
            + ", ".join(str(r) for r in self._allowed_roots)
        )

    # ── Capabilities ───────────────────────────────────────────────────────

    def read_file(self, path: str, encoding: str = "utf-8") -> dict[str, Any]:
        """Read a file and return its content.  Binary files are base64-encoded."""
        p = self._resolve(path)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
        if not p.is_file():
            raise IsADirectoryError(f"Path is a directory: {p}")

        size = p.stat().st_size
        if size > self._max_file_size_bytes:
            limit_mb = self._max_file_size_bytes / 1024 / 1024
            raise ValueError(
                f"File size {size / 1024 / 1024:.1f} MB exceeds the configured limit of {limit_mb:.0f} MB"
            )

        if encoding.lower() in ("binary", "base64"):
            content = base64.b64encode(p.read_bytes()).decode("ascii")
            return {"content": content, "encoding": "base64", "size_bytes": size, "path": str(p)}

        try:
            content = p.read_text(encoding=encoding)
            return {"content": content, "encoding": encoding, "size_bytes": size, "path": str(p)}
        except UnicodeDecodeError:
            # Auto-fallback to base64 for binary files
            content = base64.b64encode(p.read_bytes()).decode("ascii")
            return {"content": content, "encoding": "base64", "size_bytes": size, "path": str(p)}

    def write_file(
        self,
        path: str,
        content: str,
        encoding: str = "utf-8",
        create_dirs: bool = True,
    ) -> dict[str, Any]:
        """Create or overwrite a file."""
        p = self._resolve(path)
        if create_dirs:
            p.parent.mkdir(parents=True, exist_ok=True)

        existed = p.exists()
        if encoding.lower() in ("binary", "base64"):
            data = base64.b64decode(content)
            p.write_bytes(data)
            size = len(data)
        else:
            p.write_text(content, encoding=encoding)
            size = p.stat().st_size

        return {"path": str(p), "size_bytes": size, "created": not existed}

    def append_file(self, path: str, content: str, encoding: str = "utf-8") -> dict[str, Any]:
        """Append text to a file, creating it if it does not exist."""
        p = self._resolve(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding=encoding) as f:
            f.write(content)
        return {"path": str(p), "size_bytes": p.stat().st_size}

    def delete_file(self, path: str) -> dict[str, Any]:
        """Delete a file.  Requires fs_allow_delete=true."""
        if not self._allow_delete:
            raise PermissionError(
                "File deletion is disabled. Set fs_allow_delete=true in agent settings."
            )
        p = self._resolve(path)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
        if not p.is_file():
            raise IsADirectoryError(f"Path is a directory — use delete_directory: {p}")
        p.unlink()
        return {"path": str(p), "deleted": True}

    def move_file(self, source: str, destination: str) -> dict[str, Any]:
        """Move or rename a file.  Both paths must be within allowed roots."""
        src = self._resolve(source)
        dst = self._resolve(destination)
        if not src.exists():
            raise FileNotFoundError(f"Source not found: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        return {"source": str(src), "destination": str(dst)}

    def copy_file(self, source: str, destination: str) -> dict[str, Any]:
        """Copy a file.  Both paths must be within allowed roots."""
        src = self._resolve(source)
        dst = self._resolve(destination)
        if not src.exists():
            raise FileNotFoundError(f"Source not found: {src}")
        if not src.is_file():
            raise IsADirectoryError(f"Source is a directory: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dst))
        return {"source": str(src), "destination": str(dst), "size_bytes": dst.stat().st_size}

    def list_directory(
        self,
        path: str,
        pattern: str = "*",
        include_hidden: bool = False,
    ) -> dict[str, Any]:
        """List entries in a directory, optionally filtered by glob pattern."""
        p = self._resolve(path)
        if not p.exists():
            raise FileNotFoundError(f"Directory not found: {p}")
        if not p.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {p}")

        entries: list[dict[str, Any]] = []
        for child in sorted(p.iterdir()):
            if not include_hidden and child.name.startswith("."):
                continue
            if not fnmatch.fnmatch(child.name, pattern):
                continue
            try:
                stat = child.stat()
                entries.append({
                    "name": child.name,
                    "path": str(child),
                    "type": "directory" if child.is_dir() else "file",
                    "size_bytes": stat.st_size if child.is_file() else None,
                    "modified_at": stat.st_mtime,
                    "is_hidden": child.name.startswith("."),
                })
            except OSError:
                continue

        return {
            "path": str(p),
            "entries": entries,
            "count": len(entries),
            "directories": sum(1 for e in entries if e["type"] == "directory"),
            "files": sum(1 for e in entries if e["type"] == "file"),
        }

    def create_directory(self, path: str) -> dict[str, Any]:
        """Create a directory including all missing parents (mkdir -p)."""
        p = self._resolve(path)
        existed = p.exists()
        p.mkdir(parents=True, exist_ok=True)
        return {"path": str(p), "created": not existed}

    def delete_directory(self, path: str, recursive: bool = False) -> dict[str, Any]:
        """Delete a directory.  Set recursive=true to delete non-empty directories."""
        if not self._allow_delete:
            raise PermissionError(
                "Directory deletion is disabled. Set fs_allow_delete=true in agent settings."
            )
        p = self._resolve(path)
        if not p.exists():
            raise FileNotFoundError(f"Directory not found: {p}")
        if not p.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {p}")
        if recursive:
            shutil.rmtree(str(p))
        else:
            p.rmdir()   # raises OSError if non-empty
        return {"path": str(p), "deleted": True, "recursive": recursive}

    def get_file_info(self, path: str) -> dict[str, Any]:
        """Return metadata for a file or directory."""
        p = self._resolve(path)
        if not p.exists():
            raise FileNotFoundError(f"Path not found: {p}")
        stat = p.stat()
        return {
            "path": str(p),
            "name": p.name,
            "extension": p.suffix,
            "type": "directory" if p.is_dir() else "file",
            "size_bytes": stat.st_size,
            "created_at": stat.st_ctime,
            "modified_at": stat.st_mtime,
            "is_symlink": p.is_symlink(),
            "parent": str(p.parent),
            "platform": platform.system(),
        }

    def search_files(
        self,
        path: str,
        pattern: str,
        search_content: str = "",
        max_results: int = 100,
    ) -> dict[str, Any]:
        """
        Recursively search for files matching *pattern* (glob) under *path*.
        If *search_content* is provided, only files containing that text are returned.
        """
        base = self._resolve(path)
        if not base.exists():
            raise FileNotFoundError(f"Search root not found: {base}")

        max_results = max(1, min(max_results, 1000))
        results: list[dict[str, Any]] = []
        truncated = False

        for child in base.rglob(pattern):
            if len(results) >= max_results:
                truncated = True
                break
            # Skip hidden files/dirs unless explicitly matched
            if any(part.startswith(".") for part in child.parts):
                continue
            try:
                entry: dict[str, Any] = {
                    "name": child.name,
                    "path": str(child),
                    "type": "directory" if child.is_dir() else "file",
                    "size_bytes": child.stat().st_size if child.is_file() else None,
                }
                if search_content and child.is_file():
                    try:
                        text = child.read_text(errors="ignore")
                        if search_content.lower() in text.lower():
                            entry["content_match"] = True
                            results.append(entry)
                    except OSError:
                        pass
                else:
                    results.append(entry)
            except OSError:
                continue

        return {
            "search_root": str(base),
            "pattern": pattern,
            "search_content": search_content or None,
            "results": results,
            "count": len(results),
            "truncated": truncated,
        }
