"""Owner-only artifact permissions for POSIX and Windows."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

_ALLOWED_POSIX_ROOT_ALIASES = {
    Path("/etc"): Path("/private/etc"),
    Path("/tmp"): Path("/private/tmp"),
    Path("/var"): Path("/private/var"),
}


def reject_symlink_path_components(path: Path) -> None:
    """Reject symlink traversal except for standard macOS root aliases."""

    absolute = Path(os.path.abspath(path))
    cursor = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        cursor /= component
        if not cursor.is_symlink():
            continue
        allowed_target = _ALLOWED_POSIX_ROOT_ALIASES.get(cursor)
        if allowed_target is not None:
            try:
                if cursor.resolve(strict=True) == allowed_target:
                    continue
            except OSError:
                pass
        raise ValueError(f"symlink path component is not allowed: {cursor}")


def read_regular_file(path: Path) -> bytes:
    """Read one regular file without following any symlink path component."""

    reject_symlink_path_components(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError(f"not a regular file: {path}")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def ensure_owner_only_directory(path: Path) -> None:
    if path.exists():
        if not path.is_dir():
            raise NotADirectoryError(path)
        secure_owner_only(path, directory=True)
        return
    missing: list[Path] = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            raise FileNotFoundError(f"cannot resolve an existing ancestor for {path}")
        cursor = parent
    for component in reversed(missing):
        component.mkdir(mode=0o700)
        secure_owner_only(component, directory=True)


def windows_acl_args(path: Path, *, directory: bool, identity: str) -> list[str]:
    rights = "(OI)(CI)F" if directory else "F"
    return [
        str(path),
        "/inheritance:r",
        "/remove:g",
        "*S-1-1-0",
        "*S-1-5-11",
        "*S-1-5-32-545",
        "/grant:r",
        f"{identity}:{rights}",
    ]


def secure_owner_only(
    path: Path,
    *,
    directory: bool,
    platform: str | None = None,
    identity: str | None = None,
    runner: Callable[..., Any] = subprocess.run,
) -> None:
    if (platform or sys.platform) != "win32":
        os.chmod(path, 0o700 if directory else 0o600)
        return
    owner = identity or _windows_identity(os.environ)
    if not owner:
        raise RuntimeError("owner-only Windows ACL requires USERNAME")
    result = runner(
        ["icacls", *windows_acl_args(path, directory=directory, identity=owner)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"owner-only Windows ACL failed: {result.stderr or 'unknown'}")


def _windows_identity(environment: Mapping[str, str]) -> str | None:
    username = environment.get("USERNAME", "").strip()
    if not username:
        return None
    domain = environment.get("USERDOMAIN", "").strip()
    return f"{domain}\\{username}" if domain else username
