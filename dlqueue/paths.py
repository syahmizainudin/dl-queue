"""Path helpers. cbz-webp has the same ``normalize_path`` in ``cbzwebp/core.py``; keep them in step."""

from __future__ import annotations

import os
import re
from pathlib import Path

PART_SUFFIX = ".part"


def normalize_path(raw: str) -> Path:
    """Accept POSIX paths and, for WSL users, Windows paths like ``C:\\Comics``."""
    raw = raw.strip().strip('"').strip("'")
    m = re.match(r"^([A-Za-z]):[\\/](.*)$", raw)
    if m and not Path(raw).exists():
        raw = f"/mnt/{m.group(1).lower()}/" + m.group(2).replace("\\", "/")
    return Path(os.path.expanduser(raw)).resolve()
