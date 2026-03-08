"""
Operational state for Partner Awards (AF/KLM).
Stored on disk: ~/sas_awards/partner_awards_state.json
Used by runner (cooldown) and app (blocked indicator).
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

STATE_DIR = Path(os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards")))
STATE_PATH = STATE_DIR / "partner_awards_state.json"


def _ensure_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def read_state() -> dict[str, Any]:
    """Read state file. Returns dict with afkl_blocked_until, last_block_reason, etc."""
    if not STATE_PATH.exists():
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def write_state(**kwargs: Any) -> None:
    """Update state file with given fields."""
    _ensure_dir()
    data = read_state()
    data.update(kwargs)
    data["updated_at"] = datetime.utcnow().isoformat() + "Z"
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def is_blocked() -> tuple[bool, Optional[str]]:
    """
    Returns (is_blocked, blocked_until_iso).
    If blocked, caller should refuse scans until that time.
    """
    state = read_state()
    until = state.get("afkl_blocked_until")
    if not until:
        return False, None
    try:
        dt = datetime.fromisoformat(until.replace("Z", "+00:00"))
        # Accept naive datetime as UTC
        if dt.tzinfo is None:
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(dt.tzinfo)
        if now < dt:
            return True, until
    except Exception:
        pass
    return False, None


def set_blocked(minutes: int = 30, reason: str = "", host: str = "") -> None:
    """Record block: afkl_blocked_until = now + minutes. Increases cooldown after consecutive blocks."""
    from datetime import timezone, timedelta
    state = read_state()
    consecutive = int(state.get("afkl_consecutive_blocked", 0)) + 1
    mins = 60 if consecutive >= 2 else minutes
    until = datetime.now(timezone.utc) + timedelta(minutes=mins)
    write_state(
        afkl_blocked_until=until.isoformat(),
        afkl_consecutive_blocked=consecutive,
        last_block_reason=reason,
        last_block_host=host,
    )


def clear_blocked() -> None:
    """Clear block so scans can resume. Removes afkl_blocked_until and resets consecutive count."""
    state = read_state()
    state.pop("afkl_blocked_until", None)
    state["afkl_consecutive_blocked"] = 0
    state["updated_at"] = datetime.utcnow().isoformat() + "Z"
    _ensure_dir()
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
