"""
Watchlist: routes to track per program (sas | flyingblue | virgin).
"""

from __future__ import annotations

import sqlite3
from typing import Optional


def list_watch_routes(
    conn: sqlite3.Connection,
    program: str,
    origin_filter: str | None = None,
) -> list[dict]:
    """Returns rows ordered by origin, destination (alphabetical). Optional origin_filter."""
    if origin_filter:
        origin = (origin_filter or "").strip().upper()[:4]
        cur = conn.execute(
            """SELECT id, program, origin, destination, enabled, created_at, updated_at,
                      COALESCE(include_returns, 0)
               FROM partner_award_watch_routes
               WHERE program = ? AND origin = ?
               ORDER BY origin, destination""",
            (program, origin),
        )
    else:
        cur = conn.execute(
            """SELECT id, program, origin, destination, enabled, created_at, updated_at,
                      COALESCE(include_returns, 0)
               FROM partner_award_watch_routes
               WHERE program = ?
               ORDER BY origin, destination""",
            (program,),
        )
    return [
        {
            "id": r[0],
            "program": r[1],
            "origin": r[2],
            "destination": r[3],
            "enabled": bool(r[4]),
            "created_at": r[5],
            "updated_at": r[6],
            "include_returns": bool(r[7]) if len(r) > 7 else False,
        }
        for r in cur.fetchall()
    ]


def _validate_airport_code(code: str) -> str:
    """Normalize and validate: 2-4 letters A-Z. Raises ValueError if invalid."""
    s = (code or "").strip().upper()[:4]
    if not s or len(s) < 2:
        raise ValueError("Code must be 2-4 letters")
    if not all(c.isalpha() and c.isupper() for c in s):
        raise ValueError("Code must contain only letters A-Z")
    return s


def upsert_watch_route(
    conn: sqlite3.Connection,
    program: str,
    origin: str,
    destination: str,
    enabled: int = 1,
    include_returns: int = 0,
) -> int:
    """
    Insert or update. Returns row id.
    """
    origin = _validate_airport_code(origin)
    destination = _validate_airport_code(destination)
    if origin == destination:
        raise ValueError("Origin and destination must be different")

    conn.execute(
        """INSERT INTO partner_award_watch_routes (program, origin, destination, enabled, include_returns)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (program, origin, destination)
           DO UPDATE SET enabled=excluded.enabled, include_returns=excluded.include_returns, updated_at=datetime('now')""",
        (program, origin, destination, enabled, 1 if include_returns else 0),
    )
    conn.commit()
    cur = conn.execute(
        "SELECT id FROM partner_award_watch_routes WHERE program=? AND origin=? AND destination=?",
        (program, origin, destination),
    )
    row = cur.fetchone()
    return row[0] if row else 0


def set_watch_route_enabled(conn: sqlite3.Connection, route_id: int, enabled: int) -> bool:
    """Returns True if updated."""
    cur = conn.execute(
        "UPDATE partner_award_watch_routes SET enabled=?, updated_at=datetime('now') WHERE id=?",
        (1 if enabled else 0, route_id),
    )
    conn.commit()
    return cur.rowcount > 0


def set_watch_route_include_returns(conn: sqlite3.Connection, route_id: int, include_returns: int) -> bool:
    """Returns True if updated."""
    cur = conn.execute(
        "UPDATE partner_award_watch_routes SET include_returns=?, updated_at=datetime('now') WHERE id=?",
        (1 if include_returns else 0, route_id),
    )
    conn.commit()
    return cur.rowcount > 0


def delete_watch_route(conn: sqlite3.Connection, route_id: int) -> bool:
    """Returns True if deleted."""
    cur = conn.execute("DELETE FROM partner_award_watch_routes WHERE id=?", (route_id,))
    conn.commit()
    return cur.rowcount > 0
