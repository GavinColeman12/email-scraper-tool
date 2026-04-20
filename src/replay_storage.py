"""
Storage for replay runs.

A replay_run is a re-execution of scrape_with_triangulation() over every
business in a past search, stored alongside the original (never overwriting).
Because Phase 1-3 caches live 14-90 days, replays cost ~$0 and let us A/B
every logic change against historical data.

Table:
    replay_runs(id, original_search_id, label, git_sha, created_at,
                businesses_json, metrics_json)
"""
import json
import subprocess
from typing import Optional

from src.storage import _connect, _cursor, _PARAM, USE_PG, init_db


SCHEMA = """
CREATE TABLE IF NOT EXISTS replay_runs (
    id {serial},
    original_search_id INTEGER,
    label TEXT,
    git_sha TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    businesses_json TEXT,
    metrics_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_replay_original ON replay_runs(original_search_id);
"""


_INITIALIZED = False


def init_replay_tables() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        if USE_PG:
            cur.execute(SCHEMA.format(serial="SERIAL PRIMARY KEY"))
        else:
            conn.executescript(SCHEMA.format(serial="INTEGER PRIMARY KEY AUTOINCREMENT"))
        conn.commit()
        _INITIALIZED = True
    finally:
        conn.close()


def _git_sha() -> str:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        return r.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def save_replay(
    original_search_id: int,
    label: str,
    businesses: list,
    metrics: dict,
) -> int:
    init_replay_tables()
    conn = _connect()
    try:
        cur = _cursor(conn)
        payload = (
            original_search_id,
            label,
            _git_sha(),
            json.dumps(businesses, default=str),
            json.dumps(metrics, default=str),
        )
        if USE_PG:
            cur.execute(
                "INSERT INTO replay_runs (original_search_id, label, git_sha, "
                "businesses_json, metrics_json) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                payload,
            )
            new_id = cur.fetchone()["id"]
        else:
            cur.execute(
                "INSERT INTO replay_runs (original_search_id, label, git_sha, "
                "businesses_json, metrics_json) VALUES (?, ?, ?, ?, ?)",
                payload,
            )
            new_id = cur.lastrowid
        conn.commit()
        return new_id
    finally:
        conn.close()


def list_replays(original_search_id: Optional[int] = None) -> list:
    init_replay_tables()
    conn = _connect()
    try:
        cur = _cursor(conn)
        if original_search_id is not None:
            cur.execute(
                f"SELECT id, original_search_id, label, git_sha, created_at, metrics_json "
                f"FROM replay_runs WHERE original_search_id = {_PARAM} "
                f"ORDER BY created_at DESC",
                (original_search_id,),
            )
        else:
            cur.execute(
                "SELECT id, original_search_id, label, git_sha, created_at, metrics_json "
                "FROM replay_runs ORDER BY created_at DESC"
            )
        rows = cur.fetchall()
        out = []
        for r in rows:
            d = dict(r) if hasattr(r, "keys") else dict(r)
            if d.get("metrics_json"):
                try:
                    d["metrics"] = json.loads(d["metrics_json"])
                except Exception:
                    d["metrics"] = {}
            out.append(d)
        return out
    finally:
        conn.close()


def get_replay(replay_id: int) -> Optional[dict]:
    init_replay_tables()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"SELECT * FROM replay_runs WHERE id = {_PARAM}", (replay_id,)
        )
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row) if hasattr(row, "keys") else dict(row)
        for k in ("businesses_json", "metrics_json"):
            if d.get(k):
                try:
                    d[k.replace("_json", "")] = json.loads(d[k])
                except Exception:
                    d[k.replace("_json", "")] = None
        return d
    finally:
        conn.close()
