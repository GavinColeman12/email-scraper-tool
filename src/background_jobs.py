"""
Fire-and-forget background jobs with DB-persisted progress — ported from
reputation-audit-tool for bulk scrape of 200+ businesses.

Daemon threads survive Streamlit page navigation. Progress writes to
Postgres every item so any page can observe.
"""
import json
import threading
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Re-use storage's Postgres detection
from src.storage import USE_PG, _PARAM, _connect, _cursor, _row_to_dict


_CANCEL_FLAGS: dict = {}
_LOCK = threading.Lock()


SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS background_jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    search_id INTEGER,
    status TEXT DEFAULT 'pending',
    progress INTEGER DEFAULT 0,
    total INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    log_json TEXT,
    current_item TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_bgjobs_status ON background_jobs(status);
CREATE INDEX IF NOT EXISTS idx_bgjobs_search ON background_jobs(search_id);
"""

SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS background_jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    search_id INTEGER,
    status TEXT DEFAULT 'pending',
    progress INTEGER DEFAULT 0,
    total INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    log_json TEXT,
    current_item TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_bgjobs_status ON background_jobs(status);
CREATE INDEX IF NOT EXISTS idx_bgjobs_search ON background_jobs(search_id);
"""


_INIT_DONE = False


def init_db() -> None:
    global _INIT_DONE
    if _INIT_DONE:
        return
    conn = _connect()
    try:
        cur = _cursor(conn)
        if USE_PG:
            cur.execute(SCHEMA_PG)
        else:
            conn.executescript(SCHEMA_SQLITE)
        conn.commit()
        _INIT_DONE = True
    finally:
        conn.close()


def _insert_job(job_id: str, job_type: str, total: int,
                 search_id=None, metadata: dict = None) -> None:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"""INSERT INTO background_jobs
                (id, job_type, search_id, status, progress, total,
                 log_json, metadata_json, started_at)
                VALUES ({_PARAM}, {_PARAM}, {_PARAM}, 'running', 0, {_PARAM},
                        {_PARAM}, {_PARAM}, {_PARAM})""",
            (job_id, job_type, search_id, total,
             json.dumps([]), json.dumps(metadata or {}),
             datetime.utcnow().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _update_progress(job_id: str, progress: int, current_item: str = "",
                      success_inc: int = 0, error_inc: int = 0,
                      log_entry: str = None) -> None:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        if log_entry:
            cur.execute(
                f"SELECT log_json FROM background_jobs WHERE id = {_PARAM}",
                (job_id,),
            )
            existing = cur.fetchone()
            log_list = []
            if existing:
                existing_log = _row_to_dict(existing).get("log_json") or "[]"
                try:
                    log_list = json.loads(existing_log) or []
                except Exception:
                    log_list = []
            log_list.append({"ts": datetime.utcnow().isoformat(), "msg": log_entry})
            log_list = log_list[-100:]
            cur.execute(
                f"""UPDATE background_jobs
                    SET progress = {_PARAM},
                        current_item = {_PARAM},
                        success_count = success_count + {_PARAM},
                        error_count = error_count + {_PARAM},
                        log_json = {_PARAM}
                    WHERE id = {_PARAM}""",
                (progress, current_item, success_inc, error_inc,
                 json.dumps(log_list), job_id),
            )
        else:
            cur.execute(
                f"""UPDATE background_jobs
                    SET progress = {_PARAM},
                        current_item = {_PARAM},
                        success_count = success_count + {_PARAM},
                        error_count = error_count + {_PARAM}
                    WHERE id = {_PARAM}""",
                (progress, current_item, success_inc, error_inc, job_id),
            )
        conn.commit()
    finally:
        conn.close()


def _finish_job(job_id: str, status: str, error_message: str = "") -> None:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"""UPDATE background_jobs
                SET status = {_PARAM},
                    error_message = {_PARAM},
                    finished_at = {_PARAM}
                WHERE id = {_PARAM}""",
            (status, error_message, datetime.utcnow().isoformat(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def start(job_type: str, items: list, worker_fn,
          search_id=None, max_workers: int = 6,
          metadata: dict = None) -> str:
    """
    Kick off a background job and return job_id.
    worker_fn: worker_fn(item, job_id) -> (ok: bool, log_msg: str)
    """
    init_db()
    job_id = str(uuid.uuid4())
    _insert_job(job_id, job_type, len(items),
                 search_id=search_id, metadata=metadata)
    with _LOCK:
        _CANCEL_FLAGS[job_id] = False

    def _runner():
        completed = 0
        # Manual executor — we can't use `with ThreadPoolExecutor(...)` because
        # __exit__ calls shutdown(wait=True), which blocks cancellation.
        # Need shutdown(wait=False, cancel_futures=True) to actually stop
        # pending work the moment the operator clicks 🛑 Cancel.
        ex = ThreadPoolExecutor(max_workers=max_workers)
        try:
            futures = {
                ex.submit(_safe_worker, worker_fn, item, job_id): item
                for item in items
            }
            cancelled_early = False
            for fut in as_completed(futures):
                if is_cancelled(job_id):
                    # Drop pending futures from the queue. Running
                    # futures can't be killed mid-scrape, but they'll
                    # short-circuit via _safe_worker's cancel check
                    # on entry. shutdown(wait=False) returns
                    # immediately so the runner thread doesn't block.
                    ex.shutdown(wait=False, cancel_futures=True)
                    cancelled_early = True
                    break
                ok, log_msg = fut.result()
                completed += 1
                _update_progress(
                    job_id, completed,
                    current_item=str(log_msg or "")[:200],
                    success_inc=1 if ok else 0,
                    error_inc=0 if ok else 1,
                    log_entry=log_msg,
                )
            if not cancelled_early:
                ex.shutdown(wait=True)
            _finish_job(job_id, "cancelled" if is_cancelled(job_id) else "done")
        except Exception as e:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            _finish_job(job_id, "failed",
                        error_message=f"{type(e).__name__}: {e}\n{traceback.format_exc()[:1000]}")
        finally:
            with _LOCK:
                _CANCEL_FLAGS.pop(job_id, None)

    t = threading.Thread(target=_runner, name=f"bgjob-{job_type}-{job_id[:8]}",
                          daemon=True)
    t.start()
    return job_id


def _safe_worker(worker_fn, item, job_id):
    # Early-exit if the job was cancelled BEFORE this future started
    # running. Without this check, every queued worker would still
    # invoke the expensive scrape function even after a cancel —
    # making the cancel button feel broken from the operator's POV.
    if is_cancelled(job_id):
        return True, "(cancelled before start)"
    try:
        result = worker_fn(item, job_id)
        if result is None:
            return True, ""
        if isinstance(result, bool):
            return result, ""
        if isinstance(result, tuple) and len(result) == 2:
            return bool(result[0]), str(result[1] or "")
        return True, str(result)
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def get(job_id: str) -> dict:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(f"SELECT * FROM background_jobs WHERE id = {_PARAM}", (job_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    d = _row_to_dict(row)
    if d and d.get("log_json"):
        try:
            d["log"] = json.loads(d["log_json"])
        except Exception:
            d["log"] = []
    return d


def list_active() -> list:
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute("SELECT * FROM background_jobs WHERE status IN ('running','pending') "
                     "ORDER BY created_at DESC")
        rows = cur.fetchall()
    finally:
        conn.close()
    return [_row_to_dict(r) for r in rows]


def cancel(job_id: str) -> None:
    with _LOCK:
        _CANCEL_FLAGS[job_id] = True
    init_db()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"UPDATE background_jobs SET status = 'cancelling' "
            f"WHERE id = {_PARAM} AND status = 'running'",
            (job_id,),
        )
        conn.commit()
    finally:
        conn.close()


def is_cancelled(job_id: str) -> bool:
    with _LOCK:
        if _CANCEL_FLAGS.get(job_id):
            return True
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(f"SELECT status FROM background_jobs WHERE id = {_PARAM}", (job_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    if row and _row_to_dict(row).get("status") == "cancelling":
        with _LOCK:
            _CANCEL_FLAGS[job_id] = True
        return True
    return False


def cleanup_stale(max_age_hours: int = 24) -> int:
    init_db()
    from datetime import timedelta
    cutoff = (datetime.utcnow() - timedelta(hours=max_age_hours)).isoformat()
    conn = _connect()
    try:
        cur = _cursor(conn)
        cur.execute(
            f"""UPDATE background_jobs
                SET status = 'failed',
                    error_message = 'Orphaned — process likely restarted',
                    finished_at = {_PARAM}
                WHERE status IN ('running','pending') AND started_at < {_PARAM}""",
            (datetime.utcnow().isoformat(), cutoff),
        )
        n = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return n


def render_active_banner(st_mod) -> None:
    active = list_active()
    if not active:
        return
    for j in active:
        pct = int(100 * (j.get("progress", 0) or 0) /
                   max(1, j.get("total", 0) or 1))
        progress_str = f"{j.get('progress', 0)} / {j.get('total', 0)}"
        st_mod.info(
            f"🟢 **{j.get('job_type', 'job').replace('_', ' ').title()}** running — "
            f"**{progress_str}** ({pct}%)  ·  *{(j.get('current_item') or '')[:120]}*"
        )
        st_mod.progress(pct / 100)
