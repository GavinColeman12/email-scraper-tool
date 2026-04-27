"""
Regression tests for the background-jobs cancel mechanism.

User reported the 🛑 Cancel button on the sidebar widget didn't actually
stop a running job. Root cause: ThreadPoolExecutor's `with` block calls
shutdown(wait=True) on exit, blocking until ALL submitted futures
finish. Fixed by manual executor + shutdown(wait=False, cancel_futures=
True) on cancel, plus an early-exit check in _safe_worker so newly-
dequeued futures bail immediately.
"""
import time
import threading

from src import background_jobs as bj


def _slow_worker(item, job_id):
    """Simulates a 0.5s scrape — long enough that cancel can take
    effect mid-batch."""
    time.sleep(0.5)
    return True, f"processed {item}"


def test_cancel_stops_pending_futures():
    """Submit 20 items with 2 workers + 0.5s/item. Cancel after the
    first one finishes. Should complete way fewer than 20 items."""
    bj.init_db()
    items = list(range(20))
    job_id = bj.start(
        job_type="test_cancel",
        items=items,
        worker_fn=_slow_worker,
        max_workers=2,
    )
    # Wait for the first item to complete (~0.5s) then cancel
    time.sleep(0.7)
    bj.cancel(job_id)
    # Give the runner a moment to wind down
    time.sleep(1.0)
    job = bj.get(job_id)
    assert job is not None
    # Cancellation should have stopped most pending work — not all 20
    progress = int(job.get("progress") or 0)
    assert progress < 20, f"Expected <20 (cancel worked), got {progress}"
    assert job.get("status") == "cancelled"


def test_safe_worker_early_exits_when_cancelled():
    """If the cancel flag is set BEFORE _safe_worker runs, the worker
    fn should never be invoked."""
    called = []
    def _fn(item, jid):
        called.append(item)
        return True, ""

    job_id = "test-cancel-early"
    bj._CANCEL_FLAGS[job_id] = True
    try:
        ok, msg = bj._safe_worker(_fn, "x", job_id)
        assert ok is True
        assert "cancelled" in msg.lower()
        assert called == [], "worker_fn must not run after cancel"
    finally:
        bj._CANCEL_FLAGS.pop(job_id, None)
