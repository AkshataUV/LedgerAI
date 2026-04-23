"""
services/task_queue.py
──────────────────────
Background task queue for non-blocking document processing.
Prevents UI from freezing during long AI operations.

Work-Mode Keep-Alive
────────────────────
Render free-tier services spin down after 15 minutes of inactivity.
For tasks that can run > 15 min (e.g., multi-page PDF extraction),
the frontend heartbeat alone is insufficient — if the user goes AFK the
frontend stops pinging and the parser would be shut down mid-task.

Solution: while a task is RUNNING, start a background thread that fires
an HTTP GET to the parser's own /ping URL every KEEPALIVE_INTERVAL seconds.
It also pings the Node.js backend so *both* Render services stay awake.
The interval is cleared the moment the task finishes (success or failure).

Required environment variables (set in parser_backend/.env and Render):
  SELF_URL          — public URL of this parser_backend on Render,
                      e.g. https://ledgerai-parser.onrender.com
  NODE_BACKEND_URL  — public URL of the Node.js backend,
                      e.g. https://ledgerai-backend.onrender.com
"""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional
from urllib.request import urlopen
from urllib.error import URLError

logger = logging.getLogger("ledgerai.task_queue")

# ── Keep-alive config ─────────────────────────────────────────────────────────
KEEPALIVE_INTERVAL = 10 * 60  # 10 minutes (well inside Render's 15-min timer)

# Public URLs — read from env.  Both may be empty on localhost; that's fine,
# the keep-alive is silently skipped when SELF_URL is not set.
_SELF_URL         = (os.environ.get("SELF_URL") or "").rstrip("/")
_NODE_BACKEND_URL = (os.environ.get("NODE_BACKEND_URL") or "").rstrip("/")


def _do_keepalive_ping(name: str, url: str) -> None:
    """Fire a single GET /ping and log the result.  Never raises."""
    try:
        with urlopen(f"{url}/ping", timeout=10) as resp:
            logger.info("[keep-alive] ✓ %s responded %s", name, resp.status)
    except URLError as exc:
        logger.warning("[keep-alive] ✗ %s unreachable: %s", name, exc.reason)
    except Exception as exc:
        logger.warning("[keep-alive] ✗ %s error: %s", name, exc)


def _start_keepalive(document_id: int) -> Optional[threading.Timer]:
    """
    Return a recurring timer that pings SELF and backend every KEEPALIVE_INTERVAL
    seconds.  Returns None if SELF_URL is not configured (e.g., local dev).
    """
    if not _SELF_URL:
        logger.debug("[keep-alive] SELF_URL not set — skipping work-mode keep-alive.")
        return None

    def _tick():
        # 1. Ping ourselves — Render only resets its timer on *incoming* traffic.
        _do_keepalive_ping("parser_backend (self)", _SELF_URL)

        # 2. Ping the Node.js backend to keep it awake too.
        if _NODE_BACKEND_URL:
            _do_keepalive_ping("backend", _NODE_BACKEND_URL)

    # Use a repeating timer via a self-rescheduling wrapper
    _timer_holder: Dict[str, Optional[threading.Timer]] = {"timer": None}

    def _schedule():
        _tick()
        # Reschedule unless the task is already done (holder cleared externally)
        if _timer_holder["timer"] is not None:
            t = threading.Timer(KEEPALIVE_INTERVAL, _schedule)
            t.daemon = True
            _timer_holder["timer"] = t
            t.start()

    # First fire after KEEPALIVE_INTERVAL (not immediately — task just started)
    first = threading.Timer(KEEPALIVE_INTERVAL, _schedule)
    first.daemon = True
    _timer_holder["timer"] = first
    first.start()

    logger.info(
        "[keep-alive] Started for document_id=%s (interval=%ds, self=%s, backend=%s)",
        document_id, KEEPALIVE_INTERVAL, _SELF_URL, _NODE_BACKEND_URL or "n/a",
    )

    # Return the holder so _stop_keepalive can cancel the pending timer.
    return _timer_holder


def _stop_keepalive(holder: Optional[Dict]) -> None:
    """Cancel the pending keep-alive timer.  Safe to call even if holder is None."""
    if holder is None:
        return
    timer = holder.get("timer")
    if timer is not None:
        timer.cancel()
        holder["timer"] = None  # Prevent _schedule from rescheduling
    logger.info("[keep-alive] Stopped.")


# ── Task infrastructure ───────────────────────────────────────────────────────

class TaskStatus(Enum):
    QUEUED  = "QUEUED"
    RUNNING = "RUNNING"
    DONE    = "DONE"
    FAILED  = "FAILED"


@dataclass
class TaskRecord:
    document_id: int
    status: TaskStatus = TaskStatus.QUEUED
    error: Optional[str] = None


_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ledgerai-worker")
_tasks: Dict[int, TaskRecord] = {}


def _run_task(document_id: int):
    from services.processing_engine import process_document

    record = _tasks[document_id]
    record.status = TaskStatus.RUNNING

    # Start work-mode keep-alive — prevents Render from sleeping mid-task.
    _keepalive_holder = _start_keepalive(document_id)

    try:
        process_document(document_id)
        record.status = TaskStatus.DONE
        logger.info("Task done: document_id=%s", document_id)
    except Exception as exc:
        record.status = TaskStatus.FAILED
        record.error = str(exc)
        logger.error("Task failed: document_id=%s  error=%s", document_id, exc)
    finally:
        # Always clear the keep-alive timer the instant the task finishes.
        _stop_keepalive(_keepalive_holder)


def submit_document(document_id: int):
    record = TaskRecord(document_id=document_id)
    _tasks[document_id] = record
    _executor.submit(_run_task, document_id)
    logger.info("Queued document_id=%s for processing.", document_id)


def get_task_status(document_id: int) -> Optional[TaskRecord]:
    return _tasks.get(document_id)
