"""
services/task_queue.py
──────────────────────
Background task queue for non-blocking document processing.
Prevents UI from freezing during long AI operations.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional

logger = logging.getLogger("ledgerai.task_queue")


class TaskStatus(Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


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

    try:
        process_document(document_id)
        record.status = TaskStatus.DONE
        logger.info("Task done: document_id=%s", document_id)
    except Exception as exc:
        record.status = TaskStatus.FAILED
        record.error = str(exc)
        logger.error("Task failed: document_id=%s  error=%s", document_id, exc)


def submit_document(document_id: int):
    record = TaskRecord(document_id=document_id)
    _tasks[document_id] = record
    _executor.submit(_run_task, document_id)
    logger.info("Queued document_id=%s for processing.", document_id)


def get_task_status(document_id: int) -> Optional[TaskRecord]:
    return _tasks.get(document_id)
