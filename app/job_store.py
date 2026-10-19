
import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import List, Optional

from .csv_parser import CsvRow
from .models import BatchStatus, RowStatus


@dataclass
class RowState:
    source: CsvRow
    status: RowStatus = RowStatus.pending
    hospital_id: Optional[int] = None
    error: Optional[str] = None
    attempts: int = 0


@dataclass
class Job:
    batch_id: str
    rows: List[RowState]
    status: BatchStatus = BatchStatus.processing
    batch_activated: bool = False
    processing_time_seconds: float = 0.0
    # False only for a brand-new job; True once it has run or was restored
    needs_reconcile: bool = False
    created_at: float = field(default_factory=time.time)
    # one run at a time per batch: stops two resume calls racing each other
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def total(self) -> int:
        return len(self.rows)

    @property
    def created_count(self) -> int:
        return sum(r.status in (RowStatus.created, RowStatus.created_and_activated)
                   for r in self.rows)

    @property
    def failed_count(self) -> int:
        return sum(r.status == RowStatus.failed for r in self.rows)

    @property
    def finished_count(self) -> int:
        return sum(r.status != RowStatus.pending for r in self.rows)


class JobStore:
    def __init__(self, max_jobs: int = 1000):
        self._jobs: "OrderedDict[str, Job]" = OrderedDict()
        self._max = max_jobs

    def add(self, job: Job) -> None:
        self._jobs[job.batch_id] = job
        while len(self._jobs) > self._max:
            self._jobs.popitem(last=False)  # evict oldest

    def get(self, batch_id: str) -> Optional[Job]:
        return self._jobs.get(batch_id)
