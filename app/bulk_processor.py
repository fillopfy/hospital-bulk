import asyncio
import logging
import time
import uuid
from typing import Dict, List, Optional, Tuple

from .config import Settings
from .csv_parser import CsvRow
from .hospital_client import HospitalAPIError, HospitalClient, backoff_sleep, with_retry
from .job_store import Job, JobStore, RowState
from .models import BatchStatus, BulkResponse, HospitalResult, ProgressResponse, RowStatus

log = logging.getLogger(__name__)


def _key(name: str, address: str, phone: Optional[str]) -> Tuple[str, str, str]:
    return (name.strip(), address.strip(), (phone or "").strip())


class BulkProcessor:
    def __init__(self, client: HospitalClient, store: JobStore, settings: Settings):
        self.client = client
        self.store = store
        self.s = settings

    # ---------- public API used by routes ----------

    def new_job(self, rows: List[CsvRow]) -> Job:
        job = Job(batch_id=str(uuid.uuid4()), rows=[RowState(r) for r in rows])
        self.store.add(job)
        return job

    def restore_job(self, batch_id: str, rows: List[CsvRow]) -> Job:
        """Rebuild a job after a restart (store lost) from the original CSV.
        reconcile() then discovers which rows already exist upstream."""
        job = Job(batch_id=batch_id, rows=[RowState(r) for r in rows],
                  status=BatchStatus.partially_failed, needs_reconcile=True)
        self.store.add(job)
        return job

    async def run(self, job: Job) -> Job:
        async with job.lock:  # one run per batch at a time
            started = time.perf_counter()
            job.status = BatchStatus.processing

            await self._warm_up()
            await self._reconcile(job)
            job.needs_reconcile = True  # any later run is a resume

            pending = [r for r in job.rows if r.status in (RowStatus.pending, RowStatus.failed)]
            for r in pending:
                r.status, r.error = RowStatus.pending, None

            sem = asyncio.Semaphore(self.s.max_concurrency)
            claim_lock = asyncio.Lock()

            async def worker(row: RowState) -> None:
                async with sem:
                    await self._create_row(job, row, claim_lock)

            # gather never raises here: _create_row records errors on the row
            await asyncio.gather(*(worker(r) for r in pending))

            if job.failed_count == 0:
                await self._verify_and_activate(job)
            else:
                job.status = BatchStatus.partially_failed

            job.processing_time_seconds = round(time.perf_counter() - started, 2)
            log.info("batch %s finished: %s (%d/%d created) in %.2fs", job.batch_id,
                     job.status.value, job.created_count, job.total,
                     job.processing_time_seconds)
            return job

    # ---------- steps ----------

    async def _warm_up(self) -> None:
        """Wake the upstream once with a long timeout, so 20 parallel POSTs
        don't all time out together while Render boots the service."""
        try:
            await self.client.health(timeout=self.s.warmup_timeout_s)
        except HospitalAPIError as exc:
            log.warning("upstream warm-up failed, continuing anyway: %s", exc)

    async def _reconcile(self, job: Job) -> None:
        """Match rows not yet marked created against what already exists
        upstream in this batch. First run: batch is empty, nothing to do."""
        unresolved = [r for r in job.rows
                      if r.status not in (RowStatus.created, RowStatus.created_and_activated)]
        if not unresolved or not job.needs_reconcile:
            return  # brand-new job: nothing can exist upstream yet, skip the GET
        try:
            upstream = await with_retry(lambda: self.client.get_batch(job.batch_id),
                                        attempts=self.s.max_attempts,
                                        backoff_base=self.s.backoff_base_s,
                                        label="get_batch")
        except HospitalAPIError as exc:
            log.warning("reconcile skipped, could not read batch: %s", exc)
            return
        claimed = {r.hospital_id for r in job.rows if r.hospital_id is not None}
        for row in unresolved:
            match = self._find_match(row.source, upstream, claimed)
            if match:
                claimed.add(match["id"])
                row.hospital_id, row.status, row.error = match["id"], RowStatus.created, None

    @staticmethod
    def _find_match(src: CsvRow, upstream: List[dict], claimed: set) -> Optional[dict]:
        want = _key(src.name, src.address, src.phone)
        for h in upstream:
            if h["id"] not in claimed and _key(h["name"], h["address"], h.get("phone")) == want:
                return h
        return None

    async def _create_row(self, job: Job, row: RowState, claim_lock: asyncio.Lock) -> None:
        payload = row.source.to_payload(job.batch_id)
        for attempt in range(1, self.s.max_attempts + 1):
            row.attempts += 1
            try:
                created = await self.client.create_hospital(payload)
                row.hospital_id, row.status, row.error = created["id"], RowStatus.created, None
                return
            except HospitalAPIError as exc:
                row.error = str(exc)
                if not exc.retryable or attempt == self.s.max_attempts:
                    break
                if exc.ambiguous:
                    # POST is NOT idempotent: the timed-out request may have
                    # succeeded. Check before re-sending to avoid a duplicate.
                    if await self._adopt_existing(job, row, claim_lock):
                        return
                await backoff_sleep(attempt, self.s.backoff_base_s)
        row.status = RowStatus.failed

    async def _adopt_existing(self, job: Job, row: RowState, claim_lock: asyncio.Lock) -> bool:
        try:
            upstream = await self.client.get_batch(job.batch_id)
        except HospitalAPIError:
            return False
        async with claim_lock:
            claimed = {r.hospital_id for r in job.rows if r.hospital_id is not None}
            match = self._find_match(row.source, upstream, claimed)
            if match:
                row.hospital_id, row.status, row.error = match["id"], RowStatus.created, None
                return True
        return False

    async def _verify_and_activate(self, job: Job) -> None:
        # Verification: delete strays (duplicates from ambiguous retries) so
        # activation only turns on exactly the rows in the CSV. Best-effort.
        try:
            upstream = await self.client.get_batch(job.batch_id)
            ours = {r.hospital_id for r in job.rows}
            for h in upstream:
                if h["id"] not in ours:
                    log.warning("deleting stray hospital %s in batch %s", h["id"], job.batch_id)
                    await with_retry(lambda hid=h["id"]: self.client.delete_hospital(hid),
                                     attempts=self.s.max_attempts,
                                     backoff_base=self.s.backoff_base_s, label="delete_stray")
        except HospitalAPIError as exc:
            log.warning("pre-activation verification skipped: %s", exc)

        try:
            await with_retry(lambda: self.client.activate_batch(job.batch_id),
                             attempts=self.s.max_attempts,
                             backoff_base=self.s.backoff_base_s, label="activate")
        except HospitalAPIError as exc:
            log.error("activation failed for %s: %s", job.batch_id, exc)
            job.status, job.batch_activated = BatchStatus.activation_failed, False
            return
        for r in job.rows:
            r.status = RowStatus.created_and_activated
        job.status, job.batch_activated = BatchStatus.completed, True


# ---------- response builders ----------

def to_response(job: Job) -> BulkResponse:
    return BulkResponse(
        batch_id=job.batch_id,
        status=job.status,
        total_hospitals=job.total,
        processed_hospitals=job.created_count,
        failed_hospitals=job.failed_count,
        processing_time_seconds=job.processing_time_seconds,
        batch_activated=job.batch_activated,
        resumable=job.status in (BatchStatus.partially_failed, BatchStatus.activation_failed),
        hospitals=[
            HospitalResult(row=r.source.row, hospital_id=r.hospital_id, name=r.source.name,
                           status=r.status, error=r.error, attempts=r.attempts)
            for r in job.rows
        ],
    )


def to_progress(job: Job) -> ProgressResponse:
    pct = round(100 * job.finished_count / job.total, 1) if job.total else 100.0
    return ProgressResponse(
        batch_id=job.batch_id, status=job.status, total_hospitals=job.total,
        completed=job.finished_count, processed_hospitals=job.created_count,
        failed_hospitals=job.failed_count, progress_percent=pct,
        batch_activated=job.batch_activated,
    )
