# Hospital Bulk Processing API

Bulk-creates hospitals from a CSV by calling the
[Hospital Directory API](https://hospital-directory.onrender.com/docs).
It uses the API's batch IDs to make the upload **all-or-nothing**: every row is
created as *inactive* under one batch, and the batch is activated in a single call
only once every row succeeded.

**Live:** `<your-render-url>` · interactive docs at `<your-render-url>/docs`

## Endpoints

| Method | Path | What it does |
|---|---|---|
| `POST` | `/hospitals/bulk` | Upload CSV → validate → create all rows concurrently → activate batch. `?mode=async` returns `202` immediately. |
| `POST` | `/hospitals/bulk/validate` | Validate a CSV without creating anything. |
| `GET` | `/hospitals/bulk/{batch_id}` | Full result of a batch (same shape as the bulk response). |
| `GET` | `/hospitals/bulk/{batch_id}/progress` | Polling endpoint: `completed / total`, `progress_percent`. |
| `POST` | `/hospitals/bulk/{batch_id}/resume` | Retry only what didn't finish (failed rows and/or activation). |
| `GET` | `/` | Health check. |

CSV format: `name,address,phone` (phone optional, header case-insensitive, max 20 rows).

```bash
curl -F file=@samples/hospitals.csv https://<url>/hospitals/bulk
curl -F file=@samples/hospitals.csv "https://<url>/hospitals/bulk?mode=async"
curl https://<url>/hospitals/bulk/<batch_id>/progress
curl -X POST https://<url>/hospitals/bulk/<batch_id>/resume
```

Example response:

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "total_hospitals": 5,
  "processed_hospitals": 5,
  "failed_hospitals": 0,
  "processing_time_seconds": 1.42,
  "batch_activated": true,
  "resumable": false,
  "hospitals": [
    {"row": 1, "hospital_id": 101, "name": "City General Hospital",
     "status": "created_and_activated", "error": null, "attempts": 1}
  ]
}
```

`status` is one of `processing`, `completed`, `partially_failed`, `activation_failed`.
The last two are `resumable: true`.

## Design

```
routes.py            HTTP layer: upload handling, status codes
  └─ bulk_processor  orchestration: warm-up → reconcile → fan-out → verify → activate
       ├─ csv_parser      pure validation, no I/O
       ├─ hospital_client upstream calls + error classification + retry helper
       └─ job_store       in-memory per-row state (enables progress + resume)
```

### Processing flow
1. **Validate the whole CSV first.** A bad row 17 is rejected before row 1 is sent,
   so a bad file never leaves a half-created batch. Errors carry row numbers.
2. **Generate a UUID batch ID** and record a job with one state entry per row.
3. **Warm up upstream** with one `GET /` and a long timeout. The upstream is on
   Render free tier and cold-starts in ~30–60s. Without this, all parallel POSTs
   would time out together.
4. **Fan out** `POST /hospitals/` concurrently with `asyncio.gather`, bounded by a
   semaphore (`MAX_CONCURRENCY`, default 10), over one shared keep-alive
   `httpx.AsyncClient`.
5. **If every row succeeded:** verify the batch upstream (delete any stray
   duplicates), then `PATCH /hospitals/batch/{id}/activate`.
   **Otherwise:** leave the batch inactive and return per-row errors. Nothing
   half-finished is ever visible as active.

### Performance
The upstream is slow per request, so wall time is dominated by network latency.
Sequential = sum of latencies; concurrent ≈ `ceil(N / concurrency)` × latency.
With 20 rows and concurrency 10, that is roughly 2 round-trips instead of 20.
A test asserts this (`test_requests_run_concurrently_but_bounded`). Connection
reuse avoids a TLS handshake per row. The semaphore keeps us polite to a shared
free-tier service.

### Retries — and why POST is special
Failures are classified in `hospital_client.py`:

| Failure | Retry? | Ambiguous? |
|---|---|---|
| 4xx (e.g. 422 bad data) | no: same input, same answer | – |
| 429 / 5xx | yes, exponential backoff + jitter | 5xx: yes |
| connect error | yes | no (never reached server) |
| timeout / reset after sending | yes | **yes** |

*Ambiguous* means the server may have created the hospital even though we got no
answer. `POST` is not idempotent, so a blind retry could create a duplicate.
Before retrying an ambiguous failure, we read the batch and **adopt** a matching
hospital if it already exists. A final pre-activation check deletes any stray
duplicate, so activation turns on exactly the CSV's rows.
Idempotent calls (GET, PATCH activate, DELETE) use a plain retry helper.

### Resume
Each row's status and upstream `hospital_id` live in the job store, so
`/resume` re-sends only rows that are not yet created. If only activation
failed, it just activates. Every resume also **reconciles with upstream**
(`GET /hospitals/batch/{id}`) first. It is correct even if this service crashed
mid-run, as long as the original CSV is re-uploaded to `/resume`, which rebuilds
the job. A per-batch lock rejects concurrent resumes (`409`). Resuming a completed
batch is a no-op.

On failure we **don't delete the batch**. The created rows are inactive and
invisible as live data, and keeping them makes the retry cheaper.
`DELETE /hospitals/batch/{id}` remains available for a full rollback.

### Trade-offs / what I'd change for production
- **In-memory state** is lost on restart and not shared across instances. That is
  why the container runs one worker. Next step: Redis/Postgres behind the same
  `JobStore` interface, and a real queue (Celery/RQ/Arq) for async mode.
  FastAPI `BackgroundTasks` dies with the process.
- **Duplicate matching** uses `(name, address, phone)`. Two identical rows in one
  CSV are indistinguishable upstream. A client-supplied idempotency key on the
  upstream API would be the proper fix.
- Progress is via **polling**. A WebSocket or SSE stream would be a small addition
  on top of the same job state.

## Run

```bash
# local
pip install -r requirements-dev.txt
uvicorn app.main:app --reload          # http://localhost:8000/docs

# docker
docker compose up --build

# tests (26: parser unit tests + API tests against a stateful fake upstream)
pytest -q
```

The tests never hit the real API. `tests/fake_upstream.py` is a stateful fake
plugged in through `httpx.MockTransport`. It simulates 422s, 500s, timeouts before
and *after* a write, failed activation, and service restarts.

Configuration (env vars): `HOSPITAL_API_BASE_URL`, `MAX_CONCURRENCY`, `MAX_ATTEMPTS`,
`BACKOFF_BASE_S`, `REQUEST_TIMEOUT_S`, `WARMUP_TIMEOUT_S`, `MAX_CSV_ROWS`, `MAX_CSV_BYTES`.

## Deploy (Render)
Push to GitHub → Render → **New → Blueprint** → pick the repo (uses `render.yaml`).
Alternatively, **New → Web Service → Docker**.
