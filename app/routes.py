from typing import Literal, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from .bulk_processor import BulkProcessor, to_progress, to_response
from .csv_parser import ParseResult, parse_csv
from .models import (BatchStatus, BulkResponse, ProgressResponse, ValidationIssue,
                     ValidationResponse)

router = APIRouter(prefix="/hospitals/bulk", tags=["bulk"])

Mode = Literal["sync", "async"]
MODE_HELP = ("sync: wait and return the full result (assignment default). "
             "async: return 202 immediately, then poll GET /hospitals/bulk/{batch_id}/progress")


def _processor(request: Request) -> BulkProcessor:
    return request.app.state.processor


async def _read_and_parse(request: Request, file: UploadFile) -> ParseResult:
    s = _processor(request).s
    if file.filename and not file.filename.lower().endswith(".csv"):
        raise HTTPException(415, "Upload must be a .csv file")
    content = await file.read(s.max_csv_bytes + 1)  # never read more than the cap
    return parse_csv(content, max_rows=s.max_csv_rows, max_bytes=s.max_csv_bytes)


def _validation_body(parsed: ParseResult) -> ValidationResponse:
    total = len(parsed.rows) + sum(1 for e in parsed.errors if e.row is not None)
    return ValidationResponse(
        valid=parsed.ok,
        total_hospitals=total,
        errors=[ValidationIssue(row=e.row, message=e.message) for e in parsed.errors],
        warnings=parsed.warnings,
    )


@router.post("", response_model=BulkResponse,
             responses={202: {"model": ProgressResponse}, 422: {"model": ValidationResponse}})
async def bulk_create(request: Request, background: BackgroundTasks,
                      file: UploadFile = File(..., description="CSV: name,address,phone"),
                      mode: Mode = Query("sync", description=MODE_HELP)):
    """Validate the CSV, create every hospital under a new batch, activate the batch."""
    parsed = await _read_and_parse(request, file)
    if not parsed.ok:
        return JSONResponse(status_code=422, content=_validation_body(parsed).model_dump())

    proc = _processor(request)
    job = proc.new_job(parsed.rows)
    if mode == "async":
        background.add_task(proc.run, job)
        return JSONResponse(status_code=202, content=to_progress(job).model_dump(mode="json"))
    await proc.run(job)
    return to_response(job)


@router.post("/validate", response_model=ValidationResponse)
async def validate_csv(request: Request, file: UploadFile = File(...)):
    """Check a CSV without creating anything upstream."""
    return _validation_body(await _read_and_parse(request, file))


@router.get("/{batch_id}", response_model=BulkResponse)
async def get_result(request: Request, batch_id: UUID):
    job = _processor(request).store.get(str(batch_id))
    if not job:
        raise HTTPException(404, "Unknown batch_id")
    return to_response(job)


@router.get("/{batch_id}/progress", response_model=ProgressResponse)
async def get_progress(request: Request, batch_id: UUID):
    job = _processor(request).store.get(str(batch_id))
    if not job:
        raise HTTPException(404, "Unknown batch_id")
    return to_progress(job)


@router.post("/{batch_id}/resume", response_model=BulkResponse,
             responses={202: {"model": ProgressResponse}})
async def resume(request: Request, background: BackgroundTasks, batch_id: UUID,
                 file: Optional[UploadFile] = File(
                     None, description="Only needed if this service restarted and lost the job: "
                                       "re-upload the original CSV"),
                 mode: Mode = Query("sync", description=MODE_HELP)):
    """Retry only what didn't finish: failed rows, and/or the activation step.
    Rows already created upstream are never re-sent."""
    proc = _processor(request)
    job = proc.store.get(str(batch_id))

    if job is None:
        if file is None:
            raise HTTPException(404, "Unknown batch_id. If the service restarted, re-upload "
                                     "the original CSV in this request to rebuild the job.")
        parsed = await _read_and_parse(request, file)
        if not parsed.ok:
            return JSONResponse(status_code=422, content=_validation_body(parsed).model_dump())
        job = proc.restore_job(str(batch_id), parsed.rows)

    if job.status == BatchStatus.completed:
        return to_response(job)  # idempotent: nothing left to do
    if job.lock.locked():
        raise HTTPException(409, "This batch is already being processed")

    if mode == "async":
        background.add_task(proc.run, job)
        return JSONResponse(status_code=202, content=to_progress(job).model_dump(mode="json"))
    await proc.run(job)
    return to_response(job)
