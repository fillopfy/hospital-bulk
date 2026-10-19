"""Pydantic response schemas. They double as the OpenAPI docs at /docs."""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class RowStatus(str, Enum):
    pending = "pending"
    created = "created"  # exists upstream, batch not yet activated
    created_and_activated = "created_and_activated"
    failed = "failed"


class BatchStatus(str, Enum):
    processing = "processing"
    completed = "completed"  # all rows created and batch activated
    partially_failed = "partially_failed"  # some rows failed; resumable
    activation_failed = "activation_failed"  # rows created, activate call failed; resumable


class HospitalResult(BaseModel):
    row: int
    hospital_id: Optional[int] = None
    name: str
    status: RowStatus
    error: Optional[str] = None
    attempts: int = 0


class BulkResponse(BaseModel):
    batch_id: str
    status: BatchStatus
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    resumable: bool
    hospitals: List[HospitalResult]


class ProgressResponse(BaseModel):
    batch_id: str
    status: BatchStatus
    total_hospitals: int
    completed: int  # rows that have finished (created or failed)
    processed_hospitals: int
    failed_hospitals: int
    progress_percent: float
    batch_activated: bool


class ValidationIssue(BaseModel):
    row: Optional[int] = None
    message: str


class ValidationResponse(BaseModel):
    valid: bool
    total_hospitals: int
    errors: List[ValidationIssue]
    warnings: List[str]
