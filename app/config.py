import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


@dataclass(frozen=True)
class Settings:
    # The given Hospital Directory API
    hospital_api_base_url: str = os.getenv(
        "HOSPITAL_API_BASE_URL", "https://hospital-directory.onrender.com"
    )

    # CSV limits (assignment: max 20 hospitals)
    max_csv_rows: int = _int("MAX_CSV_ROWS", 20)
    max_csv_bytes: int = _int("MAX_CSV_BYTES", 1_000_000)

    # Fan-out: how many POSTs may be in flight at once
    max_concurrency: int = _int("MAX_CONCURRENCY", 10)

    # Per-request timeout, and a longer one for the first "wake-up" call,
    # because the upstream runs on Render free tier and can cold-start.
    request_timeout_s: float = _float("REQUEST_TIMEOUT_S", 30.0)
    warmup_timeout_s: float = _float("WARMUP_TIMEOUT_S", 90.0)

    # Retries for transient failures (timeouts, 5xx, 429)
    max_attempts: int = _int("MAX_ATTEMPTS", 3)
    backoff_base_s: float = _float("BACKOFF_BASE_S", 0.5)

    # In-memory job store cap (oldest jobs evicted first)
    max_jobs_in_memory: int = _int("MAX_JOBS_IN_MEMORY", 1000)


settings = Settings()
