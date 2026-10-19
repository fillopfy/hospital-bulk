import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from .bulk_processor import BulkProcessor
from .config import Settings, settings as default_settings
from .hospital_client import HospitalClient
from .job_store import JobStore
from .routes import router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def create_app(settings: Settings = default_settings,
               transport: httpx.AsyncBaseTransport = None) -> FastAPI:
    """`transport` lets tests plug in a fake upstream (httpx.MockTransport)."""
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # ONE shared client for the app's lifetime: reuses TCP/TLS connections
        # instead of a new handshake per hospital (big win for latency).
        limits = httpx.Limits(max_connections=settings.max_concurrency * 2,
                              max_keepalive_connections=settings.max_concurrency)
        async with httpx.AsyncClient(base_url=settings.hospital_api_base_url,
                                     timeout=settings.request_timeout_s,
                                     limits=limits, transport=transport) as http:
            app.state.processor = BulkProcessor(HospitalClient(http),
                                                JobStore(settings.max_jobs_in_memory), settings)
            yield

    app = FastAPI(
        title="Hospital Bulk Processing API",
        version="1.0.0",
        description="Bulk-creates hospitals from a CSV via the Hospital Directory API, "
                    "using batch IDs for all-or-nothing activation.",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.get("/", tags=["meta"])
    async def health():
        return {"status": "ok", "docs": "/docs", "upstream": settings.hospital_api_base_url}

    return app


app = create_app()
