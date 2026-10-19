"""A stateful fake of the Hospital Directory API, plugged in via
httpx.MockTransport. Lets tests simulate failures precisely: a row that
always 422s, a row that times out once, a timeout AFTER the write
succeeded (the dangerous duplicate case), a failing activate, etc."""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Set

import httpx


class FakeUpstream:
    def __init__(self, latency: float = 0.0):
        self.hospitals: Dict[int, dict] = {}
        self._next_id = 1
        self.latency = latency
        # failure knobs, keyed by hospital name
        self.always_fail_422: Set[str] = set()
        self.fail_500_times: Dict[str, int] = {}
        self.timeout_before_write_times: Dict[str, int] = {}
        self.timeout_after_write_times: Dict[str, int] = {}  # write happens, client sees timeout
        self.activate_fail_times = 0
        # observability for assertions
        self.post_count = 0
        self.max_in_flight = 0
        self._in_flight = 0
        self.activated_batches: List[str] = []

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def batch(self, batch_id: str) -> List[dict]:
        return [h for h in self.hospitals.values() if h["creation_batch_id"] == batch_id]

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if method == "GET" and path == "/":
            return httpx.Response(200, json={"status": "ok"})

        if method == "POST" and path == "/hospitals/":
            self.post_count += 1
            self._in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self._in_flight)
            try:
                if self.latency:
                    await asyncio.sleep(self.latency)
                body = json.loads(request.content)
                name = body["name"]
                if name in self.always_fail_422:
                    return httpx.Response(422, json={"detail": "invalid"})
                if self.fail_500_times.get(name, 0) > 0:
                    self.fail_500_times[name] -= 1
                    return httpx.Response(500, json={"detail": "boom"})
                if self.timeout_before_write_times.get(name, 0) > 0:
                    self.timeout_before_write_times[name] -= 1
                    raise httpx.ReadTimeout("timed out", request=request)
                h = self._create(body)
                if self.timeout_after_write_times.get(name, 0) > 0:
                    self.timeout_after_write_times[name] -= 1
                    raise httpx.ReadTimeout("timed out after write", request=request)
                return httpx.Response(200, json=h)
            finally:
                self._in_flight -= 1

        parts = path.strip("/").split("/")
        if parts[:2] == ["hospitals", "batch"]:
            batch_id = parts[2]
            if method == "GET":
                return httpx.Response(200, json=self.batch(batch_id))
            if method == "PATCH" and parts[-1] == "activate":
                if self.activate_fail_times > 0:
                    self.activate_fail_times -= 1
                    return httpx.Response(503, json={"detail": "unavailable"})
                for h in self.batch(batch_id):
                    h["active"] = True
                self.activated_batches.append(batch_id)
                return httpx.Response(200, json={"activated": len(self.batch(batch_id))})
        if method == "DELETE" and len(parts) == 2 and parts[0] == "hospitals":
            self.hospitals.pop(int(parts[1]), None)
            return httpx.Response(204)
        return httpx.Response(404, json={"detail": "not found"})

    def _create(self, body: dict) -> dict:
        h = {
            "id": self._next_id,
            "name": body["name"],
            "address": body["address"],
            "phone": body.get("phone"),
            "creation_batch_id": body.get("creation_batch_id"),
            "active": body.get("creation_batch_id") is None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.hospitals[h["id"]] = h
        self._next_id += 1
        return h
