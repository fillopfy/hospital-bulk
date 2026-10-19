"""End-to-end tests of our API against the stateful fake upstream."""

import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from tests.fake_upstream import FakeUpstream

FAST = Settings(hospital_api_base_url="http://upstream", max_concurrency=5,
                max_attempts=3, backoff_base_s=0.01, request_timeout_s=5)


def make_csv(n: int, prefix: str = "H") -> bytes:
    return ("name,address,phone\n" +
            "".join(f"{prefix}{i},{i} Main St,555-{i:04d}\n" for i in range(1, n + 1))).encode()


def upload(client, content: bytes, url="/hospitals/bulk", **params):
    return client.post(url, files={"file": ("h.csv", content, "text/csv")}, params=params)


@pytest.fixture
def upstream():
    return FakeUpstream()


@pytest.fixture
def client(upstream):
    with TestClient(create_app(FAST, upstream.transport())) as c:
        yield c


# ---------- happy path & performance ----------

def test_bulk_create_all_succeed_and_activate(client, upstream):
    r = upload(client, make_csv(20))
    assert r.status_code == 200
    body = r.json()
    assert body["total_hospitals"] == body["processed_hospitals"] == 20
    assert body["failed_hospitals"] == 0
    assert body["batch_activated"] is True and body["status"] == "completed"
    assert {h["status"] for h in body["hospitals"]} == {"created_and_activated"}
    assert [h["row"] for h in body["hospitals"]] == list(range(1, 21))
    upstream_rows = upstream.batch(body["batch_id"])
    assert len(upstream_rows) == 20 and all(h["active"] for h in upstream_rows)


def test_requests_run_concurrently_but_bounded():
    up = FakeUpstream(latency=0.05)
    with TestClient(create_app(FAST, up.transport())) as c:
        t0 = time.perf_counter()
        assert upload(c, make_csv(20)).json()["batch_activated"]
        elapsed = time.perf_counter() - t0
    # sequential would be 20 * 0.05 = 1.0s; with concurrency 5 it's ~0.2s
    assert elapsed < 0.6
    assert 1 < up.max_in_flight <= FAST.max_concurrency


# ---------- validation ----------

def test_invalid_csv_rejected_before_any_upstream_call(client, upstream):
    r = upload(client, b"name,address\nA,1 St\n,missing name\n")
    assert r.status_code == 422
    assert r.json()["errors"][0]["row"] == 2
    assert upstream.post_count == 0


def test_too_many_rows(client, upstream):
    assert upload(client, make_csv(21)).status_code == 422
    assert upstream.post_count == 0


def test_non_csv_file_rejected(client):
    r = client.post("/hospitals/bulk", files={"file": ("h.txt", b"x", "text/plain")})
    assert r.status_code == 415


def test_validate_endpoint_creates_nothing(client, upstream):
    ok = upload(client, make_csv(3), url="/hospitals/bulk/validate").json()
    bad = upload(client, b"name\nA\n", url="/hospitals/bulk/validate").json()
    assert ok["valid"] and ok["total_hospitals"] == 3
    assert not bad["valid"]
    assert upstream.post_count == 0


# ---------- retries ----------

def test_transient_500_is_retried(client, upstream):
    upstream.fail_500_times["H2"] = 1
    body = upload(client, make_csv(3)).json()
    assert body["batch_activated"]
    assert body["hospitals"][1]["attempts"] == 2


def test_timeout_after_write_does_not_create_duplicate(client, upstream):
    # The POST for H2 succeeds upstream but our client sees a timeout.
    upstream.timeout_after_write_times["H2"] = 1
    body = upload(client, make_csv(3)).json()
    assert body["batch_activated"]
    assert len(upstream.batch(body["batch_id"])) == 3  # not 4
    assert upstream.post_count == 3  # H2 was adopted, not re-posted


def test_validation_error_from_upstream_is_not_retried(client, upstream):
    upstream.always_fail_422.add("H2")
    body = upload(client, make_csv(3)).json()
    assert body["hospitals"][1]["status"] == "failed"
    assert body["hospitals"][1]["attempts"] == 1


# ---------- partial failure & resume ----------

def test_partial_failure_leaves_batch_inactive_then_resume_finishes_it(client, upstream):
    upstream.fail_500_times["H3"] = 3  # exhausts all 3 attempts on first run
    first = upload(client, make_csv(5)).json()
    batch = first["batch_id"]
    assert first["status"] == "partially_failed" and first["resumable"]
    assert first["batch_activated"] is False
    assert first["processed_hospitals"] == 4 and first["failed_hospitals"] == 1
    assert not any(h["active"] for h in upstream.batch(batch))

    posts_before = upstream.post_count
    second = client.post(f"/hospitals/bulk/{batch}/resume").json()
    assert second["status"] == "completed" and second["batch_activated"]
    assert upstream.post_count - posts_before == 1  # only the failed row re-sent
    assert len(upstream.batch(batch)) == 5
    assert all(h["active"] for h in upstream.batch(batch))
    # hospital ids for rows that succeeded the first time are unchanged
    assert [h["hospital_id"] for h in first["hospitals"] if h["hospital_id"]] == \
           [h["hospital_id"] for h in second["hospitals"] if h["row"] != 3]


def test_activation_failure_resume_only_activates(client, upstream):
    upstream.activate_fail_times = 3
    first = upload(client, make_csv(4)).json()
    assert first["status"] == "activation_failed" and first["resumable"]
    assert first["processed_hospitals"] == 4

    posts_before = upstream.post_count
    second = client.post(f"/hospitals/bulk/{first['batch_id']}/resume").json()
    assert second["batch_activated"]
    assert upstream.post_count == posts_before


def test_resume_after_service_restart_using_original_csv(upstream):
    csv = make_csv(5)
    upstream.fail_500_times["H4"] = 3
    with TestClient(create_app(FAST, upstream.transport())) as c1:
        batch = upload(c1, csv).json()["batch_id"]

    # new process: the in-memory job is gone
    with TestClient(create_app(FAST, upstream.transport())) as c2:
        assert c2.post(f"/hospitals/bulk/{batch}/resume").status_code == 404
        posts_before = upstream.post_count
        body = upload(c2, csv, url=f"/hospitals/bulk/{batch}/resume").json()
    assert body["batch_activated"] and body["processed_hospitals"] == 5
    assert upstream.post_count - posts_before == 1  # reconciled 4 rows from upstream
    assert len(upstream.batch(batch)) == 5


def test_resume_completed_batch_is_noop(client, upstream):
    batch = upload(client, make_csv(2)).json()["batch_id"]
    posts = upstream.post_count
    r = client.post(f"/hospitals/bulk/{batch}/resume")
    assert r.status_code == 200 and r.json()["status"] == "completed"
    assert upstream.post_count == posts


# ---------- async mode & progress ----------

def test_async_mode_returns_202_then_poll(client):
    r = upload(client, make_csv(4), mode="async")
    assert r.status_code == 202
    batch = r.json()["batch_id"]
    # TestClient runs background tasks before returning, so it's done now
    prog = client.get(f"/hospitals/bulk/{batch}/progress").json()
    assert prog["progress_percent"] == 100.0 and prog["batch_activated"]
    assert client.get(f"/hospitals/bulk/{batch}").json()["status"] == "completed"


def test_unknown_and_malformed_batch_ids(client):
    assert client.get("/hospitals/bulk/00000000-0000-0000-0000-000000000000").status_code == 404
    assert client.get("/hospitals/bulk/not-a-uuid").status_code == 422
