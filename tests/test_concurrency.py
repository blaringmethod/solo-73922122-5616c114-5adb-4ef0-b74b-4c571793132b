"""End-to-end concurrency checks against a real uvicorn server + SQLite file."""

from __future__ import annotations

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import planner as planner_module  # noqa: E402
from app.server_e2e import run_server  # noqa: E402

DAY = "2026-11-21"
OTHER_DAY = "2026-11-22"

# Shared gate used by the blocking-verification fixture.  The uvicorn server
# runs in its own thread, so the patch has to be installed before that thread
# starts and communicate through module-level state.
VERIFY_GATE = {"entered": threading.Event(), "release": threading.Event(), "hits": 0}
_real_verify = planner_module.build_source_ranges


def _gated_verify(stations):
    if len(stations) > 100:
        VERIFY_GATE["hits"] += 1
        VERIFY_GATE["entered"].set()
        if not VERIFY_GATE["release"].wait(timeout=30):
            raise AssertionError("verification gate was never released")
    return _real_verify(stations)


@pytest.fixture()
def gated_server(tmp_path):
    """Real server whose big-manifest verification blocks on VERIFY_GATE."""
    VERIFY_GATE["entered"].clear()
    VERIFY_GATE["release"].clear()
    VERIFY_GATE["hits"] = 0
    original = planner_module.build_source_ranges
    planner_module.build_source_ranges = _gated_verify
    try:
        with run_server(str(tmp_path / "gated.db")) as base_url:
            yield base_url
    finally:
        planner_module.build_source_ranges = original
        VERIFY_GATE["release"].set()


def stations_spec():
    return [
        {"station_id": sid, "estimated_bytes": 40,
         "current_range": ({"start_station": 1, "end_station": 2} if sid <= 2 else None)}
        for sid in range(1, 7)
    ]


@pytest.fixture()
def server(tmp_path):
    with run_server(str(tmp_path / "e2e.db")) as base_url:
        yield base_url


def plan_and_confirm(base_url, plan_id):
    with httpx.Client(base_url=base_url, timeout=30) as client:
        client.post(f"/days/{DAY}/plans/{plan_id}/confirm", json={"operator": "load"})
        return client.post(
            f"/days/{DAY}/plans/{plan_id}/publish", json={"operator": "load"}
        )


def test_concurrent_manifest_registration_allocates_unique_versions(server):
    def register(_):
        with httpx.Client(base_url=server, timeout=30) as client:
            return client.post(
                f"/days/{DAY}/source-manifests", json={"stations": stations_spec()}
            )

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(register, range(12)))

    assert all(r.status_code == 201 for r in responses), [
        (r.status_code, r.text) for r in responses if r.status_code != 201
    ]
    versions = sorted(r.json()["manifest"]["version"] for r in responses)
    assert versions == list(range(1, 13)), versions

    with httpx.Client(base_url=server) as client:
        listing = client.get(f"/days/{DAY}/source-manifests").json()
        assert listing["current_version"] == 12
        assert len(listing["manifests"]) == 12


def test_concurrent_publication_yields_single_receipt(server):
    with httpx.Client(base_url=server, timeout=30) as client:
        client.post(f"/days/{DAY}/source-manifests", json={"stations": stations_spec()})
        plan = client.post(
            f"/days/{DAY}/plans",
            json={"partition_count": 2, "max_partition_bytes": 1000},
        ).json()
    plan_id = plan["plan_id"]
    with httpx.Client(base_url=server, timeout=30) as client:
        assert client.post(
            f"/days/{DAY}/plans/{plan_id}/confirm", json={"operator": "load"}
        ).status_code == 200

    barrier = threading.Barrier(8)

    def publish(_):
        with httpx.Client(base_url=server, timeout=30) as client:
            barrier.wait()
            response = client.post(
                f"/days/{DAY}/plans/{plan_id}/publish", json={"operator": "load"}
            )
            return response

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(publish, range(8)))

    ok = [r for r in responses if r.status_code == 201]
    replay = [r for r in responses if r.status_code == 200]
    assert len(ok) == 1, [(r.status_code, r.text) for r in responses]
    assert len(replay) == 7, [(r.status_code, r.text) for r in responses]
    assert {r.json()["publication_id"] for r in responses} == {ok[0].json()["publication_id"]}
    assert all(r.json()["created"] is False for r in replay)

    with httpx.Client(base_url=server) as client:
        retry = client.post(
            f"/days/{DAY}/plans/{plan_id}/publish", json={"operator": "load"}
        )
        assert retry.status_code == 200
        assert retry.json()["publication_id"] == ok[0].json()["publication_id"]
        assert retry.json()["created"] is False

        served = client.get(f"/days/{DAY}/publication")
        assert served.status_code == 200
        assert served.json()["publication_id"] == ok[0].json()["publication_id"]


def test_concurrent_plan_requests_return_same_plan(server):
    with httpx.Client(base_url=server, timeout=30) as client:
        client.post(f"/days/{DAY}/source-manifests", json={"stations": stations_spec()})

    barrier = threading.Barrier(10)

    def plan(_):
        with httpx.Client(base_url=server, timeout=30) as client:
            barrier.wait()
            return client.post(
                f"/days/{DAY}/plans",
                json={"partition_count": 3, "max_partition_bytes": 1000},
            )

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(plan, range(10)))

    assert all(r.status_code == 200 for r in responses), [
        (r.status_code, r.text) for r in responses if r.status_code != 200
    ]
    plan_ids = {r.json()["plan_id"] for r in responses}
    assert len(plan_ids) == 1, plan_ids


def _large_manifest_spec(n=19_996, extra_ungrouped=4):
    """n range stations grouped in fours, plus ungrouped stations with id gaps.

    Total rows stay at the 20k service limit.
    """
    assert n % 4 == 0
    stations = []
    for index in range(n):
        sid = 1000 + index
        block_start = 1000 + (index // 4) * 4
        stations.append(
            {
                "station_id": sid,
                "estimated_bytes": 1_000_000,
                "current_range": {
                    "start_station": block_start,
                    "end_station": block_start + 3,
                },
            }
        )
    # Stations outside any range, with deliberate gaps in the id sequence.
    next_id = 1000 + n
    for offset in range(extra_ungrouped):
        stations.append(
            {
                "station_id": next_id + offset * 1000,
                "estimated_bytes": 500_000,
            }
        )
    return stations


def test_other_day_publish_proceeds_while_manifest_is_being_verified(gated_server):
    """Range verification of a big manifest must not hold the write lock.

    A publication for a *different* day must commit while registration is still
    inside its (lock-free) verification phase.
    """
    server = gated_server
    # Publish something on the other day first.
    with httpx.Client(base_url=server, timeout=30) as client:
        client.post(
            f"/days/{OTHER_DAY}/source-manifests", json={"stations": stations_spec()}
        )
        plan = client.post(
            f"/days/{OTHER_DAY}/plans",
            json={"partition_count": 2, "max_partition_bytes": 1000},
        ).json()
        plan_id = plan["plan_id"]
        client.post(
            f"/days/{OTHER_DAY}/plans/{plan_id}/confirm", json={"operator": "load"}
        )

    def register_big():
        with httpx.Client(base_url=server, timeout=60) as client:
            return client.post(
                f"/days/{DAY}/source-manifests",
                json={"stations": _large_manifest_spec()},
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        register_future = pool.submit(register_big)
        assert VERIFY_GATE["entered"].wait(timeout=15), (
            "registration never entered verification"
        )

        # Verification is in flight and (with the fix) holds no write lock.
        started = time.perf_counter()
        with httpx.Client(base_url=server, timeout=30) as client:
            republish = client.post(
                f"/days/{OTHER_DAY}/plans/{plan_id}/publish",
                json={"operator": "load"},
            )
            served = client.get(f"/days/{OTHER_DAY}/publication")
        publish_elapsed = time.perf_counter() - started

        VERIFY_GATE["release"].set()
        registration = register_future.result(timeout=60)

    assert VERIFY_GATE["hits"] >= 1
    # Retry of an existing publication returns 200; either way it must have
    # committed promptly instead of waiting for verification to finish.
    assert republish.status_code in (200, 201), republish.text
    assert publish_elapsed < 5.0, (
        f"cross-day publish blocked for {publish_elapsed:.2f}s during verification"
    )
    assert served.status_code == 200
    assert registration.status_code == 201, registration.text
    assert registration.json()["manifest"]["station_count"] == 20_000


def test_large_manifest_registration_interleaved_with_publishing(server):
    """End-to-end: a big registration never starves other-day writers.

    The manifest is large enough to make verification/insertion non-trivial
    while keeping the subsequent plan inside the service's DP cell budget.
    (The 20k hard limit itself is covered by test_races.py.)  The hard
    no-lock guarantee is asserted deterministically by the gated test above;
    here we additionally exercise a realistic interleave and only guard
    against a regression to the old 15s lock-timeout behaviour.
    """
    big_body = _large_manifest_spec(n=3_000, extra_ungrouped=4)  # 3,004 stations
    with httpx.Client(base_url=server, timeout=30) as client:
        client.post(
            f"/days/{OTHER_DAY}/source-manifests", json={"stations": stations_spec()}
        )
        other_plan = client.post(
            f"/days/{OTHER_DAY}/plans",
            json={"partition_count": 2, "max_partition_bytes": 1000},
        ).json()
        client.post(
            f"/days/{OTHER_DAY}/plans/{other_plan['plan_id']}/confirm",
            json={"operator": "load"},
        )

    timings = []

    def register(round_index):
        with httpx.Client(base_url=server, timeout=120) as client:
            started = time.perf_counter()
            response = client.post(
                f"/days/{DAY}/source-manifests",
                json={"version": round_index + 1, "stations": big_body},
            )
            timings.append(("register", time.perf_counter() - started))
            return response

    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(register, 0)
        # Give registration a head start, then publish on the other day.
        time.sleep(0.2)
        with httpx.Client(base_url=server, timeout=30) as client:
            started = time.perf_counter()
            response = client.post(
                f"/days/{OTHER_DAY}/plans/{other_plan['plan_id']}/publish",
                json={"operator": "load"},
            )
            timings.append(("publish", time.perf_counter() - started))
        registered = future.result(timeout=120)

    assert registered.status_code == 201, registered.text
    assert registered.json()["manifest"]["station_count"] == 3_004
    assert response.status_code in (200, 201), response.text
    publish_time = next(duration for name, duration in timings if name == "publish")
    # The old code blocked here until the 15s busy_timeout; allow generous
    # scheduling slack but stay well under that failure regime.
    assert publish_time < 10.0, f"publish took {publish_time:.2f}s"

    # The big manifest is fully readable and its ranges survived intact.
    with httpx.Client(base_url=server, timeout=30) as client:
        current = client.get(f"/days/{DAY}/source-manifest")
        assert current.status_code == 200
        assert current.json()["station_count"] == 3_004
        plan = client.post(
            f"/days/{DAY}/plans",
            # 750 reusable ranges + one partition per ungrouped station.
            json={"partition_count": 754, "max_partition_bytes": 10**12},
        )
        assert plan.status_code == 200, plan.text
        # All 750 four-station ranges can be reused; only the ungrouped
        # stations are rewritten.
        assert plan.json()["reused_bytes"] == 3_000 * 1_000_000
        assert plan.json()["rewrite_bytes"] == 4 * 500_000
