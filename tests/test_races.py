"""Targeted races and limits that are awkward to trigger over HTTP."""

from __future__ import annotations

import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import planner, service as service_module  # noqa: E402
from app.config import Settings  # noqa: E402
from app.db import Database  # noqa: E402
from app.main import create_app  # noqa: E402
from app.models import SourceManifestIn  # noqa: E402
from app.service import RepartitionService  # noqa: E402

DAY = "2026-11-21"


def stations_spec():
    return [
        {"station_id": 1, "estimated_bytes": 10,
         "current_range": {"start_station": 1, "end_station": 2}},
        {"station_id": 2, "estimated_bytes": 10,
         "current_range": {"start_station": 1, "end_station": 2}},
        {"station_id": 3, "estimated_bytes": 10},
    ]


def test_manifest_superseded_while_planning(monkeypatch, tmp_path):
    """A plan computed against v1 must not be stored once v2 exists."""

    settings = Settings(database_path=str(tmp_path / "race.db"))
    app = create_app(settings=settings)
    real_plan = planner.plan_partitions
    original_holder = {}

    def slow_plan(stations, k, cap):
        client = original_holder["client"]
        if "registered" not in original_holder:
            original_holder["registered"] = True
            response = client.post(
                f"/days/{DAY}/source-manifests",
                json={"stations": stations_spec()},
            )
            assert response.status_code == 201
            assert response.json()["manifest"]["version"] == 2
        return real_plan(stations, k, cap)

    with TestClient(app) as client:
        original_holder["client"] = client
        assert client.post(
            f"/days/{DAY}/source-manifests", json={"stations": stations_spec()}
        ).status_code == 201

    # Monkeypatch after startup so the app's service uses it.
    with TestClient(app) as client:
        monkeypatch.setattr(service_module.planner, "plan_partitions", slow_plan)
        original_holder.pop("registered", None)
        original_holder["client"] = client
        response = client.post(
            f"/days/{DAY}/plans",
            json={"partition_count": 2, "max_partition_bytes": 100},
        )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "stale_manifest"

    with TestClient(app) as client:
        listing = client.get(f"/days/{DAY}/plans").json()
        # Nothing was persisted for the superseded version.
        assert all(p["source_manifest_version"] != 1 for p in listing["plans"])


def test_manifest_size_limit(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "limit.db"), max_stations_per_manifest=2
    )
    app = create_app(settings=settings)
    with TestClient(app) as client:
        response = client.post(
            f"/days/{DAY}/source-manifests", json={"stations": stations_spec()}
        )
    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "manifest_too_large"


def test_plan_dp_budget(tmp_path):
    settings = Settings(database_path=str(tmp_path / "budget.db"), max_dp_cells=4)
    app = create_app(settings=settings)
    with TestClient(app) as client:
        client.post(f"/days/{DAY}/source-manifests", json={"stations": stations_spec()})
        response = client.post(
            f"/days/{DAY}/plans",
            json={"partition_count": 3, "max_partition_bytes": 100},
        )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "plan_too_large"


def _manifest_input(version=None, stations=None):
    return SourceManifestIn.model_validate(
        {
            **({"version": version} if version is not None else {}),
            "stations": stations or stations_spec(),
        }
    )


def test_failed_registration_leaves_nothing_behind(tmp_path):
    db = Database(str(tmp_path / "atomic.db"))
    db.initialize()
    service = RepartitionService(db)

    good = service.register_manifest(DAY, _manifest_input())
    assert good["manifest"]["version"] == 1

    # Invalid ranges: registration must fail without advancing the version,
    # inserting a manifest/stations, or touching the previous version.
    bad_stations = [
        {
            "station_id": 1,
            "estimated_bytes": 10,
            "current_range": {"start_station": 1, "end_station": 3},
        },
        {"station_id": 2, "estimated_bytes": 10},
        {
            "station_id": 3,
            "estimated_bytes": 10,
            "current_range": {"start_station": 1, "end_station": 3},
        },
    ]
    with pytest.raises(Exception):
        service.register_manifest(DAY, _manifest_input(stations=bad_stations))

    conn = sqlite3.connect(db.path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM source_manifests").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0] == 3
        assert conn.execute(
            "SELECT MAX(version) FROM source_manifests WHERE day = ?", (DAY,)
        ).fetchone()[0] == 1
    finally:
        conn.close()

    # A retry auto-allocates version 2, proving the failed attempt did not
    # consume a version number.
    retry = service.register_manifest(DAY, _manifest_input())
    assert retry["manifest"]["version"] == 2
    assert retry["previous_version"] == 1


def test_concurrent_explicit_versions_checked_at_commit_time(tmp_path):
    db = Database(str(tmp_path / "explicit.db"))
    db.initialize()
    service = RepartitionService(db)
    service.register_manifest(DAY, _manifest_input())  # version 1

    barrier = threading.Barrier(2)
    original = planner.build_source_ranges

    def synced(stations):
        barrier.wait(timeout=10)  # make both threads reach the commit together
        return original(stations)

    def register_with(version):
        return service.register_manifest(DAY, _manifest_input(version=version))

    # Two distinct explicit versions racing: every accepted version must have
    # been greater than whatever was current at its commit time, so exactly one
    # of (commit, version_not_increasing) happens per request and the stored
    # versions are always strictly increasing.
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(planner, "build_source_ranges", synced)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(register_with, 3),
                pool.submit(register_with, 4),
            ]
            outcomes = [f.exception() for f in futures]

    stored = [
        exc for exc in outcomes
        if exc is None
    ]
    rejected = [exc for exc in outcomes if exc is not None]
    # Whichever commits first wins; the other is either accepted afterwards
    # (still increasing) or rejected at commit time.
    assert 1 <= len(stored) <= 2
    assert all(exc.code == "version_not_increasing" for exc in rejected)

    # Two equal explicit versions racing: exactly one can win; the loser is
    # rejected at commit time either by the increasing-version check or (as a
    # structural backstop) by UNIQUE(day, version).
    barrier.reset()
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(planner, "build_source_ranges", synced)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(register_with, 7),
                pool.submit(register_with, 7),
            ]
            results = [f.exception() for f in futures]
    winners = [exc for exc in results if exc is None]
    losers = [exc for exc in results if exc is not None]
    assert len(winners) == 1 and len(losers) == 1
    assert losers[0].code in {"version_exists", "version_not_increasing"}

    # An explicit version below the current one is rejected against the version
    # observed at commit time, not at request time.
    with pytest.raises(Exception) as exc_info:
        service.register_manifest(DAY, _manifest_input(version=2))
    assert exc_info.value.code == "version_not_increasing"

    # Auto allocation after the races hands out the actual next version.
    highest = service.get_current_manifest(DAY).version
    auto = service.register_manifest(DAY, _manifest_input())
    assert auto["manifest"]["version"] == highest + 1


def test_concurrent_auto_registrations_never_share_a_version(tmp_path):
    db = Database(str(tmp_path / "auto.db"))
    db.initialize()
    service = RepartitionService(db)

    def register(_):
        return service.register_manifest(DAY, _manifest_input())

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(register, range(10)))

    versions = sorted(r["manifest"]["version"] for r in receipts)
    assert versions == list(range(1, 11))

    conn = sqlite3.connect(db.path)
    try:
        rows = conn.execute(
            "SELECT version, station_count FROM source_manifests WHERE day = ? "
            "ORDER BY version",
            (DAY,),
        ).fetchall()
        assert [r[0] for r in rows] == list(range(1, 11))
        # Every committed manifest has its complete station set: no partial write.
        assert all(r[1] == 3 for r in rows)
        assert conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0] == 30
    finally:
        conn.close()
