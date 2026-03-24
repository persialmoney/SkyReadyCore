"""Unit tests for user_data (delete/collect/sanitize)."""
from unittest.mock import MagicMock

import pytest

from user_data import (
    delete_postgres_user_data,
    batch_delete_dynamo_items,
    scan_delete_events_for_user,
    sanitize_export_payload,
)


def test_delete_postgres_user_data_runs_ordered_sql():
    cur = MagicMock()
    cur.rowcount = 1
    result = delete_postgres_user_data(cur, "user-abc")
    assert "logbook_entries_deleted" in result
    assert cur.execute.call_count >= 7


def test_batch_delete_dynamo_items_queries_and_deletes():
    ddb = MagicMock()
    table = MagicMock()
    ddb.Table.return_value = table
    table.query.side_effect = [
        {"Items": [{"userId": "u1", "airportCode": "KJFK"}]},
    ]
    table.batch_writer.return_value.__enter__.return_value = MagicMock()

    n = batch_delete_dynamo_items(ddb, "tbl", "userId", "u1", "airportCode")
    assert n == 1
    ddb.Table.assert_called_with("tbl")


def test_scan_delete_events_empty_table():
    ddb = MagicMock()
    table = MagicMock()
    ddb.Table.return_value = table
    table.scan.return_value = {"Items": []}

    assert scan_delete_events_for_user(ddb, "events-tbl", "u1") == 0


# ---------------------------------------------------------------------------
# sanitize_export_payload
# ---------------------------------------------------------------------------

def _make_raw_payload(**overrides):
    base = {
        "exportVersion": "1.0",
        "exportedAt": "2026-01-01T00:00:00Z",
        "dynamodb": {
            "profile": {
                "userId": "cog-sub-123",
                "id": "cog-sub-123",
                "name": "Jane Pilot",
                "email": "jane@example.com",
            },
            "saved_airports": [
                {"userId": "cog-sub-123", "airportCode": "KJFK", "name": "JFK", "savedAt": "2025-01-01"},
            ],
            "alerts": [
                {"userId": "cog-sub-123", "alertId": "a1", "alertType": "weather", "airportCode": "KJFK",
                 "condition": "VFR", "enabled": True, "createdAt": "2025-01-01", "updatedAt": "2025-01-01"},
            ],
        },
        "postgresql": {
            "logbook_entries": [
                {
                    "entry_id": "eid1", "user_id": "cog-sub-123", "date": "2025-06-01",
                    "aircraft": "C172", "total_time": 1.5,
                    "instructor_user_id": "cog-instructor-abc",
                    "student_user_id": "cog-student-xyz",
                    "mirrored_from_entry_id": "eid0",
                    "mirrored_from_user_id": "cog-other",
                    "deleted_at": None,
                    "remarks": "Great flight",
                },
            ],
            "missions": [
                {
                    "id": "m-pk-1", "mission_id": "m1", "user_id": "cog-sub-123",
                    "status": "complete", "route_hash": "abc123",
                    "latest_assessment_id": "ra-1", "deleted_at": None,
                    "_sync_pending": False, "notes": "VFR cross country",
                },
            ],
            "mission_airports": [
                {"id": "ma-pk-1", "mission_id": "m1", "icao": "KJFK", "role": "origin",
                 "order_index": 0, "_sync_pending": False},
            ],
            "readiness_assessments": [
                {
                    "id": "ra-pk-1", "mission_id": "m1", "route_hash": "abc123",
                    "minimums_profile_id": "prof-internal-1",
                    "minimums_profile_name": "IFR Mins",
                    "aggregate_result": "GO", "deleted_at": None, "_sync_pending": False,
                    "assessment_time_utc": 1700000000.0,
                },
            ],
            "proficiency_snapshots": [
                {
                    "id": "ps-pk-1", "user_id": "cog-sub-123",
                    "snapshot_date": "2025-06-01", "score": 82,
                    "_sync_pending": False, "computed_at": 1700000000.0,
                },
            ],
            "outbox": [
                {"id": "ob-1", "event_type": "sync", "user_id": "cog-sub-123",
                 "payload": "{}", "processed": True, "created_at": 1700000000.0},
            ],
        },
    }
    base.update(overrides)
    return base


def test_sanitize_strips_user_id_from_envelope():
    result = sanitize_export_payload(_make_raw_payload())
    assert "userId" not in result
    assert result["exportVersion"] == "1.0"
    assert result["exportedAt"] == "2026-01-01T00:00:00Z"


def test_sanitize_strips_profile_internal_ids():
    result = sanitize_export_payload(_make_raw_payload())
    profile = result["profile"]
    assert "userId" not in profile
    assert "id" not in profile
    assert profile["name"] == "Jane Pilot"
    assert profile["email"] == "jane@example.com"


def test_sanitize_strips_user_id_from_airports_and_alerts():
    result = sanitize_export_payload(_make_raw_payload())
    for airport in result["savedAirports"]:
        assert "userId" not in airport
    assert result["savedAirports"][0]["airportCode"] == "KJFK"

    for alert in result["alerts"]:
        assert "userId" not in alert
    assert result["alerts"][0]["alertId"] == "a1"


def test_sanitize_strips_logbook_internal_fields():
    result = sanitize_export_payload(_make_raw_payload())
    entry = result["logbookEntries"][0]
    for field in ("entry_id", "user_id", "instructor_user_id", "student_user_id",
                  "mirrored_from_entry_id", "mirrored_from_user_id", "deleted_at"):
        assert field not in entry, f"Expected {field!r} to be stripped from logbook entry"
    assert entry["date"] == "2025-06-01"
    assert entry["remarks"] == "Great flight"


def test_sanitize_strips_mission_internal_fields():
    result = sanitize_export_payload(_make_raw_payload())
    mission = result["missions"][0]
    for field in ("id", "user_id", "route_hash", "latest_assessment_id", "deleted_at", "_sync_pending"):
        assert field not in mission, f"Expected {field!r} to be stripped from mission"
    assert mission["notes"] == "VFR cross country"


def test_sanitize_strips_mission_airport_internal_fields():
    result = sanitize_export_payload(_make_raw_payload())
    ma = result["missionAirports"][0]
    for field in ("id", "mission_id", "_sync_pending"):
        assert field not in ma, f"Expected {field!r} to be stripped from mission_airport"
    assert ma["icao"] == "KJFK"


def test_sanitize_strips_assessment_internal_fields():
    result = sanitize_export_payload(_make_raw_payload())
    ra = result["readinessAssessments"][0]
    for field in ("id", "mission_id", "route_hash", "minimums_profile_id", "deleted_at", "_sync_pending"):
        assert field not in ra, f"Expected {field!r} to be stripped from readiness_assessment"
    assert ra["minimums_profile_name"] == "IFR Mins"
    assert ra["aggregate_result"] == "GO"


def test_sanitize_strips_snapshot_internal_fields():
    result = sanitize_export_payload(_make_raw_payload())
    snap = result["proficiencySnapshots"][0]
    for field in ("id", "user_id", "_sync_pending"):
        assert field not in snap, f"Expected {field!r} to be stripped from proficiency_snapshot"
    assert snap["score"] == 82


def test_sanitize_omits_outbox_entirely():
    result = sanitize_export_payload(_make_raw_payload())
    assert "outbox" not in result


def test_sanitize_handles_empty_collections():
    payload = {
        "exportVersion": "1.0",
        "exportedAt": "2026-01-01T00:00:00Z",
        "dynamodb": {"profile": None, "saved_airports": [], "alerts": []},
        "postgresql": {
            "logbook_entries": [], "missions": [], "mission_airports": [],
            "readiness_assessments": [], "proficiency_snapshots": [], "outbox": [],
        },
    }
    result = sanitize_export_payload(payload)
    assert result["profile"] == {}
    assert result["logbookEntries"] == []
    assert result["proficiencySnapshots"] == []
