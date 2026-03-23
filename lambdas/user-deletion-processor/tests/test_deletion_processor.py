"""Tests for deletion processor handler."""
from unittest.mock import MagicMock, patch

import index as proc


def test_handler_admin_purge_invokes_process_hard_delete():
    with patch.object(proc, "process_hard_delete", return_value={"logbook_entries_deleted": 2}) as phd:
        with patch.object(proc, "emit_deletion_metrics"):
            out = proc.handler({"adminPurgeUserId": "sub-123"}, None)
    assert out["ok"] is True
    assert out["userId"] == "sub-123"
    phd.assert_called_once()


def test_handler_admin_empty_id():
    out = proc.handler({"adminPurgeUserId": "  "}, None)
    assert out["ok"] is False


def test_scan_expired_requests_accumulates():
    table = MagicMock()
    table.scan.side_effect = [
        {"Items": [{"userId": "a"}], "LastEvaluatedKey": {"userId": "a"}},
        {"Items": [{"userId": "b"}]},
    ]
    rows = proc.scan_expired_requests(table, "2026-01-01T00:00:00Z")
    assert len(rows) == 2
