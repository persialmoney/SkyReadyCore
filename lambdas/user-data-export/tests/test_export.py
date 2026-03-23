"""Tests for user-data-export handler."""
from unittest.mock import MagicMock, patch

import index as exp


def test_handler_admin_export_ok():
    with patch.object(exp, "run_export", return_value={"success": True, "message": "ok"}) as run:
        out = exp.handler({"adminExportUserId": "sub-1"}, None)
    assert out["ok"] is True
    run.assert_called_once_with("sub-1", "admin")


def test_handler_appsync_unknown_field():
    try:
        exp.handler({"info": {"fieldName": "other"}, "identity": {"sub": "x"}}, None)
    except ValueError as e:
        assert "Unknown field" in str(e)
    else:
        raise AssertionError("expected ValueError")
