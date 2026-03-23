"""Unit tests for user_data (delete/collect)."""
from unittest.mock import MagicMock

import pytest

from user_data import (
    delete_postgres_user_data,
    batch_delete_dynamo_items,
    scan_delete_events_for_user,
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
