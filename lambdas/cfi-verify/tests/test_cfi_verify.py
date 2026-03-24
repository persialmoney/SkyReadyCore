"""Unit and integration tests for cfi-verify Lambda."""
import sys
import types
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Stub Lambda layer dependency before import
# ---------------------------------------------------------------------------
db_utils_stub = types.ModuleType('db_utils')
db_utils_stub.get_db_connection = MagicMock()
db_utils_stub.return_db_connection = MagicMock()
sys.modules['db_utils'] = db_utils_stub

import index as verify  # noqa: E402


# ── Helpers ───────────────────────────────────────────────────────────────────

def _faa_row(
    unique_id='1234567',
    first_middle_name='JANE A',
    last_name_suffix='SMITH',
    certificate_expire_date=None,
    ratings_raw='CFI/CFII',
    is_flight_instructor=True,
    norm_cert_number='1234567',
    norm_last_name='smith',
    source_snapshot_date='2026-03-01',
):
    return {
        'unique_id': unique_id,
        'first_middle_name': first_middle_name,
        'last_name_suffix': last_name_suffix,
        'certificate_expire_date': certificate_expire_date,
        'ratings_raw': ratings_raw,
        'is_flight_instructor': is_flight_instructor,
        'norm_cert_number': norm_cert_number,
        'norm_last_name': norm_last_name,
        'source_snapshot_date': source_snapshot_date,
    }


def _conn_returning(primary_rows, cert_only_rows=None, snapshot_date='2026-03-01'):
    """Return a mock psycopg3 connection whose cursor mimics specific query results."""
    conn = MagicMock()

    def make_cursor():
        cur = MagicMock()
        results = iter([primary_rows, cert_only_rows or [], [(snapshot_date,)]])
        cur.fetchall.side_effect = lambda: next(results, [])
        cur.fetchone.return_value = (snapshot_date,)
        # Simulate cursor.description so dict(zip(...)) works
        if primary_rows:
            cur.description = [(k,) for k in _faa_row().keys()]
        else:
            cur.description = [('norm_last_name',), ('is_flight_instructor',)]
        return cur

    conn.cursor.side_effect = make_cursor
    return conn


# ── normalize ─────────────────────────────────────────────────────────────────

class TestNormalize:
    def test_strips_hyphen_and_spaces(self):
        assert verify.normalize('Smith-Jr.') == 'smithjr'

    def test_preserves_alphanumeric(self):
        assert verify.normalize('CFI1234567') == 'cfi1234567'

    def test_none_returns_empty(self):
        assert verify.normalize(None) == ''  # type: ignore


# ── _is_expired ───────────────────────────────────────────────────────────────

class TestIsExpired:
    def test_future_date_not_expired(self):
        future = (date.today() + timedelta(days=365)).strftime('%Y%m%d')
        assert verify._is_expired(future) is False

    def test_past_date_is_expired(self):
        past = (date.today() - timedelta(days=1)).strftime('%Y%m%d')
        assert verify._is_expired(past) is True

    def test_none_not_expired(self):
        assert verify._is_expired(None) is False

    def test_unparseable_not_expired(self):
        assert verify._is_expired('INVALID') is False


# ── _check_ratings ────────────────────────────────────────────────────────────

class TestCheckRatings:
    def test_passes_when_ratings_match(self):
        row = _faa_row(ratings_raw='CFI/CFII')
        ok, _ = verify._check_ratings(row, ['CFI', 'CFII'])
        assert ok is True

    def test_fails_when_mei_not_in_ratings(self):
        row = _faa_row(ratings_raw='CFI')
        ok, reason = verify._check_ratings(row, ['MEI'])
        assert ok is False
        assert 'MEI' in reason

    def test_no_user_certs_always_passes(self):
        row = _faa_row(ratings_raw='')
        ok, _ = verify._check_ratings(row, [])
        assert ok is True


# ── determine_status ──────────────────────────────────────────────────────────

class TestDetermineStatus:
    def _mock_lookup(self, primary=None, cert_only=None):
        primary = primary or []
        cert_only = cert_only or []
        lookup_faa = MagicMock(return_value=primary)
        lookup_cert = MagicMock(return_value=cert_only)
        return lookup_faa, lookup_cert

    def test_verified_on_full_match(self):
        conn = MagicMock()
        with patch.object(verify, 'lookup_faa', return_value=[_faa_row()]), \
             patch.object(verify, 'lookup_cert_number_only', return_value=[]):
            status, row, reason = verify.determine_status(
                conn, '1234567', 'smith', None, []
            )
        assert status == 'verified'
        assert row is not None

    def test_partial_when_first_name_mismatches(self):
        conn = MagicMock()
        with patch.object(verify, 'lookup_faa', return_value=[_faa_row(first_middle_name='ROBERT')]), \
             patch.object(verify, 'lookup_cert_number_only', return_value=[]):
            status, _, reason = verify.determine_status(
                conn, '1234567', 'smith', 'jane', []
            )
        assert status == 'partial'
        assert 'first name mismatch' in reason

    def test_partial_when_ratings_inconsistent(self):
        conn = MagicMock()
        row = _faa_row(ratings_raw='CFI')
        with patch.object(verify, 'lookup_faa', return_value=[row]), \
             patch.object(verify, 'lookup_cert_number_only', return_value=[]):
            status, _, reason = verify.determine_status(
                conn, '1234567', 'smith', None, ['MEI']
            )
        assert status == 'partial'
        assert 'MEI' in reason

    def test_partial_when_cert_expired(self):
        conn = MagicMock()
        past = (date.today() - timedelta(days=1)).strftime('%Y%m%d')
        row = _faa_row(certificate_expire_date=past)
        with patch.object(verify, 'lookup_faa', return_value=[row]), \
             patch.object(verify, 'lookup_cert_number_only', return_value=[]):
            status, _, reason = verify.determine_status(
                conn, '1234567', 'smith', None, []
            )
        assert status == 'partial'
        assert 'expired' in reason

    def test_partial_when_cert_found_but_name_differs(self):
        conn = MagicMock()
        with patch.object(verify, 'lookup_faa', return_value=[]), \
             patch.object(verify, 'lookup_cert_number_only',
                          return_value=[{'norm_last_name': 'jones', 'is_flight_instructor': True}]):
            status, row, reason = verify.determine_status(
                conn, '1234567', 'smith', None, []
            )
        assert status == 'partial'
        assert row is None

    def test_unverified_when_no_match(self):
        conn = MagicMock()
        with patch.object(verify, 'lookup_faa', return_value=[]), \
             patch.object(verify, 'lookup_cert_number_only', return_value=[]):
            status, row, _ = verify.determine_status(
                conn, 'unknown', 'nobody', None, []
            )
        assert status == 'unverified'
        assert row is None


# ── write_attempt ─────────────────────────────────────────────────────────────

class TestWriteAttempt:
    def test_commits_on_success(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        verify.write_attempt(conn, 'u1', 'CFI123', 'Smith', None,
                             '1234567', 'verified', 'matched', '2026-03-01')
        conn.commit.assert_called_once()

    def test_rolls_back_and_emits_metric_on_error(self):
        conn = MagicMock()
        cur = MagicMock()
        cur.execute.side_effect = Exception('constraint violation')
        conn.cursor.return_value = cur
        with patch.object(verify, 'emit_metric') as m:
            # Should not raise — non-fatal
            verify.write_attempt(conn, 'u1', 'CFI123', 'Smith', None,
                                 None, 'unverified', 'no match', None)
        conn.rollback.assert_called_once()
        m.assert_called_with('DbWriteFailure', 1)


# ── persist_to_dynamo ─────────────────────────────────────────────────────────

class TestPersistToDynamo:
    def test_calls_update_item(self):
        table = MagicMock()
        with patch.object(verify.dynamodb, 'Table', return_value=table):
            verify.persist_to_dynamo('u1', 'verified', '2026-03-23T00:00:00Z', '2026-03-01')
        table.update_item.assert_called_once()
        call_kwargs = table.update_item.call_args[1]
        assert call_kwargs['Key'] == {'userId': 'u1'}
        assert call_kwargs['ExpressionAttributeValues'][':s'] == 'verified'

    def test_emits_metric_on_dynamo_error(self):
        table = MagicMock()
        table.update_item.side_effect = Exception('throttle')
        with patch.object(verify.dynamodb, 'Table', return_value=table), \
             patch.object(verify, 'emit_metric') as m:
            verify.persist_to_dynamo('u1', 'verified', '2026-03-23T00:00:00Z', None)
        m.assert_called_with('DbWriteFailure', 1)


# ── handler ───────────────────────────────────────────────────────────────────

class TestHandler:
    def _event(self, cert='1234567', last='Smith', first=None, user_id='user-abc'):
        args = {'certificateNumber': cert, 'lastName': last}
        if first:
            args['firstName'] = first
        return {
            'arguments': {'input': args},
            'identity': {'sub': user_id},
        }

    def test_verified_response(self):
        conn = MagicMock()
        db_utils_stub.get_db_connection.return_value = conn
        row = _faa_row()

        with patch.object(verify, 'determine_status', return_value=('verified', row, 'matched')), \
             patch.object(verify, 'get_snapshot_date', return_value='2026-03-01'), \
             patch.object(verify, 'write_attempt'), \
             patch.object(verify, 'persist_to_dynamo'), \
             patch.object(verify, 'emit_metric') as m:
            result = verify.handler(self._event(), None)

        assert result['status'] == 'verified'
        assert result['source'] == verify.FAA_SOURCE_LABEL
        assert result['certificateSummary']['isCfi'] is True
        m.assert_any_call('VerificationAttempt', 1,
                          extra_dims=[{'Name': 'Status', 'Value': 'verified'}])

    def test_unverified_response(self):
        conn = MagicMock()
        db_utils_stub.get_db_connection.return_value = conn

        with patch.object(verify, 'determine_status', return_value=('unverified', None, 'no match')), \
             patch.object(verify, 'get_snapshot_date', return_value=None), \
             patch.object(verify, 'write_attempt'), \
             patch.object(verify, 'persist_to_dynamo'), \
             patch.object(verify, 'emit_metric'):
            result = verify.handler(self._event(), None)

        assert result['status'] == 'unverified'
        assert result['certificateSummary'] is None

    def test_empty_inputs_return_unverified_immediately(self):
        result = verify.handler(
            {'arguments': {'input': {'certificateNumber': '', 'lastName': ''}},
             'identity': {'sub': 'u1'}},
            None,
        )
        assert result['status'] == 'unverified'
        db_utils_stub.get_db_connection.assert_not_called()

    def test_db_error_returns_unverified_not_exception(self):
        db_utils_stub.get_db_connection.side_effect = Exception('connection refused')
        with patch.object(verify, 'emit_metric') as m:
            result = verify.handler(self._event(), None)
        assert result['status'] == 'unverified'
        m.assert_any_call('DbReadFailure', 1)
        db_utils_stub.get_db_connection.side_effect = None  # reset

    def test_metrics_emitted_on_partial(self):
        conn = MagicMock()
        db_utils_stub.get_db_connection.return_value = conn

        with patch.object(verify, 'determine_status', return_value=('partial', None, 'name mismatch')), \
             patch.object(verify, 'get_snapshot_date', return_value='2026-03-01'), \
             patch.object(verify, 'write_attempt'), \
             patch.object(verify, 'persist_to_dynamo'), \
             patch.object(verify, 'emit_metric') as m:
            result = verify.handler(self._event(), None)

        assert result['status'] == 'partial'
        m.assert_any_call('VerificationAttempt', 1,
                          extra_dims=[{'Name': 'Status', 'Value': 'partial'}])
