"""Unit tests for faa-airmen-sync Lambda."""
import sys
import types
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Stub the Lambda layer dependency (db_utils) before importing the module
# ---------------------------------------------------------------------------
db_utils_stub = types.ModuleType('db_utils')
db_utils_stub.get_db_connection = MagicMock()
db_utils_stub.return_db_connection = MagicMock()
sys.modules['db_utils'] = db_utils_stub

import index as sync  # noqa: E402  (import after sys.modules patch)


# ── normalize ────────────────────────────────────────────────────────────────

class TestNormalize:
    def test_strips_spaces_hyphens_punctuation(self):
        assert sync.normalize('Smith-Jr.') == 'smithjr'

    def test_lowercases(self):
        assert sync.normalize('JONES') == 'jones'

    def test_strips_numbers_preserved(self):
        assert sync.normalize('CFI 1234567') == 'cfi1234567'

    def test_empty_string(self):
        assert sync.normalize('') == ''

    def test_none_like_empty(self):
        assert sync.normalize(None) == ''  # type: ignore


# ── parse_basic ──────────────────────────────────────────────────────────────

BASIC_HEADER = 'UNIQUE ID,FIRST NAME,LAST NAME,STREET 1,STREET 2,CITY,STATE,ZIP,COUNTRY,REGION,MED CLASS,MED DATE,MED EXP DATE'

class TestParseBasic:
    def test_parses_valid_row(self):
        lines = [
            BASIC_HEADER,
            '1234567,JANE A,SMITH,123 MAIN,,DALLAS,TX,75001,USA,SW,1,20230101,20260101',
        ]
        rows = sync.parse_basic(lines)
        assert len(rows) == 1
        r = rows[0]
        assert r['unique_id'] == '1234567'
        assert r['first_middle_name'] == 'JANE A'
        assert r['last_name_suffix'] == 'SMITH'
        assert r['city'] == 'DALLAS'
        assert r['state'] == 'TX'
        assert r['norm_last_name'] == 'smith'

    def test_skips_header(self):
        lines = [BASIC_HEADER]
        assert sync.parse_basic(lines) == []

    def test_skips_empty_lines(self):
        lines = [BASIC_HEADER, '', '   ']
        assert sync.parse_basic(lines) == []

    def test_skips_row_missing_unique_id(self):
        lines = [BASIC_HEADER, ',JANE,SMITH,,,,,,,,,']
        assert sync.parse_basic(lines) == []

    def test_normalizes_hyphenated_last_name(self):
        lines = [BASIC_HEADER, '9999999,JOHN,GARCIA-LOPEZ,,,,,,,,,']
        rows = sync.parse_basic(lines)
        assert rows[0]['norm_last_name'] == 'garcialopez'


# ── parse_certs ──────────────────────────────────────────────────────────────

CERT_HEADER = 'UNIQUE ID,CERTIFICATE TYPE,LEVEL,EXPIRE DATE,RATINGS'

class TestParseCerts:
    def test_parses_flight_instructor(self):
        lines = [CERT_HEADER, '1234567,F,CFI,20271231,CFI/CFII']
        rows = sync.parse_certs(lines)
        assert len(rows) == 1
        r = rows[0]
        assert r['is_flight_instructor'] is True
        assert r['certificate_type'] == 'F'
        assert r['norm_cert_number'] == '1234567'

    def test_non_instructor_cert_type(self):
        lines = [CERT_HEADER, '1234567,P,PPL,,']
        rows = sync.parse_certs(lines)
        assert rows[0]['is_flight_instructor'] is False

    def test_skips_header_and_empty(self):
        lines = [CERT_HEADER, '']
        assert sync.parse_certs(lines) == []

    def test_normalizes_cert_number(self):
        lines = [CERT_HEADER, 'CFI-123 456,F,CFI,20271231,']
        rows = sync.parse_certs(lines)
        assert rows[0]['norm_cert_number'] == 'cfi123456'


# ── validate_row_counts ───────────────────────────────────────────────────────

class TestValidateRowCounts:
    def _conn(self, prev_basic=None, prev_certs=None):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        if prev_basic is None:
            cur.fetchone.return_value = None
        else:
            cur.fetchone.return_value = (prev_basic, prev_certs)
        return conn

    def test_passes_on_no_prior_run(self):
        conn = self._conn(prev_basic=None)
        sync.validate_row_counts(conn, 100_000, 200_000)  # should not raise

    def test_passes_on_small_drop(self):
        conn = self._conn(100_000, 200_000)
        sync.validate_row_counts(conn, 95_000, 190_000)  # 5% drop — ok

    def test_rejects_large_drop(self):
        conn = self._conn(100_000, 200_000)
        with patch.object(sync, 'emit_metric') as m:
            with pytest.raises(RuntimeError, match='delta check failed'):
                sync.validate_row_counts(conn, 70_000, 200_000)
            m.assert_called_with('RowCountDeltaRejected', 1)


# ── bulk_insert_basic ─────────────────────────────────────────────────────────

class TestBulkInsertBasic:
    def test_commits_on_success(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        rows = [{'unique_id': '1', 'first_middle_name': 'A', 'last_name_suffix': 'B',
                 'city': '', 'state': '', 'country': '', 'medical_class': '',
                 'medical_date': '', 'norm_last_name': 'b'}]
        sync.bulk_insert_basic(conn, rows)
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()

    def test_rolls_back_and_emits_metric_on_error(self):
        conn = MagicMock()
        cur = MagicMock()
        cur.executemany.side_effect = Exception('DB error')
        conn.cursor.return_value = cur
        with patch.object(sync, 'emit_metric') as m:
            with pytest.raises(RuntimeError):
                sync.bulk_insert_basic(conn, [{'x': 1}])
            conn.rollback.assert_called_once()
            m.assert_called_with('StagingWriteFailure', 1)

    def test_noop_on_empty_rows(self):
        conn = MagicMock()
        sync.bulk_insert_basic(conn, [])
        conn.cursor.assert_not_called()


# ── atomic_swap ───────────────────────────────────────────────────────────────

class TestAtomicSwap:
    def test_commits_on_success(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        sync.atomic_swap(conn, 100_000, 200_000)
        # Should call COMMIT, not ROLLBACK
        execute_calls = [str(c) for c in cur.execute.call_args_list]
        assert any('COMMIT' in c for c in execute_calls)
        assert not any('ROLLBACK' in c for c in execute_calls)

    def test_rolls_back_and_emits_metric_on_error(self):
        conn = MagicMock()
        cur = MagicMock()
        cur.execute.side_effect = [None, Exception('swap error')]
        conn.cursor.return_value = cur
        with patch.object(sync, 'emit_metric') as m:
            with pytest.raises(RuntimeError):
                sync.atomic_swap(conn, 1, 1)
            execute_calls = [str(c) for c in cur.execute.call_args_list]
            assert any('ROLLBACK' in c for c in execute_calls)
            m.assert_called_with('TableSwapFailure', 1)


# ── handler ───────────────────────────────────────────────────────────────────

class TestHandler:
    def _make_zip(self) -> bytes:
        import io, zipfile
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('BASIC.csv',
                'UNIQUE ID,FIRST NAME,LAST NAME\n1234567,JANE,SMITH\n')
            zf.writestr('CERT.csv',
                'UNIQUE ID,CERTIFICATE TYPE,LEVEL,EXPIRE DATE,RATINGS\n'
                '1234567,F,CFI,20271231,CFI\n')
        return buf.getvalue()

    def test_success_path(self):
        zip_bytes = self._make_zip()
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = None  # no prior ingest metadata
        conn.cursor.return_value = cur
        db_utils_stub.get_db_connection.return_value = conn

        with patch.object(sync, 'download_zip', return_value=zip_bytes), \
             patch.object(sync, 'archive_to_s3', return_value='faa-airmen-2026-03-23.zip'), \
             patch.object(sync, 'emit_metric') as m:
            result = sync.handler({}, None)

        assert result['statusCode'] == 200
        assert result['basicRows'] == 1
        assert result['certRows'] == 1
        m.assert_any_call('IngestSuccess', 1)
        m.assert_any_call('RowCountBasic', 1)
        m.assert_any_call('RowCountCerts', 1)

    def test_download_failure_emits_metric_and_returns_500(self):
        db_utils_stub.get_db_connection.return_value = MagicMock()
        with patch.object(sync, 'download_zip', side_effect=RuntimeError('timeout')), \
             patch.object(sync, 'emit_metric') as m:
            result = sync.handler({}, None)
        assert result['statusCode'] == 500
        m.assert_any_call('IngestFailure', 1)

    def test_staging_write_failure_returns_500(self):
        zip_bytes = self._make_zip()
        conn = MagicMock()
        cur = MagicMock()
        cur.executemany.side_effect = Exception('disk full')
        conn.cursor.return_value = cur
        db_utils_stub.get_db_connection.return_value = conn

        with patch.object(sync, 'download_zip', return_value=zip_bytes), \
             patch.object(sync, 'archive_to_s3', return_value='key'), \
             patch.object(sync, 'emit_metric') as m:
            result = sync.handler({}, None)

        assert result['statusCode'] == 500
        m.assert_any_call('IngestFailure', 1)
