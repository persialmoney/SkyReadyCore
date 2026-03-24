"""
FAA Airmen Certification Sync Lambda

Downloads the FAA's Airmen Certification Releasable Database daily, archives the
raw ZIP to S3, parses BASIC.csv and CERT.csv into staging tables, validates row
counts, and atomically swaps staging → production.

Triggered by: EventBridge daily cron (1:00 AM UTC)
Data source:  https://av-info.faa.gov/data/ACS/CS012019.zip

Metrics emitted to CloudWatch namespace: SkyReady/FAA/{STAGE}
"""
import io
import json
import os
import re
import time
import urllib.request
import zipfile
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import boto3

# ── AWS clients ───────────────────────────────────────────────────────────────

s3_client = boto3.client('s3')
cw_client = boto3.client('cloudwatch')

# ── Environment ───────────────────────────────────────────────────────────────

STAGE = os.environ.get('STAGE', 'dev')
FAA_AIRMEN_BUCKET = os.environ.get('FAA_AIRMEN_BUCKET', f'sky-ready-faa-airmen-{STAGE}')
DB_SECRET_ARN = os.environ.get('DB_SECRET_ARN')
DB_ENDPOINT = os.environ.get('DB_ENDPOINT')
DB_NAME = os.environ.get('DB_NAME', 'skyready')

METRIC_NAMESPACE = f'SkyReady/FAA/{STAGE}'
METRIC_DIMS = [{'Name': 'Stage', 'Value': STAGE}]

# FAA Airmen Certification Releasable Database download URL.
# The file at this URL is updated daily by the FAA.
# The FAA publishes a new snapshot monthly at registry.faa.gov.
# URL pattern: CS{MM}{YYYY}.zip — we dynamically build this at runtime so the
# Lambda always fetches the current month's file without needing a code deploy.
FAA_AIRMEN_ZIP_BASE_URL = "https://registry.faa.gov/database"

# Reject a new snapshot if either table shrinks by more than this fraction.
MAX_DELTA_FRACTION = 0.20

# FAA certificate_type codes that indicate a flight instructor certificate.
FLIGHT_INSTRUCTOR_CERT_TYPES = {'F'}


# ── Normalization ─────────────────────────────────────────────────────────────

def normalize(value: str) -> str:
    """Strip non-alphanumeric characters and lowercase for search-key columns."""
    return re.sub(r'[^a-z0-9]', '', (value or '').lower().strip())


# ── CloudWatch metrics ────────────────────────────────────────────────────────

def emit_metric(name: str, value: float, unit: str = 'Count') -> None:
    try:
        cw_client.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[{
                'MetricName': name,
                'Value': value,
                'Unit': unit,
                'Dimensions': METRIC_DIMS,
            }],
        )
    except Exception as e:
        print(f"[FAA-Airmen-Sync] WARNING: failed to emit metric {name}: {e}")


# ── DB connection (lazy, via shared layer) ────────────────────────────────────

def _get_conn():
    """Import db_utils from the shared layer and return a connection."""
    import db_utils  # type: ignore  # provided by Lambda layer
    return db_utils.get_db_connection()


def _return_conn(conn) -> None:
    try:
        import db_utils  # type: ignore
        db_utils.return_db_connection(conn)
    except Exception:
        pass


# ── Download ──────────────────────────────────────────────────────────────────

def _build_faa_url() -> str:
    """Build the current month's FAA CSV ZIP URL: CS{MM}{YYYY}.zip."""
    today = date.today()
    return f"{FAA_AIRMEN_ZIP_BASE_URL}/CS{today.strftime('%m%Y')}.zip"


def download_zip() -> bytes:
    url = _build_faa_url()
    print(f"[FAA-Airmen-Sync] Downloading {url}")
    req = urllib.request.Request(
        url,
        headers={'User-Agent': 'SkyReady/1.0 (+https://skyready.app)'},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
        print(f"[FAA-Airmen-Sync] Downloaded {len(data):,} bytes")
        return data
    except Exception as e:
        emit_metric('DownloadFailure', 1)
        raise RuntimeError(f"FAA airmen ZIP download failed: {e}") from e


# ── S3 archive ────────────────────────────────────────────────────────────────

def archive_to_s3(zip_data: bytes) -> str:
    key = f"faa-airmen-{date.today().isoformat()}.zip"
    print(f"[FAA-Airmen-Sync] Archiving to s3://{FAA_AIRMEN_BUCKET}/{key}")
    s3_client.put_object(
        Bucket=FAA_AIRMEN_BUCKET,
        Key=key,
        Body=zip_data,
        ContentType='application/zip',
    )
    return key


# ── Parse ─────────────────────────────────────────────────────────────────────

def extract_csvs(zip_data: bytes) -> Tuple[List[str], List[str]]:
    """Return (basic_lines, cert_lines) as lists of raw CSV strings."""
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        names = zf.namelist()
        print(f"[FAA-Airmen-Sync] ZIP contains: {names}")

        def read(filename: str) -> List[str]:
            # The FAA sometimes capitalises or changes paths; match case-insensitively.
            match = next((n for n in names if n.upper().endswith(filename.upper())), None)
            if not match:
                raise ValueError(f"{filename} not found in FAA airmen ZIP (files: {names})")
            return zf.read(match).decode('latin-1', errors='replace').splitlines()

        return read('BASIC.csv'), read('CERT.csv')


def parse_basic(lines: List[str]) -> List[Dict[str, Any]]:
    """
    Parse PILOT_BASIC.csv rows into dicts ready for INSERT.

    FAA PILOT_BASIC.csv columns (0-indexed):
      0  UNIQUE ID       — internal FAA DB key (NOT the cert number on the card)
      1  FIRST NAME      (first + middle, up to 30 chars)
      2  LAST NAME       (last + suffix, up to 30 chars)
      3  STREET 1
      4  STREET 2
      5  CITY
      6  STATE
      7  ZIP CODE
      8  COUNTRY
      9  REGION
      10 MED CLASS
      11 MED DATE
      12 MED EXP DATE
      13 BASIC MED COURSE DATE
      14 BASIC MED CMEC DATE

    norm_first_name is the normalized first token of the FIRST & MIDDLE NAME
    field. Used as the primary lookup key for CFI verification since the FAA
    data does not include airmen certificate numbers.
    """
    rows = []
    for i, line in enumerate(lines):
        if i == 0 or not line.strip():
            continue
        fields = line.split(',')
        if len(fields) < 3:
            continue
        unique_id = fields[0].strip()
        if not unique_id:
            continue
        first_middle = fields[1].strip() if len(fields) > 1 else ''
        last_name_suffix = fields[2].strip()
        # Take only the first token of first+middle as the normalized first name
        # so "JOHN MICHAEL" → norm_first_name = "john"
        first_token = first_middle.split()[0] if first_middle.split() else ''
        rows.append({
            'unique_id':         unique_id,
            'first_middle_name': first_middle,
            'last_name_suffix':  last_name_suffix,
            'city':              fields[5].strip() if len(fields) > 5 else '',
            'state':             fields[6].strip() if len(fields) > 6 else '',
            'country':           fields[8].strip() if len(fields) > 8 else '',
            'medical_class':     fields[10].strip() if len(fields) > 10 else '',
            'medical_date':      fields[11].strip() if len(fields) > 11 else '',
            'norm_last_name':    normalize(last_name_suffix),
            'norm_first_name':   normalize(first_token),
        })
    return rows


def parse_certs(lines: List[str]) -> List[Dict[str, Any]]:
    """
    Parse PILOT_CERT.csv rows into dicts ready for INSERT.

    FAA PILOT_CERT.csv columns (0-indexed):
      0  UNIQUE ID           — internal FAA DB key; links to PILOT_BASIC.csv
                               NOTE: this is NOT the airmen certificate number.
                               The FAA explicitly does not release cert numbers.
      1  RECORD TYPE
      2  CERTIFICATE TYPE    ('F' = Flight Instructor)
      3  CERTIFICATE LEVEL
      4  CERTIFICATE EXPIRE DATE  — MMDDYYYY format (CFI only)
      5  RATINGS             — up to 11 ratings, 10 chars each (e.g. 'A/ASEL    ')
    """
    rows = []
    for i, line in enumerate(lines):
        if i == 0 or not line.strip():
            continue
        fields = line.split(',')
        if len(fields) < 2:
            continue
        unique_id = fields[0].strip()
        if not unique_id:
            continue
        record_type = fields[1].strip() if len(fields) > 1 else ''
        cert_type   = fields[2].strip() if len(fields) > 2 else ''
        rows.append({
            'unique_id':               unique_id,
            'record_type':             record_type,
            'certificate_type':        cert_type,
            'certificate_level':       fields[3].strip() if len(fields) > 3 else '',
            'certificate_expire_date': fields[4].strip() if len(fields) > 4 else '',
            'ratings_raw':             fields[5].strip() if len(fields) > 5 else '',
            'is_flight_instructor':    cert_type in FLIGHT_INSTRUCTOR_CERT_TYPES,
            # norm_cert_number is kept for schema compatibility but is derived from
            # UNIQUE_ID (internal FAA key), not the number on a pilot's certificate.
            'norm_cert_number':        normalize(unique_id),
        })
    return rows


# ── DB writes ─────────────────────────────────────────────────────────────────

def bulk_insert_basic(conn, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE faa_airmen_basic_staging")
        cur.executemany(
            """
            INSERT INTO faa_airmen_basic_staging
                (unique_id, first_middle_name, last_name_suffix,
                 city, state, country, medical_class, medical_date,
                 norm_last_name, norm_first_name, raw_source_updated_at)
            VALUES
                (%(unique_id)s, %(first_middle_name)s, %(last_name_suffix)s,
                 %(city)s, %(state)s, %(country)s, %(medical_class)s, %(medical_date)s,
                 %(norm_last_name)s, %(norm_first_name)s, now())
            """,
            rows,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        emit_metric('StagingWriteFailure', 1)
        raise RuntimeError(f"Staging insert failed (basic): {e}") from e
    finally:
        cur.close()


def bulk_insert_certs(conn, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE faa_airmen_certificates_staging CASCADE")
        cur.executemany(
            """
            INSERT INTO faa_airmen_certificates_staging
                (unique_id, record_type, certificate_type, certificate_level,
                 certificate_expire_date, ratings_raw,
                 is_flight_instructor, norm_cert_number, raw_source_updated_at)
            VALUES
                (%(unique_id)s, %(record_type)s, %(certificate_type)s, %(certificate_level)s,
                 %(certificate_expire_date)s, %(ratings_raw)s,
                 %(is_flight_instructor)s, %(norm_cert_number)s, now())
            """,
            rows,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        emit_metric('StagingWriteFailure', 1)
        raise RuntimeError(f"Staging insert failed (certs): {e}") from e
    finally:
        cur.close()


# ── Row-count validation ──────────────────────────────────────────────────────

def validate_row_counts(
    conn,
    new_basic: int,
    new_certs: int,
) -> None:
    """Abort if either table shrinks by more than MAX_DELTA_FRACTION vs last run."""
    cur = conn.cursor()
    cur.execute("SELECT basic_row_count, cert_row_count FROM faa_ingest_metadata WHERE id = 1")
    row = cur.fetchone()
    cur.close()

    if row is None:
        return  # No prior run — skip delta check on first ingest

    prev_basic, prev_certs = row
    for label, prev, new in [('basic', prev_basic, new_basic), ('certs', prev_certs, new_certs)]:
        if prev and prev > 0:
            delta = (prev - new) / prev
            if delta > MAX_DELTA_FRACTION:
                emit_metric('RowCountDeltaRejected', 1)
                raise RuntimeError(
                    f"Row count delta check failed for {label}: "
                    f"prev={prev}, new={new}, drop={delta:.1%} > {MAX_DELTA_FRACTION:.0%}"
                )


# ── Atomic table swap ─────────────────────────────────────────────────────────

def atomic_swap(conn, basic_count: int, cert_count: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute("DROP TABLE IF EXISTS faa_airmen_certificates CASCADE")
        cur.execute("ALTER TABLE faa_airmen_certificates_staging RENAME TO faa_airmen_certificates")
        cur.execute("DROP TABLE IF EXISTS faa_airmen_basic CASCADE")
        cur.execute("ALTER TABLE faa_airmen_basic_staging RENAME TO faa_airmen_basic")
        # Recreate staging tables immediately so next run can write into them
        cur.execute("""
            CREATE TABLE faa_airmen_basic_staging
                (LIKE faa_airmen_basic INCLUDING ALL)
        """)
        cur.execute("""
            CREATE TABLE faa_airmen_certificates_staging
                (LIKE faa_airmen_certificates INCLUDING ALL)
        """)
        cur.execute(
            """
            INSERT INTO faa_ingest_metadata (id, source_snapshot_date, ingested_at,
                                             basic_row_count, cert_row_count)
            VALUES (1, %s, now(), %s, %s)
            ON CONFLICT (id) DO UPDATE
                SET source_snapshot_date = EXCLUDED.source_snapshot_date,
                    ingested_at          = EXCLUDED.ingested_at,
                    basic_row_count      = EXCLUDED.basic_row_count,
                    cert_row_count       = EXCLUDED.cert_row_count
            """,
            (date.today().isoformat(), basic_count, cert_count),
        )
        cur.execute("COMMIT")
        print("[FAA-Airmen-Sync] Atomic table swap complete")
    except Exception as e:
        cur.execute("ROLLBACK")
        emit_metric('TableSwapFailure', 1)
        raise RuntimeError(f"Atomic table swap failed: {e}") from e
    finally:
        cur.close()


# ── Handler ───────────────────────────────────────────────────────────────────

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start = time.time()
    print(f"[FAA-Airmen-Sync] Starting. Stage={STAGE} Bucket={FAA_AIRMEN_BUCKET}")

    conn: Optional[Any] = None
    try:
        # 1. Download
        zip_data = download_zip()

        # 2. Archive to S3
        s3_key = archive_to_s3(zip_data)

        # 3. Parse
        basic_lines, cert_lines = extract_csvs(zip_data)
        basic_rows = parse_basic(basic_lines)
        cert_rows = parse_certs(cert_lines)
        print(f"[FAA-Airmen-Sync] Parsed {len(basic_rows):,} basic / {len(cert_rows):,} cert rows")

        # 4. Write staging
        conn = _get_conn()
        bulk_insert_basic(conn, basic_rows)
        bulk_insert_certs(conn, cert_rows)

        # 5. Validate row counts
        validate_row_counts(conn, len(basic_rows), len(cert_rows))

        # 6. Atomic swap
        atomic_swap(conn, len(basic_rows), len(cert_rows))

        elapsed = round(time.time() - start, 2)

        # 7. Metrics — success
        emit_metric('IngestSuccess', 1)
        emit_metric('RowCountBasic', len(basic_rows))
        emit_metric('RowCountCerts', len(cert_rows))
        emit_metric('IngestDurationSeconds', elapsed, unit='Seconds')

        result = {
            'statusCode': 200,
            'message': 'FAA airmen sync completed',
            'basicRows': len(basic_rows),
            'certRows': len(cert_rows),
            's3Key': s3_key,
            'elapsedSeconds': elapsed,
        }
        print(f"[FAA-Airmen-Sync] Done: {json.dumps(result)}")
        return result

    except Exception as e:
        elapsed = round(time.time() - start, 2)
        emit_metric('IngestFailure', 1)
        emit_metric('IngestDurationSeconds', elapsed, unit='Seconds')
        print(f"[FAA-Airmen-Sync] ERROR after {elapsed}s: {e}")
        return {
            'statusCode': 500,
            'error': str(e),
            'elapsedSeconds': elapsed,
        }
    finally:
        if conn is not None:
            _return_conn(conn)
