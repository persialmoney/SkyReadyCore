"""
CFI Certificate Verification Lambda

AppSync resolver for the verifyCfiCertificate mutation. Looks up the entered
certificate number and last name against the FAA airmen tables (imported daily
by faa-airmen-sync), applies three-state matching logic, writes an audit row,
persists the result back to DynamoDB pilotInfo, and returns CfiVerificationResult.

Verification states:
  verified   — cert# and last name match an active FAA flight instructor record
  partial    — cert# matches but name/ratings check is weak, or vice versa
  unverified — no FAA match found

Metrics emitted to CloudWatch namespace: SkyReady/FAA/{STAGE}
"""
import json
import os
import re
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3

# ── AWS clients ───────────────────────────────────────────────────────────────

dynamodb = boto3.resource('dynamodb')
cw_client = boto3.client('cloudwatch')

# ── Environment ───────────────────────────────────────────────────────────────

STAGE = os.environ.get('STAGE', 'dev')
USERS_TABLE = os.environ.get('USERS_TABLE', f'sky-ready-users-{STAGE}')

METRIC_NAMESPACE = f'SkyReady/FAA/{STAGE}'

FAA_SOURCE_LABEL = 'FAA Airmen Certification Releasable Database'

# In-app certificate labels → known FAA ratings substrings for secondary check.
# If the user selected a cert type that should be present in ratings_raw but
# is not found, we downgrade to partial.
CERT_RATINGS_MAP = {
    'CFI':  ['CFI', 'FLIGHT INSTRUCTOR'],
    'CFII': ['CFII', 'INSTRUMENT'],
    'MEI':  ['MEI', 'MULTI'],
}


# ── Normalization ─────────────────────────────────────────────────────────────

def normalize(value: str) -> str:
    """Strip non-alphanumeric characters and lowercase."""
    return re.sub(r'[^a-z0-9]', '', (value or '').lower().strip())


# ── Metrics ───────────────────────────────────────────────────────────────────

def emit_metric(name: str, value: float, unit: str = 'Count',
                extra_dims: Optional[List[Dict]] = None) -> None:
    dims = [{'Name': 'Stage', 'Value': STAGE}]
    if extra_dims:
        dims.extend(extra_dims)
    try:
        cw_client.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[{
                'MetricName': name,
                'Value': value,
                'Unit': unit,
                'Dimensions': dims,
            }],
        )
    except Exception as e:
        print(f"[cfi-verify] WARNING: failed to emit metric {name}: {e}")


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_conn():
    import db_utils  # type: ignore  # provided by Lambda layer
    return db_utils.get_db_connection()


def _return_conn(conn) -> None:
    try:
        import db_utils  # type: ignore
        db_utils.return_db_connection(conn)
    except Exception:
        pass


# ── FAA lookup ────────────────────────────────────────────────────────────────

def lookup_faa(conn, norm_cert: str, norm_last: str) -> List[Dict[str, Any]]:
    """
    Primary match: cert number + last name + is_flight_instructor.
    Returns all matching rows joined from both tables.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                b.unique_id,
                b.first_middle_name,
                b.last_name_suffix,
                b.norm_last_name,
                c.certificate_type,
                c.certificate_level,
                c.certificate_expire_date,
                c.ratings_raw,
                c.is_flight_instructor,
                c.norm_cert_number,
                m.source_snapshot_date
            FROM faa_airmen_certificates c
            JOIN faa_airmen_basic b ON b.unique_id = c.unique_id
            LEFT JOIN faa_ingest_metadata m ON m.id = 1
            WHERE c.norm_cert_number = %s
              AND b.norm_last_name   = %s
              AND c.is_flight_instructor = true
            ORDER BY c.certificate_expire_date DESC NULLS LAST
            LIMIT 10
            """,
            (norm_cert, norm_last),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()


def lookup_cert_number_only(conn, norm_cert: str) -> List[Dict[str, Any]]:
    """Partial-match fallback: cert number matches but we didn't check last name."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT b.norm_last_name, c.is_flight_instructor
            FROM faa_airmen_certificates c
            JOIN faa_airmen_basic b ON b.unique_id = c.unique_id
            WHERE c.norm_cert_number = %s
            LIMIT 5
            """,
            (norm_cert,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()


def get_snapshot_date(conn) -> Optional[str]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT source_snapshot_date FROM faa_ingest_metadata WHERE id = 1")
        row = cur.fetchone()
        return str(row[0]) if row else None
    finally:
        cur.close()


# ── Matching logic ────────────────────────────────────────────────────────────

def _is_expired(expire_date_str: Optional[str]) -> bool:
    if not expire_date_str:
        return False
    try:
        # FAA uses YYYYMMDD or similar; try both formats
        for fmt in ('%Y%m%d', '%Y-%m-%d', '%m/%d/%Y'):
            try:
                exp = datetime.strptime(expire_date_str.strip(), fmt).date()
                return exp < date.today()
            except ValueError:
                continue
    except Exception:
        pass
    return False


def _check_ratings(faa_row: Dict, user_cert_types: List[str]) -> Tuple[bool, str]:
    """Return (ok, reason) — ok=False means ratings inconsistency → partial."""
    if not user_cert_types:
        return True, ''
    ratings_raw = (faa_row.get('ratings_raw') or '').upper()
    for cert in user_cert_types:
        expected_substrings = CERT_RATINGS_MAP.get(cert, [])
        if expected_substrings and not any(s in ratings_raw for s in expected_substrings):
            return False, f"selected {cert} not reflected in FAA ratings"
    return True, ''


def determine_status(
    conn,
    norm_cert: str,
    norm_last: str,
    norm_first: Optional[str],
    user_cert_types: List[str],
) -> Tuple[str, Optional[Dict], str]:
    """
    Returns (status, best_faa_row_or_None, match_reason).
    """
    rows = lookup_faa(conn, norm_cert, norm_last)

    if rows:
        # Pick best row: prefer non-expired, then latest expiry
        best = next(
            (r for r in rows if not _is_expired(r.get('certificate_expire_date'))),
            rows[0],
        )

        reasons = []

        # Secondary: first name
        if norm_first:
            faa_first = normalize(best.get('first_middle_name') or '')
            # Match if norm_first is a prefix of the FAA first+middle field
            if not faa_first.startswith(norm_first) and norm_first not in faa_first:
                reasons.append('first name mismatch')

        # Secondary: ratings
        ratings_ok, ratings_reason = _check_ratings(best, user_cert_types)
        if not ratings_ok:
            reasons.append(ratings_reason)

        # Secondary: expiration
        if _is_expired(best.get('certificate_expire_date')):
            reasons.append('instructor certificate expired')

        if reasons:
            return 'partial', best, 'primary match succeeded; secondary checks: ' + '; '.join(reasons)

        return 'verified', best, 'cert# and last name matched active FAA instructor record'

    # No primary match — check if cert# exists at all (different name)
    cert_only_rows = lookup_cert_number_only(conn, norm_cert)
    if cert_only_rows:
        return 'partial', None, 'cert# found in FAA data but last name does not match'

    return 'unverified', None, 'no FAA record found for this cert# and last name'


# ── Audit write ───────────────────────────────────────────────────────────────

def write_attempt(
    conn,
    user_id: str,
    entered_cert: str,
    entered_last: str,
    entered_first: Optional[str],
    matched_id: Optional[str],
    status: str,
    reason: str,
    snapshot_date: Optional[str],
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO cfi_verification_attempts
                (user_id, entered_cert_number, entered_last_name, entered_first_name,
                 matched_unique_id, verification_status, match_reason, source_snapshot_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (user_id, entered_cert, entered_last, entered_first,
             matched_id, status, reason, snapshot_date),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        emit_metric('DbWriteFailure', 1)
        # Non-fatal: log but don't fail the verification response
        print(f"[cfi-verify] WARNING: audit write failed: {e}")
    finally:
        cur.close()


# ── DynamoDB write ────────────────────────────────────────────────────────────

def persist_to_dynamo(user_id: str, status: str, verified_at: str,
                      snapshot_date: Optional[str]) -> None:
    table = dynamodb.Table(USERS_TABLE)
    try:
        table.update_item(
            Key={'userId': user_id},
            UpdateExpression=(
                "SET pilotInfo.instructorVerificationStatus = :s, "
                "    pilotInfo.instructorVerifiedAt = :t, "
                "    pilotInfo.instructorSnapshotDate = :d"
            ),
            ExpressionAttributeValues={
                ':s': status,
                ':t': verified_at,
                ':d': snapshot_date or '',
            },
        )
    except Exception as e:
        emit_metric('DbWriteFailure', 1)
        print(f"[cfi-verify] WARNING: DynamoDB pilotInfo update failed: {e}")


# ── Response builder ──────────────────────────────────────────────────────────

def build_response(status: str, faa_row: Optional[Dict],
                   verified_at: str, snapshot_date: Optional[str]) -> Dict[str, Any]:
    summary = None
    display_name = None

    if faa_row:
        display_name = ' '.join(filter(None, [
            faa_row.get('first_middle_name', '').strip(),
            faa_row.get('last_name_suffix', '').strip(),
        ])) or None

        ratings_raw = faa_row.get('ratings_raw') or ''
        ratings = [r.strip() for r in ratings_raw.split('/') if r.strip()]

        summary = {
            'isCfi': bool(faa_row.get('is_flight_instructor')),
            'ratings': ratings,
            'expiresOn': faa_row.get('certificate_expire_date') or None,
        }

    return {
        'status': status,
        'displayName': display_name,
        'certificateSummary': summary,
        'source': FAA_SOURCE_LABEL,
        'verifiedAt': verified_at,
        'snapshotDate': snapshot_date,
    }


# ── Handler ───────────────────────────────────────────────────────────────────

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start = time.time()

    args = event.get('arguments', {}).get('input', {})
    user_id = event.get('identity', {}).get('sub') or event.get('userId', '')

    cert_number = args.get('certificateNumber', '')
    last_name = args.get('lastName', '')
    first_name = args.get('firstName') or None
    # User-selected instructor cert types for secondary ratings check
    user_cert_types = args.get('instructorCertificates', [])

    print(f"[cfi-verify] user={user_id} cert={cert_number!r} last={last_name!r}")

    norm_cert = normalize(cert_number)
    norm_last = normalize(last_name)
    norm_first = normalize(first_name) if first_name else None

    if not norm_cert or not norm_last:
        return {
            'status': 'unverified',
            'displayName': None,
            'certificateSummary': None,
            'source': FAA_SOURCE_LABEL,
            'verifiedAt': datetime.now(timezone.utc).isoformat(),
            'snapshotDate': None,
        }

    conn = None
    try:
        conn = _get_conn()

        status, faa_row, reason = determine_status(
            conn, norm_cert, norm_last, norm_first, user_cert_types
        )
        snapshot_date = faa_row.get('source_snapshot_date') if faa_row else get_snapshot_date(conn)
        if snapshot_date:
            snapshot_date = str(snapshot_date)

        verified_at = datetime.now(timezone.utc).isoformat()
        matched_id = faa_row.get('unique_id') if faa_row else None

        # Audit trail (non-fatal)
        write_attempt(conn, user_id, cert_number, last_name, first_name,
                      matched_id, status, reason, snapshot_date)

        # Persist to DynamoDB pilotInfo (non-fatal)
        if user_id:
            persist_to_dynamo(user_id, status, verified_at, snapshot_date)

        elapsed_ms = round((time.time() - start) * 1000)
        emit_metric('VerificationAttempt', 1, extra_dims=[{'Name': 'Status', 'Value': status}])
        emit_metric('VerifyDurationMs', elapsed_ms, unit='Milliseconds')

        print(f"[cfi-verify] status={status} reason={reason!r} elapsed={elapsed_ms}ms")
        return build_response(status, faa_row, verified_at, snapshot_date)

    except Exception as e:
        elapsed_ms = round((time.time() - start) * 1000)
        emit_metric('DbReadFailure', 1)
        emit_metric('VerifyDurationMs', elapsed_ms, unit='Milliseconds')
        print(f"[cfi-verify] ERROR: {e}")
        # Return unverified rather than raising — verification is never blocking
        return {
            'status': 'unverified',
            'displayName': None,
            'certificateSummary': None,
            'source': FAA_SOURCE_LABEL,
            'verifiedAt': datetime.now(timezone.utc).isoformat(),
            'snapshotDate': None,
        }
    finally:
        if conn is not None:
            _return_conn(conn)
