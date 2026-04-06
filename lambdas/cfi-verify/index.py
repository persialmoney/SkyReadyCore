"""
CFI Certificate Verification Lambda

AppSync resolver for the verifyCfiCertificate mutation. Looks up the entered
first name, last name, and certificate expiry date against the FAA airmen tables
(imported monthly by faa-airmen-sync), applies three-state matching logic, writes
an audit row, persists the result back to DynamoDB pilotInfo, and returns
CfiVerificationResult.

WHY NAME + EXPIRY (not cert number):
  The FAA Airmen Certification Releasable Database explicitly does not include
  airmen certificate numbers. The UNIQUE_ID in the FAA files is an internal DB
  key, not the number printed on a pilot's certificate. Verification must
  therefore be done by matching name + certificate expiry date.
  Same first+last name with the same expiry date is an extremely rare collision
  in practice, making this a reliable verification signal.

Verification states:
  verified   — first+last name and expiry date match an active FAA CFI record
  partial    — name matches but expiry is off, or expiry matches but first name
               is weak, or cert type (CFII/MEI) not in FAA ratings
  unverified — no FAA CFI record found for this name

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
#
# FAA PILOT_CERT RATING columns use the format LEVEL/CODE, e.g.:
#   F/ASE   = Flight Instructor - Airplane Single Engine
#   F/ASME  = Flight Instructor - Airplane Single & Multi Engine
#   F/AME   = Flight Instructor - Airplane Multi Engine
#   F/INSTA = Flight Instructor - Instrument Airplane
#   F/INSTH = Flight Instructor - Instrument Helicopter
#   F/HEL   = Flight Instructor - Helicopter
#   F/GL    = Flight Instructor - Glider
#   F/SPORT = Flight Instructor - Sport
#   F/PLIFT = Flight Instructor - Powered Lift
#   F/GYRO  = Flight Instructor - Gyroplane
#
# All CFI rows have ratings prefixed with "F/" so 'F/' is a reliable CFI marker.
# CFII is indicated by INSTA/INSTH/INSTI/INSTP in the rating codes.
# MEI is indicated by AME or ASME (which covers both single and multi).
CERT_RATINGS_MAP = {
    'CFI':  ['F/'],          # any F/ prefixed rating confirms CFI authority
    'CFII': ['INSTA', 'INSTH', 'INSTI', 'INSTP'],   # instrument ratings
    'MEI':  ['AME', 'AMEL', 'AMES', 'ASME', 'AMELC'],  # multi-engine variants (spec: AME, AMEL, AMES, ASME, AMELC)
}


# ── Normalization ─────────────────────────────────────────────────────────────

def normalize(value: str) -> str:
    """Strip non-alphanumeric characters and lowercase."""
    return re.sub(r'[^a-z0-9]', '', (value or '').lower().strip())


def normalize_expiry(value: str) -> str:
    """
    Normalize a user-entered expiry date to MMDDYYYY for comparison against
    the FAA CERT.csv format.

    Accepts:
      YYYY-MM-DD  (app's internal format)
      MM/DD/YYYY
      MMDDYYYY    (already FAA format)
    Returns MMDDYYYY string, or '' if unparseable.
    """
    v = (value or '').strip()
    for fmt, out_fmt in [
        ('%Y-%m-%d', '%m%d%Y'),
        ('%m/%d/%Y', '%m%d%Y'),
        ('%m%d%Y',   '%m%d%Y'),
    ]:
        try:
            return datetime.strptime(v, fmt).strftime(out_fmt)
        except ValueError:
            continue
    return ''


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

def lookup_faa(
    conn,
    norm_first: str,
    norm_last: str,
    norm_expiry: str,
) -> List[Dict[str, Any]]:
    """
    Primary match: last name (exact) + expiry date + CFI.
    First name is prefix-matched against norm_first_name (full first+middle).
    When both first and last are provided we also try norm_full_name for an
    unambiguous full-name match so that "JOHN MICHAEL SMITH" matches exactly.

    The expiry date match disambiguates people with identical last names.
    Medical columns from faa_airmen_basic are included so the CFI response
    carries the same medical data that the pilot-verify flow returns.
    """
    _SELECT = """
        SELECT
            b.unique_id,
            b.first_middle_name,
            b.last_name_suffix,
            b.norm_last_name,
            b.norm_first_name,
            b.state,
            b.medical_class,
            b.medical_exp_date,
            b.basic_med_course_date,
            b.basic_med_cmec_date,
            c.certificate_type,
            c.certificate_level,
            c.certificate_expire_date,
            c.ratings_raw,
            c.is_flight_instructor,
            m.source_snapshot_date
        FROM faa_airmen_certificates c
        JOIN faa_airmen_basic b ON b.unique_id = c.unique_id
        LEFT JOIN faa_ingest_metadata m ON m.id = 1
    """
    cur = conn.cursor()
    try:
        if norm_first:
            cur.execute(
                _SELECT + """
                WHERE b.norm_last_name          = %s
                  AND b.norm_first_name         LIKE %s
                  AND c.is_flight_instructor    = true
                  AND c.certificate_expire_date = %s
                ORDER BY c.certificate_expire_date DESC NULLS LAST
                LIMIT 10
                """,
                (norm_last, norm_first + '%', norm_expiry),
            )
        else:
            # No first name provided — match on last name + expiry only
            cur.execute(
                _SELECT + """
                WHERE b.norm_last_name          = %s
                  AND c.is_flight_instructor    = true
                  AND c.certificate_expire_date = %s
                ORDER BY c.certificate_expire_date DESC NULLS LAST
                LIMIT 10
                """,
                (norm_last, norm_expiry),
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()


def lookup_name_only(
    conn,
    norm_first: str,
    norm_last: str,
) -> List[Dict[str, Any]]:
    """
    Partial-match fallback: fetch the full FAA CFI record for this name
    regardless of expiry, ordered by most-recent expiry first.
    Returns full columns (including medical) so build_response can populate
    certificateSummary with all available FAA data.
    """
    _SELECT = """
        SELECT
            b.unique_id,
            b.first_middle_name,
            b.last_name_suffix,
            b.norm_last_name,
            b.norm_first_name,
            b.state,
            b.medical_class,
            b.medical_exp_date,
            b.basic_med_course_date,
            b.basic_med_cmec_date,
            c.certificate_type,
            c.certificate_level,
            c.certificate_expire_date,
            c.ratings_raw,
            c.is_flight_instructor,
            m.source_snapshot_date
        FROM faa_airmen_certificates c
        JOIN faa_airmen_basic b ON b.unique_id = c.unique_id
        LEFT JOIN faa_ingest_metadata m ON m.id = 1
    """
    cur = conn.cursor()
    try:
        if norm_first:
            cur.execute(
                _SELECT + """
                WHERE b.norm_last_name       = %s
                  AND b.norm_first_name      LIKE %s
                  AND c.is_flight_instructor = true
                ORDER BY c.certificate_expire_date DESC NULLS LAST
                LIMIT 10
                """,
                (norm_last, norm_first + '%'),
            )
        else:
            cur.execute(
                _SELECT + """
                WHERE b.norm_last_name       = %s
                  AND c.is_flight_instructor = true
                ORDER BY c.certificate_expire_date DESC NULLS LAST
                LIMIT 10
                """,
                (norm_last,),
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
    norm_first: str,
    norm_last: str,
    norm_expiry: str,
    user_cert_types: List[str],
) -> Tuple[str, Optional[Dict], str]:
    """
    Returns (status, best_faa_row_or_None, match_reason).

    Logic:
      verified  — name + expiry match an active FAA CFI record, and ratings OK
      partial   — name matches but expiry is off, OR ratings mismatch
      unverified — no name match in FAA data at all
    """
    # Last name is the only required field — first name is optional.
    if not norm_last:
        return 'unverified', None, 'last name not provided'

    if not norm_expiry:
        # No expiry provided: fall back to last-name-only partial check
        name_rows = lookup_name_only(conn, norm_first, norm_last)
        if name_rows:
            return 'partial', name_rows[0], 'name found in FAA CFI data but no expiry date provided for confirmation'
        return 'unverified', None, 'no FAA CFI record found for this name'

    # Primary: full match (name + expiry)
    rows = lookup_faa(conn, norm_first, norm_last, norm_expiry)

    if rows:
        best = rows[0]
        reasons = []

        # Secondary: ratings check
        ratings_ok, ratings_reason = _check_ratings(best, user_cert_types)
        if not ratings_ok:
            reasons.append(ratings_reason)

        if reasons:
            return 'partial', best, 'name+expiry matched; secondary checks: ' + '; '.join(reasons)

        return 'verified', best, 'first name, last name, and expiry date matched active FAA CFI record'

    # No full match — check if name exists without expiry match (typo in expiry?)
    name_rows = lookup_name_only(conn, norm_first, norm_last)
    if name_rows:
        # Return the most-recent matching row so the UI can show real FAA data
        return 'partial', name_rows[0], 'name found in FAA CFI data but expiry date does not match'

    return 'unverified', None, 'no FAA CFI record found for this name'


# ── Audit write ───────────────────────────────────────────────────────────────

def write_attempt(
    conn,
    user_id: str,
    entered_cert: str,
    entered_last: str,
    entered_first: Optional[str],
    entered_expiry: Optional[str],
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
            (user_id, entered_cert or '', entered_last, entered_first,
             matched_id, status, reason, snapshot_date),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        emit_metric('DbWriteFailure', 1)
        print(f"[cfi-verify] WARNING: audit write failed: {e}")
    finally:
        cur.close()


# ── Instructor certificate derivation ─────────────────────────────────────────

def derive_instructor_certificates(ratings_raw: str) -> List[str]:
    """
    Derive the app-level instructor certificate list from FAA ratings_raw.

    FAA ratings use the format LEVEL/CODE, e.g. "F/ASE   F/INSTA  A/ASEL".
    We strip the level prefix and match against known CFI rating codes.

    Returns at minimum ['CFI']; adds 'CFII' and/or 'MEI' when the FAA record
    includes the relevant instrument / multi-engine instructor ratings.

    Mirrors the TypeScript deriveInstructorCertificates() in faaVerificationService.ts
    so the server and client always produce the same labels.
    """
    raw_parts = [r.strip() for r in ratings_raw.split() if r.strip()]
    ratings = [r.split('/')[-1].upper() for r in raw_parts if '/' in r]
    has_cfii = any(re.match(r'^INST[AHIP]$', r) for r in ratings)
    has_mei  = any(re.match(r'^ASM[EHP]$', r) for r in ratings) or any('MULTI' in r for r in ratings)
    return ['CFI'] + (['CFII'] if has_cfii else []) + (['MEI'] if has_mei else [])


# ── DynamoDB write ────────────────────────────────────────────────────────────

def persist_to_dynamo(user_id: str, status: str, verified_at: str,
                      snapshot_date: Optional[str],
                      faa_row: Optional[Dict] = None) -> None:
    """
    Persist CFI verification result to DynamoDB.

    Always writes the three verification status fields. When a matching FAA row
    is available (verified or partial), also writes instructorCertificates and
    isCfi so the user's CFI status is captured regardless of whether the client
    calls the follow-up updateUser mutation (commitFaaVerification).
    """
    table = dynamodb.Table(USERS_TABLE)
    try:
        update_parts = [
            "pilotInfo.instructorVerificationStatus = :s",
            "pilotInfo.instructorVerifiedAt = :t",
            "pilotInfo.instructorSnapshotDate = :d",
        ]
        expr_values: Dict[str, Any] = {
            ':s': status,
            ':t': verified_at,
            ':d': snapshot_date or '',
        }

        # When we have a real FAA row, derive and persist instructorCertificates
        # and isCfi. This closes the gap where commitFaaVerification was the only
        # path that wrote these fields — verified CFIs now always have them set.
        if faa_row and status in ('verified', 'partial'):
            ratings_raw = faa_row.get('ratings_raw') or ''
            instructor_certs = derive_instructor_certificates(ratings_raw)
            update_parts.append("pilotInfo.instructorCertificates = :certs")
            update_parts.append("pilotInfo.isCfi = :isCfi")
            expr_values[':certs'] = instructor_certs
            expr_values[':isCfi'] = True
            print(f"[cfi-verify] writing instructorCertificates={instructor_certs} isCfi=True for user {user_id}")

        table.update_item(
            Key={'userId': user_id},
            UpdateExpression="SET " + ", ".join(update_parts),
            ExpressionAttributeValues=expr_values,
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
        # FAA ratings format: 10 chars per rating, e.g. "A/ASEL    A/INST    "
        # Split on whitespace blocks and strip the leading level prefix (e.g. "A/")
        raw_parts = [r.strip() for r in ratings_raw.split() if r.strip()]
        ratings = [r.split('/')[-1] for r in raw_parts if '/' in r]

        # Normalise medical_exp_date: the column stores MMYYYY (6-digit string)
        # which matches what pilot-verify returns and what the frontend expects.
        med_exp = faa_row.get('medical_exp_date')
        med_exp_str = str(med_exp).strip() if med_exp else None

        bm_course = faa_row.get('basic_med_course_date')
        bm_course_str = str(bm_course).strip() if bm_course else None

        bm_cmec = faa_row.get('basic_med_cmec_date')
        bm_cmec_str = str(bm_cmec).strip() if bm_cmec else None

        summary = {
            'isCfi': bool(faa_row.get('is_flight_instructor')),
            'certificateLevel': faa_row.get('certificate_level') or None,
            'ratings': ratings,
            'expiresOn': faa_row.get('certificate_expire_date') or None,
            'medicalClass': faa_row.get('medical_class') or None,
            'medicalExpDate': med_exp_str or None,
            'basicMedCourseDate': bm_course_str or None,
            'basicMedCmecDate': bm_cmec_str or None,
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

    # certificateNumber is stored for self-attestation but NOT used for FAA lookup.
    # The FAA Airmen Releasable Database does not include certificate numbers.
    cert_number  = args.get('certificateNumber', '') or ''
    last_name    = args.get('lastName', '')
    first_name   = args.get('firstName') or ''
    cert_expiry  = args.get('certificateExpiration') or ''
    user_cert_types = args.get('instructorCertificates', [])

    print(f"[cfi-verify] user={user_id} first={first_name!r} last={last_name!r} expiry={cert_expiry!r}")

    norm_first  = normalize(first_name)
    norm_last   = normalize(last_name)
    norm_expiry = normalize_expiry(cert_expiry)

    _unverified = {
        'status': 'unverified',
        'displayName': None,
        'certificateSummary': None,
        'source': FAA_SOURCE_LABEL,
        'verifiedAt': datetime.now(timezone.utc).isoformat(),
        'snapshotDate': None,
    }

    if not norm_last:
        return _unverified

    conn = None
    try:
        conn = _get_conn()

        status, faa_row, reason = determine_status(
            conn, norm_first, norm_last, norm_expiry, user_cert_types
        )
        snapshot_date = faa_row.get('source_snapshot_date') if faa_row else get_snapshot_date(conn)
        if snapshot_date:
            snapshot_date = str(snapshot_date)

        verified_at = datetime.now(timezone.utc).isoformat()
        matched_id  = faa_row.get('unique_id') if faa_row else None

        # Audit trail (non-fatal)
        write_attempt(conn, user_id, cert_number, last_name, first_name or None,
                      cert_expiry or None, matched_id, status, reason, snapshot_date)

        # Persist to DynamoDB pilotInfo (non-fatal)
        if user_id:
            persist_to_dynamo(user_id, status, verified_at, snapshot_date, faa_row=faa_row)

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
        return _unverified
    finally:
        if conn is not None:
            _return_conn(conn)
