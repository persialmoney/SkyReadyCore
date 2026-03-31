"""
Pilot Certificate Verification Lambda

AppSync resolver for the verifyPilotCertificate mutation. Looks up the user's
first name, last name, certificate level (Private/Commercial/ATP), and medical
details against the FAA Airmen Certification Releasable Database, applies
three-state matching logic, writes an audit row, persists the result to
DynamoDB pilotInfo, and returns PilotVerificationResult.

WHY NAME + MEDICAL (not cert number):
  The FAA Airmen Certification Releasable Database does not include airmen
  certificate numbers. Pilot certificates (type 'P') also carry no expiry date
  (unlike CFI 'F' certs). Verification must therefore rely on:
    1. First + last name match (norm_last_name exact, norm_first_name LIKE prefix)
    2. Certificate level match (P/A/C/P → ATP/Commercial/Private)
    3. Medical class and/or medical expiry date from PILOT_BASIC.csv

  If the user also holds a CFI certificate, the CFI expiry date is returned as
  an additional signal but is not required for pilot-level verification.

Verification states:
  verified   — name + cert level match AND medical class + medical date/expiry
               match the FAA record within tolerance
  partial    — name + cert level match but medical info is missing or off, OR
               name matches but cert level not found for this person
  unverified — no FAA record found for this name + cert level combination

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

# Medical date tolerance in days — FAA stores MMYYYY so we allow ±60 days to
# account for date parsing ambiguity (we pin to 1st of the month).
MEDICAL_DATE_TOLERANCE_DAYS = 60

# Valid pilot certificate levels accepted as input.
VALID_CERT_LEVELS = {'A', 'C', 'P'}

# Expected medical class per certificate level.
EXPECTED_MEDICAL_CLASS = {'A': '1', 'C': '2', 'P': '3'}


# ── Normalization ─────────────────────────────────────────────────────────────

def normalize(value: str) -> str:
    """Strip non-alphanumeric characters and lowercase."""
    return re.sub(r'[^a-z0-9]', '', (value or '').lower().strip())


def parse_medical_date(value: str) -> Optional[date]:
    """
    Parse a medical date into a Python date object.

    FAA stores medical dates as MMYYYY (6 digits). Users may provide:
      MMYYYY      — FAA format, pinned to 1st of month
      YYYY-MM-DD  — ISO 8601 (pinned to 1st of month for MMYYYY comparison)
      MM/YYYY     — common pilot format
      MM/DD/YYYY  — full date
    Returns None if unparseable.
    """
    v = (value or '').strip()
    for fmt in ['%m%Y', '%Y-%m-%d', '%m/%Y', '%m/%d/%Y']:
        try:
            d = datetime.strptime(v, fmt).date()
            # For formats that give day precision, snap to 1st of month so that
            # a user entering "03/2024" compares fairly to FAA's "032024".
            return d.replace(day=1)
        except ValueError:
            continue
    return None


def dates_within_tolerance(d1: Optional[date], d2: Optional[date]) -> bool:
    if d1 is None or d2 is None:
        return False
    return abs((d1 - d2).days) <= MEDICAL_DATE_TOLERANCE_DAYS


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
        print(f"[pilot-verify] WARNING: failed to emit metric {name}: {e}")


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

_SELECT_PILOT = """
    SELECT
        b.unique_id,
        b.first_middle_name,
        b.last_name_suffix,
        b.norm_last_name,
        b.norm_first_name,
        b.norm_full_name,
        b.state,
        b.medical_class,
        b.medical_date,
        b.medical_exp_date,
        b.basic_med_course_date,
        b.basic_med_cmec_date,
        c_pilot.certificate_type,
        c_pilot.certificate_level,
        c_pilot.ratings_raw,
        c_cfi.certificate_expire_date  AS cfi_expiry,
        m.source_snapshot_date
    FROM faa_airmen_basic b
    JOIN faa_airmen_certificates c_pilot
        ON  c_pilot.unique_id         = b.unique_id
        AND c_pilot.certificate_type  = 'P'
        AND c_pilot.certificate_level = %s
    LEFT JOIN faa_airmen_certificates c_cfi
        ON  c_cfi.unique_id        = b.unique_id
        AND c_cfi.certificate_type = 'F'
    LEFT JOIN faa_ingest_metadata m ON m.id = 1
"""


def lookup_pilot(
    conn,
    norm_first: str,
    norm_last: str,
    cert_level: str,
) -> List[Dict[str, Any]]:
    """
    Match by last name (exact) + first+middle prefix + pilot certificate level.
    Returns up to 10 candidates for post-filtering on medical details.
    """
    cur = conn.cursor()
    try:
        if norm_first:
            cur.execute(
                _SELECT_PILOT + """
                WHERE b.norm_last_name  = %s
                  AND b.norm_first_name LIKE %s
                ORDER BY b.norm_full_name
                LIMIT 10
                """,
                (cert_level, norm_last, norm_first + '%'),
            )
        else:
            cur.execute(
                _SELECT_PILOT + """
                WHERE b.norm_last_name = %s
                ORDER BY b.norm_full_name
                LIMIT 10
                """,
                (cert_level, norm_last),
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
    Fallback: check if name exists with ANY pilot cert (not filtered by level).
    Used to determine whether to return partial vs. unverified.
    """
    cur = conn.cursor()
    try:
        if norm_first:
            cur.execute(
                """
                SELECT b.norm_last_name, b.norm_first_name,
                       c.certificate_type, c.certificate_level
                FROM faa_airmen_basic b
                JOIN faa_airmen_certificates c ON c.unique_id = b.unique_id
                WHERE b.norm_last_name  = %s
                  AND b.norm_first_name LIKE %s
                  AND c.certificate_type = 'P'
                LIMIT 5
                """,
                (norm_last, norm_first + '%'),
            )
        else:
            cur.execute(
                """
                SELECT b.norm_last_name, b.norm_first_name,
                       c.certificate_type, c.certificate_level
                FROM faa_airmen_basic b
                JOIN faa_airmen_certificates c ON c.unique_id = b.unique_id
                WHERE b.norm_last_name  = %s
                  AND c.certificate_type = 'P'
                LIMIT 5
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

def _effective_exp_date(faa_row: Dict) -> Tuple[Optional[date], str]:
    """
    Return the best available expiry date from the FAA row and a label describing
    which field it came from. Falls back to BasicMed when standard medical exp is empty.

    Priority:
      1. medical_exp_date     — standard FAA medical expiry (class 1/2/3)
      2. basic_med_cmec_date  — BasicMed CMEC date (two-year validity window)
      3. basic_med_course_date — BasicMed course completion (weaker fallback)
    """
    med_exp_raw = (faa_row.get('medical_exp_date') or '').strip()
    if med_exp_raw:
        return parse_medical_date(med_exp_raw), 'standard medical'

    cmec_raw = (faa_row.get('basic_med_cmec_date') or '').strip()
    if cmec_raw:
        return parse_medical_date(cmec_raw), 'BasicMed CMEC'

    course_raw = (faa_row.get('basic_med_course_date') or '').strip()
    if course_raw:
        return parse_medical_date(course_raw), 'BasicMed course'

    return None, 'none'


def _score_medical_match(
    faa_row: Dict,
    user_medical_class: str,
    user_medical_date: Optional[date],
    user_medical_exp_date: Optional[date],
    cert_level: str,
) -> Tuple[int, List[str]]:
    """
    Score how well this FAA row matches the user's medical details.
    Returns (score 0-3, list of match reasons).

    Score:
      0  — nothing matches or medical class hard-fails
      1  — medical class matches
      2  — medical class + one date matches
      3  — medical class + both dates match (strongest)

    When standard medical_exp_date is empty, falls back to BasicMed dates
    (basic_med_cmec_date → basic_med_course_date) so pilots who operate
    under BasicMed instead of a standard medical can still verify.
    """
    score = 0
    reasons = []

    faa_med_class = (faa_row.get('medical_class') or '').strip()
    faa_med_date_raw = (faa_row.get('medical_date') or '').strip()

    # Medical class check — for BasicMed pilots the class field may be blank;
    # only hard-fail when both sides have a value and they disagree.
    if user_medical_class and faa_med_class:
        if user_medical_class == faa_med_class:
            score += 1
            reasons.append(f"medical class {faa_med_class} matches")
        else:
            reasons.append(f"medical class mismatch: user={user_medical_class} FAA={faa_med_class}")
            return 0, reasons  # Hard fail — wrong class
    elif user_medical_class and not faa_med_class:
        # FAA has no class (BasicMed pilot) — don't penalise but don't score
        reasons.append("FAA record has no medical class (possible BasicMed pilot)")

    # Medical issued date check
    faa_med_date = parse_medical_date(faa_med_date_raw)
    if user_medical_date and faa_med_date:
        if dates_within_tolerance(user_medical_date, faa_med_date):
            score += 1
            reasons.append("medical issued date matches")
        else:
            reasons.append(f"medical issued date off: user={user_medical_date} FAA={faa_med_date}")

    # Medical expiry date check — fall back to BasicMed if standard exp is absent
    faa_exp, exp_source = _effective_exp_date(faa_row)
    if user_medical_exp_date and faa_exp:
        if dates_within_tolerance(user_medical_exp_date, faa_exp):
            score += 1
            reasons.append(f"medical expiry matches ({exp_source})")
        else:
            reasons.append(f"medical expiry off ({exp_source}): user={user_medical_exp_date} FAA={faa_exp}")
    elif user_medical_exp_date and not faa_exp:
        reasons.append("FAA record has no medical expiry (may be BasicMed — cannot confirm expiry)")

    return score, reasons


def determine_status(
    conn,
    norm_first: str,
    norm_last: str,
    cert_level: str,
    user_medical_class: str,
    user_medical_date: Optional[date],
    user_medical_exp_date: Optional[date],
) -> Tuple[str, Optional[Dict], str]:
    """
    Returns (status, best_faa_row_or_None, match_reason).

    Logic:
      verified   — name + cert level found AND medical score >= 1
                   (class match with at least one date match)
      partial    — name + cert level found but no medical info provided,
                   OR medical class matches but dates are off,
                   OR name found but cert level doesn't match
      unverified — no FAA record found for this name at all
    """
    if not norm_last:
        return 'unverified', None, 'last name not provided'

    candidates = lookup_pilot(conn, norm_first, norm_last, cert_level)

    if not candidates:
        # No cert-level match — check if name exists with any pilot cert
        name_rows = lookup_name_only(conn, norm_first, norm_last)
        if name_rows:
            levels_found = list({r['certificate_level'] for r in name_rows})
            return (
                'partial',
                None,
                f"name found in FAA data but not at certificate level '{cert_level}'; "
                f"levels found: {levels_found}",
            )
        return 'unverified', None, 'no FAA record found for this name'

    # Score each candidate on medical details and pick the best
    no_medical_provided = not any([user_medical_class, user_medical_date, user_medical_exp_date])

    best_row = None
    best_score = -1
    best_reasons: List[str] = []

    for row in candidates:
        score, reasons = _score_medical_match(
            row, user_medical_class, user_medical_date, user_medical_exp_date, cert_level
        )
        if score > best_score:
            best_score = score
            best_row = row
            best_reasons = reasons

    if no_medical_provided:
        return (
            'partial',
            best_row,
            f"name + certificate level '{cert_level}' found in FAA data; "
            "no medical details provided for full verification",
        )

    # score >= 2 = medical class + at least one date → verified
    if best_score >= 2:
        return (
            'verified',
            best_row,
            f"name, certificate level, and medical details matched FAA record: "
            + '; '.join(best_reasons),
        )

    # score == 1 = only class matched, or class matched but dates off
    if best_score == 1:
        return (
            'partial',
            best_row,
            f"name + certificate level matched; medical partial: "
            + '; '.join(best_reasons),
        )

    # score == 0 with reasons means medical class mismatch (hard fail)
    return (
        'partial',
        best_row,
        f"name matched but medical details did not match: "
        + '; '.join(best_reasons),
    )


# ── Audit write ───────────────────────────────────────────────────────────────

def write_attempt(
    conn,
    user_id: str,
    entered_last: str,
    entered_first: Optional[str],
    cert_level: str,
    matched_id: Optional[str],
    status: str,
    reason: str,
    snapshot_date: Optional[str],
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO pilot_verification_attempts
                (user_id, entered_last_name, entered_first_name, certificate_level,
                 matched_unique_id, verification_status, match_reason, source_snapshot_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (user_id, entered_last, entered_first, cert_level,
             matched_id, status, reason, snapshot_date),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        emit_metric('DbWriteFailure', 1)
        print(f"[pilot-verify] WARNING: audit write failed (table may not exist yet): {e}")
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
                "SET pilotInfo.pilotVerificationStatus = :s, "
                "    pilotInfo.pilotVerifiedAt = :t, "
                "    pilotInfo.pilotSnapshotDate = :d"
            ),
            ExpressionAttributeValues={
                ':s': status,
                ':t': verified_at,
                ':d': snapshot_date or '',
            },
        )
    except Exception as e:
        emit_metric('DbWriteFailure', 1)
        print(f"[pilot-verify] WARNING: DynamoDB pilotInfo update failed: {e}")


# ── Response builder ──────────────────────────────────────────────────────────

def _parse_ratings(ratings_raw: str) -> List[str]:
    """
    Strip the level prefix from ratings (e.g. "A/ASEL" → "ASEL", "C/HEL" → "HEL").
    """
    parts = [r.strip() for r in (ratings_raw or '').split() if r.strip()]
    return [r.split('/')[-1] for r in parts if '/' in r]


def build_response(
    status: str,
    faa_row: Optional[Dict],
    verified_at: str,
    snapshot_date: Optional[str],
) -> Dict[str, Any]:
    summary = None
    display_name = None

    if faa_row:
        display_name = ' '.join(filter(None, [
            faa_row.get('first_middle_name', '').strip(),
            faa_row.get('last_name_suffix', '').strip(),
        ])) or None

        # Use the effective expiry (standard medical → BasicMed CMEC → BasicMed course)
        _, exp_source = _effective_exp_date(faa_row)
        summary = {
            'certificateLevel': faa_row.get('certificate_level') or '',
            'ratings': _parse_ratings(faa_row.get('ratings_raw') or ''),
            'medicalClass': faa_row.get('medical_class') or None,
            'medicalDate': faa_row.get('medical_date') or None,
            'medicalExpDate': faa_row.get('medical_exp_date') or None,
            'basicMedCourseDate': faa_row.get('basic_med_course_date') or None,
            'basicMedCmecDate': faa_row.get('basic_med_cmec_date') or None,
            'medicalSource': exp_source,  # which medical record was used
            'cfiExpiry': faa_row.get('cfi_expiry') or None,
        }

    return {
        'status': status,
        'displayName': display_name,
        'certificateSummary': summary,
        'source': FAA_SOURCE_LABEL,
        'verifiedAt': verified_at,
        'snapshotDate': str(snapshot_date) if snapshot_date else None,
    }


# ── Handler ───────────────────────────────────────────────────────────────────

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start = time.time()

    args = event.get('arguments', {}).get('input', {})
    user_id = event.get('identity', {}).get('sub') or event.get('userId', '')

    last_name       = args.get('lastName', '')
    first_name      = args.get('firstName') or ''
    cert_level      = (args.get('certificateLevel') or '').strip().upper()
    medical_class   = (args.get('medicalClass') or '').strip()
    medical_date_s  = args.get('medicalDate') or ''
    medical_exp_s   = args.get('medicalExpDate') or ''

    print(
        f"[pilot-verify] user={user_id} first={first_name!r} last={last_name!r} "
        f"level={cert_level!r} med_class={medical_class!r} "
        f"med_date={medical_date_s!r} med_exp={medical_exp_s!r}"
    )

    _unverified: Dict[str, Any] = {
        'status': 'unverified',
        'displayName': None,
        'certificateSummary': None,
        'source': FAA_SOURCE_LABEL,
        'verifiedAt': datetime.now(timezone.utc).isoformat(),
        'snapshotDate': None,
    }

    if not last_name:
        return _unverified

    if cert_level not in VALID_CERT_LEVELS:
        print(f"[pilot-verify] invalid certificateLevel: {cert_level!r}")
        return _unverified

    norm_first = normalize(first_name)
    norm_last  = normalize(last_name)
    med_date   = parse_medical_date(medical_date_s)
    med_exp    = parse_medical_date(medical_exp_s)

    conn = None
    try:
        conn = _get_conn()

        status, faa_row, reason = determine_status(
            conn, norm_first, norm_last, cert_level,
            medical_class, med_date, med_exp,
        )
        snapshot_date = (
            faa_row.get('source_snapshot_date') if faa_row else get_snapshot_date(conn)
        )
        if snapshot_date:
            snapshot_date = str(snapshot_date)

        verified_at = datetime.now(timezone.utc).isoformat()
        matched_id  = faa_row.get('unique_id') if faa_row else None

        # Audit trail (non-fatal — table may not exist in older environments)
        write_attempt(
            conn, user_id, last_name, first_name or None,
            cert_level, matched_id, status, reason, snapshot_date,
        )

        # Persist to DynamoDB pilotInfo (non-fatal)
        if user_id:
            persist_to_dynamo(user_id, status, verified_at, snapshot_date)

        elapsed_ms = round((time.time() - start) * 1000)
        emit_metric('VerificationAttempt', 1, extra_dims=[{'Name': 'Status', 'Value': status}])
        emit_metric('VerifyDurationMs', elapsed_ms, unit='Milliseconds')

        print(f"[pilot-verify] status={status} reason={reason!r} elapsed={elapsed_ms}ms")
        return build_response(status, faa_row, verified_at, snapshot_date)

    except Exception as e:
        elapsed_ms = round((time.time() - start) * 1000)
        emit_metric('DbReadFailure', 1)
        emit_metric('VerifyDurationMs', elapsed_ms, unit='Milliseconds')
        print(f"[pilot-verify] ERROR: {e}")
        return _unverified
    finally:
        if conn is not None:
            _return_conn(conn)
