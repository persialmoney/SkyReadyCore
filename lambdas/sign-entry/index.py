"""
Sign Entry Lambda - Privileged CFI signing operations.

Handles two GraphQL mutations:
  - signEntry(entryId, signature)  → marks a student's entry SIGNED, creates CFI mirror
  - returnEntry(entryId, returnNote) → marks a student's entry RETURNED for corrections

Authorization: caller must be the entry's assigned instructor_user_id.
Neither operation is bound by entry ownership (user_id), which is why these
cannot go through the standard sync-push path.
"""
import json
import hashlib
import os
import time
import uuid

import boto3
import psycopg
from psycopg.types.json import Jsonb

from db_utils import get_db_connection, return_db_connection


dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _js_number_str(value) -> str:
    """
    Replicate JavaScript's String(number) coercion.
    JS drops the decimal point for whole numbers: String(2.0) === "2", not "2.0".
    Python's str(2.0) === "2.0", so we must normalise here to match the client hash.
    """
    if value is None:
        return ''
    try:
        f = float(value)
        # Whole numbers: match JS behaviour (no trailing .0)
        if f == int(f):
            return str(int(f))
        return str(f)
    except (TypeError, ValueError):
        return str(value)


def _validate_signature_hash(entry_id, date, total_time, instructor_snapshot, signature):
    """Re-verify client-computed hash to prevent tampering."""
    snap = instructor_snapshot or {}
    hash_input = '|'.join([
        str(entry_id),
        str(date),
        _js_number_str(total_time),
        snap.get('name', ''),
        snap.get('certificateNumber', ''),
        snap.get('actingAs', ''),
        snap.get('certificateExpiration', ''),
        signature.get('signatureImage', ''),
        signature.get('timestamp', ''),
    ])
    expected = hashlib.sha256(hash_input.encode()).hexdigest()
    if signature.get('hash') != expected:
        raise ValueError(f"Signature hash mismatch for entry {entry_id}")


def _format_entry(row):
    """Map a DB row (full SELECT) to a GraphQL LogbookEntry dict."""
    return {
        'entryId':               str(row[0]),
        'userId':                row[1],
        'date':                  row[2].isoformat() if row[2] else None,
        'aircraft':              row[3],
        'tailNumber':            row[4],
        'route':                 row[5],
        'routeLegs':             row[6] if row[6] else [],
        'flightTypes':           row[7] if row[7] else [],
        'totalTime':             float(row[8]) if row[8] else 0,
        'pic':                   float(row[9]) if row[9] else 0,
        'sic':                   float(row[10]) if row[10] else 0,
        'dualReceived':          float(row[11]) if row[11] else 0,
        'dualGiven':             float(row[12]) if row[12] else 0,
        'solo':                  float(row[13]) if row[13] else 0,
        'crossCountry':          float(row[14]) if row[14] else 0,
        'night':                 float(row[15]) if row[15] else 0,
        'actualImc':             float(row[16]) if row[16] else 0,
        'simulatedInstrument':   float(row[17]) if row[17] else 0,
        'dayTakeoffs':           int(row[18]) if row[18] else 0,
        'dayLandings':           int(row[19]) if row[19] else 0,
        'nightTakeoffs':         int(row[20]) if row[20] else 0,
        'nightLandings':         int(row[21]) if row[21] else 0,
        'dayFullStopLandings':   int(row[22]) if row[22] else 0,
        'nightFullStopLandings': int(row[23]) if row[23] else 0,
        'approaches':            int(row[24]) if row[24] else 0,
        'holds':                 bool(row[25]) if row[25] is not None else False,
        'tracking':              bool(row[26]) if row[26] is not None else False,
        'instructorUserId':      row[27],
        'instructorSnapshot':    row[28],
        'studentUserId':         row[29],
        'studentSnapshot':       row[30],
        'mirroredFromEntryId':   str(row[31]) if row[31] else None,
        'mirroredFromUserId':    row[32],
        'lessonTopic':           row[33],
        'groundInstruction':     float(row[34]) if row[34] else 0,
        'maneuvers':             row[35] if row[35] else [],
        'remarks':               row[36],
        'safetyNotes':           row[37],
        'safetyRelevant':        bool(row[38]) if row[38] is not None else False,
        'status':                row[39],
        'signature':             row[40],
        'isFlightReview':        bool(row[41]) if row[41] is not None else False,
        'returnNote':            row[42],
        'createdAt':             float(int(row[43].timestamp() * 1000)),
        'updatedAt':             float(int(row[44].timestamp() * 1000)) if row[44] else float(int(row[43].timestamp() * 1000)),
    }


_ENTRY_SELECT = """
    SELECT
        entry_id, user_id, date, aircraft, tail_number,
        route, route_legs, flight_types, total_time,
        pic, sic, dual_received, dual_given, solo,
        cross_country, night, actual_imc, simulated_instrument,
        day_takeoffs, day_landings, night_takeoffs, night_landings,
        day_full_stop_landings, night_full_stop_landings,
        approaches, holds, tracking,
        instructor_user_id, instructor_snapshot, student_user_id, student_snapshot,
        mirrored_from_entry_id, mirrored_from_user_id,
        lesson_topic, ground_instruction,
        maneuvers, remarks, safety_notes, safety_relevant,
        status, signature, is_flight_review, return_note,
        created_at, updated_at
    FROM logbook_entries
    WHERE entry_id = %s AND deleted_at IS NULL
"""


def _fetch_and_authorize(cursor, entry_id, cfi_user_id, expected_status):
    """
    Fetch an entry and verify the caller is the assigned instructor.
    Returns the raw DB row.
    Raises ValueError on authorization or state mismatch.
    """
    cursor.execute(_ENTRY_SELECT, [entry_id])
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Entry {entry_id} not found")

    # Row index 27 = instructor_user_id
    if str(row[27] or '') != cfi_user_id:
        raise PermissionError(f"Caller {cfi_user_id} is not the assigned instructor for entry {entry_id}")

    # Row index 39 = status
    if row[39] != expected_status:
        raise ValueError(f"Entry {entry_id} is in status '{row[39]}', expected '{expected_status}'")

    return row


def _create_cfi_mirror_entry(cursor, row, student_user_id):
    """
    Insert a dual-given mirror entry in the CFI's own logbook.
    Idempotent — skips if a mirror already exists.
    """
    cfi_user_id = str(row[27])
    original_entry_id = str(row[0])

    cursor.execute(
        "SELECT entry_id FROM logbook_entries WHERE mirrored_from_entry_id = %s AND user_id = %s AND deleted_at IS NULL",
        [original_entry_id, cfi_user_id],
    )
    if cursor.fetchone():
        print(f"[sign-entry] Mirror already exists for {original_entry_id}")
        return

    mirror_id = str(uuid.uuid4())
    student_snap = row[30] or {}
    total_time = float(row[8]) if row[8] else 0
    dual_given = float(row[11]) if row[11] else 0  # student's dualReceived → CFI's dualGiven

    cursor.execute("""
        INSERT INTO logbook_entries (
            entry_id, user_id, date, aircraft, tail_number, route, route_legs,
            flight_types, total_time, pic, dual_given,
            cross_country, night, actual_imc, simulated_instrument,
            day_takeoffs, day_landings, night_takeoffs, night_landings,
            day_full_stop_landings, night_full_stop_landings,
            approaches, holds, tracking,
            student_user_id, student_snapshot,
            lesson_topic, ground_instruction, maneuvers,
            remarks, status, mirrored_from_entry_id, mirrored_from_user_id,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, NOW(), NOW()
        )
    """, [
        mirror_id, cfi_user_id, row[2],
        Jsonb(row[3]), row[4], row[5], Jsonb(row[6] or []),
        row[7] or [],
        total_time, total_time, dual_given,
        float(row[14]) if row[14] else 0,  # crossCountry
        float(row[15]) if row[15] else 0,  # night
        float(row[16]) if row[16] else 0,  # actualImc
        float(row[17]) if row[17] else 0,  # simulatedInstrument
        int(row[18]) if row[18] else 0,    # dayTakeoffs
        int(row[19]) if row[19] else 0,    # dayLandings
        int(row[20]) if row[20] else 0,    # nightTakeoffs
        int(row[21]) if row[21] else 0,    # nightLandings
        int(row[22]) if row[22] else 0,    # dayFullStopLandings
        int(row[23]) if row[23] else 0,    # nightFullStopLandings
        int(row[24]) if row[24] else 0,    # approaches
        bool(row[25]) if row[25] is not None else False,  # holds
        bool(row[26]) if row[26] is not None else False,  # tracking
        student_user_id, Jsonb(student_snap),
        row[33],                            # lessonTopic
        float(row[34]) if row[34] else 0,  # groundInstruction
        row[35] or [],                      # maneuvers
        row[36],                            # remarks
        'SAVED', original_entry_id, student_user_id,
    ])
    print(f"[sign-entry] Created CFI mirror entry {mirror_id} for original {original_entry_id}")


# ---------------------------------------------------------------------------
# Mutation handlers
# ---------------------------------------------------------------------------

def handle_sign_entry(event):
    cfi_user_id = event['identity']['claims']['sub']
    args = event['arguments']
    entry_id = args['entryId']
    sig_input = args['signature']

    print(f"[sign-entry] signEntry called by {cfi_user_id} for entry {entry_id}")

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        row = _fetch_and_authorize(cursor, entry_id, cfi_user_id, 'PENDING_SIGNATURE')

        # Validate the hash the client computed
        instructor_snapshot = row[28] or {}
        _validate_signature_hash(
            entry_id=entry_id,
            date=str(row[2]),
            total_time=row[8],
            instructor_snapshot=instructor_snapshot,
            signature=sig_input,
        )

        # Apply signature and flip status
        cursor.execute("""
            UPDATE logbook_entries
            SET status = 'SIGNED',
                signature = %s,
                updated_at = NOW()
            WHERE entry_id = %s AND deleted_at IS NULL
        """, [Jsonb(sig_input), entry_id])

        # Create the CFI's own dual-given mirror entry
        student_user_id = str(row[1])  # entry owner = student
        _create_cfi_mirror_entry(cursor, row, student_user_id)

        conn.commit()

        # Re-fetch the updated row to return the authoritative state
        cursor.execute(_ENTRY_SELECT, [entry_id])
        updated_row = cursor.fetchone()
        result = _format_entry(updated_row)
        print(f"[sign-entry] Entry {entry_id} signed successfully")
        return result

    except (ValueError, PermissionError) as e:
        conn.rollback()
        print(f"[sign-entry] Error signing entry {entry_id}: {e}")
        raise
    except Exception as e:
        conn.rollback()
        print(f"[sign-entry] Unexpected error for entry {entry_id}: {e}")
        raise
    finally:
        return_db_connection(conn)


def handle_return_entry(event):
    cfi_user_id = event['identity']['claims']['sub']
    args = event['arguments']
    entry_id = args['entryId']
    return_note = (args.get('returnNote') or '').strip()

    print(f"[sign-entry] returnEntry called by {cfi_user_id} for entry {entry_id}")

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        _fetch_and_authorize(cursor, entry_id, cfi_user_id, 'PENDING_SIGNATURE')

        cursor.execute("""
            UPDATE logbook_entries
            SET status = 'RETURNED',
                return_note = %s,
                updated_at = NOW()
            WHERE entry_id = %s AND deleted_at IS NULL
        """, [return_note or None, entry_id])

        conn.commit()

        cursor.execute(_ENTRY_SELECT, [entry_id])
        updated_row = cursor.fetchone()
        result = _format_entry(updated_row)
        print(f"[sign-entry] Entry {entry_id} returned to student")
        return result

    except (ValueError, PermissionError) as e:
        conn.rollback()
        print(f"[sign-entry] Error returning entry {entry_id}: {e}")
        raise
    except Exception as e:
        conn.rollback()
        print(f"[sign-entry] Unexpected error for entry {entry_id}: {e}")
        raise
    finally:
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def handler(event, context):
    field_name = event.get('info', {}).get('fieldName', 'signEntry')
    print(f"[sign-entry] Handling field: {field_name}")

    if field_name == 'returnEntry':
        return handle_return_entry(event)
    return handle_sign_entry(event)
