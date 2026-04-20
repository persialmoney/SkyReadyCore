"""
CFI Dashboard Lambda — getCfiDashboard GraphQL query resolver.

Returns all cross-user CFI data in one fresh DB call:
  1. pendingSignatures   — student entries with status=PENDING_SIGNATURE where instructor=CFI
  2. linkedStudents      — all students who have a student_cfi_shares row for this CFI
  3. sharedEntries       — all logbook entries for students who are actively sharing
  4. proficiencyShares   — latest proficiency snapshot per sharing student

Called on screen focus from InstructorProfileScreen and InstructorStudentDetailScreen,
replacing the old WatermelonDB observer pattern for cross-user data.
"""
import json
import time
from db_utils import get_db_connection, return_db_connection


# ── Column helpers ─────────────────────────────────────────────────────────────

ENTRY_COLUMNS = """
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
    status, signature, is_flight_review,
    return_note,
    created_at, updated_at
"""


def format_entry(row):
    entry = {
        'entryId':              str(row[0]),
        'userId':               row[1],
        'date':                 row[2].isoformat() if row[2] else None,
        'aircraft':             row[3],
        'tailNumber':           row[4],
        'route':                row[5],
        'routeLegs':            row[6] if row[6] else [],
        'flightTypes':          row[7] if row[7] else [],
        'totalTime':            float(row[8]) if row[8] else 0,
        'pic':                  float(row[9]) if row[9] else 0,
        'sic':                  float(row[10]) if row[10] else 0,
        'dualReceived':         float(row[11]) if row[11] else 0,
        'dualGiven':            float(row[12]) if row[12] else 0,
        'solo':                 float(row[13]) if row[13] else 0,
        'crossCountry':         float(row[14]) if row[14] else 0,
        'night':                float(row[15]) if row[15] else 0,
        'actualImc':            float(row[16]) if row[16] else 0,
        'simulatedInstrument':  float(row[17]) if row[17] else 0,
        'dayTakeoffs':          int(row[18]) if row[18] else 0,
        'dayLandings':          int(row[19]) if row[19] else 0,
        'nightTakeoffs':        int(row[20]) if row[20] else 0,
        'nightLandings':        int(row[21]) if row[21] else 0,
        'dayFullStopLandings':  int(row[22]) if row[22] else 0,
        'nightFullStopLandings': int(row[23]) if row[23] else 0,
        'approaches':           int(row[24]) if row[24] else 0,
        'holds':                bool(row[25]) if row[25] is not None else False,
        'tracking':             bool(row[26]) if row[26] is not None else False,
        'instructorUserId':     row[27],
        'instructorSnapshot':   row[28],
        'studentUserId':        row[29],
        'studentSnapshot':      row[30],
        'mirroredFromEntryId':  str(row[31]) if row[31] else None,
        'mirroredFromUserId':   row[32],
        'lessonTopic':          row[33],
        'groundInstruction':    float(row[34]) if row[34] else 0,
        'maneuvers':            row[35] if row[35] else [],
        'remarks':              row[36],
        'safetyNotes':          row[37],
        'safetyRelevant':       bool(row[38]) if row[38] is not None else False,
        'status':               row[39],
        'signature':            row[40],
        'isFlightReview':       bool(row[41]) if row[41] is not None else False,
        'returnNote':           row[42],
        'createdAt':            float(int(row[43].timestamp() * 1000)),
        'updatedAt':            float(int(row[44].timestamp() * 1000)) if row[44] else float(int(row[43].timestamp() * 1000)),
    }
    # Synthesize studentUserId from userId for student-owned entries where
    # student_user_id is NULL in the DB (student logged the entry themselves).
    if not entry.get('studentUserId'):
        entry['studentUserId'] = entry.get('userId')
    return entry


# ── Handler ────────────────────────────────────────────────────────────────────

def handler(event, context):
    print(f"[cfi-dashboard] event: {json.dumps(event, default=str)}")

    user_id = event['identity']['claims']['sub']

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # ── 1. Pending signatures ────────────────────────────────────────────
        # Student-owned entries where this CFI is the instructor and status
        # is PENDING_SIGNATURE.  We return the full entry so the client can
        # render the review / sign modal without any local cache.

        cursor.execute(f"""
            SELECT {ENTRY_COLUMNS}
            FROM logbook_entries
            WHERE instructor_user_id = %s
              AND status = 'PENDING_SIGNATURE'
              AND deleted_at IS NULL
            ORDER BY date DESC
        """, [user_id])

        pending_signatures = [format_entry(r) for r in cursor.fetchall()]
        print(f"[cfi-dashboard] pendingSignatures={len(pending_signatures)}")

        # ── 2. Linked students ───────────────────────────────────────────────
        # Every student who has a student_cfi_shares row for this CFI,
        # regardless of whether sharing is currently enabled.
        # Falls back to the student_snapshot on any logbook entry they have
        # submitted to this CFI when the shares row snapshot is NULL.

        cursor.execute("""
            SELECT
                s.student_id,
                s.sharing,
                s.student_snapshot,
                s.updated_at,
                (
                    SELECT le.student_snapshot
                    FROM logbook_entries le
                    WHERE le.user_id = s.student_id
                      AND le.instructor_user_id = %s
                      AND le.student_snapshot IS NOT NULL
                    ORDER BY le.updated_at DESC
                    LIMIT 1
                ) AS entry_student_snapshot
            FROM student_cfi_shares s
            WHERE s.cfi_user_id = %s
            ORDER BY s.updated_at DESC
        """, [user_id, user_id])

        linked_students = []
        for row in cursor.fetchall():
            raw_snap = row[2] or row[4]  # prefer shares snapshot, fall back to entry snapshot
            print(f"[cfi-dashboard] student_snapshot raw type={type(raw_snap).__name__} value={repr(raw_snap)}")
            snap = raw_snap if isinstance(raw_snap, dict) else {}
            if isinstance(raw_snap, str):
                try:
                    import json as _json
                    snap = _json.loads(raw_snap)
                except Exception as parse_err:
                    print(f"[cfi-dashboard] Failed to parse student_snapshot string: {parse_err}")
            linked_students.append({
                'studentUserId':          row[0],
                'studentName':            snap.get('name') or 'Unknown',
                'studentCertificateType': snap.get('certificateType'),
                'sharing':                bool(row[1]),
                'linkedAt':               float(int(row[3].timestamp() * 1000)) if row[3] else 0.0,
            })
            print(f"[cfi-dashboard] linkedStudent: id={row[0]} name='{snap.get('name')}' sharing={row[1]}")
        print(f"[cfi-dashboard] linkedStudents={len(linked_students)}")

        # ── 3. Shared logbook entries ────────────────────────────────────────
        # Two categories of entries are returned:
        #   a) All entries from students who are actively sharing (sharing=TRUE)
        #   b) SIGNED entries from ANY student who has ever had this CFI —
        #      signed entries are a permanent record of instruction and must
        #      remain visible even if the student later disables sharing.
        # We fetch ALL (no incremental filter) because this is a live query,
        # not a sync delta — the client discards and re-renders from scratch.

        cursor.execute("""
            SELECT student_id FROM student_cfi_shares
            WHERE cfi_user_id = %s AND sharing = TRUE
        """, [user_id])
        sharing_student_ids = [r[0] for r in cursor.fetchall()]

        shared_entries = []
        if sharing_student_ids:
            placeholders = ','.join(['%s'] * len(sharing_student_ids))
            cursor.execute(f"""
                SELECT {ENTRY_COLUMNS}
                FROM logbook_entries
                WHERE instructor_user_id = %s
                  AND (
                      user_id IN ({placeholders})
                      OR status = 'SIGNED'
                  )
                  AND deleted_at IS NULL
                ORDER BY date DESC
            """, [user_id] + sharing_student_ids)
        else:
            # No actively-sharing students — still return signed entries
            cursor.execute(f"""
                SELECT {ENTRY_COLUMNS}
                FROM logbook_entries
                WHERE instructor_user_id = %s
                  AND status = 'SIGNED'
                  AND deleted_at IS NULL
                ORDER BY date DESC
            """, [user_id])

        seen = set()
        for row in cursor.fetchall():
            eid = str(row[0])
            if eid in seen:
                continue
            seen.add(eid)
            shared_entries.append(format_entry(row))

        print(f"[cfi-dashboard] sharedEntries={len(shared_entries)} from {len(sharing_student_ids)} sharing students sharing_ids={sharing_student_ids}")

        # ── 4. Proficiency shares ────────────────────────────────────────────
        # Latest proficiency snapshot for each student who is actively sharing.

        proficiency_shares = []
        if sharing_student_ids:
            placeholders = ','.join(['%s'] * len(sharing_student_ids))
            cursor.execute(f"""
                SELECT DISTINCT ON (user_id)
                    user_id,
                    snapshot_date, score, recency, exposure, envelope, consistency,
                    score_core_vfr, score_night, score_ifr, score_tailwheel, score_multi,
                    score_seaplane, score_rotorcraft,
                    active_domains, computed_at
                FROM proficiency_snapshots
                WHERE user_id IN ({placeholders})
                ORDER BY user_id, snapshot_date DESC
            """, sharing_student_ids)

            for row in cursor.fetchall():
                proficiency_shares.append({
                    'studentUserId':  row[0],
                    'snapshotDate':   row[1],
                    'score':          int(row[2]),
                    'recency':        int(row[3]),
                    'exposure':       int(row[4]),
                    'envelope':       int(row[5]),
                    'consistency':    int(row[6]),
                    'scoreCoreVfr':   int(row[7]) if row[7] is not None else None,
                    'scoreNight':     int(row[8]) if row[8] is not None else None,
                    'scoreIfr':       int(row[9]) if row[9] is not None else None,
                    'scoreTailwheel': int(row[10]) if row[10] is not None else None,
                    'scoreMulti':     int(row[11]) if row[11] is not None else None,
                    'scoreSeaplane':  int(row[12]) if row[12] is not None else None,
                    'scoreRotorcraft': int(row[13]) if row[13] is not None else None,
                    'activeDomains':  row[14],
                    'computedAt':     float(int(row[15])),
                })

        print(f"[cfi-dashboard] proficiencyShares={len(proficiency_shares)}")

        return {
            'pendingSignatures': pending_signatures,
            'linkedStudents':    linked_students,
            'sharedEntries':     shared_entries,
            'proficiencyShares': proficiency_shares,
        }

    except Exception as e:
        print(f"[cfi-dashboard] Error: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            return_db_connection(conn)
