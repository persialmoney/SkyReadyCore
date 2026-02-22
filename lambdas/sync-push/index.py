"""
Sync Push Lambda - Process bulk changes from client (outbox pattern)
Clean implementation - no old CRUD fallbacks
Includes: server-side signature hash validation, CFI auto-copy mirror logic
"""
import json
import time
import os
import uuid
import hashlib
from db_utils import get_db_connection, return_db_connection
import psycopg


def validate_signature_hash(entry):
    """
    Re-verify signature hash to prevent tampering.
    Hash must include: entry fields + instructor snapshot + signature image
    Returns True if valid, raises ValueError if invalid.
    """
    signature = entry.get('signature')
    if not signature:
        return True  # No signature to validate
    
    instructor_snapshot = entry.get('instructorSnapshot', {})
    
    # Build hash input string (must match client-side algorithm)
    hash_input = '|'.join([
        str(entry.get('entryId', '')),
        str(entry.get('date', '')),
        str(entry.get('totalTime', '')),
        instructor_snapshot.get('name', ''),
        instructor_snapshot.get('certificateNumber', ''),
        instructor_snapshot.get('actingAs', ''),
        instructor_snapshot.get('certificateExpiration', ''),
        signature.get('signatureImage', ''),
        signature.get('timestamp', '')
    ])
    
    # Compute expected hash
    expected_hash = hashlib.sha256(hash_input.encode()).hexdigest()
    
    # Compare with provided hash
    if signature.get('hash') != expected_hash:
        raise ValueError(f"Signature hash mismatch for entry {entry.get('entryId')} - possible tampering detected")
    
    return True


def create_cfi_mirror_entry(cursor, student_entry, student_user_id):
    """
    Create a mirrored entry in the CFI's logbook when a student's entry is signed.
    Transforms: dualReceived -> dualGiven, PIC = totalTime, student snapshot populated.
    Idempotent: skips if mirror already exists for this (mirroredFromEntryId, cfiUserId).
    """
    cfi_user_id = student_entry.get('instructorUserId')
    
    if not cfi_user_id:
        print(f"[cfi-mirror] No instructorUserId in entry {student_entry.get('entryId')}, skipping mirror")
        return
    
    original_entry_id = student_entry.get('entryId')
    
    # Check if mirror already exists (idempotency)
    cursor.execute("""
        SELECT entry_id FROM logbook_entries 
        WHERE mirrored_from_entry_id = %s AND user_id = %s AND deleted_at IS NULL
    """, [original_entry_id, cfi_user_id])
    
    if cursor.fetchone():
        print(f"[cfi-mirror] Mirror already exists for entry {original_entry_id}, CFI {cfi_user_id}")
        return
    
    # Generate new mirror entry ID
    mirror_entry_id = str(uuid.uuid4())
    
    # Build student snapshot from student's profile (or use provided data)
    # For now, use basic name from the entry (student's name would come from their user profile)
    # TODO: Look up student's full profile to populate certificateNumber and certificateType
    student_snapshot = {
        'name': student_entry.get('studentName', 'Student'),  # Frontend should provide this
        'certificateNumber': None,  # Would need to lookup from student's user profile
        'certificateType': None
    }
    
    # Transform times: dualReceived -> dualGiven, PIC = totalTime
    total_time = student_entry.get('totalTime', 0)
    dual_given = student_entry.get('dualReceived', 0)
    
    # Create mirror entry
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
        mirror_entry_id, cfi_user_id, student_entry['date'],
        student_entry.get('aircraft'), student_entry.get('tailNumber'),
        student_entry.get('route'), student_entry.get('routeLegs', []),
        student_entry.get('flightTypes', []),
        total_time,  # Total time same
        total_time,  # CFI logs PIC time while instructing
        dual_given,  # Dual given (was dual received for student)
        student_entry.get('crossCountry', 0), student_entry.get('night', 0),
        student_entry.get('actualImc', 0), student_entry.get('simulatedInstrument', 0),
        student_entry.get('dayTakeoffs', 0), student_entry.get('dayLandings', 0),
        student_entry.get('nightTakeoffs', 0), student_entry.get('nightLandings', 0),
        student_entry.get('dayFullStopLandings', 0), student_entry.get('nightFullStopLandings', 0),
        student_entry.get('approaches', 0), student_entry.get('holds', False),
        student_entry.get('tracking', False),
        student_user_id,  # Link to student's account
        student_snapshot,
        student_entry.get('lessonTopic'), student_entry.get('groundInstruction', 0),
        student_entry.get('maneuvers', []),
        student_entry.get('remarks'),
        'SAVED',  # CFI's mirror is automatically saved (not pending signature)
        original_entry_id,  # Link back to student's original entry
        student_user_id
    ])
    
    print(f"[cfi-mirror] Created mirror entry {mirror_entry_id} for CFI {cfi_user_id} from student entry {original_entry_id}")


def handler(event, context):
    """
    Process bulk changes from client (outbox pattern)
    """
    print(f"[sync-push] Processing sync request: {json.dumps(event, default=str)}")
    
    user_id = event['identity']['claims']['sub']
    changes = event['arguments']['changes']
    last_pulled_at = event['arguments'].get('lastPulledAt', 0)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        conflicts = []
        timestamp = int(time.time() * 1000)
        
        # Process created entries
        for entry in changes.get('logbookEntries', {}).get('created', []):
            try:
                print(f"[sync-push] Creating entry: {entry.get('entryId')}")
                
                # Validate signature hash if entry is signed
                if entry.get('status') == 'SIGNED':
                    validate_signature_hash(entry)
                
                cursor.execute("""
                    INSERT INTO logbook_entries (
                        entry_id, user_id, date, aircraft, tail_number,
                        route, route_legs, flight_types, total_time,
                        pic, sic, dual_received, dual_given, solo,
                        cross_country, night, actual_imc, simulated_instrument,
                        day_takeoffs, day_landings, night_takeoffs, night_landings,
                        day_full_stop_landings, night_full_stop_landings,
                        approaches, holds, tracking,
                        instructor_user_id, instructor_snapshot,
                        student_user_id, student_snapshot,
                        lesson_topic, ground_instruction,
                        maneuvers, remarks, safety_notes, safety_relevant,
                        status, signature, is_flight_review,
                        mirrored_from_entry_id, mirrored_from_user_id,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        NOW(), NOW()
                    )
                """, [
                    entry['entryId'], user_id, entry['date'],
                    entry.get('aircraft'), entry.get('tailNumber'),
                    entry.get('route'), entry.get('routeLegs', []),
                    entry.get('flightTypes', []), entry.get('totalTime', 0),
                    entry.get('pic', 0), entry.get('sic', 0),
                    entry.get('dualReceived', 0), entry.get('dualGiven', 0),
                    entry.get('solo', 0), entry.get('crossCountry', 0),
                    entry.get('night', 0), entry.get('actualImc', 0),
                    entry.get('simulatedInstrument', 0),
                    entry.get('dayTakeoffs', 0), entry.get('dayLandings', 0),
                    entry.get('nightTakeoffs', 0), entry.get('nightLandings', 0),
                    entry.get('dayFullStopLandings', 0),
                    entry.get('nightFullStopLandings', 0), entry.get('approaches', 0),
                    entry.get('holds', False), entry.get('tracking', False),
                    entry.get('instructorUserId'), entry.get('instructorSnapshot'),
                    entry.get('studentUserId'), entry.get('studentSnapshot'),
                    entry.get('lessonTopic'), entry.get('groundInstruction', 0),
                    entry.get('maneuvers', []), entry.get('remarks'),
                    entry.get('safetyNotes'), entry.get('safetyRelevant', False),
                    entry.get('status', 'DRAFT'), entry.get('signature'),
                    entry.get('isFlightReview', False),
                    entry.get('mirroredFromEntryId'), entry.get('mirroredFromUserId')
                ])
                
                # If entry is signed and has an instructor, create CFI mirror
                if entry.get('status') == 'SIGNED' and entry.get('instructorUserId'):
                    create_cfi_mirror_entry(cursor, entry, user_id)
                
            except psycopg.errors.UniqueViolation as e:
                # psycopg3 puts the connection in an error state on any exception;
                # must rollback before continuing to process remaining entries.
                conn.rollback()
                print(f"[sync-push] Conflict creating entry {entry['entryId']}: {e}")
                conflicts.append({
                    'entryId': entry['entryId'],
                    'type': 'ALREADY_EXISTS',
                    'serverTimestamp': timestamp
                })
            except ValueError as e:
                # Signature validation error
                print(f"[sync-push] Signature validation failed for entry {entry['entryId']}: {e}")
                conflicts.append({
                    'entryId': entry['entryId'],
                    'type': 'SIGNATURE_INVALID',
                    'serverTimestamp': timestamp
                })
        
        # Process updated entries
        for update in changes.get('logbookEntries', {}).get('updated', []):
            entry_id = update['entryId']
            entry_data = update['data']
            
            print(f"[sync-push] Updating entry: {entry_id}")
            
            # Validate signature hash if entry is signed
            if entry_data.get('status') == 'SIGNED':
                try:
                    validate_signature_hash(entry_data)
                except ValueError as e:
                    print(f"[sync-push] Signature validation failed for entry {entry_id}: {e}")
                    conflicts.append({
                        'entryId': entry_id,
                        'type': 'SIGNATURE_INVALID',
                        'serverTimestamp': timestamp
                    })
                    continue
            
            # Check for conflicts (timestamp-based: server wins)
            cursor.execute("""
                SELECT updated_at FROM logbook_entries
                WHERE entry_id = %s AND user_id = %s AND deleted_at IS NULL
            """, [entry_id, user_id])
            
            row = cursor.fetchone()
            if row:
                server_updated_at = int(row[0].timestamp() * 1000)
                if server_updated_at > last_pulled_at:
                    print(f"[sync-push] Conflict: server newer for {entry_id}")
                    conflicts.append({
                        'entryId': entry_id,
                        'type': 'SERVER_NEWER',
                        'serverTimestamp': server_updated_at
                    })
                    continue
            
            cursor.execute("""
                UPDATE logbook_entries SET
                    date = %s, aircraft = %s, tail_number = %s, route = %s,
                    route_legs = %s, flight_types = %s,
                    total_time = %s, pic = %s, sic = %s,
                    dual_received = %s, dual_given = %s, solo = %s,
                    cross_country = %s, night = %s, actual_imc = %s,
                    simulated_instrument = %s, day_takeoffs = %s, day_landings = %s,
                    night_takeoffs = %s, night_landings = %s,
                    day_full_stop_landings = %s, night_full_stop_landings = %s,
                    approaches = %s, holds = %s, tracking = %s,
                    instructor_user_id = %s, instructor_snapshot = %s,
                    student_user_id = %s, student_snapshot = %s,
                    lesson_topic = %s, ground_instruction = %s, maneuvers = %s,
                    remarks = %s, safety_notes = %s, safety_relevant = %s,
                    status = %s, signature = %s, is_flight_review = %s,
                    updated_at = NOW()
                WHERE entry_id = %s AND user_id = %s AND deleted_at IS NULL
            """, [
                entry_data['date'],
                entry_data.get('aircraft'), entry_data.get('tailNumber'),
                entry_data.get('route'), entry_data.get('routeLegs', []),
                entry_data.get('flightTypes', []),
                entry_data.get('totalTime', 0),
                entry_data.get('pic', 0), entry_data.get('sic', 0),
                entry_data.get('dualReceived', 0), entry_data.get('dualGiven', 0),
                entry_data.get('solo', 0), entry_data.get('crossCountry', 0),
                entry_data.get('night', 0), entry_data.get('actualImc', 0),
                entry_data.get('simulatedInstrument', 0),
                entry_data.get('dayTakeoffs', 0), entry_data.get('dayLandings', 0),
                entry_data.get('nightTakeoffs', 0), entry_data.get('nightLandings', 0),
                entry_data.get('dayFullStopLandings', 0),
                entry_data.get('nightFullStopLandings', 0),
                entry_data.get('approaches', 0), entry_data.get('holds', False),
                entry_data.get('tracking', False),
                entry_data.get('instructorUserId'), entry_data.get('instructorSnapshot'),
                entry_data.get('studentUserId'), entry_data.get('studentSnapshot'),
                entry_data.get('lessonTopic'), entry_data.get('groundInstruction', 0),
                entry_data.get('maneuvers', []),
                entry_data.get('remarks'), entry_data.get('safetyNotes'),
                entry_data.get('safetyRelevant', False),
                entry_data.get('status', 'DRAFT'), entry_data.get('signature'),
                entry_data.get('isFlightReview', False),
                entry_id, user_id
            ])
            
            # If entry was just signed and has an instructor, create CFI mirror
            if entry_data.get('status') == 'SIGNED' and entry_data.get('instructorUserId'):
                create_cfi_mirror_entry(cursor, entry_data, user_id)
        
        # Process deleted entries (soft delete)
        deleted_entry_ids = changes.get('logbookEntries', {}).get('deleted', [])
        
        for entry_id in deleted_entry_ids:
            # Perform soft delete
            cursor.execute("""
                UPDATE logbook_entries SET
                    deleted_at = NOW(),
                    updated_at = NOW()
                WHERE entry_id = %s AND user_id = %s AND deleted_at IS NULL
            """, [entry_id, user_id])
            
            # Log only if update failed (critical error)
            if cursor.rowcount == 0:
                print(f"[sync-push] WARNING: Failed to delete entry {entry_id} - not found or already deleted")
        
        # Write to outbox for pub/sub
        cursor.execute("""
            INSERT INTO outbox (event_type, user_id, payload, created_at)
            VALUES (%s, %s, %s, NOW())
        """, ['sync_push', user_id, json.dumps(changes)])
        
        conn.commit()

        print(f"[sync-push] Success: {len(conflicts)} conflicts")

        return {
            'timestamp': timestamp,
            'conflicts': conflicts
        }

    except Exception as e:
        conn.rollback()
        print(f"[sync-push] Error: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            return_db_connection(conn)
