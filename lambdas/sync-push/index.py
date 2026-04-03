"""
Sync Push Lambda - Process bulk changes from client (outbox pattern)
- Logbook entries: PostgreSQL
- User data (preferences, aircraft, personal minimums): DynamoDB
- Missions, mission airports, readiness assessments: PostgreSQL
- Proficiency snapshots: PostgreSQL (upsert by user_id + snapshot_date)
Includes: server-side signature hash validation, CFI auto-copy mirror logic
"""
import json
import time
import os
import uuid
import hashlib
import boto3
from datetime import datetime
from db_utils import get_db_connection, return_db_connection
import psycopg
from psycopg.types.json import Jsonb

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))


def _js_number_str(value) -> str:
    """
    Replicate JavaScript's String(number) coercion.
    JS drops the decimal point for whole numbers: String(2.0) === "2", not "2.0".
    Python's str(2.0) === "2.0", so normalise here to match the client hash.
    """
    if value is None:
        return ''
    try:
        f = float(value)
        if f == int(f):
            return str(int(f))
        return str(f)
    except (TypeError, ValueError):
        return str(value)


def validate_signature_hash(entry):
    """
    Re-verify signature hash to prevent tampering.
    """
    signature = entry.get('signature')
    if not signature:
        return True
    
    instructor_snapshot = entry.get('instructorSnapshot', {})
    
    hash_input = '|'.join([
        str(entry.get('entryId', '')),
        str(entry.get('date', '')),
        _js_number_str(entry.get('totalTime', '')),
        instructor_snapshot.get('name', ''),
        instructor_snapshot.get('certificateNumber', ''),
        instructor_snapshot.get('actingAs', ''),
        instructor_snapshot.get('certificateExpiration', ''),
        signature.get('signatureImage', ''),
        signature.get('timestamp', '')
    ])
    
    expected_hash = hashlib.sha256(hash_input.encode()).hexdigest()
    
    if signature.get('hash') != expected_hash:
        raise ValueError(f"Signature hash mismatch for entry {entry.get('entryId')}")
    
    return True


def create_cfi_mirror_entry(cursor, student_entry, student_user_id):
    """
    Create a mirrored entry in the CFI's logbook when a student's entry is signed.
    """
    cfi_user_id = student_entry.get('instructorUserId')
    
    if not cfi_user_id:
        return
    
    original_entry_id = student_entry.get('entryId')
    
    # Check if mirror already exists (idempotency)
    cursor.execute("""
        SELECT entry_id FROM logbook_entries 
        WHERE mirrored_from_entry_id = %s AND user_id = %s AND deleted_at IS NULL
    """, [original_entry_id, cfi_user_id])
    
    if cursor.fetchone():
        print(f"[cfi-mirror] Mirror already exists for entry {original_entry_id}")
        return
    
    mirror_entry_id = str(uuid.uuid4())
    
    student_snapshot = {
        'name': student_entry.get('studentName', 'Student'),
        'certificateNumber': None,
        'certificateType': None
    }
    
    total_time = student_entry.get('totalTime', 0)
    dual_given = student_entry.get('dualReceived', 0)
    
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
        Jsonb(student_entry.get('aircraft')), student_entry.get('tailNumber'),
        student_entry.get('route'), Jsonb(student_entry.get('routeLegs', [])),
        student_entry.get('flightTypes', []),
        total_time, total_time, dual_given,
        student_entry.get('crossCountry', 0), student_entry.get('night', 0),
        student_entry.get('actualImc', 0), student_entry.get('simulatedInstrument', 0),
        student_entry.get('dayTakeoffs', 0), student_entry.get('dayLandings', 0),
        student_entry.get('nightTakeoffs', 0), student_entry.get('nightLandings', 0),
        student_entry.get('dayFullStopLandings', 0), student_entry.get('nightFullStopLandings', 0),
        student_entry.get('approaches', 0), student_entry.get('holds', False),
        student_entry.get('tracking', False),
        student_user_id, Jsonb(student_snapshot),
        student_entry.get('lessonTopic'), student_entry.get('groundInstruction', 0),
        student_entry.get('maneuvers', []),
        student_entry.get('remarks'),
        'SAVED', original_entry_id, student_user_id
    ])
    
    print(f"[cfi-mirror] Created mirror entry {mirror_entry_id}")


def handler(event, context):
    """
    Process bulk changes from client (outbox pattern)
    """
    print(f"[sync-push] Processing sync request: {json.dumps(event, default=str)}")
    
    user_id = event['identity']['claims']['sub']
    changes = event['arguments']['changes']
    # lastPulledAt arrives as epoch milliseconds (Int scalar). All BIGINT columns store
    # epoch ms, so no conversion needed for those comparisons.
    # For TIMESTAMPTZ (logbook) conflict checks we need epoch seconds via // 1000.
    last_pulled_at = event['arguments'].get('lastPulledAt', 0) or 0  # epoch ms

    # Look up the pushing user's own name once so we can inject it into
    # student_snapshot for PENDING_SIGNATURE entries (clients never send their own snapshot).
    pushing_user_name: str | None = None
    try:
        user_resp = users_table.get_item(Key={'userId': user_id}, ProjectionExpression='#n', ExpressionAttributeNames={'#n': 'name'})
        pushing_user_name = user_resp.get('Item', {}).get('name')
    except Exception as e:
        print(f"[sync-push] Warning: could not fetch pushing user name: {e}")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        conflicts = []
        timestamp_ms = int(time.time() * 1000)   # epoch ms — used for DB BIGINT columns
        timestamp = float(timestamp_ms)           # float so AppSync serializes as JSON Float, not Long
        
        def build_student_snapshot(entry_or_data: dict) -> dict | None:
            """Return a student_snapshot with name filled in from DynamoDB if the client didn't send one."""
            snap = entry_or_data.get('studentSnapshot') or {}
            if not snap.get('name') and pushing_user_name:
                snap = dict(snap)
                snap['name'] = pushing_user_name
            return snap if snap else None

        # Pre-scan: check if this push includes any PENDING_SIGNATURE entry targeting a CFI.
        # We record it here so the subscription payload can be set after all entries are written.
        # Uses the first valid entry found (same "first CFI" policy as studentCfiShares).
        _pending_sig_cfi_id = None
        for _e in list(changes.get('logbookEntries', {}).get('created', [])) + \
                  list(changes.get('logbookEntries', {}).get('updated', [])):
            if _e.get('status') == 'PENDING_SIGNATURE' and _e.get('instructorUserId') \
                    and _e.get('instructorUserId') != user_id:
                _pending_sig_cfi_id = _e.get('instructorUserId')
                break

        # ========== LOGBOOK ENTRIES (PostgreSQL) ==========
        
        # Process created entries
        for entry in changes.get('logbookEntries', {}).get('created', []):
            try:
                print(f"[sync-push] Creating entry: {entry.get('entryId')}")

                # Self-endorsement guard: a user cannot be both the entry owner and
                # the instructor for the same entry. Strip instructor_user_id silently
                # so the entry is saved but treated as un-endorsed.
                if entry.get('instructorUserId') and entry.get('instructorUserId') == user_id:
                    print(f"[sync-push] WARN: self-endorsement blocked for entry {entry.get('entryId')} "
                          f"(user_id={user_id}). Stripping instructorUserId.")
                    entry = dict(entry)
                    entry['instructorUserId'] = None
                    entry['instructorSnapshot'] = None
                    if entry.get('status') == 'PENDING_SIGNATURE':
                        entry['status'] = 'SAVED'

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
                    Jsonb(entry.get('aircraft')), entry.get('tailNumber'),
                    entry.get('route'), Jsonb(entry.get('routeLegs', [])),
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
                    entry.get('instructorUserId'), Jsonb(entry.get('instructorSnapshot')),
                    entry.get('studentUserId'), Jsonb(build_student_snapshot(entry)),
                    entry.get('lessonTopic'), entry.get('groundInstruction', 0),
                    entry.get('maneuvers', []), entry.get('remarks'),
                    entry.get('safetyNotes'), entry.get('safetyRelevant', False),
                    entry.get('status', 'DRAFT'), Jsonb(entry.get('signature')),
                    entry.get('isFlightReview', False),
                    entry.get('mirroredFromEntryId'), entry.get('mirroredFromUserId')
                ])
                
                if entry.get('status') == 'SIGNED' and entry.get('instructorUserId'):
                    create_cfi_mirror_entry(cursor, entry, user_id)
                
            except psycopg.errors.UniqueViolation as e:
                conn.rollback()
                print(f"[sync-push] Conflict creating entry {entry['entryId']}: {e}")
                conflicts.append({
                    'entryId': entry['entryId'],
                    'type': 'ALREADY_EXISTS',
                    'serverTimestamp': timestamp
                })
            except ValueError as e:
                print(f"[sync-push] Signature validation failed: {e}")
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

            # Self-endorsement guard: strip instructor link if user is signing their own entry.
            if entry_data.get('instructorUserId') and entry_data.get('instructorUserId') == user_id:
                print(f"[sync-push] WARN: self-endorsement blocked for entry {entry_id} "
                      f"(user_id={user_id}). Stripping instructorUserId.")
                entry_data = dict(entry_data)
                entry_data['instructorUserId'] = None
                entry_data['instructorSnapshot'] = None
                if entry_data.get('status') == 'PENDING_SIGNATURE':
                    entry_data['status'] = 'SAVED'

            if entry_data.get('status') == 'SIGNED':
                try:
                    validate_signature_hash(entry_data)
                except ValueError as e:
                    conflicts.append({
                        'entryId': entry_id,
                        'type': 'SIGNATURE_INVALID',
                        'serverTimestamp': timestamp
                    })
                    continue
            
            cursor.execute("""
                SELECT updated_at FROM logbook_entries
                WHERE entry_id = %s AND user_id = %s AND deleted_at IS NULL
            """, [entry_id, user_id])
            
            row = cursor.fetchone()
            if row:
                server_updated_at_ms = int(row[0].timestamp() * 1000)
                if server_updated_at_ms > last_pulled_at:
                    conflicts.append({
                        'entryId': entry_id,
                        'type': 'SERVER_NEWER',
                        'serverTimestamp': float(server_updated_at_ms),  # epoch ms as float for AppSync
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
                Jsonb(entry_data.get('aircraft')), entry_data.get('tailNumber'),
                entry_data.get('route'), Jsonb(entry_data.get('routeLegs', [])),
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
                entry_data.get('instructorUserId'), Jsonb(entry_data.get('instructorSnapshot')),
                entry_data.get('studentUserId'), Jsonb(build_student_snapshot(entry_data)),
                entry_data.get('lessonTopic'), entry_data.get('groundInstruction', 0),
                entry_data.get('maneuvers', []),
                entry_data.get('remarks'), entry_data.get('safetyNotes'),
                entry_data.get('safetyRelevant', False),
                entry_data.get('status', 'DRAFT'), Jsonb(entry_data.get('signature')),
                entry_data.get('isFlightReview', False),
                entry_id, user_id
            ])
            
            if entry_data.get('status') == 'SIGNED' and entry_data.get('instructorUserId'):
                create_cfi_mirror_entry(cursor, entry_data, user_id)
        
        # Process deleted entries (soft delete)
        for entry_id in changes.get('logbookEntries', {}).get('deleted', []):
            cursor.execute("""
                UPDATE logbook_entries SET
                    deleted_at = NOW(),
                    updated_at = NOW()
                WHERE entry_id = %s AND user_id = %s AND deleted_at IS NULL
            """, [entry_id, user_id])
        
        # Write to outbox for pub/sub (logbook entries only)
        # User data changes already tracked in DynamoDB updatedAt
        if changes.get('logbookEntries'):
            cursor.execute("""
                INSERT INTO outbox (event_type, user_id, payload, created_at)
                VALUES (%s, %s, %s, NOW())
            """, ['sync_push', user_id, Jsonb({'logbookEntries': changes.get('logbookEntries')})])
        
        conn.commit()
        
        # ========== USER DATA (DynamoDB) ==========
        
        # Get current user object
        user_response = users_table.get_item(Key={'userId': user_id})
        user_item = user_response.get('Item', {})
        
        # Track if user object needs updating
        user_updated = False
        
        # Process personal minimums profiles
        personal_minimums = user_item.get('personalMinimumsProfiles', [])
        
        for profile in changes.get('personalMinimumsProfiles', {}).get('created', []):
            profile_id = profile.get('id')
            
            # Check if profile already exists (prevent duplicates)
            if any(p.get('id') == profile_id for p in personal_minimums):
                continue
            
            personal_minimums.append({
                'id': profile['id'],
                'userId': user_id,
                'name': profile['name'],
                'kind': profile['kind'],
                'isDefault': profile.get('isDefault', False),
                'nightAllowed': profile.get('nightAllowed', True),
                'ifrAllowed': profile.get('ifrAllowed', False),
                'passengersAllowed': profile.get('passengersAllowed', True),
                'maxDaysSinceLastFlight': profile.get('maxDaysSinceLastFlight'),
                'minCeilingFt': profile.get('minCeilingFt'),
                'minVisibilityTenthsSm': profile.get('minVisibilityTenthsSm'),
                'maxWindKt': profile.get('maxWindKt'),
                'maxCrosswindKt': profile.get('maxCrosswindKt'),
                'maxGustSpreadKt': profile.get('maxGustSpreadKt'),
                'comfortCrosswindKt': profile.get('comfortCrosswindKt'),
                'comfortGustSpreadKt': profile.get('comfortGustSpreadKt'),
                'createdAt': datetime.utcnow().isoformat() + 'Z',
                'updatedAt': datetime.utcnow().isoformat() + 'Z',
                'version': profile.get('version', 0)
            })
            user_updated = True
        
        for update in changes.get('personalMinimumsProfiles', {}).get('updated', []):
            profile_id = update['id']
            profile_data = update['data']
            client_updated_at = update.get('clientUpdatedAt', 0)  # Timestamp from client
            
            # Find the profile in the list
            profile_found = False
            for i, profile in enumerate(personal_minimums):
                if profile.get('id') == profile_id:
                    profile_found = True
                    # Check for conflict: is server version newer than client?
                    server_updated_at_str = profile.get('updatedAt', '')
                    try:
                        server_updated_at = datetime.fromisoformat(server_updated_at_str.replace('Z', '+00:00'))
                        server_timestamp_ms = int(server_updated_at.timestamp() * 1000)
                        
                        # Conflict: server is newer than client's version
                        if server_timestamp_ms > client_updated_at:
                            conflicts.append({
                                'entryId': profile_id,
                                'type': 'SERVER_NEWER',
                                'serverTimestamp': float(server_timestamp_ms),  # epoch ms as float for AppSync
                            })
                            continue  # Skip update, server wins
                    except Exception as e:
                        pass  # If can't parse, proceed with update (first update case)
                    
                    # No conflict, apply update
                    personal_minimums[i].update({
                        'name': profile_data['name'],
                        'kind': profile_data['kind'],
                        'isDefault': profile_data.get('isDefault', False),
                        'nightAllowed': profile_data.get('nightAllowed', True),
                        'ifrAllowed': profile_data.get('ifrAllowed', False),
                        'passengersAllowed': profile_data.get('passengersAllowed', True),
                        'maxDaysSinceLastFlight': profile_data.get('maxDaysSinceLastFlight'),
                        'minCeilingFt': profile_data.get('minCeilingFt'),
                        'minVisibilityTenthsSm': profile_data.get('minVisibilityTenthsSm'),
                        'maxWindKt': profile_data.get('maxWindKt'),
                        'maxCrosswindKt': profile_data.get('maxCrosswindKt'),
                        'maxGustSpreadKt': profile_data.get('maxGustSpreadKt'),
                        'comfortCrosswindKt': profile_data.get('comfortCrosswindKt'),
                        'comfortGustSpreadKt': profile_data.get('comfortGustSpreadKt'),
                        'updatedAt': datetime.utcnow().isoformat() + 'Z',
                        'version': profile_data.get('version', 0) + 1  # Increment version
                    })
                    user_updated = True
                    break
        
        for profile_id in changes.get('personalMinimumsProfiles', {}).get('deleted', []):
            print(f"[sync-push] Deleting personal minimums profile: {profile_id}")
            for i, profile in enumerate(personal_minimums):
                if profile.get('id') == profile_id:
                    personal_minimums[i]['deletedAt'] = datetime.utcnow().isoformat() + 'Z'
                    user_updated = True
                    break
        
        # Process aircraft
        aircraft_list = user_item.get('aircraft', [])
        
        for aircraft in changes.get('userAircraft', {}).get('created', []):
            print(f"[sync-push] Creating aircraft: {aircraft.get('tailNumber')}")
            aircraft_list.append({
                'tailNumber': aircraft['tailNumber'],
                'make': aircraft.get('make'),
                'model': aircraft.get('model'),
                'category': aircraft.get('category'),
                'class': aircraft.get('class'),
                'notes': aircraft.get('notes'),
                'complex': aircraft.get('complex', False),
                'highPerformance': aircraft.get('highPerformance', False),
                'tailwheel': aircraft.get('tailwheel', False),
                'isManual': aircraft.get('isManual', False),
                'builderCertification': aircraft.get('builderCertification'),
                'airworthinessDate': aircraft.get('airworthinessDate'),
                'usageCount': aircraft.get('usageCount', 0),
                'isArchived': aircraft.get('isArchived', False),
                'addedAt': datetime.utcnow().isoformat() + 'Z'
            })
            user_updated = True
        
        for update in changes.get('userAircraft', {}).get('updated', []):
            tail_number = update['tailNumber']
            aircraft_data = update['data']
            print(f"[sync-push] Updating aircraft: {tail_number}")
            
            for i, aircraft in enumerate(aircraft_list):
                if aircraft.get('tailNumber') == tail_number:
                    aircraft_list[i].update({
                        'make': aircraft_data.get('make'),
                        'model': aircraft_data.get('model'),
                        'category': aircraft_data.get('category'),
                        'class': aircraft_data.get('class'),
                        'notes': aircraft_data.get('notes'),
                        'complex': aircraft_data.get('complex', False),
                        'highPerformance': aircraft_data.get('highPerformance', False),
                        'tailwheel': aircraft_data.get('tailwheel', False),
                        'usageCount': aircraft_data.get('usageCount', 0),
                        'isArchived': aircraft_data.get('isArchived', False)
                    })
                    user_updated = True
                    break
        
        for tail_number in changes.get('userAircraft', {}).get('deleted', []):
            print(f"[sync-push] Deleting aircraft: {tail_number}")
            aircraft_list = [a for a in aircraft_list if a.get('tailNumber') != tail_number]
            user_updated = True
        
        # Process preferences
        for prefs in changes.get('userPreferences', {}).get('created', []) + changes.get('userPreferences', {}).get('updated', []):
            print(f"[sync-push] Updating preferences for user: {user_id}")
            user_item['preferences'] = {
                'defaultUnits': prefs.get('defaultUnits'),
                'notificationEnabled': prefs.get('notificationEnabled', True),
                'criticalAlertThreshold': prefs.get('criticalAlertThreshold'),
                'defaultAirport': prefs.get('defaultAirport'),
                'enabledCurrencies': prefs.get('enabledCurrencies', []),
                'flyingStyles': prefs.get('flyingStyles', [])
            }
            user_updated = True
        
        # Update DynamoDB if any user data changed
        if user_updated:
            user_item['personalMinimumsProfiles'] = personal_minimums
            user_item['aircraft'] = aircraft_list
            user_item['updatedAt'] = datetime.utcnow().isoformat() + 'Z'
            
            users_table.put_item(Item=user_item)
            print(f"[sync-push] Updated DynamoDB user object")

        # ========== MISSIONS (PostgreSQL) ==========

        for mission in changes.get('missions', {}).get('created', []):
            mission_id = mission.get('id')
            print(f"[sync-push] Creating mission: {mission_id}")
            try:
                cursor.execute("SAVEPOINT mission_create")
                cursor.execute("""
                    INSERT INTO missions (
                        id, mission_id, user_id, status, is_operational,
                        mission_type, scheduled_time_utc, time_precision,
                        tail_number, aircraft_label, notes, forecast_reviewed_at,
                        route_hash, latest_assessment_id, latest_result,
                        latest_checked_time, latest_reason_short, latest_reason_long,
                        created_at, updated_at, deleted_at, _sync_pending
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE
                    )
                    ON CONFLICT (id) DO NOTHING
                """, [
                    mission_id, mission.get('missionId', mission_id), user_id,
                    mission.get('status', 'SCHEDULED'),
                    mission.get('isOperational', False),
                    mission.get('missionType', 'LOCAL'),
                    mission.get('scheduledTimeUtc'),
                    mission.get('timePrecision'),
                    mission.get('tailNumber'),
                    mission.get('aircraftLabel'),
                    mission.get('notes'),
                    mission.get('forecastReviewedAt'),
                    mission.get('routeHash'),
                    mission.get('latestAssessmentId'),
                    mission.get('latestResult'),
                    mission.get('latestCheckedTime'),
                    mission.get('latestReasonShort'),
                    mission.get('latestReasonLong'),
                    mission.get('createdAt', timestamp_ms),
                    mission.get('updatedAt', timestamp_ms),
                    mission.get('deletedAt'),
                ])
                cursor.execute("RELEASE SAVEPOINT mission_create")
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT mission_create")
                print(f"[sync-push] Error creating mission {mission_id}: {e}")

        for update in changes.get('missions', {}).get('updated', []):
            mission_id = update.get('id')
            data = update.get('data', update)  # flat update or wrapped in .data
            print(f"[sync-push] Upserting mission: {mission_id}")

            # Conflict check: if the row exists and the server version is newer than the
            # client's lastPulledAt, reject the update and let the client re-pull.
            # If the row doesn't exist at all (client race: createdAt < lastPulledAt caused
            # the client to misclassify a new mission as 'updated'), fall through to the
            # upsert so the row is created now.
            cursor.execute("""
                SELECT updated_at FROM missions
                WHERE id = %s AND user_id = %s AND deleted_at IS NULL
            """, [mission_id, user_id])
            row = cursor.fetchone()
            if row and row[0] and row[0] > last_pulled_at:
                conflicts.append({
                    'entryId': mission_id,
                    'type': 'SERVER_NEWER',
                    'serverTimestamp': float(row[0]),  # epoch ms as float for AppSync
                })
                continue

            # Upsert: INSERT if the row doesn't exist yet (handles the race where the client
            # sent 'updated' for a mission the server has never seen), otherwise UPDATE.
            # The WHERE clause on the DO UPDATE path re-applies the conflict guard so a
            # concurrent write between the SELECT above and this statement can't overwrite
            # a newer server version.
            cursor.execute("""
                INSERT INTO missions (
                    id, mission_id, user_id, status, is_operational, mission_type,
                    scheduled_time_utc, time_precision, tail_number, aircraft_label,
                    notes, forecast_reviewed_at, route_hash, latest_assessment_id,
                    latest_result, latest_checked_time, latest_reason_short,
                    latest_reason_long, created_at, updated_at, deleted_at, _sync_pending
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE
                )
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    is_operational = EXCLUDED.is_operational,
                    mission_type = EXCLUDED.mission_type,
                    scheduled_time_utc = EXCLUDED.scheduled_time_utc,
                    time_precision = EXCLUDED.time_precision,
                    tail_number = EXCLUDED.tail_number,
                    aircraft_label = EXCLUDED.aircraft_label,
                    notes = EXCLUDED.notes,
                    forecast_reviewed_at = EXCLUDED.forecast_reviewed_at,
                    route_hash = EXCLUDED.route_hash,
                    latest_assessment_id = EXCLUDED.latest_assessment_id,
                    latest_result = EXCLUDED.latest_result,
                    latest_checked_time = EXCLUDED.latest_checked_time,
                    latest_reason_short = EXCLUDED.latest_reason_short,
                    latest_reason_long = EXCLUDED.latest_reason_long,
                    updated_at = EXCLUDED.updated_at,
                    deleted_at = EXCLUDED.deleted_at
                WHERE missions.updated_at <= %s OR missions.updated_at IS NULL
            """, [
                mission_id, data.get('missionId', mission_id), user_id,
                data.get('status'), data.get('isOperational', False),
                data.get('missionType'), data.get('scheduledTimeUtc'),
                data.get('timePrecision'), data.get('tailNumber'),
                data.get('aircraftLabel'), data.get('notes'),
                data.get('forecastReviewedAt'), data.get('routeHash'),
                data.get('latestAssessmentId'), data.get('latestResult'),
                data.get('latestCheckedTime'), data.get('latestReasonShort'),
                data.get('latestReasonLong'),
                data.get('createdAt', timestamp_ms),
                data.get('updatedAt', timestamp_ms),
                data.get('deletedAt'),
                last_pulled_at,
            ])

        for mission_id in changes.get('missions', {}).get('deleted', []):
            print(f"[sync-push] Soft-deleting mission: {mission_id}")
            cursor.execute("""
                UPDATE missions SET deleted_at = %s, updated_at = %s
                WHERE id = %s AND user_id = %s AND deleted_at IS NULL
            """, [timestamp_ms, timestamp_ms, mission_id, user_id])

        # ========== MISSION AIRPORTS (PostgreSQL) ==========
        # Replace-all semantics: the client always pushes the complete airport set for a
        # mission whenever the route changes (old rows are destroyPermanently() locally).
        # We group incoming airports by mission_id, delete all existing rows for those
        # missions, then insert the new set. This keeps server and client in sync even
        # when airports are removed or reordered without an explicit delete signal.

        created_airports = changes.get('missionAirports', {}).get('created', [])
        if created_airports:
            # Collect unique mission_ids in this batch
            affected_mission_ids = list({a.get('missionId') for a in created_airports if a.get('missionId')})
            if affected_mission_ids:
                # Verify missions belong to this user before touching their airports
                placeholders = ','.join(['%s'] * len(affected_mission_ids))
                cursor.execute(
                    f"SELECT id FROM missions WHERE id IN ({placeholders}) AND user_id = %s",
                    affected_mission_ids + [user_id]
                )
                verified_ids = {row[0] for row in cursor.fetchall()}

                for mission_id in verified_ids:
                    print(f"[sync-push] Replacing airports for mission: {mission_id}")
                    cursor.execute("DELETE FROM mission_airports WHERE mission_id = %s", [mission_id])

            for airport in created_airports:
                airport_id = airport.get('id')
                mission_id = airport.get('missionId')
                if mission_id not in verified_ids:
                    print(f"[sync-push] Skipping airport for unverified mission: {mission_id}")
                    continue
                print(f"[sync-push] Inserting mission airport: {airport_id}")
                try:
                    cursor.execute("SAVEPOINT airport_insert")
                    cursor.execute("""
                        INSERT INTO mission_airports (
                            id, mission_id, icao, role, order_index, display_name, _sync_pending
                        ) VALUES (%s, %s, %s, %s, %s, %s, FALSE)
                        ON CONFLICT (id) DO UPDATE SET
                            icao = EXCLUDED.icao,
                            role = EXCLUDED.role,
                            order_index = EXCLUDED.order_index,
                            display_name = EXCLUDED.display_name
                    """, [
                        airport_id,
                        mission_id,
                        airport.get('icao', '').upper(),
                        airport.get('role', 'DEPARTURE'),
                        airport.get('orderIndex', 0),
                        airport.get('displayName'),
                    ])
                    cursor.execute("RELEASE SAVEPOINT airport_insert")
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT airport_insert")
                    print(f"[sync-push] Error inserting mission airport {airport_id}: {e}")

        for update in changes.get('missionAirports', {}).get('updated', []):
            airport_id = update.get('id')
            data = update.get('data', update)
            cursor.execute("""
                UPDATE mission_airports SET
                    icao = %s, role = %s, order_index = %s, display_name = %s
                WHERE id = %s
            """, [
                data.get('icao', '').upper(),
                data.get('role'),
                data.get('orderIndex', 0),
                data.get('displayName'),
                airport_id,
            ])

        for airport_id in changes.get('missionAirports', {}).get('deleted', []):
            print(f"[sync-push] Deleting mission airport: {airport_id}")
            cursor.execute("DELETE FROM mission_airports WHERE id = %s", [airport_id])

        # ========== READINESS ASSESSMENTS (PostgreSQL) ==========
        # Assessments are immutable events — only create and soft-delete, no updates.

        for assessment in changes.get('readinessAssessments', {}).get('created', []):
            assessment_id = assessment.get('id')
            print(f"[sync-push] Creating readiness assessment: {assessment_id}")
            try:
                cursor.execute("""
                    INSERT INTO readiness_assessments (
                        id, mission_id,
                        assessment_time_utc, target_time_utc, target_time_kind,
                        route_hash, route_airports_json,
                        minimums_profile_id, minimums_profile_name, data_source,
                        aggregate_result, aggregate_reason_short, aggregate_reason_long,
                        dominant_factor, dominant_message, margin_message,
                        airports_evaluated_count, airports_missing_data_count,
                        flags_summary, max_severity_score,
                        near_limit_airport_count, no_go_airport_count,
                        airport_checks_json, stale_threshold_minutes,
                        created_at, deleted_at, _sync_pending
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, FALSE
                    )
                    ON CONFLICT (id) DO NOTHING
                """, [
                    assessment_id,
                    assessment.get('missionId'),
                    assessment.get('assessmentTimeUtc'),
                    assessment.get('targetTimeUtc'),
                    assessment.get('targetTimeKind', 'NOW'),
                    assessment.get('routeHash'),
                    Jsonb(assessment.get('routeAirportsJson')) if assessment.get('routeAirportsJson') else None,
                    assessment.get('minimumsProfileId'),
                    assessment.get('minimumsProfileName'),
                    assessment.get('dataSource'),
                    assessment.get('aggregateResult', 'UNKNOWN'),
                    assessment.get('aggregateReasonShort'),
                    assessment.get('aggregateReasonLong'),
                    assessment.get('dominantFactor'),
                    assessment.get('dominantMessage'),
                    assessment.get('marginMessage'),
                    assessment.get('airportsEvaluatedCount', 0),
                    assessment.get('airportsMissingDataCount', 0),
                    assessment.get('flagsSummary'),
                    assessment.get('maxSeverityScore', 0),
                    assessment.get('nearLimitAirportCount', 0),
                    assessment.get('noGoAirportCount', 0),
                    Jsonb(assessment.get('airportChecksJson')) if assessment.get('airportChecksJson') else None,
                    assessment.get('staleThresholdMinutes', 360),
                    assessment.get('createdAt', timestamp_ms),
                    assessment.get('deletedAt'),
                ])
            except Exception as e:
                conn.rollback()
                print(f"[sync-push] Error creating assessment {assessment_id}: {e}")

        # Assessments have no updates (immutable events)

        for assessment_id in changes.get('readinessAssessments', {}).get('deleted', []):
            print(f"[sync-push] Soft-deleting assessment: {assessment_id}")
            cursor.execute("""
                UPDATE readiness_assessments SET deleted_at = %s
                WHERE id = %s AND deleted_at IS NULL
            """, [timestamp_ms, assessment_id])

        # ========== PROFICIENCY SNAPSHOTS ==========
        # Snapshots are upserted by (user_id, snapshot_date) — the business key.
        # The server always takes the client's value; there are no conflicts.

        for snap in changes.get('proficiencySnapshots', {}).get('upserted', []):
            snap_id = snap.get('id')
            print(f"[sync-push] Upserting proficiency snapshot: {snap.get('snapshotDate')} score={snap.get('score')}")
            cursor.execute("""
                INSERT INTO proficiency_snapshots (
                    id, user_id, snapshot_date,
                    score, recency, exposure, envelope, consistency,
                    score_core_vfr, score_night, score_ifr, score_tailwheel, score_multi,
                    active_domains, computed_at, _sync_pending
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                ON CONFLICT (id) DO UPDATE SET
                    score          = EXCLUDED.score,
                    recency        = EXCLUDED.recency,
                    exposure       = EXCLUDED.exposure,
                    envelope       = EXCLUDED.envelope,
                    consistency    = EXCLUDED.consistency,
                    score_core_vfr = EXCLUDED.score_core_vfr,
                    score_night    = EXCLUDED.score_night,
                    score_ifr      = EXCLUDED.score_ifr,
                    score_tailwheel = EXCLUDED.score_tailwheel,
                    score_multi    = EXCLUDED.score_multi,
                    active_domains = EXCLUDED.active_domains,
                    computed_at    = EXCLUDED.computed_at
            """, [
                snap_id,
                user_id,
                snap.get('snapshotDate'),
                snap.get('score'),
                snap.get('recency', 0),
                snap.get('exposure', 0),
                snap.get('envelope', 0),
                snap.get('consistency', 0),
                snap.get('scoreCoreVfr'),
                snap.get('scoreNight'),
                snap.get('scoreIfr'),
                snap.get('scoreTailwheel'),
                snap.get('scoreMulti'),
                snap.get('activeDomains'),
                int(snap.get('computedAt', timestamp_ms)),
            ])

        # ========== STUDENT CFI SHARES (consent) ==========
        # Student explicitly opts in/out of sharing proficiency data with a specific CFI.
        # Self-link guard: reject cfi_user_id == caller's user_id.

        # Track which CFI was affected so we can populate the AppSync subscription payload.
        # Initialised here so both proficiency snapshot and studentCfiShares blocks can set it.
        affected_cfi_user_id = None
        affected_event_type = None

        # If the student pushed new proficiency snapshots and is actively sharing with a CFI,
        # notify that CFI via cfiDataChanged so they see updated scores immediately.
        if changes.get('proficiencySnapshots', {}).get('upserted'):
            cursor.execute("""
                SELECT cfi_user_id FROM student_cfi_shares
                WHERE student_id = %s AND sharing = TRUE
                LIMIT 1
            """, [user_id])
            row = cursor.fetchone()
            if row:
                affected_cfi_user_id = row[0]
                affected_event_type = 'proficiencyUpdated'
                print(f"[sync-push] Proficiency snapshot will notify CFI: {affected_cfi_user_id}")

        for share in changes.get('studentCfiShares', {}).get('upserted', []):
            cfi_user_id = share.get('cfiUserId', '')
            sharing = bool(share.get('sharing', True))
            student_name = share.get('studentName') or None
            student_cert_type = share.get('studentCertificateType') or None
            if not cfi_user_id or cfi_user_id == user_id:
                print(f"[sync-push] Skipping invalid studentCfiShare: cfiUserId={cfi_user_id}")
                continue

            # Fall back to the DynamoDB name if the client didn't send one.
            # This covers the case where the Apollo cache was empty on the client
            # (e.g. app cold-started directly into the sharing screen).
            if not student_name and pushing_user_name:
                student_name = pushing_user_name
                print(f"[sync-push] studentName missing from payload; falling back to DynamoDB name: {repr(student_name)}")

            print(f"[sync-push] Upserting studentCfiShare: cfi={cfi_user_id} sharing={sharing} studentName={repr(student_name)} certType={repr(student_cert_type)}")
            student_snapshot = None
            if student_name or student_cert_type:
                import json as _json
                student_snapshot = _json.dumps({
                    'name': student_name,
                    'certificateType': student_cert_type,
                })
            print(f"[sync-push] student_snapshot to write: {repr(student_snapshot)}")
            cursor.execute("""
                INSERT INTO student_cfi_shares (student_id, cfi_user_id, sharing, updated_at, student_snapshot)
                VALUES (%s, %s, %s, NOW(), %s)
                ON CONFLICT (student_id, cfi_user_id)
                DO UPDATE SET sharing = EXCLUDED.sharing,
                              updated_at = NOW(),
                              student_snapshot = COALESCE(EXCLUDED.student_snapshot, student_cfi_shares.student_snapshot)
            """, [user_id, cfi_user_id, sharing, student_snapshot])

            # Capture the first affected CFI for the subscription event.
            # eventType tells the CFI why they're being notified.
            if affected_cfi_user_id is None:
                affected_cfi_user_id = cfi_user_id
                affected_event_type = 'sharingChanged' if not sharing else 'proficiencyUpdated'

        # If no sharing/proficiency event set a CFI yet, check for a pending signature request.
        if affected_cfi_user_id is None and _pending_sig_cfi_id:
            affected_cfi_user_id = _pending_sig_cfi_id
            affected_event_type = 'signatureRequested'
            print(f"[sync-push] Signature request will notify CFI: {affected_cfi_user_id}")

        conn.commit()

        print(f"[sync-push] Success: {len(conflicts)} conflicts")

        return {
            'timestamp': timestamp,
            'conflicts': conflicts,
            # Populated when a studentCfiShares row was processed — AppSync uses
            # these fields to fan-out the cfiDataChanged subscription event to
            # the affected CFI's connected clients.
            'cfiUserId': affected_cfi_user_id,
            'eventType': affected_event_type,
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
