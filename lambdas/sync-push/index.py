"""
Sync Push Lambda - Process bulk changes from client (outbox pattern)
- Logbook entries: PostgreSQL
- User data (preferences, aircraft, personal minimums): DynamoDB
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
        str(entry.get('totalTime', '')),
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
    last_pulled_at = event['arguments'].get('lastPulledAt', 0)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        conflicts = []
        timestamp = int(time.time() * 1000)
        
        # ========== LOGBOOK ENTRIES (PostgreSQL) ==========
        
        # Process created entries
        for entry in changes.get('logbookEntries', {}).get('created', []):
            try:
                print(f"[sync-push] Creating entry: {entry.get('entryId')}")
                
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
                    entry.get('studentUserId'), Jsonb(entry.get('studentSnapshot')),
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
                server_updated_at = int(row[0].timestamp() * 1000)
                if server_updated_at > last_pulled_at:
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
                entry_data.get('studentUserId'), Jsonb(entry_data.get('studentSnapshot')),
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
            print(f"[sync-push] Creating personal minimums profile: {profile.get('id')}")
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
            
            print(f"[sync-push] Updating personal minimums profile: {profile_id}")
            
            for i, profile in enumerate(personal_minimums):
                if profile.get('id') == profile_id:
                    # Check for conflict: is server version newer than client?
                    server_updated_at_str = profile.get('updatedAt', '')
                    try:
                        server_updated_at = datetime.fromisoformat(server_updated_at_str.replace('Z', '+00:00'))
                        server_timestamp_ms = int(server_updated_at.timestamp() * 1000)
                        
                        # Conflict: server is newer than client's version
                        if server_timestamp_ms > client_updated_at:
                            print(f"[sync-push] Conflict detected for profile {profile_id}: server={server_timestamp_ms} > client={client_updated_at}")
                            conflicts.append({
                                'entryId': profile_id,
                                'type': 'SERVER_NEWER',
                                'serverTimestamp': server_timestamp_ms
                            })
                            continue  # Skip update, server wins
                    except:
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
                'enabledCurrencies': prefs.get('enabledCurrencies', [])
            }
            user_updated = True
        
        # Update DynamoDB if any user data changed
        if user_updated:
            user_item['personalMinimumsProfiles'] = personal_minimums
            user_item['aircraft'] = aircraft_list
            user_item['updatedAt'] = datetime.utcnow().isoformat() + 'Z'
            
            users_table.put_item(Item=user_item)
            print(f"[sync-push] Updated DynamoDB user object")

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
