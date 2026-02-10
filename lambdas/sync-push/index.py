"""
Sync Push Lambda - Process bulk changes from client (outbox pattern)
Clean implementation - no old CRUD fallbacks
"""
import json
import time
import os
from db_utils import get_db_connection
import psycopg2

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
        cursor.execute('BEGIN')
        
        conflicts = []
        timestamp = int(time.time() * 1000)
        
        # Process created entries
        for entry in changes.get('logbookEntries', {}).get('created', []):
            try:
                print(f"[sync-push] Creating entry: {entry.get('entryId')}")
                
                cursor.execute("""
                    INSERT INTO logbook_entries (
                        entry_id, user_id, date, aircraft, tail_number,
                        route, route_legs, flight_types, total_time,
                        pic, sic, dual_received, dual_given, solo,
                        cross_country, night, actual_imc, simulated_instrument,
                        day_takeoffs, day_landings, night_takeoffs, night_landings,
                        full_stop_landings, approaches, holds, tracking,
                        instructor, student, lesson_topic, ground_instruction,
                        maneuvers, remarks, safety_notes, safety_relevant,
                        status, signature, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                    )
                """, [
                    entry['entryId'], user_id, entry['date'],
                    json.dumps(entry.get('aircraft')), entry.get('tailNumber'),
                    entry.get('route'), json.dumps(entry.get('routeLegs', [])),
                    entry.get('flightTypes', []), entry.get('totalTime', 0),
                    entry.get('pic', 0), entry.get('sic', 0),
                    entry.get('dualReceived', 0), entry.get('dualGiven', 0),
                    entry.get('solo', 0), entry.get('crossCountry', 0),
                    entry.get('night', 0), entry.get('actualImc', 0),
                    entry.get('simulatedInstrument', 0),
                    entry.get('dayTakeoffs', 0), entry.get('dayLandings', 0),
                    entry.get('nightTakeoffs', 0), entry.get('nightLandings', 0),
                    entry.get('fullStopLandings', 0), entry.get('approaches', 0),
                    entry.get('holds', False), entry.get('tracking', False),
                    json.dumps(entry.get('instructor')), json.dumps(entry.get('student')),
                    entry.get('lessonTopic'), entry.get('groundInstruction'),
                    entry.get('maneuvers', []), entry.get('remarks'),
                    entry.get('safetyNotes'), entry.get('safetyRelevant', False),
                    entry.get('status', 'draft'), json.dumps(entry.get('signature'))
                ])
                
            except psycopg2.IntegrityError as e:
                print(f"[sync-push] Conflict creating entry {entry['entryId']}: {e}")
                conflicts.append({
                    'entryId': entry['entryId'],
                    'type': 'ALREADY_EXISTS',
                    'serverTimestamp': timestamp
                })
        
        # Process updated entries
        for update in changes.get('logbookEntries', {}).get('updated', []):
            entry_id = update['entryId']
            entry_data = update['data']
            
            print(f"[sync-push] Updating entry: {entry_id}")
            
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
                    date = %s, total_time = %s, pic = %s, sic = %s,
                    dual_received = %s, dual_given = %s, solo = %s,
                    cross_country = %s, night = %s, actual_imc = %s,
                    simulated_instrument = %s, day_takeoffs = %s, day_landings = %s,
                    night_takeoffs = %s, night_landings = %s, approaches = %s,
                    holds = %s, remarks = %s, status = %s, updated_at = NOW()
                WHERE entry_id = %s AND user_id = %s AND deleted_at IS NULL
            """, [
                entry_data['date'], entry_data.get('totalTime', 0),
                entry_data.get('pic', 0), entry_data.get('sic', 0),
                entry_data.get('dualReceived', 0), entry_data.get('dualGiven', 0),
                entry_data.get('solo', 0), entry_data.get('crossCountry', 0),
                entry_data.get('night', 0), entry_data.get('actualImc', 0),
                entry_data.get('simulatedInstrument', 0),
                entry_data.get('dayTakeoffs', 0), entry_data.get('dayLandings', 0),
                entry_data.get('nightTakeoffs', 0), entry_data.get('nightLandings', 0),
                entry_data.get('approaches', 0), entry_data.get('holds', False),
                entry_data.get('remarks'), entry_data.get('status', 'draft'),
                entry_id, user_id
            ])
        
        # Process deleted entries (soft delete)
        for entry_id in changes.get('logbookEntries', {}).get('deleted', []):
            print(f"[sync-push] Deleting entry: {entry_id}")
            cursor.execute("""
                UPDATE logbook_entries SET
                    deleted_at = NOW(),
                    updated_at = NOW()
                WHERE entry_id = %s AND user_id = %s AND deleted_at IS NULL
            """, [entry_id, user_id])
        
        # Write to outbox for pub/sub
        cursor.execute("""
            INSERT INTO outbox (event_type, user_id, payload, created_at)
            VALUES (%s, %s, %s, NOW())
        """, ['sync_push', user_id, json.dumps(changes)])
        
        cursor.execute('COMMIT')
        
        print(f"[sync-push] Success: {len(conflicts)} conflicts")
        
        return {
            'timestamp': timestamp,
            'conflicts': conflicts
        }
    
    except Exception as e:
        cursor.execute('ROLLBACK')
        print(f"[sync-push] Error: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()
