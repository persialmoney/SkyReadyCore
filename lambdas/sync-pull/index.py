"""
Sync Pull Lambda - Send changes since lastPulledAt (cursor pattern)
- Logbook entries: PostgreSQL
- User data (preferences, aircraft, personal minimums): DynamoDB
"""
import json
import time
import os
import boto3
from datetime import datetime
from db_utils import get_db_connection, return_db_connection

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))

def handler(event, context):
    """
    Send changes since lastPulledAt (cursor pattern)
    """
    print(f"[sync-pull] Processing sync request: {json.dumps(event, default=str)}")
    
    user_id = event['identity']['claims']['sub']
    last_pulled_at = event['arguments'].get('lastPulledAt', 0)
    cursor_arg = event['arguments'].get('cursor')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        limit = 100
        # Handle None/null cursor (first request)
        offset = int(cursor_arg) if cursor_arg is not None else 0
        timestamp = int(time.time() * 1000)
        
        # Convert timestamp to PostgreSQL timestamp
        last_pulled_datetime = datetime.fromtimestamp(last_pulled_at / 1000.0)
        
        print(f"[sync-pull] Fetching changes since {last_pulled_datetime} for user {user_id}")
        
        # ========== LOGBOOK ENTRIES (PostgreSQL) ==========
        
        # Query 1: Get active entries (created or updated) - EXCLUDE deleted entries
        cursor.execute("""
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
                status, signature, is_flight_review,
                created_at, updated_at
            FROM logbook_entries
            WHERE user_id = %s
              AND deleted_at IS NULL
              AND (created_at > %s OR updated_at > %s)
            ORDER BY GREATEST(created_at, updated_at)
            LIMIT %s OFFSET %s
        """, [user_id, last_pulled_datetime, last_pulled_datetime, limit, offset])
        
        rows = cursor.fetchall()
        
        print(f"[sync-pull] Found {len(rows)} active entries")
        
        # Categorize changes
        created = []
        updated = []
        
        for row in rows:
            entry_id = row[0]
            created_at = int(row[42].timestamp() * 1000)
            updated_at = int(row[43].timestamp() * 1000) if row[43] else created_at
            
            if created_at > last_pulled_at:
                created.append(format_entry(row))
            else:
                updated.append(format_entry(row))
        
        # Query 2: Get deleted entry IDs only (don't send full entry data)
        cursor.execute("""
            SELECT entry_id
            FROM logbook_entries
            WHERE user_id = %s
              AND deleted_at IS NOT NULL
              AND deleted_at > %s
            ORDER BY deleted_at
        """, [user_id, last_pulled_datetime])
        
        deleted = [str(row[0]) for row in cursor.fetchall()]

        # Commit the read-only transaction
        conn.commit()
        
        # ========== USER DATA (DynamoDB) ==========
        
        # Get user object from DynamoDB
        user_response = users_table.get_item(Key={'userId': user_id})
        user_item = user_response.get('Item', {})
        user_updated_at = user_item.get('updatedAt', '')
        
        # Parse DynamoDB timestamp (ISO 8601)
        try:
            user_updated_datetime = datetime.fromisoformat(user_updated_at.replace('Z', '+00:00'))
            user_updated_timestamp = int(user_updated_datetime.timestamp() * 1000)
        except:
            user_updated_timestamp = 0
        
        # Only return user data if it's been updated since last pull
        user_data_changed = user_updated_timestamp > last_pulled_at
        
        # Personal Minimums Profiles
        profiles_created = []
        profiles_updated = []
        profiles_deleted = []
        
        if user_data_changed:
            personal_minimums = user_item.get('personalMinimumsProfiles', [])
            for profile in personal_minimums:
                # Check if profile was created or updated since last pull
                profile_created_at = parse_timestamp(profile.get('createdAt', ''))
                profile_updated_at = parse_timestamp(profile.get('updatedAt', ''))
                
                if profile.get('deletedAt'):
                    deleted_at_ts = parse_timestamp(profile.get('deletedAt'))
                    if deleted_at_ts > last_pulled_at:
                        profiles_deleted.append(profile.get('id'))
                elif profile_created_at > last_pulled_at:
                    profiles_created.append(format_minimums_profile(profile))
                elif profile_updated_at > last_pulled_at:
                    profiles_updated.append(format_minimums_profile(profile))
        
        # User Aircraft
        aircraft_created = []
        aircraft_updated = []
        aircraft_deleted = []
        
        if user_data_changed:
            aircraft_list = user_item.get('aircraft', [])
            for aircraft in aircraft_list:
                added_at_ts = parse_timestamp(aircraft.get('addedAt', ''))
                # For aircraft, we treat any change as "created" since we don't track individual updates
                if added_at_ts > last_pulled_at:
                    aircraft_created.append(format_aircraft(aircraft))
        
        # User Preferences
        prefs_created = []
        prefs_updated = []
        
        if user_data_changed:
            preferences = user_item.get('preferences', {})
            if preferences:
                # For preferences, we only send if user object was updated
                prefs_updated.append(format_preferences(user_id, preferences))

        result = {
            'changes': {
                'logbookEntries': {
                    'created': created,
                    'updated': updated,
                    'deleted': deleted
                },
                'personalMinimumsProfiles': {
                    'created': profiles_created,
                    'updated': profiles_updated,
                    'deleted': profiles_deleted
                },
                'userAircraft': {
                    'created': aircraft_created,
                    'updated': aircraft_updated,
                    'deleted': aircraft_deleted
                },
                'userPreferences': {
                    'created': prefs_created,
                    'updated': prefs_updated
                }
            },
            'cursor': str(offset + len(rows)),
            'hasMore': len(rows) == limit,
            'timestamp': timestamp
        }

        print(f"[sync-pull] Returning logbook({len(created)} created, {len(updated)} updated, {len(deleted)} deleted) profiles({len(profiles_created)} created, {len(profiles_updated)} updated) aircraft({len(aircraft_created)} created) preferences({len(prefs_updated)} updated)")
        return result

    except Exception as e:
        print(f"[sync-pull] Error: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        return_db_connection(conn)

def parse_timestamp(iso_string):
    """Parse ISO 8601 timestamp to milliseconds since epoch"""
    if not iso_string:
        return 0
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except:
        return 0

def format_entry(row):
    """Format database row to GraphQL entry."""
    return {
        'entryId': str(row[0]),
        'userId': row[1],
        'date': row[2].isoformat() if row[2] else None,
        'aircraft': row[3],
        'tailNumber': row[4],
        'route': row[5],
        'routeLegs': row[6] if row[6] else [],
        'flightTypes': row[7] if row[7] else [],
        'totalTime': float(row[8]) if row[8] else 0,
        'pic': float(row[9]) if row[9] else 0,
        'sic': float(row[10]) if row[10] else 0,
        'dualReceived': float(row[11]) if row[11] else 0,
        'dualGiven': float(row[12]) if row[12] else 0,
        'solo': float(row[13]) if row[13] else 0,
        'crossCountry': float(row[14]) if row[14] else 0,
        'night': float(row[15]) if row[15] else 0,
        'actualImc': float(row[16]) if row[16] else 0,
        'simulatedInstrument': float(row[17]) if row[17] else 0,
        'dayTakeoffs': int(row[18]) if row[18] else 0,
        'dayLandings': int(row[19]) if row[19] else 0,
        'nightTakeoffs': int(row[20]) if row[20] else 0,
        'nightLandings': int(row[21]) if row[21] else 0,
        'dayFullStopLandings': int(row[22]) if row[22] else 0,
        'nightFullStopLandings': int(row[23]) if row[23] else 0,
        'approaches': int(row[24]) if row[24] else 0,
        'holds': bool(row[25]) if row[25] is not None else False,
        'tracking': bool(row[26]) if row[26] is not None else False,
        'instructorUserId': row[27],
        'instructorSnapshot': row[28],
        'studentUserId': row[29],
        'studentSnapshot': row[30],
        'mirroredFromEntryId': str(row[31]) if row[31] else None,
        'mirroredFromUserId': row[32],
        'lessonTopic': row[33],
        'groundInstruction': float(row[34]) if row[34] else 0,
        'maneuvers': row[35] if row[35] else [],
        'remarks': row[36],
        'safetyNotes': row[37],
        'safetyRelevant': bool(row[38]) if row[38] is not None else False,
        'status': row[39],
        'signature': row[40],
        'isFlightReview': bool(row[41]) if row[41] is not None else False,
        'createdAt': int(row[42].timestamp() * 1000),
        'updatedAt': int(row[43].timestamp() * 1000) if row[43] else int(row[42].timestamp() * 1000),
    }

def format_minimums_profile(profile):
    """Format DynamoDB personal minimums profile to GraphQL"""
    return {
        'id': profile.get('id'),
        'userId': profile.get('userId'),
        'name': profile.get('name'),
        'kind': profile.get('kind'),
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
        'createdAt': parse_timestamp(profile.get('createdAt', '')),
        'updatedAt': parse_timestamp(profile.get('updatedAt', '')),
        'version': profile.get('version', 0),
    }

def format_aircraft(aircraft):
    """Format DynamoDB aircraft to GraphQL"""
    return {
        'tailNumber': aircraft.get('tailNumber'),
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
        'addedAt': parse_timestamp(aircraft.get('addedAt', '')),
    }

def format_preferences(user_id, prefs):
    """Format DynamoDB preferences to GraphQL"""
    return {
        'userId': user_id,
        'defaultUnits': prefs.get('defaultUnits'),
        'notificationEnabled': prefs.get('notificationEnabled', True),
        'criticalAlertThreshold': prefs.get('criticalAlertThreshold'),
        'defaultAirport': prefs.get('defaultAirport'),
        'enabledCurrencies': prefs.get('enabledCurrencies', []),
        'createdAt': parse_timestamp(prefs.get('createdAt', '')),
        'updatedAt': parse_timestamp(prefs.get('updatedAt', '')),
    }
