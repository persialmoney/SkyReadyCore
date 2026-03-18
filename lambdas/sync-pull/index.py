"""
Sync Pull Lambda - Send changes since lastPulledAt (cursor pattern)
- Logbook entries: PostgreSQL
- User data (preferences, aircraft, personal minimums): DynamoDB
- Missions, mission airports, readiness assessments: PostgreSQL
- Proficiency snapshots: PostgreSQL (last 90 days, incremental via computed_at)
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
    # lastPulledAt arrives as epoch milliseconds (Int scalar). All BIGINT columns in
    # PostgreSQL also store epoch ms, so no conversion is needed for those.
    # The only conversion needed is for TIMESTAMPTZ (logbook_entries) which requires
    # a Python datetime — done via last_pulled_at_sec = last_pulled_at // 1000.
    last_pulled_at = event['arguments'].get('lastPulledAt', 0) or 0  # epoch ms
    last_pulled_at_sec = last_pulled_at // 1000  # epoch seconds — for TIMESTAMPTZ only
    cursor_arg = event['arguments'].get('cursor')

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        limit = 100
        # Handle None/null cursor (first request)
        offset = int(cursor_arg) if cursor_arg is not None else 0
        timestamp_ms = int(time.time() * 1000)   # epoch ms — used for BIGINT comparisons
        timestamp = float(timestamp_ms)           # float so AppSync serializes as JSON Float, not Long

        # Convert to PostgreSQL timestamp for TIMESTAMPTZ columns (logbook, user data)
        last_pulled_datetime = datetime.fromtimestamp(last_pulled_at_sec)
        
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
        except Exception as e:
            user_updated_timestamp = 0
        
        # Only return user data if it's been updated since last pull
        user_data_changed = user_updated_timestamp > last_pulled_at
        
        # Personal Minimums Profiles
        profiles_created = []
        profiles_updated = []
        profiles_deleted = []
        
        if user_data_changed:
            personal_minimums = user_item.get('personalMinimumsProfiles', []) or []
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
            aircraft_list = user_item.get('aircraft', []) or []
            for aircraft in aircraft_list:
                added_at_ts = parse_timestamp(aircraft.get('addedAt', ''))
                # For aircraft, we treat any change as "created" since we don't track individual updates
                if added_at_ts > last_pulled_at:
                    aircraft_created.append(format_aircraft(aircraft))
        
        # User Preferences
        prefs_created = []
        prefs_updated = []
        
        if user_data_changed:
            preferences = user_item.get('preferences', {}) or {}
            if preferences:
                # For preferences, we only send if user object was updated
                prefs_updated.append(format_preferences(user_id, preferences))

        # ========== MISSIONS (PostgreSQL) ==========

        cursor.execute("""
            SELECT
                id, mission_id, user_id, status, is_operational,
                mission_type, scheduled_time_utc, time_precision,
                tail_number, aircraft_label, notes, forecast_reviewed_at,
                route_hash, latest_assessment_id, latest_result,
                latest_checked_time, latest_reason_short, latest_reason_long,
                created_at, updated_at, deleted_at
            FROM missions
            WHERE user_id = %s
              AND (created_at > %s OR updated_at > %s)
            ORDER BY GREATEST(created_at, updated_at)
        """, [user_id, last_pulled_at, last_pulled_at])

        missions_created = []
        missions_updated = []
        missions_deleted = []

        for row in cursor.fetchall():
            deleted_at = row[20]
            created_at = row[18]
            if deleted_at and deleted_at > last_pulled_at:
                missions_deleted.append(str(row[0]))
            elif created_at and created_at > last_pulled_at:
                missions_created.append(format_mission(row))
            else:
                missions_updated.append(format_mission(row))

        # ========== MISSION AIRPORTS (PostgreSQL) ==========
        # Sync strategy:
        #   • "created" list = all current airports for missions that changed since last pull.
        #     The client applies these as a replace-all (same semantics as sync-push), so the
        #     client will upsert new airports and ignore any that it already has.
        #   • "deleted" list = airport IDs whose parent mission was soft-deleted AND whose IDs
        #     the client would still have locally (mission soft-deleted since last pull).
        #
        # We intentionally do NOT try to diff individual airport rows server-side; the client
        # always reconciles by replacing the full set for any mission that changed.

        cursor.execute("""
            SELECT
                ma.id, ma.mission_id, ma.icao, ma.role, ma.order_index, ma.display_name
            FROM mission_airports ma
            INNER JOIN missions m ON m.id = ma.mission_id
            WHERE m.user_id = %s
              AND m.deleted_at IS NULL
              AND (m.created_at > %s OR m.updated_at > %s)
            ORDER BY ma.mission_id, ma.order_index
        """, [user_id, last_pulled_at, last_pulled_at])

        airports_created = []

        for row in cursor.fetchall():
            airports_created.append(format_mission_airport(row))

        # Deletions: airports whose parent mission was soft-deleted since last pull.
        # The client will destroy all airports linked to any deleted mission.
        cursor.execute("""
            SELECT ma.id
            FROM mission_airports ma
            INNER JOIN missions m ON m.id = ma.mission_id
            WHERE m.user_id = %s
              AND m.deleted_at IS NOT NULL
              AND m.deleted_at > %s
        """, [user_id, last_pulled_at])

        airports_deleted = [str(row[0]) for row in cursor.fetchall()]

        # ========== READINESS ASSESSMENTS (PostgreSQL) ==========

        cursor.execute("""
            SELECT
                ra.id, ra.mission_id,
                ra.assessment_time_utc, ra.target_time_utc, ra.target_time_kind,
                ra.route_hash, ra.route_airports_json,
                ra.minimums_profile_id, ra.minimums_profile_name, ra.data_source,
                ra.aggregate_result, ra.aggregate_reason_short, ra.aggregate_reason_long,
                ra.dominant_factor, ra.dominant_message, ra.margin_message,
                ra.airports_evaluated_count, ra.airports_missing_data_count,
                ra.flags_summary, ra.max_severity_score,
                ra.near_limit_airport_count, ra.no_go_airport_count,
                ra.airport_checks_json, ra.stale_threshold_minutes,
                ra.created_at, ra.deleted_at
            FROM readiness_assessments ra
            INNER JOIN missions m ON m.id = ra.mission_id
            WHERE m.user_id = %s
              AND (ra.created_at > %s OR ra.deleted_at > %s)
            ORDER BY ra.created_at
        """, [user_id, last_pulled_at, last_pulled_at])

        assessments_created = []
        assessments_deleted = []

        for row in cursor.fetchall():
            deleted_at = row[25]
            created_at = row[24]
            if deleted_at and deleted_at > last_pulled_at:
                assessments_deleted.append(str(row[0]))
            else:
                assessments_created.append(format_assessment(row))

        # ========== PROFICIENCY SNAPSHOTS (PostgreSQL) ==========
        # Return snapshots whose computed_at > lastPulledAt so the client can
        # populate the historical chart on fresh install and receive recomputed
        # scores on subsequent syncs.
        # LIMIT 90 caps history to ~3 months (one snapshot per calendar day).
        # Fresh install (lastPulledAt = 0): computed_at > 0 matches all rows.

        cursor.execute("""
            SELECT id, snapshot_date, score, recency, exposure, envelope, consistency,
                   score_core_vfr, score_night, score_ifr, score_tailwheel, score_multi,
                   active_domains, computed_at
            FROM proficiency_snapshots
            WHERE user_id = %s
              AND computed_at > %s
            ORDER BY snapshot_date DESC
            LIMIT 90
        """, [user_id, last_pulled_at])

        snapshots_created = []
        for row in cursor.fetchall():
            snapshots_created.append(format_snapshot(row))

        conn.commit()

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
                },
                'missions': {
                    'created': missions_created,
                    'updated': missions_updated,
                    'deleted': missions_deleted
                },
                'missionAirports': {
                    'created': airports_created,
                    'updated': [],
                    'deleted': airports_deleted
                },
                'readinessAssessments': {
                    'created': assessments_created,
                    'updated': [],
                    'deleted': assessments_deleted
                },
                'proficiencySnapshots': {
                    'created': snapshots_created,
                    'deleted': [],
                },
            },
            'cursor': str(offset + len(rows)),
            'hasMore': len(rows) == limit,
            'timestamp': timestamp
        }

        print(
            f"[sync-pull] logbook({len(created)}c/{len(updated)}u/{len(deleted)}d) "
            f"profiles({len(profiles_created)}c/{len(profiles_updated)}u) "
            f"aircraft({len(aircraft_created)}c) "
            f"missions({len(missions_created)}c/{len(missions_updated)}u/{len(missions_deleted)}d) "
            f"airports({len(airports_created)}c/{len(airports_deleted)}d) "
            f"assessments({len(assessments_created)}c/{len(assessments_deleted)}d) "
            f"snapshots({len(snapshots_created)}c)"
        )
        return result

    except Exception as e:
        print(f"[sync-pull] Error: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        return_db_connection(conn)

def parse_timestamp(iso_string):
    """Parse ISO 8601 timestamp to milliseconds since epoch as float.
    Returns float so AppSync serializes as JSON Float (not Long integer).
    """
    if not iso_string:
        return 0.0
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return float(int(dt.timestamp() * 1000))
    except:
        return 0.0

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
        'createdAt': float(int(row[42].timestamp() * 1000)),
        'updatedAt': float(int(row[43].timestamp() * 1000)) if row[43] else float(int(row[42].timestamp() * 1000)),
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


def format_mission(row):
    """Format missions DB row to GraphQL Mission type.

    Column order matches the SELECT in sync-pull:
      0:id 1:mission_id 2:user_id 3:status 4:is_operational
      5:mission_type 6:scheduled_time_utc 7:time_precision
      8:tail_number 9:aircraft_label 10:notes 11:forecast_reviewed_at
      12:route_hash 13:latest_assessment_id 14:latest_result
      15:latest_checked_time 16:latest_reason_short 17:latest_reason_long
      18:created_at 19:updated_at 20:deleted_at
    """
    return {
        'id': str(row[0]),
        'missionId': str(row[1]) if row[1] else str(row[0]),
        'userId': row[2],
        'status': row[3],
        'isOperational': bool(row[4]) if row[4] is not None else False,
        'missionType': row[5],
        'scheduledTimeUtc': float(int(row[6])) if row[6] is not None else None,
        'timePrecision': row[7],
        'tailNumber': row[8],
        'aircraftLabel': row[9],
        'notes': row[10],
        'forecastReviewedAt': float(int(row[11])) if row[11] is not None else None,
        'routeHash': row[12],
        'latestAssessmentId': row[13],
        'latestResult': row[14],
        'latestCheckedTime': float(int(row[15])) if row[15] is not None else None,
        'latestReasonShort': row[16],
        'latestReasonLong': row[17],
        'createdAt': float(int(row[18])) if row[18] is not None else None,
        'updatedAt': float(int(row[19])) if row[19] is not None else None,
        'deletedAt': float(int(row[20])) if row[20] is not None else None,
    }


def format_mission_airport(row):
    """Format mission_airports DB row to GraphQL MissionAirport type.

    Column order:
      0:id 1:mission_id 2:icao 3:role 4:order_index 5:display_name
    """
    return {
        'id': str(row[0]),
        'missionId': str(row[1]),
        'icao': row[2],
        'role': row[3],
        'orderIndex': int(row[4]) if row[4] is not None else 0,
        'displayName': row[5],
    }


def format_snapshot(row):
    """Format proficiency_snapshots DB row to GraphQL ProficiencySnapshot type.

    Column order matches SELECT in sync-pull:
      0:id 1:snapshot_date 2:score 3:recency 4:exposure 5:envelope 6:consistency
      7:score_core_vfr 8:score_night 9:score_ifr 10:score_tailwheel 11:score_multi
      12:active_domains 13:computed_at
    """
    return {
        'id':             str(row[0]),
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
        'activeDomains':  row[12],
        'computedAt':     float(int(row[13])),
    }


def format_assessment(row):
    """Format readiness_assessments DB row to GraphQL ReadinessAssessment type.

    Column order matches SELECT in sync-pull:
      0:id 1:mission_id
      2:assessment_time_utc 3:target_time_utc 4:target_time_kind
      5:route_hash 6:route_airports_json
      7:minimums_profile_id 8:minimums_profile_name 9:data_source
      10:aggregate_result 11:aggregate_reason_short 12:aggregate_reason_long
      13:dominant_factor 14:dominant_message 15:margin_message
      16:airports_evaluated_count 17:airports_missing_data_count
      18:flags_summary 19:max_severity_score
      20:near_limit_airport_count 21:no_go_airport_count
      22:airport_checks_json 23:stale_threshold_minutes
      24:created_at 25:deleted_at
    """
    return {
        'id': str(row[0]),
        'missionId': str(row[1]),
        'assessmentTimeUtc': float(int(row[2])) if row[2] is not None else None,
        'targetTimeUtc': float(int(row[3])) if row[3] is not None else None,
        'targetTimeKind': row[4],
        'routeHash': row[5],
        'routeAirportsJson': row[6],          # already parsed JSONB from psycopg
        'minimumsProfileId': row[7],
        'minimumsProfileName': row[8],
        'dataSource': row[9],
        'aggregateResult': row[10],
        'aggregateReasonShort': row[11],
        'aggregateReasonLong': row[12],
        'dominantFactor': row[13],
        'dominantMessage': row[14],
        'marginMessage': row[15],
        'airportsEvaluatedCount': int(row[16]) if row[16] is not None else 0,
        'airportsMissingDataCount': int(row[17]) if row[17] is not None else 0,
        'flagsSummary': row[18],
        'maxSeverityScore': int(row[19]) if row[19] is not None else 0,
        'nearLimitAirportCount': int(row[20]) if row[20] is not None else 0,
        'noGoAirportCount': int(row[21]) if row[21] is not None else 0,
        'airportChecksJson': row[22],         # already parsed JSONB from psycopg
        'staleThresholdMinutes': int(row[23]) if row[23] is not None else 360,
        'createdAt': float(int(row[24])) if row[24] is not None else None,
        'deletedAt': float(int(row[25])) if row[25] is not None else None,
    }
