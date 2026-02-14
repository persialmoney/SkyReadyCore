"""
Sync Pull Lambda - Send changes since lastPulledAt (cursor pattern)
"""
import json
import time
import os
from datetime import datetime
from db_utils import get_db_connection, return_db_connection

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
                instructor, student, lesson_topic, ground_instruction,
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
            created_at = int(row[38].timestamp() * 1000)
            updated_at = int(row[39].timestamp() * 1000) if row[39] else created_at
            
            if created_at > last_pulled_at:
                created.append(format_entry(row))
            else:
                updated.append(format_entry(row))
        
        # Query 2: Get deleted entry IDs only (don't send full entry data)
        print(f"[sync-pull] Querying for deleted entries since {last_pulled_datetime}")
        cursor.execute("""
            SELECT entry_id, deleted_at, updated_at, date, status
            FROM logbook_entries
            WHERE user_id = %s
              AND deleted_at IS NOT NULL
              AND deleted_at > %s
            ORDER BY deleted_at
        """, [user_id, last_pulled_datetime])
        
        deleted_rows = cursor.fetchall()
        deleted = [row[0] for row in deleted_rows]
        
        # Log detailed info about each deleted entry
        print(f"[sync-pull] Found {len(deleted)} deleted entries since last pull")
        for row in deleted_rows:
            print(f"[sync-pull] Deleted entry - entry_id: {row[0]}, deleted_at: {row[1]}, updated_at: {row[2]}, date: {row[3]}, status: {row[4]}")
        
        # Also query total count of all deleted entries for this user (for debugging)
        cursor.execute("""
            SELECT COUNT(*) FROM logbook_entries
            WHERE user_id = %s AND deleted_at IS NOT NULL
        """, [user_id])
        total_deleted_count = cursor.fetchone()[0]
        print(f"[sync-pull] Total deleted entries in DB for user: {total_deleted_count}")
        
        result = {
            'changes': {
                'logbookEntries': {
                    'created': created,
                    'updated': updated,
                    'deleted': deleted
                }
            },
            'cursor': str(offset + len(rows)),
            'hasMore': len(rows) == limit,
            'timestamp': timestamp
        }
        
        print(f"[sync-pull] Returning {len(created)} created, {len(updated)} updated, {len(deleted)} deleted")
        
        return result
    
    except Exception as e:
        print(f"[sync-pull] Error: {e}")
        raise e
    finally:
        cursor.close()
        return_db_connection(conn)

def format_entry(row):
    """Format database row to GraphQL entry"""
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
        'instructor': row[27],
        'student': row[28],
        'lessonTopic': row[29],
        'groundInstruction': float(row[30]) if row[30] else 0,
        'maneuvers': row[31] if row[31] else [],
        'remarks': row[32],
        'safetyNotes': row[33],
        'safetyRelevant': bool(row[34]) if row[34] is not None else False,
        'status': row[35],
        'signature': row[36],
        'isFlightReview': bool(row[37]) if row[37] is not None else False,
        'createdAt': int(row[38].timestamp() * 1000),
        'updatedAt': int(row[39].timestamp() * 1000) if row[39] else int(row[38].timestamp() * 1000),
    }
