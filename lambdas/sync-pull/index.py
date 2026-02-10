"""
Sync Pull Lambda - Send changes since lastPulledAt (cursor pattern)
"""
import json
import time
import os
import sys
from datetime import datetime

# Add shared modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))

from db_utils import get_db_connection

def handler(event, context):
    """
    Send changes since lastPulledAt (cursor pattern)
    """
    print(f"[sync-pull] Processing sync request: {json.dumps(event, default=str)}")
    
    user_id = event['identity']['claims']['sub']
    last_pulled_at = event['arguments'].get('lastPulledAt', 0)
    cursor_arg = event['arguments'].get('cursor', '0')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        limit = 100
        offset = int(cursor_arg)
        timestamp = int(time.time() * 1000)
        
        # Convert timestamp to PostgreSQL timestamp
        last_pulled_datetime = datetime.fromtimestamp(last_pulled_at / 1000.0)
        
        print(f"[sync-pull] Fetching changes since {last_pulled_datetime} for user {user_id}")
        
        # Get entries changed since lastPulledAt
        cursor.execute("""
            SELECT 
                entry_id, user_id, date, aircraft, tail_number,
                route, route_legs, flight_types, total_time,
                pic, sic, dual_received, dual_given, solo,
                cross_country, night, actual_imc, simulated_instrument,
                day_takeoffs, day_landings, night_takeoffs, night_landings,
                full_stop_landings, approaches, holds, tracking,
                instructor, student, lesson_topic, ground_instruction,
                maneuvers, remarks, safety_notes, safety_relevant,
                status, signature,
                created_at, updated_at, deleted_at
            FROM logbook_entries
            WHERE user_id = %s
              AND (
                created_at > %s
                OR updated_at > %s
                OR (deleted_at IS NOT NULL AND deleted_at > %s)
              )
            ORDER BY GREATEST(
                created_at,
                updated_at,
                COALESCE(deleted_at, created_at)
            )
            LIMIT %s OFFSET %s
        """, [user_id, last_pulled_datetime, last_pulled_datetime, last_pulled_datetime, limit, offset])
        
        rows = cursor.fetchall()
        
        print(f"[sync-pull] Found {len(rows)} changed entries")
        
        # Categorize changes
        created = []
        updated = []
        deleted = []
        
        for row in rows:
            entry_id = row[0]
            created_at = int(row[36].timestamp() * 1000)
            updated_at = int(row[37].timestamp() * 1000) if row[37] else created_at
            deleted_at = int(row[38].timestamp() * 1000) if row[38] else None
            
            if deleted_at and deleted_at > last_pulled_at:
                deleted.append(entry_id)
            elif created_at > last_pulled_at:
                created.append(format_entry(row))
            else:
                updated.append(format_entry(row))
        
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
        conn.close()

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
        'fullStopLandings': int(row[22]) if row[22] else 0,
        'approaches': int(row[23]) if row[23] else 0,
        'holds': bool(row[24]) if row[24] is not None else False,
        'tracking': bool(row[25]) if row[25] is not None else False,
        'instructor': row[26],
        'student': row[27],
        'lessonTopic': row[28],
        'groundInstruction': row[29],
        'maneuvers': row[30] if row[30] else [],
        'remarks': row[31],
        'safetyNotes': row[32],
        'safetyRelevant': bool(row[33]) if row[33] is not None else False,
        'status': row[34],
        'signature': row[35],
        'createdAt': int(row[36].timestamp() * 1000),
        'updatedAt': int(row[37].timestamp() * 1000) if row[37] else int(row[36].timestamp() * 1000),
    }
