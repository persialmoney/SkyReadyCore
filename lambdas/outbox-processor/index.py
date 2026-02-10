"""
Outbox Processor Lambda - Process outbox events to DynamoDB (every 1 minute)
"""
import boto3
import json
import os
from db_utils import get_db_connection

dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    """Process outbox events to DynamoDB (every 1 minute)"""
    
    events_table_name = os.environ.get('EVENTS_TABLE_NAME', 'sky-ready-events-dev')
    events_table = dynamodb.Table(events_table_name)
    
    print(f"[outbox-processor] Starting processing to table {events_table_name}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT id, event_type, user_id, payload, created_at
            FROM outbox
            WHERE processed = false
            ORDER BY created_at
            LIMIT 100
        """)
        
        rows = cursor.fetchall()
        
        print(f"[outbox-processor] Found {len(rows)} unprocessed events")
        
        for row in rows:
            event_id, event_type, user_id, payload, created_at = row
            
            try:
                # Write to DynamoDB for pub/sub
                events_table.put_item(Item={
                    'id': str(event_id),
                    'type': event_type,
                    'userId': user_id,
                    'payload': payload,
                    'timestamp': int(created_at.timestamp() * 1000)
                })
                
                # Mark as processed
                cursor.execute("""
                    UPDATE outbox SET
                        processed = true,
                        processed_at = NOW()
                    WHERE id = %s
                """, [event_id])
                
                conn.commit()
                
                print(f"[outbox-processor] Processed event {event_id}")
                
            except Exception as e:
                print(f"[outbox-processor] Error processing event {event_id}: {e}")
                conn.rollback()
        
        return {'processed': len(rows)}
    
    except Exception as e:
        print(f"[outbox-processor] Error: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()
