"""
Outbox Processor Lambda - Process outbox events to DynamoDB (every 1 minute)
"""
import boto3
import json
import os
from decimal import Decimal
from db_utils import get_db_connection, return_db_connection

dynamodb = boto3.resource('dynamodb')

def convert_floats_to_decimal(obj):
    """
    Recursively convert all float values to Decimal for DynamoDB compatibility.
    DynamoDB does not support Python float types - must use Decimal.
    """
    if isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, float):
        # Convert float to Decimal, preserving precision
        return Decimal(str(obj))
    else:
        return obj

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
        
        successful_count = 0
        failed_count = 0
        
        for row in rows:
            event_id, event_type, user_id, payload, created_at = row
            
            try:
                # Convert payload floats to Decimal for DynamoDB
                converted_payload = convert_floats_to_decimal(payload)
                
                print(f"[outbox-processor] Processing event {event_id}, type: {event_type}, user: {user_id}")
                
                # Write to DynamoDB for pub/sub
                events_table.put_item(Item={
                    'id': str(event_id),
                    'type': event_type,
                    'userId': user_id,
                    'payload': converted_payload,
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
                
                print(f"[outbox-processor] Successfully processed event {event_id}")
                successful_count += 1
                
            except Exception as e:
                print(f"[outbox-processor] Error processing event {event_id}: {e}")
                print(f"[outbox-processor] Event details - type: {event_type}, user: {user_id}")
                failed_count += 1
                conn.rollback()
        
        print(f"[outbox-processor] Completed: {successful_count} successful, {failed_count} failed")
        
        return {
            'processed': successful_count,
            'failed': failed_count,
            'total': len(rows)
        }
    
    except Exception as e:
        print(f"[outbox-processor] Error: {e}")
        raise e
    finally:
        cursor.close()
        return_db_connection(conn)
