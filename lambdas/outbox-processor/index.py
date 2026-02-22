"""
Outbox Processor Lambda - Process outbox events to DynamoDB (every 1 minute)
"""
import boto3
import json
import os
import time
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

        # Commit the read so the connection is in a clean state before any
        # per-row write transactions. psycopg3 starts a transaction on every
        # statement; not committing leaves the connection in INTRANS state,
        # which causes the pool to warn and roll back on return.
        conn.commit()

        if len(rows) == 0:
            return {'processed': 0, 'failed': 0, 'total': 0}

        print(f"[outbox-processor] Processing {len(rows)} events")

        successful_count = 0
        failed_count = 0

        for row in rows:
            event_id, event_type, user_id, payload, created_at = row

            try:
                # Convert payload floats to Decimal for DynamoDB
                converted_payload = convert_floats_to_decimal(payload)

                # Calculate TTL: 2 years from now (in seconds since epoch)
                ttl = int(time.time()) + (2 * 365 * 24 * 60 * 60)

                # Write to DynamoDB for pub/sub
                events_table.put_item(Item={
                    'id': str(event_id),
                    'type': event_type,
                    'userId': user_id,
                    'payload': converted_payload,
                    'timestamp': int(created_at.timestamp() * 1000),
                    'ttl': ttl
                })

                # Mark as processed and commit this row's transaction
                cursor.execute("""
                    UPDATE outbox SET
                        processed = true,
                        processed_at = NOW()
                    WHERE id = %s
                """, [event_id])

                conn.commit()
                successful_count += 1

            except Exception as e:
                print(f"[outbox-processor] Error processing event {event_id}: {e}")
                failed_count += 1
                conn.rollback()

        if failed_count > 0:
            print(f"[outbox-processor] Completed: {successful_count} successful, {failed_count} failed")

        return {
            'processed': successful_count,
            'failed': failed_count,
            'total': len(rows)
        }

    except Exception as e:
        print(f"[outbox-processor] Error: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        return_db_connection(conn)
