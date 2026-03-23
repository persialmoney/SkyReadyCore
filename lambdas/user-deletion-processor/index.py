"""
Scheduled Lambda (EventBridge daily) for permanently deleting user data after
the 30-day grace period has expired.

Scans the deletion_requests table for items with status=GRACE_PERIOD and
scheduledHardDeleteAt <= now, then cascading-deletes all user data.
"""
import json
import os
import time
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from typing import Dict, Any, List

_dynamodb_resource = None
_deletion_requests_table = None
_users_table = None
_saved_airports_table = None
_alerts_table = None
_deletion_otps_table = None
_cognito_client = None

STAGE = os.environ.get('STAGE', 'dev')
USERS_TABLE = os.environ.get('USERS_TABLE', f'sky-ready-users-{STAGE}')
SAVED_AIRPORTS_TABLE = os.environ.get('SAVED_AIRPORTS_TABLE', f'sky-ready-saved-airports-{STAGE}')
ALERTS_TABLE = os.environ.get('ALERTS_TABLE', f'sky-ready-alerts-{STAGE}')
DELETION_OTPS_TABLE = os.environ.get('DELETION_OTPS_TABLE', f'sky-ready-deletion-otps-{STAGE}')
DELETION_REQUESTS_TABLE = os.environ.get('DELETION_REQUESTS_TABLE', f'sky-ready-deletion-requests-{STAGE}')
EVENTS_TABLE = os.environ.get('EVENTS_TABLE', f'sky-ready-events-{STAGE}')
USER_POOL_ID = os.environ.get('USER_POOL_ID', '')


def get_dynamodb():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource('dynamodb')
    return _dynamodb_resource


def get_table(table_ref, table_name):
    return get_dynamodb().Table(table_name)


def get_cognito_client():
    global _cognito_client
    if _cognito_client is None:
        _cognito_client = boto3.client('cognito-idp')
    return _cognito_client


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    - Scheduled (EventBridge): scan deletion_requests for expired GRACE_PERIOD rows.
    - Direct invoke (admin CLI): `{"adminPurgeUserId": "<cognito sub>"}` — immediate
      full delete using the same process_hard_delete path as the scheduled job.
    """
    # Admin / break-glass immediate purge (Lambda invoke from trusted principal)
    admin_uid = event.get("adminPurgeUserId") if isinstance(event, dict) else None
    if admin_uid:
        user_id = str(admin_uid).strip()
        print(f"[DeletionProcessor] Admin immediate purge requested for userId={user_id}")
        if not user_id:
            return {"ok": False, "error": "adminPurgeUserId is empty"}
        try:
            summary = process_hard_delete(user_id, {}, admin_purge=True)
            print(f"[DeletionProcessor] Admin purge completed: {summary}")
            return {"ok": True, "userId": user_id, "summary": summary}
        except Exception as e:
            print(f"[DeletionProcessor] Admin purge FAILED for {user_id}: {e}")
            return {"ok": False, "userId": user_id, "error": str(e)}

    # Scheduled job: process all expired grace-period deletion requests.
    print(f"[DeletionProcessor] Starting hard-delete scan at {datetime.utcnow().isoformat()}")

    requests_table = get_dynamodb().Table(DELETION_REQUESTS_TABLE)
    now_iso = datetime.utcnow().isoformat() + 'Z'

    expired_requests = scan_expired_requests(requests_table, now_iso)
    print(f"[DeletionProcessor] Found {len(expired_requests)} expired deletion requests")

    results = []
    for request in expired_requests:
        user_id = request['userId']
        try:
            summary = process_hard_delete(user_id, request, admin_purge=False)
            results.append({'userId': user_id, 'status': 'completed', 'summary': summary})
            print(f"[DeletionProcessor] Successfully hard-deleted user {user_id}: {summary}")
        except Exception as e:
            results.append({'userId': user_id, 'status': 'failed', 'error': str(e)})
            print(f"[DeletionProcessor] FAILED to hard-delete user {user_id}: {e}")

    print(f"[DeletionProcessor] Finished. Processed {len(results)} requests.")
    return {'processed': len(results), 'results': results}


def scan_expired_requests(table, now_iso: str) -> List[Dict]:
    """Find all deletion requests that have passed their grace period."""
    expired = []
    scan_kwargs = {
        'FilterExpression': '#s = :status AND scheduledHardDeleteAt <= :now',
        'ExpressionAttributeNames': {'#s': 'status'},
        'ExpressionAttributeValues': {
            ':status': 'GRACE_PERIOD',
            ':now': now_iso,
        },
    }

    while True:
        response = table.scan(**scan_kwargs)
        expired.extend(response.get('Items', []))
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

    return expired


def process_hard_delete(user_id: str, request: Dict, *, admin_purge: bool = False) -> Dict:
    """Execute permanent deletion of all user data across all stores."""
    summary = {
        'logbook_entries_deleted': 0,
        'outbox_entries_deleted': 0,
        'missions_deleted': 0,
        'mission_airports_deleted': 0,
        'assessments_deleted': 0,
        'saved_airports_deleted': 0,
        'alerts_deleted': 0,
        'events_deleted': 0,
        'cognito_deleted': False,
        'dynamodb_user_deleted': False,
    }

    # 1. PostgreSQL: Null out instructor/student references in OTHER users' entries
    # 2. PostgreSQL: Hard delete logbook entries, missions, airports, assessments
    # 3. PostgreSQL: Delete outbox entries for this user
    pg_summary = hard_delete_postgres_data(user_id)
    summary.update(pg_summary)

    # 4. DynamoDB: Delete saved airports
    summary['saved_airports_deleted'] = batch_delete_dynamo_items(
        SAVED_AIRPORTS_TABLE, 'userId', user_id, sort_key='airportCode'
    )

    # 5. DynamoDB: Delete alerts
    summary['alerts_deleted'] = batch_delete_dynamo_items(
        ALERTS_TABLE, 'userId', user_id, sort_key='alertId'
    )

    # 5b. DynamoDB: Delete outbox-derived events (userId + logbook payloads)
    summary['events_deleted'] = scan_delete_events_for_user(EVENTS_TABLE, user_id)

    # 6. DynamoDB: Delete stale OTP (if any)
    try:
        get_dynamodb().Table(DELETION_OTPS_TABLE).delete_item(Key={'userId': user_id})
    except Exception:
        pass

    # 7. DynamoDB: Delete user profile
    try:
        get_dynamodb().Table(USERS_TABLE).delete_item(Key={'userId': user_id})
        summary['dynamodb_user_deleted'] = True
    except Exception as e:
        print(f"[DeletionProcessor] Error deleting DynamoDB user: {e}")

    # 8. Cognito: Delete user permanently
    try:
        get_cognito_client().admin_delete_user(
            UserPoolId=USER_POOL_ID,
            Username=user_id,
        )
        summary['cognito_deleted'] = True
    except get_cognito_client().exceptions.UserNotFoundException:
        summary['cognito_deleted'] = True  # Already gone
    except Exception as e:
        print(f"[DeletionProcessor] Error deleting Cognito user: {e}")

    # 9. Update deletion_requests audit row (scheduled flow) or admin audit (CLI purge)
    _finalize_deletion_request_record(user_id, summary, admin_purge=admin_purge)

    return summary


def _finalize_deletion_request_record(user_id: str, summary: Dict, *, admin_purge: bool) -> None:
    """Mark deletion_requests COMPLETED, or create an admin audit row if none existed."""
    now_iso = datetime.utcnow().isoformat() + 'Z'
    table = get_dynamodb().Table(DELETION_REQUESTS_TABLE)
    audit_ttl = int(time.time()) + (365 * 86400)

    if admin_purge:
        try:
            table.update_item(
                Key={'userId': user_id},
                UpdateExpression=(
                    'SET #s = :status, completedAt = :completedAt, '
                    'dataSnapshot = :snapshot, requestedBy = :rb'
                ),
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={
                    ':status': 'COMPLETED',
                    ':completedAt': now_iso,
                    ':snapshot': summary,
                    ':rb': 'admin_cli_immediate',
                },
                ConditionExpression='attribute_exists(userId)',
            )
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code != 'ConditionalCheckFailedException':
                print(f"[DeletionProcessor] Error updating deletion_requests: {e}")
                raise
            table.put_item(Item={
                'userId': user_id,
                'status': 'COMPLETED',
                'requestedAt': now_iso,
                'completedAt': now_iso,
                'requestedBy': 'admin_cli_immediate',
                'reason': 'Immediate admin purge (no prior grace-period record)',
                'dataSnapshot': summary,
                'ttl': audit_ttl,
            })
            print(f"[DeletionProcessor] Wrote admin audit deletion_requests item for {user_id}")
        return

    table.update_item(
        Key={'userId': user_id},
        UpdateExpression='SET #s = :status, completedAt = :completedAt, dataSnapshot = :snapshot',
        ExpressionAttributeNames={'#s': 'status'},
        ExpressionAttributeValues={
            ':status': 'COMPLETED',
            ':completedAt': now_iso,
            ':snapshot': summary,
        },
    )


def hard_delete_postgres_data(user_id: str) -> Dict:
    """Hard-delete all PostgreSQL data for this user."""
    db_secret_arn = os.environ.get('DB_SECRET_ARN')
    db_endpoint = os.environ.get('DB_ENDPOINT')
    db_name = os.environ.get('DB_NAME', 'logbook')

    result = {
        'logbook_entries_deleted': 0,
        'outbox_entries_deleted': 0,
        'references_nulled': 0,
        'missions_deleted': 0,
        'mission_airports_deleted': 0,
        'assessments_deleted': 0,
        'proficiency_snapshots_deleted': 0,
    }

    if not db_secret_arn or not db_endpoint:
        print("[DeletionProcessor] PostgreSQL not configured, skipping")
        return result

    import psycopg

    secrets_client = boto3.client('secretsmanager')
    secret = json.loads(
        secrets_client.get_secret_value(SecretId=db_secret_arn)['SecretString']
    )

    conn = psycopg.connect(
        host=db_endpoint,
        port=5432,
        dbname=db_name,
        user=secret['username'],
        password=secret['password'],
    )

    try:
        with conn.cursor() as cur:
            # ------------------------------------------------------------------
            # LOGBOOK ENTRIES
            # ------------------------------------------------------------------

            # Null out instructor/student/mirror references in OTHER users' entries
            cur.execute(
                "UPDATE logbook_entries SET instructor_user_id = NULL "
                "WHERE instructor_user_id = %s",
                (user_id,)
            )
            refs_nulled = cur.rowcount

            cur.execute(
                "UPDATE logbook_entries SET student_user_id = NULL "
                "WHERE student_user_id = %s",
                (user_id,)
            )
            refs_nulled += cur.rowcount

            cur.execute(
                "UPDATE logbook_entries SET mirrored_from_user_id = NULL "
                "WHERE mirrored_from_user_id = %s",
                (user_id,)
            )
            refs_nulled += cur.rowcount
            result['references_nulled'] = refs_nulled

            # Hard delete logbook entries (cascades to logbook_entry_embeddings via FK)
            cur.execute(
                "DELETE FROM logbook_entries WHERE user_id = %s",
                (user_id,)
            )
            result['logbook_entries_deleted'] = cur.rowcount

            # ------------------------------------------------------------------
            # MISSIONS, AIRPORTS, ASSESSMENTS
            # Delete in dependency order: assessments → airports → missions
            # ------------------------------------------------------------------

            # Delete readiness assessments for all of this user's missions
            cur.execute("""
                DELETE FROM readiness_assessments
                WHERE mission_id IN (
                    SELECT id FROM missions WHERE user_id = %s
                )
            """, (user_id,))
            result['assessments_deleted'] = cur.rowcount

            # Delete mission airports for all of this user's missions
            cur.execute("""
                DELETE FROM mission_airports
                WHERE mission_id IN (
                    SELECT id FROM missions WHERE user_id = %s
                )
            """, (user_id,))
            result['mission_airports_deleted'] = cur.rowcount

            # Hard delete missions owned by this user
            cur.execute(
                "DELETE FROM missions WHERE user_id = %s",
                (user_id,)
            )
            result['missions_deleted'] = cur.rowcount

            # ------------------------------------------------------------------
            # OUTBOX
            # ------------------------------------------------------------------
            cur.execute(
                "DELETE FROM outbox WHERE user_id = %s",
                (user_id,)
            )
            result['outbox_entries_deleted'] = cur.rowcount

            # Proficiency score history (sync-push upserts to proficiency_snapshots)
            cur.execute(
                "DELETE FROM proficiency_snapshots WHERE user_id = %s",
                (user_id,)
            )
            result['proficiency_snapshots_deleted'] = cur.rowcount

        conn.commit()
        print(f"[DeletionProcessor] PostgreSQL cleanup: {result}")
    except Exception as e:
        conn.rollback()
        print(f"[DeletionProcessor] PostgreSQL error: {e}")
        raise
    finally:
        conn.close()

    return result


def scan_delete_events_for_user(table_name: str, user_id: str) -> int:
    """
    Remove DynamoDB events copied from PostgreSQL outbox (outbox-processor).
    PK is (id, timestamp); there is no userId index, so we scan with a filter.
    """
    if not table_name:
        return 0
    table = get_dynamodb().Table(table_name)
    deleted_count = 0
    scan_kwargs: Dict[str, Any] = {
        'FilterExpression': 'userId = :uid',
        'ExpressionAttributeValues': {':uid': user_id},
        'ProjectionExpression': 'id, #ts',
        'ExpressionAttributeNames': {'#ts': 'timestamp'},
    }
    while True:
        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])
        if items:
            with table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={'id': item['id'], 'timestamp': item['timestamp']})
            deleted_count += len(items)
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    return deleted_count


def batch_delete_dynamo_items(table_name: str, pk_name: str, pk_value: str, sort_key: str = None) -> int:
    """Query and batch-delete all items for a given partition key."""
    table = get_dynamodb().Table(table_name)
    deleted_count = 0

    query_kwargs = {
        'KeyConditionExpression': f'{pk_name} = :pk',
        'ExpressionAttributeValues': {':pk': pk_value},
    }
    if sort_key:
        query_kwargs['ProjectionExpression'] = f'{pk_name}, {sort_key}'
    else:
        query_kwargs['ProjectionExpression'] = pk_name

    while True:
        response = table.query(**query_kwargs)
        items = response.get('Items', [])
        if not items:
            break

        with table.batch_writer() as batch:
            for item in items:
                key = {pk_name: item[pk_name]}
                if sort_key and sort_key in item:
                    key[sort_key] = item[sort_key]
                batch.delete_item(Key=key)
                deleted_count += 1

        if 'LastEvaluatedKey' not in response:
            break
        query_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

    return deleted_count
