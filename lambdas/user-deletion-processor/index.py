"""
Scheduled Lambda (EventBridge daily) for permanently deleting user data after
the 30-day grace period has expired.

Scans the deletion_requests table for items with status=GRACE_PERIOD and
scheduledHardDeleteAt <= now, then cascading-deletes all user data.

User data operations are centralized in shared user_data module (kept in sync
with user-data-export for GDPR/CCPA).
"""
import json
import os
import time
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from typing import Dict, Any, List

from user_data import (
    batch_delete_dynamo_items,
    delete_postgres_user_data,
    scan_delete_events_for_user,
)

_dynamodb_resource = None
_cognito_client = None
_cloudwatch_client = None

STAGE = os.environ.get('STAGE', 'dev')
USERS_TABLE = os.environ.get('USERS_TABLE', f'sky-ready-users-{STAGE}')
SAVED_AIRPORTS_TABLE = os.environ.get('SAVED_AIRPORTS_TABLE', f'sky-ready-saved-airports-{STAGE}')
ALERTS_TABLE = os.environ.get('ALERTS_TABLE', f'sky-ready-alerts-{STAGE}')
DELETION_OTPS_TABLE = os.environ.get('DELETION_OTPS_TABLE', f'sky-ready-deletion-otps-{STAGE}')
DELETION_REQUESTS_TABLE = os.environ.get('DELETION_REQUESTS_TABLE', f'sky-ready-deletion-requests-{STAGE}')
EVENTS_TABLE = os.environ.get('EVENTS_TABLE', f'sky-ready-events-{STAGE}')
USER_POOL_ID = os.environ.get('USER_POOL_ID', '')

METRIC_NAMESPACE = f"SkyReady/UserData/{STAGE}"


def get_dynamodb():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource('dynamodb')
    return _dynamodb_resource


def get_cognito_client():
    global _cognito_client
    if _cognito_client is None:
        _cognito_client = boto3.client('cognito-idp')
    return _cognito_client


def get_cloudwatch_client():
    global _cloudwatch_client
    if _cloudwatch_client is None:
        _cloudwatch_client = boto3.client('cloudwatch')
    return _cloudwatch_client


def emit_deletion_metrics(
    *,
    deletions_processed: int,
    deletions_failed: int,
    logbook_entries_deleted: int,
    missions_deleted: int,
    tombstones_purged: int = 0,
) -> None:
    try:
        get_cloudwatch_client().put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {
                    'MetricName': 'DeletionsProcessed',
                    'Value': float(deletions_processed),
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stage', 'Value': STAGE}],
                },
                {
                    'MetricName': 'DeletionsFailed',
                    'Value': float(deletions_failed),
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stage', 'Value': STAGE}],
                },
                {
                    'MetricName': 'LogbookEntriesDeleted',
                    'Value': float(logbook_entries_deleted),
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stage', 'Value': STAGE}],
                },
                {
                    'MetricName': 'MissionsDeleted',
                    'Value': float(missions_deleted),
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stage', 'Value': STAGE}],
                },
                {
                    'MetricName': 'TombstonesPurged',
                    'Value': float(tombstones_purged),
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stage', 'Value': STAGE}],
                },
            ],
        )
    except Exception as e:
        print(f"[DeletionProcessor] CloudWatch metrics skipped: {e}")


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    - Scheduled (EventBridge): scan deletion_requests for expired GRACE_PERIOD rows.
    - Direct invoke (admin CLI): `{"adminPurgeUserId": "<cognito sub>"}` — immediate
      full delete using the same process_hard_delete path as the scheduled job.
    """
    admin_uid = event.get("adminPurgeUserId") if isinstance(event, dict) else None
    if admin_uid:
        user_id = str(admin_uid).strip()
        print(f"[DeletionProcessor] Admin immediate purge requested for userId={user_id}")
        if not user_id:
            return {"ok": False, "error": "adminPurgeUserId is empty"}
        try:
            summary = process_hard_delete(user_id, {}, admin_purge=True)
            print(f"[DeletionProcessor] Admin purge completed: {summary}")
            emit_deletion_metrics(
                deletions_processed=1,
                deletions_failed=0,
                logbook_entries_deleted=summary.get('logbook_entries_deleted', 0),
                missions_deleted=summary.get('missions_deleted', 0),
            )
            return {"ok": True, "userId": user_id, "summary": summary}
        except Exception as e:
            print(f"[DeletionProcessor] Admin purge FAILED for {user_id}: {e}")
            emit_deletion_metrics(
                deletions_processed=0,
                deletions_failed=1,
                logbook_entries_deleted=0,
                missions_deleted=0,
            )
            return {"ok": False, "userId": user_id, "error": str(e)}

    print(f"[DeletionProcessor] Starting hard-delete scan at {datetime.utcnow().isoformat()}")

    requests_table = get_dynamodb().Table(DELETION_REQUESTS_TABLE)
    now_iso = datetime.utcnow().isoformat() + 'Z'

    expired_requests = scan_expired_requests(requests_table, now_iso)
    print(f"[DeletionProcessor] Found {len(expired_requests)} expired deletion requests")

    results = []
    total_logbook = 0
    total_missions = 0
    failed = 0
    for request in expired_requests:
        user_id = request['userId']
        try:
            summary = process_hard_delete(user_id, request, admin_purge=False)
            total_logbook += summary.get('logbook_entries_deleted', 0)
            total_missions += summary.get('missions_deleted', 0)
            results.append({'userId': user_id, 'status': 'completed', 'summary': summary})
            print(f"[DeletionProcessor] Successfully hard-deleted user {user_id}: {summary}")
        except Exception as e:
            failed += 1
            results.append({'userId': user_id, 'status': 'failed', 'error': str(e)})
            print(f"[DeletionProcessor] FAILED to hard-delete user {user_id}: {e}")

    ok_count = len(expired_requests) - failed

    tombstones_purged = purge_stale_tombstones()

    emit_deletion_metrics(
        deletions_processed=ok_count,
        deletions_failed=failed,
        logbook_entries_deleted=total_logbook,
        missions_deleted=total_missions,
        tombstones_purged=tombstones_purged,
    )

    print(f"[DeletionProcessor] Finished. Processed {len(results)} requests.")
    return {'processed': len(results), 'results': results, 'tombstones_purged': tombstones_purged}


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

    pg_summary = hard_delete_postgres_data(user_id)
    summary.update(pg_summary)

    ddb = get_dynamodb()
    summary['saved_airports_deleted'] = batch_delete_dynamo_items(
        ddb, SAVED_AIRPORTS_TABLE, 'userId', user_id, sort_key='airportCode'
    )

    summary['alerts_deleted'] = batch_delete_dynamo_items(
        ddb, ALERTS_TABLE, 'userId', user_id, sort_key='alertId'
    )

    summary['events_deleted'] = scan_delete_events_for_user(ddb, EVENTS_TABLE, user_id)

    try:
        ddb.Table(DELETION_OTPS_TABLE).delete_item(Key={'userId': user_id})
    except Exception:
        pass

    try:
        ddb.Table(USERS_TABLE).delete_item(Key={'userId': user_id})
        summary['dynamodb_user_deleted'] = True
    except Exception as e:
        print(f"[DeletionProcessor] Error deleting DynamoDB user: {e}")

    try:
        get_cognito_client().admin_delete_user(
            UserPoolId=USER_POOL_ID,
            Username=user_id,
        )
        summary['cognito_deleted'] = True
    except get_cognito_client().exceptions.UserNotFoundException:
        summary['cognito_deleted'] = True
    except Exception as e:
        print(f"[DeletionProcessor] Error deleting Cognito user: {e}")

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


def purge_stale_tombstones() -> int:
    """Hard-delete soft-deleted rows older than 30 days across all synced tables.

    Soft-deleted rows (deleted_at IS NOT NULL) serve as sync tombstones so
    other devices learn about the deletion on their next pull. After 30 days
    the tombstone has served its purpose and the content can be permanently
    removed. 30 days matches the account-deletion grace period, so any device
    that hasn't synced in over 30 days will trigger a full-pull reset anyway.
    """
    db_secret_arn = os.environ.get('DB_SECRET_ARN')
    db_endpoint = os.environ.get('DB_ENDPOINT')
    db_name = os.environ.get('DB_NAME', 'logbook')

    if not db_secret_arn or not db_endpoint:
        print("[DeletionProcessor] PostgreSQL not configured, skipping tombstone purge")
        return 0

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

    total_purged = 0
    try:
        with conn.cursor() as cur:
            for table in ('logbook_entries', 'missions', 'readiness_assessments'):
                cur.execute(
                    f"DELETE FROM {table} WHERE deleted_at IS NOT NULL AND deleted_at < now() - interval '30 days'",
                )
                purged = cur.rowcount
                total_purged += purged
                print(f"[DeletionProcessor] Tombstone purge: {table} → {purged} rows deleted")
        conn.commit()
        print(f"[DeletionProcessor] Tombstone purge complete: {total_purged} total rows deleted")
    except Exception as e:
        conn.rollback()
        print(f"[DeletionProcessor] Tombstone purge error: {e}")
    finally:
        conn.close()

    return total_purged


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
            pg = delete_postgres_user_data(cur, user_id)
            result.update(pg)
        conn.commit()
        print(f"[DeletionProcessor] PostgreSQL cleanup: {result}")
    except Exception as e:
        conn.rollback()
        print(f"[DeletionProcessor] PostgreSQL error: {e}")
        raise
    finally:
        conn.close()

    return result
