"""
Single source of truth for user-scoped data: PostgreSQL + DynamoDB reads (export)
and deletes (account hard-delete).

Keep collect_* and delete_* in sync when adding new stores.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# PostgreSQL — delete (hard delete processor)
# ---------------------------------------------------------------------------


def delete_postgres_user_data(cursor, user_id: str) -> Dict[str, int]:
    """
    Null cross-user references, then delete this user's rows.
    Caller must commit or rollback the connection.
    Returns row counts per operation.
    """
    result: Dict[str, int] = {
        "references_nulled": 0,
        "logbook_entries_deleted": 0,
        "assessments_deleted": 0,
        "mission_airports_deleted": 0,
        "missions_deleted": 0,
        "outbox_entries_deleted": 0,
        "proficiency_snapshots_deleted": 0,
    }

    cursor.execute(
        "UPDATE logbook_entries SET instructor_user_id = NULL "
        "WHERE instructor_user_id = %s",
        (user_id,),
    )
    refs = cursor.rowcount

    cursor.execute(
        "UPDATE logbook_entries SET student_user_id = NULL "
        "WHERE student_user_id = %s",
        (user_id,),
    )
    refs += cursor.rowcount

    cursor.execute(
        "UPDATE logbook_entries SET mirrored_from_user_id = NULL "
        "WHERE mirrored_from_user_id = %s",
        (user_id,),
    )
    refs += cursor.rowcount
    result["references_nulled"] = refs

    cursor.execute(
        "DELETE FROM logbook_entries WHERE user_id = %s",
        (user_id,),
    )
    result["logbook_entries_deleted"] = cursor.rowcount

    cursor.execute(
        """
        DELETE FROM readiness_assessments
        WHERE mission_id IN (
            SELECT id FROM missions WHERE user_id = %s
        )
        """,
        (user_id,),
    )
    result["assessments_deleted"] = cursor.rowcount

    cursor.execute(
        """
        DELETE FROM mission_airports
        WHERE mission_id IN (
            SELECT id FROM missions WHERE user_id = %s
        )
        """,
        (user_id,),
    )
    result["mission_airports_deleted"] = cursor.rowcount

    cursor.execute(
        "DELETE FROM missions WHERE user_id = %s",
        (user_id,),
    )
    result["missions_deleted"] = cursor.rowcount

    cursor.execute(
        "DELETE FROM outbox WHERE user_id = %s",
        (user_id,),
    )
    result["outbox_entries_deleted"] = cursor.rowcount

    cursor.execute(
        "DELETE FROM proficiency_snapshots WHERE user_id = %s",
        (user_id,),
    )
    result["proficiency_snapshots_deleted"] = cursor.rowcount

    return result


# ---------------------------------------------------------------------------
# PostgreSQL — collect (export)
# ---------------------------------------------------------------------------


def _row_to_dict(columns: List[str], row: tuple) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for i, col in enumerate(columns):
        v = row[i]
        if isinstance(v, Decimal):
            v = float(v)
        out[col] = v
    return out


def collect_postgres_user_data(cursor, user_id: str) -> Dict[str, Any]:
    """Read all PostgreSQL rows for this user (for GDPR/CCPA export)."""

    def fetch_all(sql: str, params: tuple) -> List[Dict[str, Any]]:
        cursor.execute(sql, params)
        if cursor.description is None:
            return []
        cols = [d.name for d in cursor.description]
        return [_row_to_dict(cols, tuple(r)) for r in cursor.fetchall()]

    cursor.execute(
        "SELECT * FROM logbook_entries WHERE user_id = %s ORDER BY date",
        (user_id,),
    )
    le_cols = [d.name for d in cursor.description] if cursor.description else []
    logbook_entries = [
        _row_to_dict(le_cols, tuple(r)) for r in cursor.fetchall()
    ]

    missions = fetch_all(
        "SELECT * FROM missions WHERE user_id = %s ORDER BY updated_at",
        (user_id,),
    )

    mission_ids = [m.get("id") for m in missions if m.get("id")]
    mission_airports: List[Dict[str, Any]] = []
    readiness_assessments: List[Dict[str, Any]] = []
    if mission_ids:
        placeholders = ",".join(["%s"] * len(mission_ids))
        mission_airports = fetch_all(
            f"SELECT * FROM mission_airports WHERE mission_id IN ({placeholders})",
            tuple(mission_ids),
        )
        readiness_assessments = fetch_all(
            f"SELECT * FROM readiness_assessments WHERE mission_id IN ({placeholders})",
            tuple(mission_ids),
        )

    outbox = fetch_all(
        "SELECT * FROM outbox WHERE user_id = %s ORDER BY created_at",
        (user_id,),
    )

    proficiency_snapshots = fetch_all(
        "SELECT * FROM proficiency_snapshots WHERE user_id = %s ORDER BY snapshot_date DESC",
        (user_id,),
    )

    return {
        "logbook_entries": logbook_entries,
        "missions": missions,
        "mission_airports": mission_airports,
        "readiness_assessments": readiness_assessments,
        "outbox": outbox,
        "proficiency_snapshots": proficiency_snapshots,
    }


# ---------------------------------------------------------------------------
# DynamoDB — shared helpers (used by deletion processor)
# ---------------------------------------------------------------------------


def batch_delete_dynamo_items(
    dynamodb_resource: Any,
    table_name: str,
    pk_name: str,
    pk_value: str,
    sort_key: Optional[str] = None,
) -> int:
    """Query and batch-delete all items for a given partition key."""
    table = dynamodb_resource.Table(table_name)
    deleted_count = 0

    query_kwargs: Dict[str, Any] = {
        "KeyConditionExpression": f"{pk_name} = :pk",
        "ExpressionAttributeValues": {":pk": pk_value},
    }
    if sort_key:
        query_kwargs["ProjectionExpression"] = f"{pk_name}, {sort_key}"
    else:
        query_kwargs["ProjectionExpression"] = pk_name

    while True:
        response = table.query(**query_kwargs)
        items = response.get("Items", [])
        if not items:
            break

        with table.batch_writer() as batch:
            for item in items:
                key = {pk_name: item[pk_name]}
                if sort_key and sort_key in item:
                    key[sort_key] = item[sort_key]
                batch.delete_item(Key=key)
                deleted_count += 1

        if "LastEvaluatedKey" not in response:
            break
        query_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    return deleted_count


def scan_delete_events_for_user(
    dynamodb_resource: Any,
    table_name: str,
    user_id: str,
) -> int:
    """Scan events table by userId and delete (composite key id + timestamp)."""
    if not table_name:
        return 0
    table = dynamodb_resource.Table(table_name)
    deleted_count = 0
    scan_kwargs: Dict[str, Any] = {
        "FilterExpression": "userId = :uid",
        "ExpressionAttributeValues": {":uid": user_id},
        "ProjectionExpression": "id, #ts",
        "ExpressionAttributeNames": {"#ts": "timestamp"},
    }
    while True:
        response = table.scan(**scan_kwargs)
        items = response.get("Items", [])
        if items:
            with table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(
                        Key={"id": item["id"], "timestamp": item["timestamp"]}
                    )
            deleted_count += len(items)
        if "LastEvaluatedKey" not in response:
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    return deleted_count


def collect_dynamo_user_data(
    dynamodb_resource: Any,
    users_table: str,
    saved_airports_table: str,
    alerts_table: str,
    user_id: str,
) -> Dict[str, Any]:
    """Read all DynamoDB user-scoped items for export."""
    profile = dynamodb_resource.Table(users_table).get_item(
        Key={"userId": user_id}
    ).get("Item")

    saved: List[Dict[str, Any]] = []
    sat = dynamodb_resource.Table(saved_airports_table)
    q = {
        "KeyConditionExpression": "userId = :pk",
        "ExpressionAttributeValues": {":pk": user_id},
    }
    while True:
        r = sat.query(**q)
        saved.extend(r.get("Items", []))
        if "LastEvaluatedKey" not in r:
            break
        q["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    alerts_list: List[Dict[str, Any]] = []
    alt = dynamodb_resource.Table(alerts_table)
    q2 = {
        "KeyConditionExpression": "userId = :pk",
        "ExpressionAttributeValues": {":pk": user_id},
    }
    while True:
        r = alt.query(**q2)
        alerts_list.extend(r.get("Items", []))
        if "LastEvaluatedKey" not in r:
            break
        q2["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    return {
        "profile": profile,
        "saved_airports": saved,
        "alerts": alerts_list,
    }


def dynamo_to_jsonable(obj: Any) -> Any:
    """Convert DynamoDB types (Decimal, etc.) for JSON."""
    if isinstance(obj, list):
        return [dynamo_to_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {k: dynamo_to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj
