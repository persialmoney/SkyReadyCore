"""
GDPR/CCPA: export all user data to S3 and email a presigned download link.
Uses shared user_data module (same inventory as user-deletion-processor).
"""
import json
import os
import uuid
import boto3
from datetime import datetime
from typing import Any, Dict

from user_data import (
    collect_dynamo_user_data,
    collect_postgres_user_data,
    dynamo_to_jsonable,
    sanitize_export_payload,
)

STAGE = os.environ.get("STAGE", "dev")
USERS_TABLE = os.environ.get("USERS_TABLE", f"sky-ready-users-{STAGE}")
SAVED_AIRPORTS_TABLE = os.environ.get(
    "SAVED_AIRPORTS_TABLE", f"sky-ready-saved-airports-{STAGE}"
)
ALERTS_TABLE = os.environ.get("ALERTS_TABLE", f"sky-ready-alerts-{STAGE}")
EXPORT_BUCKET = os.environ.get("EXPORT_BUCKET", "")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "noreply@skyready.app")

METRIC_NAMESPACE = f"SkyReady/UserData/{STAGE}"

_dynamodb = None
_s3 = None
_ses = None
_cloudwatch = None


def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def get_ses():
    global _ses
    if _ses is None:
        _ses = boto3.client("ses")
    return _ses


def get_cloudwatch():
    global _cloudwatch
    if _cloudwatch is None:
        _cloudwatch = boto3.client("cloudwatch")
    return _cloudwatch


def emit_export_metrics(
    *,
    success: bool,
    source: str,
    size_bytes: int = 0,
) -> None:
    try:
        dims = [
            {"Name": "Stage", "Value": STAGE},
            {"Name": "Source", "Value": source},
        ]
        data = [
            {
                "MetricName": "ExportsRequested" if success else "ExportsFailed",
                "Value": 1.0,
                "Unit": "Count",
                "Dimensions": dims,
            },
        ]
        if success and size_bytes > 0:
            data.append(
                {
                    "MetricName": "ExportSizeBytes",
                    "Value": float(size_bytes),
                    "Unit": "Bytes",
                    "Dimensions": dims,
                }
            )
        get_cloudwatch().put_metric_data(Namespace=METRIC_NAMESPACE, MetricData=data)
    except Exception as e:
        print(f"[UserDataExport] CloudWatch metrics skipped: {e}")


def _collect_all(user_id: str) -> Dict[str, Any]:
    ddb = get_dynamodb()
    dynamo_part = collect_dynamo_user_data(
        ddb,
        USERS_TABLE,
        SAVED_AIRPORTS_TABLE,
        ALERTS_TABLE,
        user_id,
    )
    dynamo_part = dynamo_to_jsonable(dynamo_part)

    db_secret_arn = os.environ.get("DB_SECRET_ARN")
    db_endpoint = os.environ.get("DB_ENDPOINT")
    db_name = os.environ.get("DB_NAME", "logbook")

    postgres_part: Dict[str, Any] = {}
    if db_secret_arn and db_endpoint:
        import psycopg

        secrets_client = boto3.client("secretsmanager")
        secret = json.loads(
            secrets_client.get_secret_value(SecretId=db_secret_arn)["SecretString"]
        )
        conn = psycopg.connect(
            host=db_endpoint,
            port=5432,
            dbname=db_name,
            user=secret["username"],
            password=secret["password"],
        )
        try:
            with conn.cursor() as cur:
                postgres_part = collect_postgres_user_data(cur, user_id)
            postgres_part = dynamo_to_jsonable(postgres_part)
        finally:
            conn.close()
    else:
        print("[UserDataExport] PostgreSQL not configured, DynamoDB-only export")

    raw = {
        "exportVersion": "1.0",
        "exportedAt": datetime.utcnow().isoformat() + "Z",
        "dynamodb": dynamo_part,
        "postgresql": postgres_part,
    }
    return sanitize_export_payload(raw)


def _email_link(to_email: str, name: str, url: str) -> None:
    body_text = (
        f"Hello {name},\n\n"
        "You requested a copy of your SkyReady data. Use the link below to download "
        "your export (JSON). This link expires in 7 days.\n\n"
        f"{url}\n\n"
        "If you did not request this, you can ignore this email.\n"
    )
    body_html = f"""<html><body><p>Hello {name},</p>
<p>You requested a copy of your SkyReady data. Use the link below to download your export (JSON).
This link expires in 7 days.</p>
<p><a href="{url}">Download my data</a></p>
<p>If you did not request this, you can ignore this email.</p>
</body></html>"""
    get_ses().send_email(
        Source=SENDER_EMAIL,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": "Your SkyReady data export", "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": body_text, "Charset": "UTF-8"},
                "Html": {"Data": body_html, "Charset": "UTF-8"},
            },
        },
    )


def run_export(user_id: str, source: str) -> Dict[str, Any]:
    """Build JSON, upload to S3, email presigned URL. Raises on failure."""
    if not EXPORT_BUCKET:
        raise ValueError("EXPORT_BUCKET is not configured")

    profile = (
        get_dynamodb()
        .Table(USERS_TABLE)
        .get_item(Key={"userId": user_id})
        .get("Item")
    )
    if not profile:
        raise ValueError("User profile not found")
    email = profile.get("email")
    if not email:
        raise ValueError("No email address on file")

    payload = _collect_all(user_id)
    raw = json.dumps(payload, default=str).encode("utf-8")
    size_b = len(raw)
    key = f"exports/{user_id}/{uuid.uuid4()}.json"

    get_s3().put_object(
        Bucket=EXPORT_BUCKET,
        Key=key,
        Body=raw,
        ContentType="application/json",
    )

    url = get_s3().generate_presigned_url(
        "get_object",
        Params={"Bucket": EXPORT_BUCKET, "Key": key},
        ExpiresIn=7 * 24 * 3600,
    )

    _email_link(email, profile.get("name") or "Pilot", url)
    emit_export_metrics(success=True, source=source, size_bytes=size_b)

    return {
        "success": True,
        "message": "We sent a download link to your email address.",
    }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    admin_uid = event.get("adminExportUserId") if isinstance(event, dict) else None
    if admin_uid:
        uid = str(admin_uid).strip()
        if not uid:
            return {"ok": False, "error": "adminExportUserId is empty"}
        try:
            result = run_export(uid, "admin")
            return {"ok": True, "userId": uid, **result}
        except Exception as e:
            print(f"[UserDataExport] Admin export failed: {e}")
            emit_export_metrics(success=False, source="admin", size_bytes=0)
            return {"ok": False, "userId": uid, "error": str(e)}

    info = event.get("info", {})
    field_name = info.get("fieldName")
    if field_name != "requestDataExport":
        raise ValueError(f"Unknown field: {field_name}")

    identity = event.get("identity", {})
    user_id = identity.get("sub")
    if not user_id:
        raise ValueError("Authentication required")

    try:
        return run_export(user_id, "user")
    except Exception as e:
        print(f"[UserDataExport] Export failed: {e}")
        emit_export_metrics(success=False, source="user", size_bytes=0)
        raise
