"""
AppSync resolver — Query.getAttachmentUploadUrl

Authenticated users only (@aws_cognito_user_pools). Returns a presigned PUT URL
for uploading a screenshot or short video to the support attachments bucket.

Rate limit: UPLOAD_LIMIT uploads per WINDOW_SECONDS (default 5 per 24h).
"""

import os
import time
import uuid
import boto3
from botocore.exceptions import ClientError

STAGE = os.environ.get("STAGE", "dev")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
RATE_LIMIT_TABLE = os.environ.get("RATE_LIMIT_TABLE", "")
UPLOAD_LIMIT = int(os.environ.get("UPLOAD_LIMIT", "5"))
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "86400"))
METRIC_NAMESPACE = f"SkyReady/Support/{STAGE}"

# content_type -> (extension, max_bytes)
ALLOWED = {
    "image/jpeg": ("jpg", 5 * 1024 * 1024),
    "image/png": ("png", 5 * 1024 * 1024),
    "image/webp": ("webp", 5 * 1024 * 1024),
    "video/mp4": ("mp4", 30 * 1024 * 1024),
    "video/quicktime": ("mov", 30 * 1024 * 1024),
}

_ddb = None
_s3 = None
_cw = None


def get_ddb():
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb


def get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def get_cw():
    global _cw
    if _cw is None:
        _cw = boto3.client("cloudwatch")
    return _cw


def emit_metric(name: str, value: float = 1.0) -> None:
    try:
        get_cw().put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {
                    "MetricName": name,
                    "Value": value,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Stage", "Value": STAGE}],
                }
            ],
        )
    except Exception:
        pass


def check_and_increment_upload_count(user_id: str) -> None:
    if not RATE_LIMIT_TABLE:
        return
    now = int(time.time())
    table = get_ddb().Table(RATE_LIMIT_TABLE)
    r = table.get_item(Key={"userId": user_id})
    item = r.get("Item") or {}
    ws = int(item.get("windowStart", 0))
    rc = int(item.get("requestCount", 0))
    uc = int(item.get("uploadCount", 0))

    if not item:
        ws, rc, uc = now, 0, 0
    elif now - ws >= WINDOW_SECONDS:
        ws, rc, uc = now, 0, 0

    if uc >= UPLOAD_LIMIT:
        print("RATE_LIMITED upload url")
        emit_metric("UploadUrlsRateLimited")
        raise Exception(
            "TOO_MANY_REQUESTS: Attachment upload limit reached. Try again tomorrow."
        )

    uc += 1
    table.put_item(
        Item={
            "userId": user_id,
            "windowStart": ws,
            "requestCount": rc,
            "uploadCount": uc,
            "expiresAt": ws + WINDOW_SECONDS + 3600,
        }
    )


def handler(event, context):
    identity = event.get("identity", {})
    user_id = identity.get("sub")
    if not user_id:
        emit_metric("UploadUrlsRejected")
        raise Exception("Authentication required")

    arguments = event.get("arguments", {})
    content_type = (arguments.get("contentType") or "").strip().lower()
    if content_type not in ALLOWED:
        print(f"RATE_LIMITED invalid type: {content_type}")
        emit_metric("UploadUrlsRejected")
        raise Exception(
            "Invalid content type. Allowed: JPEG, PNG, WebP, MP4, MOV."
        )

    ext, _max_bytes = ALLOWED[content_type]
    check_and_increment_upload_count(user_id)

    key = f"{STAGE}/{user_id}/{uuid.uuid4().hex}.{ext}"

    try:
        url = get_s3().generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": BUCKET_NAME,
                "Key": key,
                "ContentType": content_type,
            },
            ExpiresIn=900,
            HttpMethod="PUT",
        )
    except ClientError as e:
        emit_metric("UploadUrlsRejected")
        raise Exception(f"Could not create upload URL: {e}") from e

    emit_metric("UploadUrlsIssued")
    return {"uploadUrl": url, "s3Key": key}
