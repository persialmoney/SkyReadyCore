"""
AppSync resolver — Mutation.sendSupportRequest

Authenticated users only. Sends support email via SES. Optional attachmentKey
must be an object the same user uploaded (S3 key prefix {stage}/{userId}/).

Rate limit: REQUEST_LIMIT support submissions per WINDOW_SECONDS (default 3 / 24h).
"""

import os
import time
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

STAGE = os.environ.get("STAGE", "dev")
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL", "support@skyready.app")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "noreply@skyready.app")
RATE_LIMIT_TABLE = os.environ.get("RATE_LIMIT_TABLE", "")
REQUEST_LIMIT = int(os.environ.get("REQUEST_LIMIT", "3"))
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "86400"))
ATTACHMENTS_BUCKET = os.environ.get("ATTACHMENTS_BUCKET", "")
METRIC_NAMESPACE = f"SkyReady/Support/{STAGE}"

_ses_client = None
_s3_client = None
_ddb = None
_cw = None


def get_ses():
    global _ses_client
    if _ses_client is None:
        _ses_client = boto3.client("ses")
    return _ses_client


def get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def get_ddb():
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb


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


def check_and_increment_request_count(user_id: str) -> None:
    if not RATE_LIMIT_TABLE:
        return
    now = int(time.time())
    table = get_ddb().Table(RATE_LIMIT_TABLE)
    r = table.get_item(Key={"userId": user_id})
    item = r.get("Item")
    if not item:
        ws, rc, uc = now, 0, 0
    else:
        ws = int(item.get("windowStart", 0))
        rc = int(item.get("requestCount", 0))
        uc = int(item.get("uploadCount", 0))
        if now - ws >= WINDOW_SECONDS:
            ws, rc, uc = now, 0, 0

    if rc >= REQUEST_LIMIT:
        print("RATE_LIMITED support request")
        emit_metric("RequestsRateLimited")
        raise Exception(
            "TOO_MANY_REQUESTS: Support request limit reached. Try again tomorrow."
        )

    rc += 1
    table.put_item(
        Item={
            "userId": user_id,
            "windowStart": ws,
            "requestCount": rc,
            "uploadCount": uc,
            "expiresAt": ws + WINDOW_SECONDS + 3600,
        }
    )


CATEGORY_LABELS = {
    "bug": "Bug Report",
    "feature": "Feature Request",
    "account": "Account Issue",
    "weather": "Weather Data",
    "sync": "Flight Sync",
    "other": "Other",
}

SEVERITY_LABELS = {
    "low": "Low — minor inconvenience",
    "medium": "Medium — impacts workflow",
    "high": "High — crash or data issue",
}


def build_attachment_section(attachment_key: str, user_id: str) -> tuple[str, str]:
    """Returns (text_block, html_block) for email body."""
    if not attachment_key or not str(attachment_key).strip():
        return "", ""

    key = str(attachment_key).strip()
    expected = f"{STAGE}/{user_id}/"
    if not key.startswith(expected):
        emit_metric("RequestsFailed")
        raise Exception("Invalid attachment reference.")

    if not ATTACHMENTS_BUCKET:
        emit_metric("RequestsFailed")
        raise Exception("Attachments are not configured.")

    try:
        view_url = get_s3().generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": ATTACHMENTS_BUCKET, "Key": key},
            ExpiresIn=7 * 24 * 3600,
        )
    except ClientError as e:
        emit_metric("RequestsFailed")
        raise Exception(f"Could not generate attachment link: {e}") from e

    text = f"\nAttachment (view link expires in 7 days):\n{view_url}\n"
    html = f"""
  <h3 style="margin-bottom: 8px;">Attachment</h3>
  <p style="margin-bottom: 8px;"><a href="{view_url}" style="color: #2563eb;">View uploaded file</a> (link expires in 7 days)</p>
  <p style="font-size: 12px; color: #6b7280;">S3 key: {key}</p>
"""
    return text, html


def handler(event, context):
    arguments = event.get("arguments", {})
    inp = arguments.get("input", {})

    identity = event.get("identity", {})
    user_id = identity.get("sub")
    if not user_id:
        emit_metric("RequestsFailed")
        raise Exception("Authentication required")

    category = inp.get("category", "other")
    subject_text = inp.get("subject", "(no subject)")
    description = inp.get("description", "")
    severity = inp.get("severity")
    device_info = inp.get("deviceInfo", "Unknown")
    attachment_key = inp.get("attachmentKey")

    check_and_increment_request_count(user_id)

    user_email = identity.get("claims", {}).get("email", "unknown")

    category_label = CATEGORY_LABELS.get(category, category.title())
    severity_label = SEVERITY_LABELS.get(severity, "N/A") if severity else "N/A"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    att_text, att_html = build_attachment_section(attachment_key, user_id)

    email_subject = f"[SkyReady {STAGE.upper()}] [{category_label}] {subject_text}"

    email_body_text = f"""
SkyReady Support Request
========================

Category : {category_label}
Severity : {severity_label}
Subject  : {subject_text}
Submitted: {timestamp}

User ID  : {user_id}
Email    : {user_email}
Device   : {device_info}

Description
-----------
{description}
{att_text}
---
This message was submitted via the SkyReady in-app support form.
"""

    email_body_html = f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family: system-ui, sans-serif; max-width: 640px; margin: 0 auto; padding: 24px; color: #111827;">
  <h2 style="color: #2563eb; margin-bottom: 4px;">SkyReady Support Request</h2>
  <p style="color: #6b7280; margin-top: 0;">{timestamp} &bull; Stage: {STAGE.upper()}</p>

  <table style="width: 100%; border-collapse: collapse; margin-bottom: 24px;">
    <tr>
      <td style="padding: 8px 12px; background: #f3f4f6; font-weight: 600; width: 120px; border-radius: 4px;">Category</td>
      <td style="padding: 8px 12px;">{category_label}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px; background: #f3f4f6; font-weight: 600; border-radius: 4px;">Severity</td>
      <td style="padding: 8px 12px;">{severity_label}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px; background: #f3f4f6; font-weight: 600; border-radius: 4px;">Subject</td>
      <td style="padding: 8px 12px;">{subject_text}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px; background: #f3f4f6; font-weight: 600; border-radius: 4px;">User ID</td>
      <td style="padding: 8px 12px; font-size: 13px; color: #6b7280;">{user_id}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px; background: #f3f4f6; font-weight: 600; border-radius: 4px;">Email</td>
      <td style="padding: 8px 12px;">{user_email}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px; background: #f3f4f6; font-weight: 600; border-radius: 4px;">Device</td>
      <td style="padding: 8px 12px;">{device_info}</td>
    </tr>
  </table>

  <h3 style="margin-bottom: 8px;">Description</h3>
  <div style="background: #f9fafb; border-left: 4px solid #2563eb; padding: 16px; border-radius: 4px; white-space: pre-wrap; font-size: 15px; line-height: 1.6;">
{description}
  </div>
  {att_html}

  <p style="margin-top: 32px; font-size: 12px; color: #9ca3af;">
    Submitted via SkyReady in-app support form. Reply directly to this email to respond to the user.
  </p>
</body>
</html>
"""

    try:
        get_ses().send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [SUPPORT_EMAIL]},
            ReplyToAddresses=[user_email] if user_email != "unknown" else [],
            Message={
                "Subject": {"Data": email_subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": email_body_text, "Charset": "UTF-8"},
                    "Html": {"Data": email_body_html, "Charset": "UTF-8"},
                },
            },
        )
        emit_metric("RequestsSubmitted")
        return True
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        error_msg = exc.response["Error"]["Message"]
        emit_metric("RequestsFailed")
        raise Exception(f"SES send failed [{error_code}]: {error_msg}") from exc
