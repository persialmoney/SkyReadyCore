"""
AppSync Lambda Resolver for account deletion operations.
Handles OTP generation/verification, Cognito disablement, cancellation, and status queries.
"""
import json
import os
import hashlib
import secrets
import time
import jwt
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Lazy initialization
_dynamodb_resource = None
_users_table = None
_deletion_otps_table = None
_deletion_requests_table = None
_cognito_client = None
_ses_client = None

STAGE = os.environ.get('STAGE', 'dev')
USERS_TABLE = os.environ.get('USERS_TABLE', f'sky-ready-users-{STAGE}')
DELETION_OTPS_TABLE = os.environ.get('DELETION_OTPS_TABLE', f'sky-ready-deletion-otps-{STAGE}')
DELETION_REQUESTS_TABLE = os.environ.get('DELETION_REQUESTS_TABLE', f'sky-ready-deletion-requests-{STAGE}')
USER_POOL_ID = os.environ.get('USER_POOL_ID', '')
RESTORE_TOKEN_SECRET = os.environ.get('RESTORE_TOKEN_SECRET', 'sky-ready-restore-secret')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'noreply@skyready.app')

OTP_EXPIRY_SECONDS = 600  # 10 minutes
OTP_MAX_ATTEMPTS = 3
OTP_RATE_LIMIT_PER_HOUR = 3
GRACE_PERIOD_DAYS = 30
AUDIT_TTL_DAYS = 365


def get_dynamodb():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource('dynamodb')
    return _dynamodb_resource


def get_users_table():
    global _users_table
    if _users_table is None:
        _users_table = get_dynamodb().Table(USERS_TABLE)
    return _users_table


def get_otps_table():
    global _deletion_otps_table
    if _deletion_otps_table is None:
        _deletion_otps_table = get_dynamodb().Table(DELETION_OTPS_TABLE)
    return _deletion_otps_table


def get_requests_table():
    global _deletion_requests_table
    if _deletion_requests_table is None:
        _deletion_requests_table = get_dynamodb().Table(DELETION_REQUESTS_TABLE)
    return _deletion_requests_table


def get_cognito_client():
    global _cognito_client
    if _cognito_client is None:
        _cognito_client = boto3.client('cognito-idp')
    return _cognito_client


def get_ses_client():
    global _ses_client
    if _ses_client is None:
        _ses_client = boto3.client('ses')
    return _ses_client


def hash_otp(otp: str) -> str:
    return hashlib.sha256(otp.encode()).hexdigest()


def generate_restore_token(user_id: str) -> str:
    payload = {
        'userId': user_id,
        'action': 'restore_account',
        'exp': int(time.time()) + (GRACE_PERIOD_DAYS * 86400),
        'iat': int(time.time()),
    }
    return jwt.encode(payload, RESTORE_TOKEN_SECRET, algorithm='HS256')


def verify_restore_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, RESTORE_TOKEN_SECRET, algorithms=['HS256'])
        if payload.get('action') != 'restore_account':
            return None
        return payload.get('userId')
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        info = event.get('info', {})
        field_name = info.get('fieldName')

        if field_name == 'checkAccountStatus':
            return handle_check_account_status(event)

        identity = event.get('identity', {})
        user_id = identity.get('sub')

        if not user_id:
            raise ValueError("Authentication required")

        if field_name == 'requestDeletionOtp':
            return handle_request_otp(user_id)
        elif field_name == 'deleteUser':
            return handle_delete_user(user_id, event)
        elif field_name == 'cancelUserDeletion':
            return handle_cancel_deletion(user_id)
        elif field_name == 'getUserDeletionStatus':
            return handle_get_deletion_status(user_id)
        else:
            raise ValueError(f"Unknown field: {field_name}")

    except Exception as e:
        print(f"[UserDeletion] ERROR: {str(e)}")
        print(f"[UserDeletion] fieldName: {event.get('info', {}).get('fieldName', 'UNKNOWN')}")
        raise


def handle_request_otp(user_id: str) -> Dict[str, Any]:
    """Generate and send a 6-digit OTP for deletion confirmation."""
    user = get_users_table().get_item(Key={'userId': user_id}).get('Item')
    if not user:
        raise ValueError("User not found")

    email = user.get('email')
    if not email:
        raise ValueError("No email address associated with this account")

    now = int(time.time())
    otps_table = get_otps_table()

    existing = otps_table.get_item(Key={'userId': user_id}).get('Item')
    if existing:
        request_count = existing.get('requestCount', 0)
        last_request = existing.get('lastRequestAt', 0)
        if now - last_request < 300 and request_count >= OTP_RATE_LIMIT_PER_HOUR:
            raise ValueError("Too many OTP requests. Please try again later.")

    otp = ''.join([str(secrets.randbelow(10)) for _ in range(6)])
    otp_hash = hash_otp(otp)
    expires_at = now + OTP_EXPIRY_SECONDS

    request_count = 1
    if existing and now - existing.get('lastRequestAt', 0) < 3600:
        request_count = existing.get('requestCount', 0) + 1

    otps_table.put_item(Item={
        'userId': user_id,
        'otpHash': otp_hash,
        'expiresAt': expires_at,
        'attempts': 0,
        'requestCount': request_count,
        'lastRequestAt': now,
    })

    send_otp_email(email, otp, user.get('name', 'User'))

    return {
        'success': True,
        'message': 'A verification code has been sent to your email.',
        'expiresAt': datetime.utcfromtimestamp(expires_at).isoformat() + 'Z',
    }


def handle_delete_user(user_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    """Verify OTP and initiate account deletion (Cognito disable + grace period record)."""
    arguments = event.get('arguments', {})
    user_input = arguments.get('input', {})
    otp = user_input.get('otp', '')
    reason = user_input.get('reason', '')

    if not otp:
        raise ValueError("Verification code is required")

    otps_table = get_otps_table()
    otp_record = otps_table.get_item(Key={'userId': user_id}).get('Item')

    if not otp_record:
        raise ValueError("No verification code found. Please request a new code.")

    now = int(time.time())
    if otp_record.get('expiresAt', 0) < now:
        otps_table.delete_item(Key={'userId': user_id})
        raise ValueError("Verification code has expired. Please request a new code.")

    attempts = otp_record.get('attempts', 0)
    if attempts >= OTP_MAX_ATTEMPTS:
        otps_table.delete_item(Key={'userId': user_id})
        raise ValueError("Too many failed attempts. Please request a new code.")

    if hash_otp(otp) != otp_record.get('otpHash'):
        otps_table.update_item(
            Key={'userId': user_id},
            UpdateExpression='SET attempts = attempts + :one',
            ExpressionAttributeValues={':one': 1},
        )
        remaining = OTP_MAX_ATTEMPTS - attempts - 1
        raise ValueError(f"Invalid verification code. {remaining} attempt(s) remaining.")

    otps_table.delete_item(Key={'userId': user_id})

    return execute_deletion(user_id, reason, 'self')


def execute_deletion(user_id: str, reason: str, requested_by: str) -> Dict[str, Any]:
    """
    Initiate account deletion:
    1. Disable in Cognito (blocks all auth immediately)
    2. Write grace-period record to deletion_requests

    Logbook data and the users record are left completely untouched during the
    grace period -- Cognito disablement already prevents all data access.
    Everything is cleaned up permanently by the processor Lambda on day 30.
    """
    now_iso = datetime.utcnow().isoformat() + 'Z'
    scheduled_date = datetime.utcnow() + timedelta(days=GRACE_PERIOD_DAYS)
    scheduled_iso = scheduled_date.isoformat() + 'Z'
    audit_ttl = int(time.time()) + (AUDIT_TTL_DAYS * 86400)

    user = get_users_table().get_item(Key={'userId': user_id}).get('Item')
    if not user:
        raise ValueError("User not found")

    email = user.get('email', '')
    name = user.get('name', 'User')

    try:
        get_cognito_client().admin_disable_user(
            UserPoolId=USER_POOL_ID,
            Username=user_id,
        )
    except Exception as e:
        print(f"[UserDeletion] Failed to disable Cognito user: {e}")
        raise ValueError("Failed to disable account. Please try again.")

    get_requests_table().put_item(Item={
        'userId': user_id,
        'requestedAt': now_iso,
        'scheduledHardDeleteAt': scheduled_iso,
        'status': 'GRACE_PERIOD',
        'requestedBy': requested_by,
        'reason': reason or 'No reason provided',
        'email': email,
        'ttl': audit_ttl,
    })

    restore_token = generate_restore_token(user_id)
    try:
        send_deletion_scheduled_email(email, name, scheduled_iso, restore_token)
    except Exception as e:
        print(f"[UserDeletion] Failed to send deletion email: {e}")

    return {
        'success': True,
        'scheduledDeletionDate': scheduled_iso,
        'message': f'Your account is scheduled for permanent deletion on {scheduled_date.strftime("%B %d, %Y")}. Check your email for restoration instructions.',
    }


def handle_cancel_deletion(user_id: str) -> Dict[str, Any]:
    """
    Cancel pending deletion: re-enable Cognito and mark the request CANCELLED.
    No data was touched during the grace period, so nothing else needs undoing.
    """
    requests_table = get_requests_table()
    request = requests_table.get_item(Key={'userId': user_id}).get('Item')

    if not request or request.get('status') != 'GRACE_PERIOD':
        raise ValueError("No pending deletion found for this account.")

    try:
        get_cognito_client().admin_enable_user(
            UserPoolId=USER_POOL_ID,
            Username=user_id,
        )
    except Exception as e:
        print(f"[UserDeletion] Failed to re-enable Cognito user: {e}")
        raise ValueError("Failed to restore account. Please contact support.")

    now_iso = datetime.utcnow().isoformat() + 'Z'
    requests_table.update_item(
        Key={'userId': user_id},
        UpdateExpression='SET #s = :status, cancelledAt = :cancelledAt',
        ExpressionAttributeNames={'#s': 'status'},
        ExpressionAttributeValues={
            ':status': 'CANCELLED',
            ':cancelledAt': now_iso,
        },
    )

    return {
        'success': True,
        'message': 'Your account has been restored.',
    }


def handle_get_deletion_status(user_id: str) -> Optional[Dict[str, Any]]:
    """Return current deletion status for the authenticated user."""
    request = get_requests_table().get_item(Key={'userId': user_id}).get('Item')

    if not request or request.get('status') != 'GRACE_PERIOD':
        return {
            'status': 'ACTIVE',
            'requestedAt': None,
            'scheduledDeletionDate': None,
            'daysRemaining': None,
        }

    scheduled = request.get('scheduledHardDeleteAt', '')
    days_remaining = None
    if scheduled:
        try:
            sched_dt = datetime.fromisoformat(scheduled.replace('Z', '+00:00'))
            now = datetime.utcnow().replace(tzinfo=sched_dt.tzinfo)
            days_remaining = max(0, (sched_dt - now).days)
        except Exception:
            pass

    return {
        'status': 'GRACE_PERIOD',
        'requestedAt': request.get('requestedAt'),
        'scheduledDeletionDate': scheduled,
        'daysRemaining': days_remaining,
    }


def handle_check_account_status(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Public (API key auth) query to check if an email has a pending deletion.
    Used by the login flow to distinguish disabled-due-to-deletion from wrong-password.
    Queries the deletion_requests email-index GSI -- no users table scan needed.
    """
    email = event.get('arguments', {}).get('email', '').strip().lower()
    if not email:
        return {'status': 'NOT_FOUND', 'scheduledDeletionDate': None, 'daysRemaining': None}

    response = get_requests_table().query(
        IndexName='email-index',
        KeyConditionExpression='email = :email',
        FilterExpression='#s = :status',
        ExpressionAttributeNames={'#s': 'status'},
        ExpressionAttributeValues={':email': email, ':status': 'GRACE_PERIOD'},
        Limit=1,
    )

    items = response.get('Items', [])
    if not items:
        return {'status': 'ACTIVE', 'scheduledDeletionDate': None, 'daysRemaining': None}

    request = items[0]
    scheduled = request.get('scheduledHardDeleteAt', '')
    days_remaining = None
    if scheduled:
        try:
            sched_dt = datetime.fromisoformat(scheduled.replace('Z', '+00:00'))
            now = datetime.utcnow().replace(tzinfo=sched_dt.tzinfo)
            days_remaining = max(0, (sched_dt - now).days)
        except Exception:
            pass

    return {
        'status': 'GRACE_PERIOD',
        'scheduledDeletionDate': scheduled,
        'daysRemaining': days_remaining,
    }


def send_otp_email(email: str, otp: str, name: str):
    """Send the deletion OTP verification email via SES."""
    subject = "SkyReady - Account Deletion Verification Code"
    html_body = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #1a1a2e;">Account Deletion Request</h2>
        <p>Hi {name},</p>
        <p>You've requested to delete your SkyReady account. Enter the verification code below to confirm:</p>
        <div style="background: #f0f4ff; border-radius: 8px; padding: 24px; text-align: center; margin: 24px 0;">
            <span style="font-size: 32px; font-weight: bold; letter-spacing: 8px; color: #1a1a2e;">{otp}</span>
        </div>
        <p>This code expires in <strong>10 minutes</strong>.</p>
        <p style="color: #666;">If you did not request this, you can safely ignore this email. Your account will not be affected.</p>
        <hr style="border: none; border-top: 1px solid #eee; margin: 24px 0;" />
        <p style="color: #999; font-size: 12px;">SkyReady Aviation Logbook</p>
    </body>
    </html>
    """

    text_body = f"Hi {name},\n\nYour SkyReady account deletion verification code is: {otp}\n\nThis code expires in 10 minutes.\n\nIf you did not request this, ignore this email."

    try:
        get_ses_client().send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {
                    'Html': {'Data': html_body, 'Charset': 'UTF-8'},
                    'Text': {'Data': text_body, 'Charset': 'UTF-8'},
                },
            },
        )
        print(f"[UserDeletion] OTP email sent to {email}")
    except Exception as e:
        print(f"[UserDeletion] Failed to send OTP email: {e}")
        raise ValueError("Failed to send verification email. Please try again.")


def send_deletion_scheduled_email(email: str, name: str, scheduled_date: str, restore_token: str):
    """Send confirmation email with restore link after deletion is scheduled."""
    restore_url = f"https://app.skyready.com/restore?token={restore_token}"
    
    try:
        formatted_date = datetime.fromisoformat(scheduled_date.replace('Z', '+00:00')).strftime('%B %d, %Y')
    except Exception:
        formatted_date = scheduled_date

    subject = "SkyReady - Account Deletion Scheduled"
    html_body = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #1a1a2e;">Account Deletion Scheduled</h2>
        <p>Hi {name},</p>
        <p>Your SkyReady account has been scheduled for permanent deletion on <strong>{formatted_date}</strong>.</p>
        <p>During the 30-day grace period:</p>
        <ul>
            <li>Your account is disabled and you cannot log in</li>
            <li>Your logbook data is fully preserved and can be recovered</li>
            <li>After {formatted_date}, all data will be permanently removed</li>
        </ul>
        <div style="background: #f0f4ff; border-radius: 8px; padding: 24px; text-align: center; margin: 24px 0;">
            <p style="margin: 0 0 12px 0; font-weight: bold;">Changed your mind?</p>
            <a href="{restore_url}" style="display: inline-block; background: #2563eb; color: white; padding: 12px 32px; border-radius: 8px; text-decoration: none; font-weight: bold;">Keep My Account</a>
        </div>
        <p>You can also restore your account by opening the SkyReady app and logging in during the grace period.</p>
        <p style="color: #666;">This restore link expires on {formatted_date}.</p>
        <hr style="border: none; border-top: 1px solid #eee; margin: 24px 0;" />
        <p style="color: #999; font-size: 12px;">SkyReady Aviation Logbook</p>
    </body>
    </html>
    """

    text_body = (
        f"Hi {name},\n\n"
        f"Your SkyReady account has been scheduled for permanent deletion on {formatted_date}.\n\n"
        f"Changed your mind? Restore your account here: {restore_url}\n\n"
        f"Or open the SkyReady app and log in during the grace period.\n\n"
        f"SkyReady Aviation Logbook"
    )

    get_ses_client().send_email(
        Source=SENDER_EMAIL,
        Destination={'ToAddresses': [email]},
        Message={
            'Subject': {'Data': subject, 'Charset': 'UTF-8'},
            'Body': {
                'Html': {'Data': html_body, 'Charset': 'UTF-8'},
                'Text': {'Data': text_body, 'Charset': 'UTF-8'},
            },
        },
    )
    print(f"[UserDeletion] Deletion scheduled email sent to {email}")
