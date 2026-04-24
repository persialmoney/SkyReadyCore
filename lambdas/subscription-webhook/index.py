"""
RevenueCat Subscription Webhook Lambda.

Receives POST events from RevenueCat and updates DynamoDB user.subscription.
Authentication: HMAC-SHA256 signature in X-RevenueCat-Signature header.
Secret is stored in Secrets Manager and fetched at cold start.

Event type mapping:
  INITIAL_PURCHASE / RENEWAL / PRODUCT_CHANGE / GRANT_ENTITLEMENT → pro/active
  TRIAL_STARTED / TRIAL_CANCELLED                                  → pro/trialing
  TRIAL_CONVERTED                                                  → pro/active
  CANCELLATION                                                     → pro/cancelled
  EXPIRATION                                                       → free/expired
  BILLING_ISSUE                                                    → pro/active (RC manages grace period)
"""
import json
import os
import hmac
import hashlib
import boto3
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

dynamodb = boto3.resource('dynamodb')
secrets_client = boto3.client('secretsmanager')

USERS_TABLE = os.environ.get('USERS_TABLE', '')
RC_WEBHOOK_SECRET_ARN = os.environ.get('RC_WEBHOOK_SECRET_ARN', '')

_webhook_secret: Optional[str] = None


def get_webhook_secret() -> str:
    global _webhook_secret
    if not _webhook_secret:
        response = secrets_client.get_secret_value(SecretId=RC_WEBHOOK_SECRET_ARN)
        _webhook_secret = response['SecretString']
    return _webhook_secret


# RC event type → (plan, status). None = ignore silently.
_EVENT_MAP: Dict[str, Optional[Tuple[str, str]]] = {
    'INITIAL_PURCHASE':  ('pro',  'active'),
    'RENEWAL':           ('pro',  'active'),
    'PRODUCT_CHANGE':    ('pro',  'active'),
    'GRANT_ENTITLEMENT': ('pro',  'active'),
    'TRIAL_STARTED':     ('pro',  'trialing'),
    'TRIAL_CONVERTED':   ('pro',  'active'),
    'TRIAL_CANCELLED':   ('pro',  'trialing'),  # still active until period ends
    'CANCELLATION':      ('pro',  'cancelled'),
    'EXPIRATION':        ('free', 'expired'),
    'BILLING_ISSUE':     ('pro',  'active'),    # RC handles the grace period window
    'SUBSCRIBER_ALIAS':  None,
    'TRANSFER':          None,
    'UNCANCELLATION':    ('pro',  'active'),
    'NON_RENEWING_PURCHASE': ('pro', 'active'),
    'PAUSE':             None,
    'RESUME':            ('pro',  'active'),
}


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        body_raw: str = event.get('body', '') or ''
        headers = event.get('headers') or {}
        # API Gateway v2 lowercases headers
        signature = headers.get('x-revenuecat-signature') or headers.get('X-RevenueCat-Signature', '')

        if not signature:
            print("[SubscriptionWebhook] Missing X-RevenueCat-Signature header")
            return _response(401, 'Missing signature')

        secret = get_webhook_secret()
        expected = hmac.new(secret.encode(), body_raw.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            print("[SubscriptionWebhook] Signature mismatch")
            return _response(401, 'Invalid signature')

        body = json.loads(body_raw)
        rc_event = body.get('event', {})
        event_type: str = rc_event.get('type', '')
        app_user_id: str = rc_event.get('app_user_id', '')
        expires_at_ms = rc_event.get('expiration_at_ms')

        if not app_user_id:
            return _response(400, 'Missing app_user_id')

        if event_type not in _EVENT_MAP:
            print(f"[SubscriptionWebhook] Unknown event type: {event_type}")
            return _response(200, 'Unknown event type — ignored')

        mapping = _EVENT_MAP[event_type]
        if mapping is None:
            print(f"[SubscriptionWebhook] Ignored event type: {event_type}")
            return _response(200, 'Event ignored')

        plan, status = mapping
        expires_iso = None
        if expires_at_ms:
            expires_iso = datetime.fromtimestamp(expires_at_ms / 1000, tz=timezone.utc).isoformat()

        table = dynamodb.Table(USERS_TABLE)
        table.update_item(
            Key={'userId': app_user_id},
            UpdateExpression='SET subscription = :sub',
            ExpressionAttributeValues={
                ':sub': {
                    'plan': plan,
                    'status': status,
                    'expiresAt': expires_iso,
                    'rcUserId': app_user_id,
                    'updatedAt': datetime.now(tz=timezone.utc).isoformat(),
                }
            },
        )

        print(f"[SubscriptionWebhook] {event_type} → plan={plan} status={status} user={app_user_id}")
        return _response(200, 'OK')

    except json.JSONDecodeError:
        return _response(400, 'Invalid JSON body')
    except Exception as e:
        import traceback
        print(f"[SubscriptionWebhook] ERROR: {e}\n{traceback.format_exc()}")
        return _response(500, 'Internal server error')


def _response(status: int, message: str) -> Dict[str, Any]:
    return {
        'statusCode': status,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'message': message}),
    }
