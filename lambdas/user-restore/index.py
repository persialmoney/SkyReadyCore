"""
API Gateway Lambda handler for unauthenticated account restoration via signed JWT token.
Called when user clicks the restore link in the deletion email or uses the guided re-login flow.
"""
import json
import os
import jwt
import boto3
from datetime import datetime
from typing import Dict, Any, Optional

_dynamodb_resource = None
_deletion_requests_table = None
_cognito_client = None

STAGE = os.environ.get('STAGE', 'dev')
DELETION_REQUESTS_TABLE = os.environ.get('DELETION_REQUESTS_TABLE', f'sky-ready-deletion-requests-{STAGE}')
USER_POOL_ID = os.environ.get('USER_POOL_ID', '')
RESTORE_TOKEN_SECRET = os.environ.get('RESTORE_TOKEN_SECRET', 'sky-ready-restore-secret')


def get_dynamodb():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource('dynamodb')
    return _dynamodb_resource


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
    """
    API Gateway Lambda proxy handler.
    Expects POST with JSON body: { "token": "<signed-jwt>" }
    """
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    }

    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': headers, 'body': ''}

    try:
        body = json.loads(event.get('body', '{}'))
        token = body.get('token', '')

        if not token:
            return {
                'statusCode': 400,
                'headers': headers,
                'body': json.dumps({'success': False, 'message': 'Restore token is required.'}),
            }

        user_id = verify_restore_token(token)
        if not user_id:
            return {
                'statusCode': 401,
                'headers': headers,
                'body': json.dumps({'success': False, 'message': 'Invalid or expired restore link. Please contact support.'}),
            }

        request = get_requests_table().get_item(Key={'userId': user_id}).get('Item')
        if not request or request.get('status') != 'GRACE_PERIOD':
            return {
                'statusCode': 409,
                'headers': headers,
                'body': json.dumps({'success': False, 'message': 'No pending deletion found or account has already been permanently deleted.'}),
            }

        try:
            get_cognito_client().admin_enable_user(
                UserPoolId=USER_POOL_ID,
                Username=user_id,
            )
        except Exception as e:
            print(f"[UserRestore] Failed to re-enable Cognito user: {e}")
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps({'success': False, 'message': 'Failed to restore account. Please contact support.'}),
            }

        now_iso = datetime.utcnow().isoformat() + 'Z'
        get_requests_table().update_item(
            Key={'userId': user_id},
            UpdateExpression='SET #s = :status, cancelledAt = :cancelledAt, restoredVia = :via',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':status': 'CANCELLED',
                ':cancelledAt': now_iso,
                ':via': 'restore_token',
            },
        )

        print(f"[UserRestore] Successfully restored account for user {user_id}")

        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'success': True,
                'message': 'Your account has been restored. Please sign in to continue.',
            }),
        }

    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps({'success': False, 'message': 'Invalid request body.'}),
        }
    except Exception as e:
        print(f"[UserRestore] Unexpected error: {e}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'success': False, 'message': 'An unexpected error occurred.'}),
        }
