"""
Shared database utilities for SkyReady Lambda functions.
Provides PostgreSQL and DynamoDB connection management.
"""
import json
import os
import boto3
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from typing import Dict, Any, Optional

# Initialize AWS clients
secrets_client = boto3.client('secretsmanager')
dynamodb = boto3.resource('dynamodb')

# Database connection pool (initialized lazily)
db_pool: Optional[ThreadedConnectionPool] = None

# Environment variables
DB_SECRET_ARN = os.environ.get('DB_SECRET_ARN')
DB_ENDPOINT = os.environ.get('DB_ENDPOINT')
DB_NAME = os.environ.get('DB_NAME', 'logbook')
STAGE = os.environ.get('STAGE', 'dev')


def get_db_credentials() -> Dict[str, str]:
    """Retrieve database credentials from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
        secret = json.loads(response['SecretString'])
        return {
            'host': DB_ENDPOINT,
            'port': 5432,
            'database': DB_NAME,
            'user': secret['username'],
            'password': secret['password']
        }
    except Exception as e:
        print(f"Error retrieving DB credentials: {str(e)}")
        raise


def get_db_connection():
    """Get database connection from pool"""
    global db_pool
    
    if db_pool is None:
        credentials = get_db_credentials()
        db_pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            **credentials
        )
    
    return db_pool.getconn()


def return_db_connection(conn):
    """Return connection to pool"""
    if db_pool:
        db_pool.putconn(conn)


def get_user_pilot_info(user_id: str) -> Dict[str, Any]:
    """Get user pilot info from DynamoDB Users table"""
    try:
        users_table = dynamodb.Table(f'sky-ready-users-{STAGE}')
        response = users_table.get_item(Key={'userId': user_id})
        return response.get('Item', {})
    except Exception as e:
        print(f"[db_utils] Error fetching user from DynamoDB: {e}")
        return {}
