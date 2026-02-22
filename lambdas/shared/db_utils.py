"""
Shared database utilities for SkyReady Lambda functions.
Provides PostgreSQL and DynamoDB connection management.
"""
import json
import os
import boto3
import psycopg
from psycopg_pool import ConnectionPool
from typing import Dict, Any, Optional

# Initialize AWS clients
secrets_client = boto3.client('secretsmanager')
dynamodb = boto3.resource('dynamodb')

# Database connection pool (initialized lazily on first invocation and reused
# across warm Lambda executions).
db_pool: Optional[ConnectionPool] = None

# Environment variables
DB_SECRET_ARN = os.environ.get('DB_SECRET_ARN')
DB_ENDPOINT = os.environ.get('DB_ENDPOINT')
DB_NAME = os.environ.get('DB_NAME', 'logbook')
STAGE = os.environ.get('STAGE', 'dev')


def get_db_conninfo() -> str:
    """Retrieve database credentials from Secrets Manager and return a conninfo string."""
    try:
        response = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
        secret = json.loads(response['SecretString'])
        return (
            f"host={DB_ENDPOINT} port=5432 dbname={DB_NAME} "
            f"user={secret['username']} password={secret['password']}"
        )
    except Exception as e:
        print(f"Error retrieving DB credentials: {str(e)}")
        raise


def get_db_connection() -> psycopg.Connection:
    """Get a database connection from the pool.

    psycopg3's ConnectionPool must be opened before use. open=True starts the
    pool background thread and opens the minimum connections immediately. The
    pool is created once per Lambda container and reused across warm invocations.
    """
    global db_pool

    if db_pool is None:
        conninfo = get_db_conninfo()
        db_pool = ConnectionPool(conninfo=conninfo, min_size=1, max_size=5, open=True)

    return db_pool.getconn()


def return_db_connection(conn: psycopg.Connection) -> None:
    """Return a connection to the pool."""
    if db_pool:
        db_pool.putconn(conn)


def get_user_pilot_info(user_id: str) -> Dict[str, Any]:
    """Get user pilot info from DynamoDB Users table."""
    try:
        users_table = dynamodb.Table(f'sky-ready-users-{STAGE}')
        response = users_table.get_item(Key={'userId': user_id})
        return response.get('Item', {})
    except Exception as e:
        print(f"[db_utils] Error fetching user from DynamoDB: {e}")
        return {}
