"""
Copilot History Lambda — direct AppSync resolvers for chat history.

Handles three fields dispatched by AppSync:
  - listCopilotConversations  (Query)
  - getCopilotMessages        (Query)
  - deleteCopilotConversation (Mutation)

User identity comes from the Cognito authorizer via event['identity']['claims']['sub'].
No lastPulledAt cursor — these are direct, non-sync queries.
"""
import json
import os
import boto3
import psycopg

secretsmanager = boto3.client('secretsmanager')

_db_conn = None


def get_db_connection():
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        secret = json.loads(
            secretsmanager.get_secret_value(SecretId=os.environ['DB_SECRET_ARN'])['SecretString']
        )
        _db_conn = psycopg.connect(
            host=secret['host'],
            port=secret.get('port', 5432),
            dbname=os.environ.get('DB_NAME', secret.get('dbname', 'skyready')),
            user=secret['username'],
            password=secret['password'],
        )
    return _db_conn


def handler(event, context):
    field = event['info']['fieldName']
    user_id = event['identity']['claims']['sub']

    conn = get_db_connection()
    try:
        if field == 'listCopilotConversations':
            return list_conversations(conn, user_id)
        elif field == 'getCopilotMessages':
            conversation_id = event['arguments']['conversationId']
            return get_messages(conn, user_id, conversation_id)
        elif field == 'deleteCopilotConversation':
            conversation_id = event['arguments']['id']
            return delete_conversation(conn, user_id, conversation_id)
        else:
            raise ValueError(f"Unknown field: {field}")
    except Exception as e:
        conn.rollback()
        print(f"[copilot-history] Error in {field}: {e}")
        raise
    finally:
        conn.commit()


def list_conversations(conn, user_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.user_id, c.title, c.created_at, c.updated_at,
                   COUNT(m.id) AS message_count
            FROM copilot_conversations c
            LEFT JOIN copilot_messages m ON m.conversation_id = c.id
            WHERE c.user_id = %s
            GROUP BY c.id, c.user_id, c.title, c.created_at, c.updated_at
            ORDER BY c.updated_at DESC
            LIMIT 50
        """, [user_id])
        rows = cur.fetchall()

    result = [format_conversation(row) for row in rows]
    print(f"[copilot-history] listCopilotConversations: {len(result)} conversations for {user_id}")
    return result


def get_messages(conn, user_id, conversation_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.id, m.conversation_id, m.role, m.content, m.model_id, m.created_at
            FROM copilot_messages m
            JOIN copilot_conversations c ON c.id = m.conversation_id
            WHERE m.conversation_id = %s
              AND c.user_id = %s
            ORDER BY m.created_at ASC
            LIMIT 100
        """, [conversation_id, user_id])
        rows = cur.fetchall()

    result = [format_message(row) for row in rows]
    print(f"[copilot-history] getCopilotMessages: {len(result)} messages for conversation {conversation_id}")
    return result


def delete_conversation(conn, user_id, conversation_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM copilot_conversations
            WHERE id = %s AND user_id = %s
        """, [conversation_id, user_id])
        deleted = cur.rowcount

    print(f"[copilot-history] deleteCopilotConversation: {conversation_id} (deleted={deleted > 0})")
    return deleted > 0


def format_conversation(row):
    """Column order: 0:id 1:user_id 2:title 3:created_at 4:updated_at 5:message_count"""
    updated_at_ms = float(int(row[4].timestamp() * 1000))
    return {
        'id':            str(row[0]),
        'userId':        row[1],
        'title':         row[2] or '',
        'messageCount':  int(row[5]),
        'lastMessageAt': updated_at_ms,
        'createdAt':     float(int(row[3].timestamp() * 1000)),
        'updatedAt':     updated_at_ms,
    }


def format_message(row):
    """Column order: 0:id 1:conversation_id 2:role 3:content 4:model_id 5:created_at"""
    return {
        'id':             str(row[0]),
        'conversationId': str(row[1]),
        'role':           row[2],
        'content':        row[3],
        'modelId':        row[4],
        'createdAt':      float(int(row[5].timestamp() * 1000)),
    }
