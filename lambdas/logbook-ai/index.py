"""
AppSync Lambda Resolver for logbook AI features.
Handles semantic search and conversational AI queries using AWS Bedrock.
"""
import json
import os
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.pool import ThreadedConnectionPool
from typing import Dict, Any, Optional, List
from datetime import datetime

# Initialize AWS clients
secrets_client = boto3.client('secretsmanager')
bedrock_client = boto3.client('bedrock-runtime', region_name=os.environ.get('BEDROCK_REGION', 'us-east-1'))

# Database connection pool (initialized lazily)
db_pool: Optional[ThreadedConnectionPool] = None

# Environment variables
DB_SECRET_ARN = os.environ.get('DB_SECRET_ARN')
DB_ENDPOINT = os.environ.get('DB_ENDPOINT')
DB_NAME = os.environ.get('DB_NAME', 'logbook')
EMBEDDING_MODEL_ID = os.environ.get('EMBEDDING_MODEL_ID', 'amazon.titan-embed-text-v1')
CHAT_MODEL_ID = os.environ.get('CHAT_MODEL_ID', 'anthropic.claude-sonnet-4-20250517-v1:0')


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


def generate_embedding(text: str) -> List[float]:
    """Generate vector embedding using AWS Bedrock Titan Embeddings"""
    try:
        body = json.dumps({
            "inputText": text
        })
        
        response = bedrock_client.invoke_model(
            modelId=EMBEDDING_MODEL_ID,
            body=body,
            contentType="application/json",
            accept="application/json"
        )
        
        response_body = json.loads(response['body'].read())
        embedding = response_body.get('embedding', [])
        
        if not embedding:
            raise ValueError("No embedding returned from Bedrock")
        
        return embedding
    
    except Exception as e:
        print(f"Error generating embedding: {str(e)}")
        raise


def invoke_claude_chat(messages: List[Dict[str, str]], system_prompt: str) -> str:
    """Invoke Claude Sonnet 4.5 for chat/analysis"""
    try:
        # Prepare messages for Claude
        claude_messages = []
        for msg in messages:
            claude_messages.append({
                "role": msg['role'],
                "content": msg['content']
            })
        
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "system": system_prompt,
            "messages": claude_messages
        })
        
        response = bedrock_client.invoke_model(
            modelId=CHAT_MODEL_ID,
            body=body,
            contentType="application/json",
            accept="application/json"
        )
        
        response_body = json.loads(response['body'].read())
        
        # Extract text from Claude's response
        if 'content' in response_body:
            content = response_body['content']
            if isinstance(content, list) and len(content) > 0:
                return content[0].get('text', '')
            elif isinstance(content, str):
                return content
        
        raise ValueError("Unexpected response format from Claude")
    
    except Exception as e:
        print(f"Error invoking Claude: {str(e)}")
        raise


def convert_db_entry_to_graphql(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Convert database entry format to GraphQL format"""
    return {
        'entryId': entry['entry_id'],
        'userId': entry['user_id'],
        'date': entry['date'].isoformat() if isinstance(entry['date'], datetime) else str(entry['date']),
        'aircraft': entry.get('aircraft'),
        'tailNumber': entry.get('tail_number'),
        'routeLegs': entry.get('route_legs', []),
        'route': entry.get('route'),
        'flightTypes': entry.get('flight_types', []),
        'totalTime': float(entry['total_time']) if entry.get('total_time') else None,
        'blockOut': entry.get('block_out'),
        'blockIn': entry.get('block_in'),
        'pic': float(entry['pic']) if entry.get('pic') else None,
        'sic': float(entry['sic']) if entry.get('sic') else None,
        'dualReceived': float(entry['dual_received']) if entry.get('dual_received') else None,
        'dualGiven': float(entry['dual_given']) if entry.get('dual_given') else None,
        'solo': float(entry['solo']) if entry.get('solo') else None,
        'crossCountry': float(entry['cross_country']) if entry.get('cross_country') else None,
        'night': float(entry['night']) if entry.get('night') else None,
        'actualImc': float(entry['actual_imc']) if entry.get('actual_imc') else None,
        'simulatedInstrument': float(entry['simulated_instrument']) if entry.get('simulated_instrument') else None,
        'dayTakeoffs': entry.get('day_takeoffs'),
        'dayLandings': entry.get('day_landings'),
        'nightTakeoffs': entry.get('night_takeoffs'),
        'nightLandings': entry.get('night_landings'),
        'dayFullStopLandings': entry.get('day_full_stop_landings'),
        'nightFullStopLandings': entry.get('night_full_stop_landings'),
        'approaches': entry.get('approaches'),
        'holds': entry.get('holds', False),
        'tracking': entry.get('tracking', False),
        'instructor': entry.get('instructor'),
        'student': entry.get('student'),
        'lessonTopic': entry.get('lesson_topic'),
        'groundInstruction': entry.get('ground_instruction'),
        'maneuvers': entry.get('maneuvers', []),
        'remarks': entry.get('remarks'),
        'safetyNotes': entry.get('safety_notes'),
        'safetyRelevant': entry.get('safety_relevant', False),
        'status': entry.get('status', 'draft'),
        'signature': entry.get('signature'),
        'createdAt': entry['created_at'].isoformat() if isinstance(entry['created_at'], datetime) else str(entry['created_at']),
        'updatedAt': entry['updated_at'].isoformat() if isinstance(entry['updated_at'], datetime) else str(entry['updated_at']),
    }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AppSync Lambda resolver handler for logbook AI features.
    
    Event structure from AppSync:
    {
        "arguments": {...},
        "identity": {
            "sub": "user-id-uuid"
        },
        "info": {
            "fieldName": "searchLogbookEntries" or "chatLogbookQuery",
            "parentTypeName": "Query"
        }
    }
    """
    try:
        # Extract common data with defensive null checks
        identity = event.get('identity') if event else {}
        if identity is None:
            identity = {}
        
        arguments = event.get('arguments') if event else {}
        if arguments is None:
            arguments = {}
        
        info = event.get('info') if event else {}
        if info is None:
            info = {}
        
        user_id = identity.get('sub')
        field_name = info.get('fieldName', '')
        
        if not user_id:
            raise ValueError("User ID (identity.sub) is required but was missing")
        
        # Route to appropriate handler
        if field_name == "searchLogbookEntries":
            return handle_semantic_search(user_id, arguments)
        elif field_name == "chatLogbookQuery":
            return handle_chat_query(user_id, arguments)
        else:
            raise ValueError(f"Unknown field name: {field_name}")
    
    except Exception as e:
        import traceback
        error_message = f"Error in logbook AI operation: {str(e)}"
        print(f"[LogbookAI] ERROR: {error_message}")
        print(f"[LogbookAI] Full traceback: {traceback.format_exc()}")
        print(f"[LogbookAI] Event received: {json.dumps(event, default=str)}")
        print(f"[LogbookAI] fieldName: {event.get('info', {}).get('fieldName', 'UNKNOWN') if event and event.get('info') else 'UNKNOWN'}")
        print(f"[LogbookAI] userId: {event.get('identity', {}).get('sub', 'UNKNOWN')}")
        raise Exception(error_message)


def handle_semantic_search(user_id: str, arguments: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Perform semantic search using vector similarity"""
    if arguments is None:
        arguments = {}
    
    query_text = arguments.get('query', '')
    limit = arguments.get('limit', 10)
    
    if not query_text:
        raise ValueError("query is required")
    
    # Generate embedding for query
    query_embedding = generate_embedding(query_text)
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Use pgvector cosine similarity search
        # The <=> operator computes cosine distance (lower = more similar)
        # We convert to similarity score (higher = more similar)
        # pgvector expects the embedding as a string in format '[0.1,0.2,...]'
        embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'
        cur.execute("""
            SELECT 
                le.*,
                1 - (e.embedding <=> %s::vector) as similarity
            FROM logbook_entry_embeddings e
            JOIN logbook_entries le ON e.entry_id = le.entry_id
            WHERE le.user_id = %s
            ORDER BY similarity DESC
            LIMIT %s
        """, (embedding_str, user_id, limit))
        
        results = [dict(row) for row in cur.fetchall()]
        cur.close()
        
        # Convert to GraphQL format
        entries = [convert_db_entry_to_graphql(entry) for entry in results]
        
        return entries
    
    finally:
        if conn:
            return_db_connection(conn)


def handle_chat_query(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle conversational AI query about logbook"""
    if arguments is None:
        arguments = {}
    
    query = arguments.get('query', '')
    
    if not query:
        raise ValueError("query is required")
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get user's logbook entries for context
        cur.execute("""
            SELECT le.*, e.searchable_text
            FROM logbook_entries le
            LEFT JOIN logbook_entry_embeddings e ON le.entry_id = e.entry_id
            WHERE le.user_id = %s
            ORDER BY le.date DESC
            LIMIT 50
        """, (user_id,))
        
        entries = [dict(row) for row in cur.fetchall()]
        cur.close()
        
        # Build context from entries
        context_parts = []
        for entry in entries[:20]:  # Use top 20 entries for context
            context_parts.append(entry.get('searchable_text', ''))
        
        context = "\n\n".join(context_parts)
        
        # System prompt for Claude
        system_prompt = """You are an AI assistant helping a pilot analyze their flight logbook. 
You have access to their logbook entries and can answer questions about:
- Flight history and patterns
- Training progress
- Currency status
- Flight statistics
- Specific flights or experiences

Be helpful, accurate, and aviation-focused. If you need specific data that's not in the context, 
say so rather than making assumptions."""

        # User message
        user_message = f"""Here are my recent logbook entries:

{context}

Question: {query}

Please provide a helpful answer based on my logbook data."""
        
        # Invoke Claude
        messages = [
            {
                "role": "user",
                "content": user_message
            }
        ]
        
        response_text = invoke_claude_chat(messages, system_prompt)
        
        return {
            'query': query,
            'response': response_text,
            'entriesUsed': len(entries)
        }
    
    finally:
        if conn:
            return_db_connection(conn)
