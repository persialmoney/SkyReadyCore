"""
AppSync Lambda Resolver for logbook operations.
Handles CRUD operations for logbook entries and generates vector embeddings.
"""
import json
import os
import boto3
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from decimal import Decimal

# Initialize AWS clients
secrets_client = boto3.client('secretsmanager')
bedrock_client = boto3.client('bedrock-runtime', region_name=os.environ.get('BEDROCK_REGION', 'us-east-1'))
dynamodb = boto3.resource('dynamodb')

# Database connection pool (initialized lazily)
db_pool: Optional[ThreadedConnectionPool] = None

# Environment variables
DB_SECRET_ARN = os.environ.get('DB_SECRET_ARN')
DB_ENDPOINT = os.environ.get('DB_ENDPOINT')
DB_NAME = os.environ.get('DB_NAME', 'logbook')
EMBEDDING_MODEL_ID = os.environ.get('EMBEDDING_MODEL_ID', 'amazon.titan-embed-text-v1')
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


def generate_embedding(text: str) -> List[float]:
    """Generate vector embedding using AWS Bedrock Titan Embeddings"""
    try:
        # Prepare request for Bedrock
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


def concatenate_logbook_entry(entry: Dict[str, Any]) -> str:
    """Concatenate complete logbook entry into searchable text"""
    parts = []
    
    # Basic info
    if entry.get('date'):
        parts.append(f"Date: {entry['date']}")
    
    # Aircraft info
    aircraft = entry.get('aircraft')
    if aircraft:
        if isinstance(aircraft, dict):
            tail = aircraft.get('tailNumber', entry.get('tailNumber', ''))
            make = aircraft.get('make', '')
            model = aircraft.get('model', '')
            category = aircraft.get('category', '')
            class_type = aircraft.get('class', '')
            parts.append(f"Aircraft: {make} {model} {tail} {category} {class_type}".strip())
        else:
            parts.append(f"Aircraft: {aircraft}")
    
    if entry.get('tailNumber') and not aircraft:
        parts.append(f"Tail Number: {entry['tailNumber']}")
    
    # Route
    route_legs = entry.get('routeLegs', [])
    if route_legs:
        route_str = " -> ".join([f"{leg.get('from', '')} to {leg.get('to', '')}" for leg in route_legs])
        parts.append(f"Route: {route_str}")
    elif entry.get('route'):
        parts.append(f"Route: {entry['route']}")
    
    # Flight types
    flight_types = entry.get('flightTypes', [])
    if flight_types:
        parts.append(f"Flight Types: {', '.join(flight_types)}")
    
    # Time information
    if entry.get('totalTime'):
        parts.append(f"Total Time: {entry['totalTime']} hours")
    if entry.get('pic'):
        parts.append(f"PIC: {entry['pic']} hours")
    if entry.get('sic'):
        parts.append(f"SIC: {entry['sic']} hours")
    if entry.get('dualReceived'):
        parts.append(f"Dual Received: {entry['dualReceived']} hours")
    if entry.get('dualGiven'):
        parts.append(f"Dual Given: {entry['dualGiven']} hours")
    if entry.get('solo'):
        parts.append(f"Solo: {entry['solo']} hours")
    if entry.get('crossCountry'):
        parts.append(f"Cross Country: {entry['crossCountry']} hours")
    if entry.get('night'):
        parts.append(f"Night: {entry['night']} hours")
    if entry.get('actualImc'):
        parts.append(f"Actual IMC: {entry['actualImc']} hours")
    if entry.get('simulatedInstrument'):
        parts.append(f"Simulated Instrument: {entry['simulatedInstrument']} hours")
    
    # Landings
    if entry.get('dayTakeoffs'):
        parts.append(f"Day Takeoffs: {entry['dayTakeoffs']}")
    if entry.get('dayLandings'):
        parts.append(f"Day Landings: {entry['dayLandings']}")
    if entry.get('nightTakeoffs'):
        parts.append(f"Night Takeoffs: {entry['nightTakeoffs']}")
    if entry.get('nightLandings'):
        parts.append(f"Night Landings: {entry['nightLandings']}")
    if entry.get('fullStopLandings'):
        parts.append(f"Full Stop Landings: {entry['fullStopLandings']}")
    
    # Instrument
    if entry.get('approaches'):
        parts.append(f"Approaches: {entry['approaches']}")
    if entry.get('holds'):
        parts.append("Holds: Performed")
    if entry.get('tracking'):
        parts.append("Tracking/Intercepting: Performed")
    
    # Crew/Instruction
    instructor = entry.get('instructor')
    if instructor:
        if isinstance(instructor, dict):
            parts.append(f"Instructor: {instructor.get('name', '')} {instructor.get('certificateNumber', '')}")
        else:
            parts.append(f"Instructor: {instructor}")
    
    student = entry.get('student')
    if student:
        if isinstance(student, dict):
            parts.append(f"Student: {student.get('name', '')}")
        else:
            parts.append(f"Student: {student}")
    
    if entry.get('lessonTopic'):
        parts.append(f"Lesson Topic: {entry['lessonTopic']}")
    if entry.get('groundInstruction'):
        parts.append(f"Ground Instruction: {entry['groundInstruction']} hours")
    
    maneuvers = entry.get('maneuvers', [])
    if maneuvers:
        parts.append(f"Maneuvers: {', '.join(maneuvers)}")
    
    # Notes
    if entry.get('remarks'):
        parts.append(f"Remarks: {entry['remarks']}")
    if entry.get('safetyNotes'):
        parts.append(f"Safety Notes: {entry['safetyNotes']}")
    if entry.get('safetyRelevant'):
        parts.append("Safety Relevant: Yes")
    
    # Status
    if entry.get('status'):
        parts.append(f"Status: {entry['status']}")
    
    return ". ".join(parts)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AppSync Lambda resolver handler for logbook operations.
    
    Event structure from AppSync:
    {
        "arguments": {...},  # Query/mutation arguments
        "identity": {
            "sub": "user-id-uuid"
        },
        "info": {
            "fieldName": "createLogbookEntry",  # or "getLogbookEntry", etc.
            "parentTypeName": "Mutation" or "Query"
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
        if field_name == "createLogbookEntry":
            return handle_create_entry(user_id, arguments)
        elif field_name == "updateLogbookEntry":
            return handle_update_entry(user_id, arguments)
        elif field_name == "deleteLogbookEntry":
            return handle_delete_entry(user_id, arguments)
        elif field_name == "getLogbookEntry":
            return handle_get_entry(user_id, arguments)
        elif field_name == "listLogbookEntries":
            return handle_list_entries(user_id, arguments)
        elif field_name == "requestSignature":
            return handle_request_signature(user_id, arguments)
        elif field_name == "signEntry":
            return handle_sign_entry(user_id, arguments)
        elif field_name == "getCurrency":
            return handle_get_currency(user_id, arguments)
        else:
            raise ValueError(f"Unknown field name: {field_name}")
    
    except Exception as e:
        import traceback
        error_message = f"Error in logbook operation: {str(e)}"
        print(f"[LogbookOperations] ERROR: {error_message}")
        print(f"[LogbookOperations] Full traceback: {traceback.format_exc()}")
        print(f"[LogbookOperations] Event received: {json.dumps(event, default=str)}")
        print(f"[LogbookOperations] fieldName: {event.get('info', {}).get('fieldName', 'UNKNOWN') if event and event.get('info') else 'UNKNOWN'}")
        print(f"[LogbookOperations] userId: {event.get('identity', {}).get('sub', 'UNKNOWN') if event and event.get('identity') else 'UNKNOWN'}")
        raise Exception(error_message)


def handle_create_entry(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new logbook entry with embedding"""
    if arguments is None:
        arguments = {}
    
    input_data = arguments.get('input', {})
    if not input_data:
        raise ValueError("input is required")
    
    entry_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    
    # Prepare entry data
    entry = {
        'entryId': entry_id,
        'userId': user_id,
        **input_data,
        'createdAt': now,
        'updatedAt': now,
    }
    
    # Generate embedding from concatenated entry text
    searchable_text = concatenate_logbook_entry(entry)
    embedding = generate_embedding(searchable_text)
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Insert logbook entry
        cur.execute("""
            INSERT INTO logbook_entries (
                entry_id, user_id, date, aircraft, tail_number, route_legs, route,
                flight_types, total_time, block_out, block_in,
                pic, sic, dual_received, dual_given, solo, cross_country, night,
                actual_imc, simulated_instrument,
                day_takeoffs, day_landings, night_takeoffs, night_landings, full_stop_landings,
                approaches, holds, tracking,
                instructor, student, lesson_topic, ground_instruction, maneuvers,
                remarks, safety_notes, safety_relevant,
                status, signature, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """, (
            entry_id, user_id, input_data.get('date'),
            Json(input_data.get('aircraft')) if input_data.get('aircraft') else None,
            input_data.get('tailNumber'),
            Json(input_data.get('routeLegs', [])) if input_data.get('routeLegs') else None,
            input_data.get('route'),
            input_data.get('flightTypes', []),
            float(input_data['totalTime']) if input_data.get('totalTime') else None,
            input_data.get('blockOut'), input_data.get('blockIn'),
            float(input_data['pic']) if input_data.get('pic') else None,
            float(input_data['sic']) if input_data.get('sic') else None,
            float(input_data['dualReceived']) if input_data.get('dualReceived') else None,
            float(input_data['dualGiven']) if input_data.get('dualGiven') else None,
            float(input_data['solo']) if input_data.get('solo') else None,
            float(input_data['crossCountry']) if input_data.get('crossCountry') else None,
            float(input_data['night']) if input_data.get('night') else None,
            float(input_data['actualImc']) if input_data.get('actualImc') else None,
            float(input_data['simulatedInstrument']) if input_data.get('simulatedInstrument') else None,
            int(input_data['dayTakeoffs']) if input_data.get('dayTakeoffs') else None,
            int(input_data['dayLandings']) if input_data.get('dayLandings') else None,
            int(input_data['nightTakeoffs']) if input_data.get('nightTakeoffs') else None,
            int(input_data['nightLandings']) if input_data.get('nightLandings') else None,
            int(input_data['fullStopLandings']) if input_data.get('fullStopLandings') else None,
            int(input_data['approaches']) if input_data.get('approaches') else None,
            input_data.get('holds', False),
            input_data.get('tracking', False),
            Json(input_data.get('instructor')) if input_data.get('instructor') else None,
            Json(input_data.get('student')) if input_data.get('student') else None,
            input_data.get('lessonTopic'),
            input_data.get('groundInstruction'),
            input_data.get('maneuvers', []),
            input_data.get('remarks'),
            input_data.get('safetyNotes'),
            input_data.get('safetyRelevant', False),
            input_data.get('status', 'draft'),
            Json(input_data.get('signature')) if input_data.get('signature') else None,
            now, now
        ))
        
        # Insert embedding
        # pgvector expects the embedding as a string in format '[0.1,0.2,...]'
        embedding_str = '[' + ','.join(map(str, embedding)) + ']'
        cur.execute("""
            INSERT INTO logbook_entry_embeddings (entry_id, embedding, searchable_text, created_at)
            VALUES (%s, %s::vector, %s, %s)
        """, (
            entry_id,
            embedding_str,
            searchable_text,
            now
        ))
        
        conn.commit()
        
        # Fetch and return created entry
        cur.execute("""
            SELECT * FROM logbook_entries WHERE entry_id = %s
        """, (entry_id,))
        
        result = dict(cur.fetchone())
        cur.close()
        
        return convert_db_entry_to_graphql(result)
    
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_db_connection(conn)


def handle_update_entry(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Update an existing logbook entry and regenerate embedding"""
    if arguments is None:
        arguments = {}
    
    entry_id = arguments.get('entryId')
    if not entry_id:
        raise ValueError("entryId is required")
    
    input_data = arguments.get('input', {})
    if not input_data:
        raise ValueError("input is required")
    
    now = datetime.utcnow().isoformat()
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # First, get existing entry to merge with updates
        cur.execute("SELECT * FROM logbook_entries WHERE entry_id = %s AND user_id = %s", (entry_id, user_id))
        existing = cur.fetchone()
        
        if not existing:
            raise ValueError(f"Entry {entry_id} not found for user {user_id}")
        
        existing_dict = dict(existing)
        updated_entry = {**existing_dict, **input_data, 'updatedAt': now}
        
        # Generate new embedding
        searchable_text = concatenate_logbook_entry(updated_entry)
        embedding = generate_embedding(searchable_text)
        
        # Build update query dynamically
        update_fields = []
        values = []
        
        field_mapping = {
            'date': 'date',
            'aircraft': ('aircraft', Json),
            'tailNumber': 'tail_number',
            'routeLegs': ('route_legs', Json),
            'route': 'route',
            'flightTypes': 'flight_types',
            'totalTime': ('total_time', float),
            'blockOut': 'block_out',
            'blockIn': 'block_in',
            'pic': ('pic', float),
            'sic': ('sic', float),
            'dualReceived': ('dual_received', float),
            'dualGiven': ('dual_given', float),
            'solo': ('solo', float),
            'crossCountry': ('cross_country', float),
            'night': ('night', float),
            'actualImc': ('actual_imc', float),
            'simulatedInstrument': ('simulated_instrument', float),
            'dayTakeoffs': ('day_takeoffs', int),
            'dayLandings': ('day_landings', int),
            'nightTakeoffs': ('night_takeoffs', int),
            'nightLandings': ('night_landings', int),
            'fullStopLandings': ('full_stop_landings', int),
            'approaches': ('approaches', int),
            'holds': 'holds',
            'tracking': 'tracking',
            'instructor': ('instructor', Json),
            'student': ('student', Json),
            'lessonTopic': 'lesson_topic',
            'groundInstruction': 'ground_instruction',
            'maneuvers': 'maneuvers',
            'remarks': 'remarks',
            'safetyNotes': 'safety_notes',
            'safetyRelevant': 'safety_relevant',
            'status': 'status',
            'signature': ('signature', Json),
        }
        
        for key, value in input_data.items():
            if key in field_mapping:
                db_field = field_mapping[key]
                if isinstance(db_field, tuple):
                    db_name, converter = db_field
                    if converter == Json:
                        update_fields.append(f"{db_name} = %s")
                        values.append(Json(value) if value else None)
                    else:
                        update_fields.append(f"{db_name} = %s")
                        values.append(converter(value) if value else None)
                else:
                    update_fields.append(f"{db_field} = %s")
                    values.append(value)
        
        update_fields.append("updated_at = %s")
        values.append(now)
        values.extend([entry_id, user_id])
        
        update_query = f"""
            UPDATE logbook_entries
            SET {', '.join(update_fields)}
            WHERE entry_id = %s AND user_id = %s
        """
        
        cur.execute(update_query, values)
        
        # Update embedding
        # pgvector expects the embedding as a string in format '[0.1,0.2,...]'
        embedding_str = '[' + ','.join(map(str, embedding)) + ']'
        cur.execute("""
            UPDATE logbook_entry_embeddings
            SET embedding = %s::vector, searchable_text = %s
            WHERE entry_id = %s
        """, (embedding_str, searchable_text, entry_id))
        
        conn.commit()
        
        # Fetch and return updated entry
        cur.execute("SELECT * FROM logbook_entries WHERE entry_id = %s", (entry_id,))
        result = dict(cur.fetchone())
        cur.close()
        
        return convert_db_entry_to_graphql(result)
    
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_db_connection(conn)


def handle_delete_entry(user_id: str, arguments: Dict[str, Any]) -> bool:
    """Delete a logbook entry and its embedding"""
    if arguments is None:
        arguments = {}
    
    entry_id = arguments.get('entryId')
    if not entry_id:
        raise ValueError("entryId is required")
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Delete embedding first (foreign key constraint)
        cur.execute("DELETE FROM logbook_entry_embeddings WHERE entry_id = %s", (entry_id,))
        
        # Delete entry
        cur.execute("DELETE FROM logbook_entries WHERE entry_id = %s AND user_id = %s", (entry_id, user_id))
        
        if cur.rowcount == 0:
            raise ValueError(f"Entry {entry_id} not found for user {user_id}")
        
        conn.commit()
        cur.close()
        
        return True
    
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_db_connection(conn)


def handle_get_entry(user_id: str, arguments: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Get a single logbook entry by ID"""
    if arguments is None:
        arguments = {}
    
    entry_id = arguments.get('entryId')
    if not entry_id:
        raise ValueError("entryId is required")
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT * FROM logbook_entries
            WHERE entry_id = %s AND user_id = %s
        """, (entry_id, user_id))
        
        result = cur.fetchone()
        cur.close()
        
        if not result:
            return None
        
        return convert_db_entry_to_graphql(dict(result))
    
    finally:
        if conn:
            return_db_connection(conn)


def handle_list_entries(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """List logbook entries with optional filters"""
    # Ensure arguments is not None
    if arguments is None:
        arguments = {}
    
    filters = arguments.get('filters', {})
    if filters is None:
        filters = {}
    
    limit = arguments.get('limit', 50)
    next_token = arguments.get('nextToken')
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build query with filters
        query = "SELECT * FROM logbook_entries WHERE user_id = %s"
        params = [user_id]
        
        if filters.get('dateFrom'):
            query += " AND date >= %s"
            params.append(filters['dateFrom'])
        
        if filters.get('dateTo'):
            query += " AND date <= %s"
            params.append(filters['dateTo'])
        
        if filters.get('aircraftId'):
            query += " AND aircraft->>'id' = %s"
            params.append(filters['aircraftId'])
        
        if filters.get('flightTypes'):
            query += " AND flight_types && %s"
            params.append(filters['flightTypes'])
        
        if filters.get('status'):
            query += " AND status = %s"
            params.append(filters['status'])
        
        if filters.get('instructorId'):
            query += " AND instructor->>'id' = %s"
            params.append(filters['instructorId'])
        
        query += " ORDER BY date DESC, created_at DESC LIMIT %s"
        params.append(limit + 1)  # Fetch one extra to check for next page
        
        cur.execute(query, params)
        results = [dict(row) for row in cur.fetchall()]
        cur.close()
        
        # Check if there's a next page
        has_next = len(results) > limit
        if has_next:
            results = results[:limit]
            # Generate next token (simple offset-based for now)
            next_token = str(len(results))
        
        entries = [convert_db_entry_to_graphql(entry) for entry in results]
        
        return {
            'items': entries,
            'nextToken': next_token if has_next else None
        }
    
    finally:
        if conn:
            return_db_connection(conn)


def handle_request_signature(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Request signature for a logbook entry (change status to pending-signature)"""
    if arguments is None:
        arguments = {}
    
    entry_id = arguments.get('entryId')
    instructor_id = arguments.get('instructorId')
    
    if not entry_id:
        raise ValueError("entryId is required")
    
    now = datetime.utcnow().isoformat()
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Update entry status to pending-signature
        cur.execute("""
            UPDATE logbook_entries
            SET status = %s, updated_at = %s
            WHERE entry_id = %s AND user_id = %s
            RETURNING *
        """, ('pending-signature', now, entry_id, user_id))
        
        result = cur.fetchone()
        
        if not result:
            raise ValueError(f"Entry {entry_id} not found for user {user_id}")
        
        conn.commit()
        cur.close()
        
        return convert_db_entry_to_graphql(dict(result))
    
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_db_connection(conn)


def handle_sign_entry(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Sign a logbook entry (add signature and change status to signed)"""
    if arguments is None:
        arguments = {}
    
    entry_id = arguments.get('entryId')
    signature_input = arguments.get('signature')
    
    if not entry_id:
        raise ValueError("entryId is required")
    if not signature_input:
        raise ValueError("signature is required")
    
    now = datetime.utcnow().isoformat()
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Update entry with signature and status
        cur.execute("""
            UPDATE logbook_entries
            SET signature = %s, status = %s, updated_at = %s
            WHERE entry_id = %s AND user_id = %s
            RETURNING *
        """, (Json(signature_input), 'signed', now, entry_id, user_id))
        
        result = cur.fetchone()
        
        if not result:
            raise ValueError(f"Entry {entry_id} not found for user {user_id}")
        
        conn.commit()
        cur.close()
        
        return convert_db_entry_to_graphql(dict(result))
    
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_db_connection(conn)


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
        'fullStopLandings': entry.get('full_stop_landings'),
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


# ============================================================================
# CURRENCY CALCULATION FUNCTIONS
# ============================================================================

def get_user_pilot_info(user_id: str) -> Dict[str, Any]:
    """Get user pilot info from DynamoDB Users table"""
    try:
        users_table = dynamodb.Table(f'sky-ready-users-{STAGE}')
        response = users_table.get_item(Key={'userId': user_id})
        return response.get('Item', {})
    except Exception as e:
        print(f"[getCurrency] Error fetching user from DynamoDB: {e}")
        return {}


def handle_get_currency(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate pilot currency - uses ONLY existing tables!"""
    conn = None
    try:
        print(f"[getCurrency] Calculating currency for user: {user_id}")
        
        # Get medical data from DynamoDB (existing Users table)
        user_data = get_user_pilot_info(user_id)
        
        # Get PostgreSQL connection for logbook queries
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Calculate all currencies
        day_currency = calculate_day_currency(cursor, user_id)
        night_currency = calculate_night_currency(cursor, user_id)
        ifr_currency = calculate_ifr_currency(cursor, user_id)
        flight_review = calculate_flight_review(cursor, user_id)
        medical = calculate_medical_certificate(user_data)
        
        return {
            'dayCurrency': day_currency,
            'nightCurrency': night_currency,
            'ifrCurrency': ifr_currency,
            'flightReview': flight_review,
            'medicalCertificate': medical,
        }
    
    except Exception as e:
        print(f"[getCurrency] Error calculating currency: {str(e)}")
        import traceback
        print(f"[getCurrency] Traceback: {traceback.format_exc()}")
        raise
    
    finally:
        if conn:
            return_db_connection(conn)


def calculate_day_currency(cursor, user_id: str) -> Dict[str, Any]:
    """Calculate day currency from logbook entries (14 CFR 61.57(a))"""
    query = """
        SELECT 
            COALESCE(SUM(day_landings), 0) as total_landings,
            MAX(date) as last_landing_date
        FROM logbook_entries
        WHERE user_id = %s 
          AND date >= CURRENT_DATE - INTERVAL '90 days'
          AND day_landings > 0
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    
    total_landings = int(result['total_landings'])
    last_landing_date = result['last_landing_date']
    
    if total_landings >= 3 and last_landing_date:
        # Calculate expiration (90 days from last landing)
        expiration_date = last_landing_date + timedelta(days=90)
        days_remaining = (expiration_date - datetime.now().date()).days
        
        # Determine status
        if days_remaining > 15:
            status = 'CURRENT'
        elif days_remaining > 0:
            status = 'EXPIRING'
        else:
            status = 'EXPIRED'
        
        return {
            'name': 'Day Currency',
            'status': status,
            'daysRemaining': days_remaining if days_remaining > 0 else 0,
            'validUntil': expiration_date.strftime('%b %d, %Y'),
            'details': f'{total_landings} T/O & Ldg in last 90 days',
            'explanation': f'You are current to carry passengers during the day. You have completed {total_landings} takeoffs and landings in the preceding 90 days.',
            'requirements': '• 3 takeoffs and landings within preceding 90 days\n• In same category and class of aircraft\n• Full stop landings required for tailwheel aircraft\n• May use flight simulator with instructor for currency'
        }
    else:
        return {
            'name': 'Day Currency',
            'status': 'EXPIRED',
            'daysRemaining': None,
            'validUntil': None,
            'details': f'Only {total_landings} landings in last 90 days',
            'explanation': 'You are NOT current to carry passengers during the day. You need 3 takeoffs and landings within the preceding 90 days.',
            'requirements': '• 3 takeoffs and landings within preceding 90 days\n• In same category and class of aircraft\n• Full stop landings required for tailwheel aircraft\n• May use flight simulator with instructor for currency'
        }


def calculate_night_currency(cursor, user_id: str) -> Dict[str, Any]:
    """Calculate night currency from logbook entries (14 CFR 61.57(b))"""
    query = """
        SELECT 
            COALESCE(SUM(night_landings), 0) as night_landings,
            COALESCE(SUM(full_stop_landings), 0) as full_stops,
            MAX(date) as last_night_date
        FROM logbook_entries
        WHERE user_id = %s 
          AND date >= CURRENT_DATE - INTERVAL '90 days'
          AND night_landings > 0
          AND full_stop_landings > 0
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    
    night_landings = int(result['night_landings'])
    full_stops = int(result['full_stops'])
    last_night_date = result['last_night_date']
    
    # Night currency requires 3 night landings AND 3 full stops
    if night_landings >= 3 and full_stops >= 3 and last_night_date:
        expiration_date = last_night_date + timedelta(days=90)
        days_remaining = (expiration_date - datetime.now().date()).days
        
        if days_remaining > 15:
            status = 'CURRENT'
        elif days_remaining > 0:
            status = 'EXPIRING'
        else:
            status = 'EXPIRED'
        
        return {
            'name': 'Night Currency',
            'status': status,
            'daysRemaining': days_remaining if days_remaining > 0 else 0,
            'validUntil': expiration_date.strftime('%b %d, %Y'),
            'details': f'{night_landings} night landings (full stop) in last 90 days',
            'explanation': f'You are current to carry passengers at night. You have completed {night_landings} night full-stop landings in the preceding 90 days.',
            'requirements': '• 3 takeoffs and landings to a full stop\n• Within preceding 90 days\n• Between 1 hour after sunset and 1 hour before sunrise\n• In same category and class of aircraft\n• Required to carry passengers at night'
        }
    else:
        return {
            'name': 'Night Currency',
            'status': 'EXPIRED',
            'daysRemaining': None,
            'validUntil': None,
            'details': f'Only {night_landings} night full-stop landings in last 90 days',
            'explanation': 'You are NOT current to carry passengers at night. You need 3 night takeoffs and landings to a full stop within the preceding 90 days.',
            'requirements': '• 3 takeoffs and landings to a full stop\n• Within preceding 90 days\n• Between 1 hour after sunset and 1 hour before sunrise\n• In same category and class of aircraft\n• Required to carry passengers at night'
        }


def calculate_ifr_currency(cursor, user_id: str) -> Dict[str, Any]:
    """Calculate IFR currency from logbook entries (14 CFR 61.57(c))"""
    query = """
        SELECT 
            COALESCE(SUM(approaches), 0) as total_approaches,
            BOOL_OR(holds) as has_holds,
            BOOL_OR(tracking) as has_tracking,
            MAX(date) as last_ifr_date
        FROM logbook_entries
        WHERE user_id = %s 
          AND date >= CURRENT_DATE - INTERVAL '6 months'
          AND approaches > 0
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    
    total_approaches = int(result['total_approaches'])
    has_holds = result['has_holds'] or False
    has_tracking = result['has_tracking'] or False
    last_ifr_date = result['last_ifr_date']
    
    # IFR currency requires 6 approaches AND holds AND tracking
    if total_approaches >= 6 and has_holds and has_tracking and last_ifr_date:
        expiration_date = last_ifr_date + timedelta(days=180)  # 6 months
        days_remaining = (expiration_date - datetime.now().date()).days
        
        if days_remaining > 15:
            status = 'CURRENT'
        elif days_remaining > 0:
            status = 'EXPIRING'
        else:
            status = 'EXPIRED'
        
        return {
            'name': 'IFR Currency',
            'status': status,
            'daysRemaining': days_remaining if days_remaining > 0 else 0,
            'validUntil': expiration_date.strftime('%b %d, %Y'),
            'details': f'{total_approaches} approaches in last 6 months',
            'explanation': f'You are current to file and fly under IFR. You have logged {total_approaches} approaches, holding procedures, and intercepting/tracking courses within the last 6 months.',
            'requirements': '• 6 instrument approaches\n• Holding procedures and tasks\n• Intercepting and tracking courses through navigation aids\n• Within preceding 6 calendar months\n• May use approved flight simulator or training device'
        }
    else:
        missing = []
        if total_approaches < 6:
            missing.append(f'only {total_approaches} approaches')
        if not has_holds:
            missing.append('no holding procedures')
        if not has_tracking:
            missing.append('no tracking/intercepting')
        
        details = f'Not current: {", ".join(missing)}'
        
        return {
            'name': 'IFR Currency',
            'status': 'EXPIRED',
            'daysRemaining': None,
            'validUntil': None,
            'details': details,
            'explanation': 'You are NOT current to file and fly under IFR. You need 6 approaches, holding procedures, and tracking/intercepting within the preceding 6 months.',
            'requirements': '• 6 instrument approaches\n• Holding procedures and tasks\n• Intercepting and tracking courses through navigation aids\n• Within preceding 6 calendar months\n• May use approved flight simulator or training device'
        }


def calculate_flight_review(cursor, user_id: str) -> Dict[str, Any]:
    """Calculate flight review from logbook entries with 'Flight Review' flight type (14 CFR 61.56)"""
    query = """
        SELECT date
        FROM logbook_entries
        WHERE user_id = %s 
          AND 'Flight Review' = ANY(flight_types)
          AND status = 'SIGNED'
        ORDER BY date DESC
        LIMIT 1
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    
    if result and result['date']:
        review_date = result['date']
        expiration_date = review_date + timedelta(days=730)  # 24 months
        days_remaining = (expiration_date - datetime.now().date()).days
        
        if days_remaining > 30:
            status = 'CURRENT'
        elif days_remaining > 0:
            status = 'EXPIRING'
        else:
            status = 'EXPIRED'
        
        return {
            'name': 'Flight Review',
            'status': status,
            'daysRemaining': days_remaining if days_remaining > 0 else 0,
            'validUntil': expiration_date.strftime('%b %d, %Y'),
            'details': f'Last review: {review_date.strftime("%b %d, %Y")}',
            'explanation': f'Your last flight review was completed on {review_date.strftime("%b %d, %Y")}. Flight reviews are required every 24 calendar months.',
            'requirements': '• Minimum 1 hour ground instruction\n• Minimum 1 hour flight instruction\n• Review of Part 91 General Operating Rules\n• Must be endorsed by authorized instructor'
        }
    else:
        return {
            'name': 'Flight Review',
            'status': 'EXPIRED',
            'daysRemaining': None,
            'validUntil': None,
            'details': 'No flight review found',
            'explanation': 'No flight review found in your logbook. Add a logbook entry with type "Flight Review" after your next review.',
            'requirements': '• Required every 24 calendar months\n• Must be logged with instructor signature\n• Log as flight type "Flight Review"'
        }


def calculate_medical_certificate(user_data: Dict) -> Dict[str, Any]:
    """Calculate medical certificate expiration from DynamoDB user data (14 CFR 61.23)"""
    pilot_info = user_data.get('pilotInfo', {})
    medical_date_str = pilot_info.get('medicalCertificateDate')
    medical_class = pilot_info.get('medicalCertificateClass')
    dob_str = pilot_info.get('dateOfBirth')
    
    if not medical_date_str or not medical_class:
        return {
            'name': 'Medical Certificate',
            'status': 'NOT_APPLICABLE',
            'daysRemaining': None,
            'validUntil': None,
            'details': 'Not entered',
            'explanation': 'Medical certificate information not found. Add your medical exam date in Settings.',
            'requirements': '• Class 1: 12 months (6 if over 40 for Part 121)\n• Class 2: 12 months (commercial), 24/60 months (private)\n• Class 3: 24 months (over 40), 60 months (under 40)\n• Add in Settings > Pilot Info'
        }
    
    try:
        medical_date = datetime.fromisoformat(medical_date_str.replace('Z', '+00:00')).date()
    except:
        medical_date = datetime.strptime(medical_date_str, '%Y-%m-%d').date()
    
    # Calculate expiration based on class and age
    age_at_exam = None
    if dob_str:
        try:
            dob = datetime.fromisoformat(dob_str.replace('Z', '+00:00')).date()
            age_at_exam = (medical_date - dob).days // 365
        except:
            pass
    
    # FAA medical expiration rules (simplified - assumes private operations)
    if medical_class == "1":
        expiration_months = 60 if age_at_exam and age_at_exam < 40 else 24
    elif medical_class == "2":
        expiration_months = 60 if age_at_exam and age_at_exam < 40 else 24
    elif medical_class == "3":
        expiration_months = 60 if age_at_exam and age_at_exam < 40 else 24
    else:
        expiration_months = 24  # Default
    
    expiration_date = medical_date + timedelta(days=expiration_months * 30)
    days_remaining = (expiration_date - datetime.now().date()).days
    
    if days_remaining > 30:
        status = 'CURRENT'
    elif days_remaining > 0:
        status = 'EXPIRING'
    else:
        status = 'EXPIRED'
    
    return {
        'name': f'Medical (Class {medical_class})',
        'status': status,
        'daysRemaining': days_remaining if days_remaining > 0 else 0,
        'validUntil': expiration_date.strftime('%b %d, %Y'),
        'details': f'Exam date: {medical_date.strftime("%b %d, %Y")}',
        'explanation': f'Your Class {medical_class} medical certificate is valid for {expiration_months} months for private operations.',
        'requirements': f'• Valid for {expiration_months} months for your age\n• Must be issued by FAA Aviation Medical Examiner (AME)\n• BasicMed may be an alternative for certain operations'
    }
