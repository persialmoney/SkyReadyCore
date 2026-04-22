"""
Copilot AI Lambda — SkyReady aviation assistant backend.

Invoked via API Gateway HTTP API (POST /copilot/query).
Authenticated: Cognito JWT validated by API Gateway JWT authorizer.

Context sources (in priority order):
  1. FAA currency + pilot status  (always included, from RDS logbook)
  2. POH context                  (client-provided from on-device RAG)
  3. Weather                      (fetched from AWC API for detected airports)
  4. Logbook summary              (recent entries from RDS)

Pluggable: add new context sources by adding to build_context().
"""
import json
import os
import re
import time
import uuid
import urllib.request
import urllib.error
import boto3
import psycopg
from jose import jwt as jose_jwt
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

# ── AWS clients ────────────────────────────────────────────────────────────────
secrets_client   = boto3.client('secretsmanager')
bedrock_client   = boto3.client('bedrock-runtime', region_name=os.environ.get('BEDROCK_REGION', 'us-east-1'))
dynamodb_client  = boto3.client('dynamodb')

# ── Config ─────────────────────────────────────────────────────────────────────
DB_SECRET_ARN       = os.environ.get('DB_SECRET_ARN')
DB_ENDPOINT         = os.environ.get('DB_ENDPOINT')
DB_NAME             = os.environ.get('DB_NAME', 'skyready')
CHAT_MODEL_ID       = os.environ.get('CHAT_MODEL_ID', 'us.anthropic.claude-sonnet-4-6')
USAGE_TABLE         = os.environ.get('USAGE_TABLE_NAME')
USER_POOL_ID        = os.environ.get('USER_POOL_ID', '')
USER_POOL_CLIENT_ID = os.environ.get('USER_POOL_CLIENT_ID', '')
AWC_BASE_URL        = "https://aviationweather.gov/api/data"

# JWKS cache — populated on first call, persists across warm invocations
_jwks_cache = None


def _get_jwks():
    global _jwks_cache
    if not _jwks_cache:
        region = os.environ.get('AWS_REGION', 'us-east-1')
        url = f"https://cognito-idp.{region}.amazonaws.com/{USER_POOL_ID}/.well-known/jwks.json"
        with urllib.request.urlopen(url, timeout=5) as r:
            _jwks_cache = json.loads(r.read())
    return _jwks_cache


def _verify_token(token: str) -> str:
    """Validate a Cognito JWT and return the user sub. Raises on invalid token."""
    claims = jose_jwt.decode(token, _get_jwks(), algorithms=["RS256"],
                             audience=USER_POOL_CLIENT_ID)
    return claims['sub']

# Bedrock pricing (USD per 1M tokens) — Claude Sonnet 4
_INPUT_TOKEN_PRICE  = 3.00
_OUTPUT_TOKEN_PRICE = 15.00

# ── DB pool ────────────────────────────────────────────────────────────────────
db_pool: Optional[ConnectionPool] = None

SYSTEM_PROMPT = """You are SkyReady Copilot, an AI assistant for general aviation pilots.
You have access to the pilot's logbook data, FAA currency status, current weather, and their aircraft's POH.
Answer questions about FAA currency, flight planning, weather interpretation, aircraft performance,
procedures, and general aviation topics. Be concise, precise, and always prioritize safety.
When currency status is provided, state it clearly. When you don't have enough data, say so."""


# ── DB helpers ──────────────────────────────────────────────────────────────────

def get_db_connection():
    global db_pool
    if db_pool is None:
        response = secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
        secret = json.loads(response['SecretString'])
        conninfo = (
            f"host={DB_ENDPOINT} port=5432 dbname={DB_NAME} "
            f"user={secret['username']} password={secret['password']}"
        )
        db_pool = ConnectionPool(conninfo=conninfo, min_size=1, max_size=5, open=True)
    return db_pool.getconn()


def return_db_connection(conn):
    if db_pool:
        db_pool.putconn(conn)


# ── Context builders ────────────────────────────────────────────────────────────

def build_logbook_summary(user_id: str, conn) -> str:
    """Fetch recent entries and compute totals + currency indicators."""
    cur = conn.cursor(row_factory=dict_row)
    cur.execute("""
        SELECT date, tail_number, route, total_time, pic, night,
               actual_imc, simulated_instrument,
               day_full_stop_landings, night_full_stop_landings,
               approaches, is_flight_review
        FROM logbook_entries
        WHERE user_id = %s AND deleted_at IS NULL
        ORDER BY date DESC
        LIMIT 100
    """, (user_id,))
    entries = cur.fetchall()
    cur.close()

    if not entries:
        return "No logbook entries found."

    total_time = sum(float(e['total_time'] or 0) for e in entries)
    total_night = sum(float(e['night'] or 0) for e in entries)
    total_imc = sum(float(e['actual_imc'] or 0) for e in entries)
    total_sim_inst = sum(float(e['simulated_instrument'] or 0) for e in entries)
    total_approaches = sum(int(e['approaches'] or 0) for e in entries)
    last_flight_date = str(entries[0]['date']) if entries else 'Unknown'
    entry_count = len(entries)

    lines = [
        f"Recent logbook summary ({entry_count} entries shown, most recent {last_flight_date}):",
        f"Total time: {total_time:.1f}h | Night: {total_night:.1f}h | IMC: {total_imc:.1f}h",
        f"Sim instrument: {total_sim_inst:.1f}h | IFR approaches: {total_approaches}",
        "",
        "Last 5 flights:",
    ]
    for e in entries[:5]:
        lines.append(f"  {e['date']} | {e['tail_number']} | {e['route']} | {float(e['total_time'] or 0):.1f}h")

    return "\n".join(lines)


def build_currency_status(user_id: str, conn) -> str:
    """Compute FAA currency indicators from logbook data.

    Windows per FAA regs:
      §61.57(a/b) Day/night currency  — preceding 90 calendar days
      §61.57(c)   IFR currency        — preceding 6 calendar months
      §61.56      Flight review       — preceding 24 calendar months
    """
    cur = conn.cursor(row_factory=dict_row)

    # Single query covering the widest window (24 months); filter in Python per rule.
    # Uses date arithmetic so the comparison is timezone-agnostic (DATE vs INTERVAL).
    cur.execute("""
        SELECT date, day_full_stop_landings, night_full_stop_landings,
               day_touch_and_go_landings, night_touch_and_go_landings,
               actual_imc, simulated_instrument, approaches, is_flight_review
        FROM logbook_entries
        WHERE user_id = %s AND deleted_at IS NULL
          AND date >= CURRENT_DATE - INTERVAL '730 days'
        ORDER BY date DESC
    """, (user_id,))
    all_entries = cur.fetchall()
    cur.close()

    from datetime import date as date_type
    today = date_type.today()

    def days_ago(n: int):
        from datetime import timedelta
        return today - timedelta(days=n)

    cutoff_90d  = days_ago(90)
    cutoff_180d = days_ago(180)
    cutoff_730d = days_ago(730)

    last_90  = [e for e in all_entries if e['date'] >= cutoff_90d]
    last_180 = [e for e in all_entries if e['date'] >= cutoff_180d]
    last_730 = all_entries  # query already bounded to 730 days

    day_landings = sum(
        int(e['day_full_stop_landings'] or 0) + int(e['day_touch_and_go_landings'] or 0)
        for e in last_90
    )
    night_landings = sum(
        int(e['night_full_stop_landings'] or 0) + int(e['night_touch_and_go_landings'] or 0)
        for e in last_90
    )
    # §61.57(c): 6 approaches in preceding 6 calendar months
    ifr_approaches = sum(int(e['approaches'] or 0) for e in last_180)

    day_current   = day_landings >= 3
    night_current = night_landings >= 3
    ifr_current   = ifr_approaches >= 6

    # §61.56: flight review within preceding 24 calendar months
    flight_review_ok = any(e['is_flight_review'] for e in last_730)

    lines = ["FAA Currency Status:"]
    lines.append(f"  Day currency (last 90d): {'CURRENT' if day_current else 'NOT CURRENT'} ({day_landings} landings, need 3)")
    lines.append(f"  Night currency (last 90d): {'CURRENT' if night_current else 'NOT CURRENT'} ({night_landings} landings, need 3)")
    lines.append(f"  IFR currency (last 6mo): {'CURRENT' if ifr_current else 'NOT CURRENT'} ({ifr_approaches} approaches, need 6)")
    lines.append(f"  Flight review (last 24mo): {'CURRENT' if flight_review_ok else 'CHECK RECORDS'}")

    return "\n".join(lines)


def fetch_weather(airport_codes: List[str]) -> str:
    """Fetch METAR + TAF for up to 4 airport codes from AWC."""
    if not airport_codes:
        return ""

    codes = airport_codes[:4]
    ids_param = "%20".join(codes)

    results = []
    for endpoint, label in [("metar", "METAR"), ("taf", "TAF")]:
        url = f"{AWC_BASE_URL}/{endpoint}?ids={ids_param}&format=raw&taf=false"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "SkyReady/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = resp.read().decode('utf-8').strip()
                if data:
                    results.append(f"{label}:\n{data}")
        except Exception:
            pass

    return "\n\n".join(results) if results else ""


def extract_airport_codes(query: str) -> List[str]:
    """Extract ICAO airport codes from the query string."""
    # Match K-prefixed 4-letter US codes and standard ICAO codes
    matches = re.findall(r'\b([A-Z]{4}|K[A-Z]{3})\b', query.upper())
    return list(dict.fromkeys(matches))  # deduplicate, preserve order


def invoke_claude(messages: List[Dict], system_prompt: str):
    """Invoke Claude via Bedrock. Returns (text, input_tokens, output_tokens)."""
    payload = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 2048,
        "system": system_prompt,
        "messages": messages,
    })
    response = bedrock_client.invoke_model(
        modelId=CHAT_MODEL_ID,
        body=payload,
        contentType="application/json",
        accept="application/json",
    )
    body = json.loads(response['body'].read())
    usage = body.get('usage', {})
    content = body.get('content', [])
    text = content[0].get('text', '') if isinstance(content, list) and content else ''
    return text, usage.get('input_tokens', 0), usage.get('output_tokens', 0)


def write_usage(user_id: str, request_id: str, conversation_id: str,
                input_tokens: int, output_tokens: int) -> None:
    """Write a usage record to DynamoDB. Non-fatal — never blocks the response."""
    if not USAGE_TABLE:
        return
    cost = (input_tokens * _INPUT_TOKEN_PRICE + output_tokens * _OUTPUT_TOKEN_PRICE) / 1_000_000
    ttl  = int(time.time()) + 90 * 86400  # 90-day retention
    try:
        # PK=user_id, SK=request_id (unique per request — no collision risk)
        # created_at is a regular attribute projected into the GSI for date-range queries
        dynamodb_client.put_item(
            TableName=USAGE_TABLE,
            Item={
                'user_id':         {'S': user_id},
                'request_id':      {'S': request_id or str(uuid.uuid4())},
                'created_at':      {'S': datetime.now(timezone.utc).isoformat()},
                'conversation_id': {'S': conversation_id or ''},
                'input_tokens':    {'N': str(input_tokens)},
                'output_tokens':   {'N': str(output_tokens)},
                'model_id':        {'S': CHAT_MODEL_ID},
                'cost_usd':        {'N': str(round(cost, 8))},
                'ttl':             {'N': str(ttl)},
            }
        )
    except Exception as e:
        print(f"[CopilotAI] Usage write failed (non-fatal): {e}")


# ── Main handler ────────────────────────────────────────────────────────────────

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    API Gateway HTTP API proxy integration handler.

    Request body (JSON):
      query              string   required
      conversationHistory  [{role, content}]  optional, last N turns
      pohContext         string   optional, from on-device RAG
      airportCodes       [string] optional, explicit airports (supplements auto-detection)
    """
    try:
        # Extract user ID from JWT claims (API Gateway HTTP API JWT authorizer)
        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        jwt_claims = authorizer.get('jwt', {}).get('claims', {})
        user_id = jwt_claims.get('sub')

        if not user_id:
            return _error(401, "Unauthorized")

        body_raw = event.get('body', '{}') or '{}'
        body = json.loads(body_raw)

        query = body.get('query', '').strip()
        if not query:
            return _error(400, "query is required")

        conversation_history: List[Dict] = body.get('conversationHistory', []) or []
        poh_context: str = body.get('pohContext', '') or ''
        explicit_airports: List[str] = body.get('airportCodes', []) or []

        # Detect airports from query + merge explicit list
        detected_airports = extract_airport_codes(query)
        all_airports = list(dict.fromkeys(explicit_airports + detected_airports))

        # ── Build context ───────────────────────────────────────────────────────
        conn = None
        context_parts = []
        context_labels = []

        try:
            conn = get_db_connection()

            # 1. Currency (highest priority)
            currency_text = build_currency_status(user_id, conn)
            context_parts.append(f"### Currency Status\n{currency_text}")
            context_labels.append("currency")

            # 2. POH (client-supplied from on-device RAG)
            if poh_context:
                context_parts.append(f"### Aircraft POH\n{poh_context}")
                context_labels.append("POH")

            # 3. Weather
            if all_airports:
                weather_text = fetch_weather(all_airports)
                if weather_text:
                    context_parts.append(f"### Current Weather ({', '.join(all_airports)})\n{weather_text}")
                    context_labels.append(f"weather at {', '.join(all_airports)}")

            # 4. Logbook summary
            logbook_text = build_logbook_summary(user_id, conn)
            context_parts.append(f"### Logbook Summary\n{logbook_text}")
            context_labels.append("logbook")

            conn.commit()
        finally:
            if conn:
                return_db_connection(conn)

        # ── Build Claude messages ───────────────────────────────────────────────
        system_context = "\n\n".join(context_parts)
        full_system = f"{SYSTEM_PROMPT}\n\n## Pilot Data\n\n{system_context}"

        # Include conversation history for multi-turn
        messages = []
        for turn in conversation_history[-8:]:  # last 8 turns = 4 pairs
            role = turn.get('role', '')
            content = turn.get('content', '')
            if role in ('user', 'assistant') and content:
                messages.append({"role": role, "content": content})

        # Add current user message
        messages.append({"role": "user", "content": query})

        # ── Invoke Claude ───────────────────────────────────────────────────────
        response_text, input_tokens, output_tokens = invoke_claude(messages, full_system)

        # ── Track usage for billing (non-fatal) ────────────────────────────────
        request_id      = event.get('requestContext', {}).get('requestId', '')
        conversation_id = body.get('conversationId', '')
        write_usage(user_id, request_id, conversation_id, input_tokens, output_tokens)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({
                "response": response_text,
                "contextUsed": context_labels,
                "airports": all_airports,
            }),
        }

    except Exception as e:
        import traceback
        print(f"[CopilotAI] ERROR: {e}\n{traceback.format_exc()}")
        return _error(500, "Internal server error")


def streaming_handler(event: Dict[str, Any], context: Any):
    """
    Lambda Function URL streaming entry point (RESPONSE_STREAM invoke mode).
    Yields NDJSON chunks: {"text":"..."}\n per token, then {"done":true,"contextUsed":[...]}\n

    Auth: Cognito JWT in Authorization header (no API Gateway JWT authorizer).
    """
    auth_header = (event.get('headers') or {}).get('authorization', '')
    token = auth_header.removeprefix('Bearer ').strip()
    try:
        user_id = _verify_token(token)
    except Exception:
        yield json.dumps({'error': 'Unauthorized'}).encode() + b'\n'
        return

    body = json.loads(event.get('body') or '{}')
    query = body.get('query', '').strip()
    if not query:
        yield json.dumps({'error': 'query is required'}).encode() + b'\n'
        return

    conn = None
    context_parts: List[str] = []
    context_labels: List[str] = []
    try:
        conn = get_db_connection()

        currency_text = build_currency_status(user_id, conn)
        context_parts.append(f"### Currency Status\n{currency_text}")
        context_labels.append('currency')

        poh_context: str = body.get('pohContext', '') or ''
        if poh_context:
            context_parts.append(f"### Aircraft POH\n{poh_context}")
            context_labels.append('POH')

        detected = extract_airport_codes(query)
        explicit_airports: List[str] = body.get('airportCodes', []) or []
        all_airports = list(dict.fromkeys(explicit_airports + detected))
        if all_airports:
            weather = fetch_weather(all_airports)
            if weather:
                context_parts.append(f"### Current Weather ({', '.join(all_airports)})\n{weather}")
                context_labels.append(f"weather at {', '.join(all_airports)}")

        context_parts.append(f"### Logbook Summary\n{build_logbook_summary(user_id, conn)}")
        context_labels.append('logbook')
        conn.commit()
    finally:
        if conn:
            return_db_connection(conn)

    full_system = f"{SYSTEM_PROMPT}\n\n## Pilot Data\n\n" + "\n\n".join(context_parts)

    messages = []
    for turn in (body.get('conversationHistory') or [])[-8:]:
        role, content = turn.get('role', ''), turn.get('content', '')
        if role in ('user', 'assistant') and content:
            messages.append({'role': role, 'content': content})
    messages.append({'role': 'user', 'content': query})

    payload = json.dumps({
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 2048,
        'system': full_system,
        'messages': messages,
    })
    stream_resp = bedrock_client.invoke_model_with_response_stream(
        modelId=CHAT_MODEL_ID,
        body=payload,
        contentType='application/json',
        accept='application/json',
    )

    input_tokens = output_tokens = 0
    for evt in stream_resp['body']:
        chunk = json.loads(evt['chunk']['bytes'])
        chunk_type = chunk.get('type')

        if chunk_type == 'content_block_delta':
            text = chunk.get('delta', {}).get('text', '')
            if text:
                yield json.dumps({'text': text}).encode() + b'\n'
        elif chunk_type == 'message_start':
            input_tokens = chunk.get('message', {}).get('usage', {}).get('input_tokens', 0)
        elif chunk_type == 'message_delta':
            output_tokens = chunk.get('usage', {}).get('output_tokens', 0)

    yield json.dumps({'done': True, 'contextUsed': context_labels}).encode() + b'\n'

    request_id = (event.get('requestContext') or {}).get('requestId', '')
    write_usage(user_id, request_id, body.get('conversationId', ''), input_tokens, output_tokens)


def _error(status: int, message: str) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"error": message}),
    }
