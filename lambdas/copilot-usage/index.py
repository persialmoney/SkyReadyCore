"""
Copilot Usage Lambda — AppSync resolver for getCopilotUsage.

Returns aggregated token usage and cost for the authenticated user
over a given date range. Used for usage dashboards and future billing.
"""
import os
import boto3
from typing import Dict, Any

dynamodb_client = boto3.client('dynamodb')
USAGE_TABLE = os.environ.get('USAGE_TABLE_NAME', '')


def handler(event: Dict[str, Any], context: Any) -> Dict:
    # AppSync Cognito identity — user_id is the Cognito sub claim
    identity = event.get('identity') or {}
    user_id = identity.get('sub') or identity.get('username')

    if not user_id:
        raise Exception('Unauthorized')

    args       = event.get('arguments', {})
    start_date = args.get('startDate', '')  # ISO-8601 date, e.g. "2026-01-01"
    end_date   = args.get('endDate', '')    # ISO-8601 date, e.g. "2026-04-30"

    if not start_date or not end_date:
        raise Exception('startDate and endDate are required')

    if not USAGE_TABLE:
        raise Exception('USAGE_TABLE_NAME not configured')

    # Pad end_date to end-of-day so the BETWEEN covers the full day
    end_ts = end_date if end_date.endswith('Z') else end_date + 'T23:59:59Z'

    # Query the GSI — scoped to this user, filtered by date range
    paginator  = dynamodb_client.get_paginator('query')
    page_iter  = paginator.paginate(
        TableName=USAGE_TABLE,
        IndexName='user_id-created_at-index',
        KeyConditionExpression='user_id = :uid AND created_at BETWEEN :s AND :e',
        ExpressionAttributeValues={
            ':uid': {'S': user_id},
            ':s':   {'S': start_date},
            ':e':   {'S': end_ts},
        },
    )

    total_input  = 0
    total_output = 0
    total_cost   = 0.0
    count        = 0

    for page in page_iter:
        for item in page.get('Items', []):
            total_input  += int(item.get('input_tokens',  {}).get('N', 0))
            total_output += int(item.get('output_tokens', {}).get('N', 0))
            total_cost   += float(item.get('cost_usd',    {}).get('N', 0))
            count        += 1

    return {
        'totalInputTokens':  total_input,
        'totalOutputTokens': total_output,
        'totalCostUsd':      round(total_cost, 6),
        'requestCount':      count,
        'periodStart':       start_date,
        'periodEnd':         end_date,
    }
