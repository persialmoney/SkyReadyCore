"""
AppSync Lambda Resolver for all DynamoDB data operations.
Handles getUser, getSavedAirports, getAlerts, and all mutations for saved airports and alerts.
"""
import json
import os
import boto3
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional, List

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))
saved_airports_table = dynamodb.Table(os.environ.get('SAVED_AIRPORTS_TABLE', 'sky-ready-saved-airports-dev'))
alerts_table = dynamodb.Table(os.environ.get('ALERTS_TABLE', 'sky-ready-alerts-dev'))


def convert_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively convert DynamoDB types to JSON-serializable types"""
    if item is None:
        return None
    if isinstance(item, dict):
        return {k: convert_item(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convert_item(v) for v in item]
    elif isinstance(item, Decimal):
        # Convert Decimal to float or int
        if item % 1 == 0:
            return int(item)
        return float(item)
    else:
        return item


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AppSync Lambda resolver handler for all data operations.
    
    Event structure from AppSync:
    {
        "arguments": {...},  # Query/mutation arguments
        "identity": {
            "sub": "user-id-uuid"
        },
        "info": {
            "fieldName": "getUser",  # or "saveAirport", etc.
            "parentTypeName": "Query" or "Mutation"
        }
    }
    """
    try:
        # Extract common data
        identity = event.get('identity', {})
        user_id = identity.get('sub')
        arguments = event.get('arguments', {})
        info = event.get('info', {})
        field_name = info.get('fieldName', '')
        
        if not user_id:
            error_msg = "User ID (identity.sub) is required but was missing"
            print(f"[DataOperations] ERROR: {error_msg}")
            print(f"[DataOperations] identity: {json.dumps(identity, default=str)}")
            raise ValueError(error_msg)
        
        # Route to appropriate handler based on field name
        if field_name == "getUser":
            result = handle_get_user(user_id)
            # Ensure we always return a dict or None, never an empty dict or other type
            if result is None:
                return None
            # Ensure result is a dict (should always be the case)
            if not isinstance(result, dict):
                print(f"[DataOperations] ERROR: getUser result is not a dict: {type(result)}")
                return None
            return result
        
        elif field_name == "getSavedAirports":
            return handle_get_saved_airports(user_id)
        
        elif field_name == "getAlerts":
            return handle_get_alerts(user_id)
        
        elif field_name == "saveAirport":
            return handle_save_airport(user_id, arguments)
        
        elif field_name == "removeAirport":
            return handle_remove_airport(user_id, arguments)
        
        elif field_name == "createAlert":
            return handle_create_alert(user_id, arguments)
        
        elif field_name == "updateAlert":
            return handle_update_alert(user_id, arguments)
        
        elif field_name == "deleteAlert":
            return handle_delete_alert(user_id, arguments)
        
        else:
            raise ValueError(f"Unknown field name: {field_name}")
    
    except Exception as e:
        error_message = f"Error in data operation: {str(e)}"
        # Log error with context for root cause analysis
        print(f"[DataOperations] ERROR: {error_message}")
        print(f"[DataOperations] fieldName: {event.get('info', {}).get('fieldName', 'UNKNOWN')}")
        print(f"[DataOperations] userId: {event.get('identity', {}).get('sub', 'UNKNOWN')}")
        print(f"[DataOperations] exception_type: {type(e).__name__}")
        print(f"[DataOperations] arguments: {json.dumps(event.get('arguments', {}), default=str)}")
        # Re-raise to let AppSync handle it properly
        raise Exception(error_message)


def handle_get_user(user_id: str) -> Optional[Dict[str, Any]]:
    """Get user by userId"""
    try:
        response = users_table.get_item(Key={'userId': user_id})
        item = response.get('Item')
        
        if not item:
            return None
        
        # Ensure 'id' field exists (GraphQL schema expects it)
        if 'id' not in item:
            item['id'] = item.get('userId', user_id)
        
        converted_item = convert_item(item)
        
        # Ensure required GraphQL schema fields are never null
        # Schema requires: id: ID! and name: String!
        if not converted_item.get('id'):
            converted_item['id'] = converted_item.get('userId', user_id)
        
        # GraphQL schema requires name: String! (non-nullable)
        # Provide default value if name is null or empty
        if not converted_item.get('name'):
            # Try to use email as fallback, otherwise use empty string
            default_name = converted_item.get('email', '')
            if not default_name:
                default_name = ''  # Empty string is valid for String! (non-nullable)
            converted_item['name'] = default_name
        
        return converted_item
    
    except Exception as e:
        # Log error with context for root cause analysis
        print(f"[DataOperations] ERROR in handle_get_user: {str(e)}")
        print(f"[DataOperations] userId: {user_id}")
        print(f"[DataOperations] exception_type: {type(e).__name__}")
        raise


def handle_get_saved_airports(user_id: str) -> List[Dict[str, Any]]:
    """Get all saved airports for a user"""
    response = saved_airports_table.query(
        KeyConditionExpression='userId = :userId',
        ExpressionAttributeValues={':userId': user_id}
    )
    
    items = response.get('Items', [])
    return [convert_item(item) for item in items]


def handle_get_alerts(user_id: str) -> List[Dict[str, Any]]:
    """Get all alerts for a user"""
    response = alerts_table.query(
        KeyConditionExpression='userId = :userId',
        ExpressionAttributeValues={':userId': user_id}
    )
    
    items = response.get('Items', [])
    return [convert_item(item) for item in items]


def handle_save_airport(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Save an airport to user's saved airports"""
    airport_code = arguments.get('airportCode')
    if not airport_code:
        raise ValueError("airportCode is required")
    
    name = arguments.get('name')
    saved_at = datetime.utcnow().isoformat()
    
    item = {
        'userId': user_id,
        'airportCode': airport_code,
        'savedAt': saved_at
    }
    
    if name:
        item['name'] = name
    
    saved_airports_table.put_item(Item=item)
    
    return convert_item(item)


def handle_remove_airport(user_id: str, arguments: Dict[str, Any]) -> bool:
    """Remove an airport from user's saved airports"""
    airport_code = arguments.get('airportCode')
    if not airport_code:
        raise ValueError("airportCode is required")
    
    saved_airports_table.delete_item(
        Key={
            'userId': user_id,
            'airportCode': airport_code
        }
    )
    
    return True


def handle_create_alert(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new alert"""
    input_data = arguments.get('input', {})
    
    if not input_data:
        raise ValueError("input is required")
    
    alert_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    
    item = {
        'userId': user_id,
        'alertId': alert_id,
        'alertType': input_data['alertType'],
        'condition': input_data['condition'],
        'enabled': input_data['enabled'],
        'createdAt': now,
        'updatedAt': now
    }
    
    # Optional fields
    if 'airportCode' in input_data and input_data['airportCode'] is not None:
        item['airportCode'] = input_data['airportCode']
    
    if 'threshold' in input_data and input_data['threshold'] is not None:
        item['threshold'] = input_data['threshold']
    
    alerts_table.put_item(Item=item)
    
    return convert_item(item)


def handle_update_alert(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Update an existing alert"""
    alert_id = arguments.get('alertId')
    if not alert_id:
        raise ValueError("alertId is required")
    
    input_data = arguments.get('input', {})
    if not input_data:
        raise ValueError("input is required")
    
    # Build update expression
    update_expression_parts = []
    expression_values = {}
    expression_names = {}
    
    # Always update updatedAt
    update_expression_parts.append("updatedAt = :updatedAt")
    expression_values[":updatedAt"] = datetime.utcnow().isoformat()
    
    # Update required fields (always present in AlertInput)
    update_expression_parts.append("alertType = :alertType")
    expression_values[":alertType"] = input_data['alertType']
    
    update_expression_parts.append("#condition = :condition")
    expression_names["#condition"] = "condition"
    expression_values[":condition"] = input_data['condition']
    
    update_expression_parts.append("enabled = :enabled")
    expression_values[":enabled"] = input_data['enabled']
    
    # Update optional fields (may be None to clear them)
    if 'airportCode' in input_data:
        update_expression_parts.append("airportCode = :airportCode")
        expression_values[":airportCode"] = input_data['airportCode']
    
    if 'threshold' in input_data:
        update_expression_parts.append("threshold = :threshold")
        expression_values[":threshold"] = input_data['threshold']
    
    if not update_expression_parts:
        raise ValueError("No fields to update")
    
    update_expression = "SET " + ", ".join(update_expression_parts)
    
    # Prepare update parameters
    update_params = {
        'Key': {
            'userId': user_id,
            'alertId': alert_id
        },
        'UpdateExpression': update_expression,
        'ExpressionAttributeValues': expression_values,
        'ReturnValues': 'ALL_NEW'
    }
    
    if expression_names:
        update_params['ExpressionAttributeNames'] = expression_names
    
    response = alerts_table.update_item(**update_params)
    updated_item = response.get('Attributes', {})
    
    if not updated_item:
        raise ValueError(f"Alert {alert_id} not found for user {user_id}")
    
    return convert_item(updated_item)


def handle_delete_alert(user_id: str, arguments: Dict[str, Any]) -> bool:
    """Delete an alert"""
    alert_id = arguments.get('alertId')
    if not alert_id:
        raise ValueError("alertId is required")
    
    alerts_table.delete_item(
        Key={
            'userId': user_id,
            'alertId': alert_id
        }
    )
    
    return True

