"""
AppSync Lambda Resolver for updating user information.
Handles conditional updates to user profile and preferences.
"""
import json
import os
import boto3
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional

# Lazy initialization - DynamoDB resources created on first use to reduce cold start time
_dynamodb_resource = None
_users_table = None


def get_users_table():
    """Get DynamoDB table reference with lazy initialization."""
    global _users_table, _dynamodb_resource
    
    if _users_table is None:
        # Use resource for table operations (simpler API)
        # Initialize only when needed to reduce cold start time
        _dynamodb_resource = boto3.resource('dynamodb')
        _users_table = _dynamodb_resource.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))
    
    return _users_table


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AppSync Lambda resolver handler for user mutations.
    
    Handles:
    - updateUser: Update user profile/preferences
    - addUserAircraft: Add aircraft to user's aircraft array
    - updateUserAircraft: Update notes for an aircraft
    - removeUserAircraft: Remove aircraft from user's aircraft array
    
    Event structure from AppSync:
    {
        "arguments": {
            "input": {...} or "tailNumber": "...", "notes": "..."
        },
        "identity": {
            "sub": "user-id-uuid"
        },
        "info": {
            "fieldName": "updateUser" | "addUserAircraft" | "updateUserAircraft" | "removeUserAircraft",
            "parentTypeName": "Mutation"
        }
    }
    """
    try:
        # Extract user ID from Cognito identity
        identity = event.get('identity', {})
        user_id = identity.get('sub')
        
        if not user_id:
            raise ValueError("User ID (identity.sub) is required")
        
        # Get field name to determine operation
        info = event.get('info', {})
        field_name = info.get('fieldName')
        
        # Route to appropriate handler
        if field_name == 'updateUser':
            return update_user(user_id, event)
        elif field_name == 'addUserAircraft':
            return add_user_aircraft(user_id, event)
        elif field_name == 'updateUserAircraft':
            return update_user_aircraft(user_id, event)
        elif field_name == 'removeUserAircraft':
            return remove_user_aircraft(user_id, event)
        else:
            raise ValueError(f"Unknown field name: {field_name}")
    
    except Exception as e:
        error_message = f"Error in user mutation: {str(e)}"
        print(f"[UserMutation] ERROR: {error_message}")
        print(f"[UserMutation] userId: {event.get('identity', {}).get('sub', 'UNKNOWN')}")
        print(f"[UserMutation] fieldName: {event.get('info', {}).get('fieldName', 'UNKNOWN')}")
        print(f"[UserMutation] exception_type: {type(e).__name__}")
        raise Exception(error_message)


def convert_item(item):
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


def update_user(user_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update user profile and preferences
    
    Args:
        user_id: User's Cognito sub ID
        event: AppSync event with input data
    
    Returns:
        Updated user object
    """
    """
    AppSync Lambda resolver handler for updateUser mutation.
    
    Event structure from AppSync:
    {
        "arguments": {
            "input": {
                "name": "string",
                "email": "string",
                "preferences": {
                    "defaultAirport": "string",
                    "defaultUnits": "string",
                    "notificationEnabled": boolean,
                    "criticalAlertThreshold": "string"
                }
            }
        },
        "identity": {
            "sub": "user-id-uuid"
        },
        "info": {
            "fieldName": "updateUser",
            "parentTypeName": "Mutation"
        }
    }
    """
    try:
        # Extract user ID from Cognito identity
        identity = event.get('identity', {})
        user_id = identity.get('sub')
        
        if not user_id:
            raise ValueError("User ID (identity.sub) is required")
    """
    Update user profile and preferences
    
    Args:
        user_id: User's Cognito sub ID
        event: AppSync event with input data
    
    Returns:
        Updated user object
    """
    # Extract input data
    arguments = event.get('arguments', {})
    input_data = arguments.get('input', {})
    
    if not input_data:
        raise ValueError("Input data is required")
    
    # Build update expression parts
    update_expression_parts = []
    expression_names = {}
    expression_values = {}
    
    # Always update updatedAt
    update_expression_parts.append("updatedAt = :updatedAt")
    expression_values[":updatedAt"] = datetime.utcnow().isoformat()
    
    # Conditionally update name if provided
    if 'name' in input_data and input_data['name'] is not None:
        update_expression_parts.append("#name = :name")
        expression_names["#name"] = "name"
        expression_values[":name"] = input_data['name']
    
    # Conditionally update email if provided
    if 'email' in input_data and input_data['email'] is not None:
        update_expression_parts.append("email = :email")
        expression_values[":email"] = input_data['email']
    
    # Conditionally update preferences if provided
    preferences = input_data.get('preferences')
    if preferences is not None:
        if 'defaultAirport' in preferences and preferences['defaultAirport'] is not None:
            update_expression_parts.append("preferences.defaultAirport = :defaultAirport")
            expression_values[":defaultAirport"] = preferences['defaultAirport']
        
        if 'defaultUnits' in preferences and preferences['defaultUnits'] is not None:
            update_expression_parts.append("preferences.defaultUnits = :defaultUnits")
            expression_values[":defaultUnits"] = preferences['defaultUnits']
        
        if 'notificationEnabled' in preferences and preferences['notificationEnabled'] is not None:
            update_expression_parts.append("preferences.notificationEnabled = :notificationEnabled")
            expression_values[":notificationEnabled"] = preferences['notificationEnabled']
        
        if 'criticalAlertThreshold' in preferences and preferences['criticalAlertThreshold'] is not None:
            update_expression_parts.append("preferences.criticalAlertThreshold = :criticalAlertThreshold")
            expression_values[":criticalAlertThreshold"] = preferences['criticalAlertThreshold']
    
    # Conditionally update aircraft array if provided (bulk update)
    aircraft = input_data.get('aircraft')
    if aircraft is not None:
        update_expression_parts.append("aircraft = :aircraft")
        expression_values[":aircraft"] = aircraft
    
    # Build the update expression string
    if not update_expression_parts:
        raise ValueError("No fields to update")
    
    update_expression = "SET " + ", ".join(update_expression_parts)
    
    # Prepare update parameters
    update_params = {
        'Key': {'userId': user_id},
        'UpdateExpression': update_expression,
        'ExpressionAttributeValues': expression_values,
        'ReturnValues': 'ALL_NEW'
    }
    
    # Add expression names if we have any (for reserved words like 'name')
    if expression_names:
        update_params['ExpressionAttributeNames'] = expression_names
    
    # Get table reference (lazy initialization reduces cold start time)
    users_table = get_users_table()
    
    # Perform the update
    response = users_table.update_item(**update_params)
    
    # Get the updated item
    updated_item = response.get('Attributes', {})
    
    # Convert DynamoDB item to GraphQL format
    # Ensure 'id' field exists (GraphQL schema expects it)
    if 'id' not in updated_item:
        updated_item['id'] = updated_item.get('userId', user_id)
    
    user_data = convert_item(updated_item)
    
    return user_data


def add_user_aircraft(user_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add aircraft to user's aircraft array
    
    Args:
        user_id: User's Cognito sub ID
        event: AppSync event with tailNumber and notes
    
    Returns:
        Updated user object
    """
    arguments = event.get('arguments', {})
    tail_number = arguments.get('tailNumber')
    notes = arguments.get('notes', '')
    
    if not tail_number:
        raise ValueError("tailNumber is required")
    
    # Normalize tail number
    tail_number = tail_number.upper().strip()
    
    # Create aircraft ref object
    aircraft_ref = {
        'tailNumber': tail_number,
        'notes': notes,
        'addedAt': datetime.utcnow().isoformat()
    }
    
    # Get table reference
    users_table = get_users_table()
    
    # First, check if aircraft already exists
    user_response = users_table.get_item(Key={'userId': user_id})
    user_item = user_response.get('Item', {})
    existing_aircraft = user_item.get('aircraft', [])
    
    # Check for duplicate
    for aircraft in existing_aircraft:
        if aircraft.get('tailNumber') == tail_number:
            raise ValueError(f"Aircraft {tail_number} already in your list")
    
    # Append to aircraft list
    response = users_table.update_item(
        Key={'userId': user_id},
        UpdateExpression="SET aircraft = list_append(if_not_exists(aircraft, :empty_list), :new_aircraft), updatedAt = :updatedAt",
        ExpressionAttributeValues={
            ':new_aircraft': [aircraft_ref],
            ':empty_list': [],
            ':updatedAt': datetime.utcnow().isoformat()
        },
        ReturnValues='ALL_NEW'
    )
    
    # Get the updated item
    updated_item = response.get('Attributes', {})
    if 'id' not in updated_item:
        updated_item['id'] = updated_item.get('userId', user_id)
    
    return convert_item(updated_item)


def update_user_aircraft(user_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update notes for an aircraft in user's aircraft array
    
    Args:
        user_id: User's Cognito sub ID
        event: AppSync event with tailNumber and notes
    
    Returns:
        Updated user object
    """
    arguments = event.get('arguments', {})
    tail_number = arguments.get('tailNumber')
    notes = arguments.get('notes', '')
    
    if not tail_number:
        raise ValueError("tailNumber is required")
    
    # Normalize tail number
    tail_number = tail_number.upper().strip()
    
    # Get table reference
    users_table = get_users_table()
    
    # Get current user item
    user_response = users_table.get_item(Key={'userId': user_id})
    user_item = user_response.get('Item', {})
    aircraft_list = user_item.get('aircraft', [])
    
    # Find and update the aircraft
    found = False
    for i, aircraft in enumerate(aircraft_list):
        if aircraft.get('tailNumber') == tail_number:
            aircraft_list[i]['notes'] = notes
            found = True
            break
    
    if not found:
        raise ValueError(f"Aircraft {tail_number} not found in your list")
    
    # Update the entire aircraft list
    response = users_table.update_item(
        Key={'userId': user_id},
        UpdateExpression="SET aircraft = :aircraft, updatedAt = :updatedAt",
        ExpressionAttributeValues={
            ':aircraft': aircraft_list,
            ':updatedAt': datetime.utcnow().isoformat()
        },
        ReturnValues='ALL_NEW'
    )
    
    # Get the updated item
    updated_item = response.get('Attributes', {})
    if 'id' not in updated_item:
        updated_item['id'] = updated_item.get('userId', user_id)
    
    return convert_item(updated_item)


def remove_user_aircraft(user_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove aircraft from user's aircraft array
    
    Args:
        user_id: User's Cognito sub ID
        event: AppSync event with tailNumber
    
    Returns:
        Updated user object
    """
    arguments = event.get('arguments', {})
    tail_number = arguments.get('tailNumber')
    
    if not tail_number:
        raise ValueError("tailNumber is required")
    
    # Normalize tail number
    tail_number = tail_number.upper().strip()
    
    # Get table reference
    users_table = get_users_table()
    
    # Get current user item
    user_response = users_table.get_item(Key={'userId': user_id})
    user_item = user_response.get('Item', {})
    aircraft_list = user_item.get('aircraft', [])
    
    # Filter out the aircraft
    new_aircraft_list = [a for a in aircraft_list if a.get('tailNumber') != tail_number]
    
    if len(new_aircraft_list) == len(aircraft_list):
        raise ValueError(f"Aircraft {tail_number} not found in your list")
    
    # Update the aircraft list
    response = users_table.update_item(
        Key={'userId': user_id},
        UpdateExpression="SET aircraft = :aircraft, updatedAt = :updatedAt",
        ExpressionAttributeValues={
            ':aircraft': new_aircraft_list,
            ':updatedAt': datetime.utcnow().isoformat()
        },
        ReturnValues='ALL_NEW'
    )
    
    # Get the updated item
    updated_item = response.get('Attributes', {})
    if 'id' not in updated_item:
        updated_item['id'] = updated_item.get('userId', user_id)
    
    return convert_item(updated_item)
