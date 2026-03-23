"""
AppSync Lambda Resolver for updating user information.
Handles conditional updates to user profile and preferences.
Note: Aircraft management is now handled via sync protocol.
"""
import json
import os
import boto3
import secrets
import string
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional

# Lazy initialization - DynamoDB resources created on first use to reduce cold start time
_dynamodb_resource = None
_users_table = None


def generate_invite_code() -> str:
    """
    Generate a unique 8-character alphanumeric invite code (uppercase).
    Uses cryptographically secure random number generator.
    Format: ABCD1234
    """
    alphabet = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(8))


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
    
    Event structure from AppSync:
    {
        "arguments": {
            "input": {...}
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
        
        # Get field name to determine operation
        info = event.get('info', {})
        field_name = info.get('fieldName')
        
        # Route to appropriate handler
        if field_name == 'updateUser':
            return update_user(user_id, event)
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
        
        if 'enabledCurrencies' in preferences and preferences['enabledCurrencies'] is not None:
            update_expression_parts.append("preferences.enabledCurrencies = :enabledCurrencies")
            expression_values[":enabledCurrencies"] = preferences['enabledCurrencies']
        
        if 'onboardingComplete' in preferences and preferences['onboardingComplete'] is not None:
            update_expression_parts.append("preferences.onboardingComplete = :onboardingComplete")
            expression_values[":onboardingComplete"] = preferences['onboardingComplete']
    
    # Conditionally update pilotInfo if provided
    pilot_info = input_data.get('pilotInfo')
    if pilot_info is not None:
        # Student/pilot fields
        if 'licenseNumber' in pilot_info and pilot_info['licenseNumber'] is not None:
            update_expression_parts.append("pilotInfo.licenseNumber = :licenseNumber")
            expression_values[":licenseNumber"] = pilot_info['licenseNumber']
        
        if 'certificateType' in pilot_info and pilot_info['certificateType'] is not None:
            update_expression_parts.append("pilotInfo.certificateType = :certificateType")
            expression_values[":certificateType"] = pilot_info['certificateType']
        
        if 'aircraftRatings' in pilot_info and pilot_info['aircraftRatings'] is not None:
            update_expression_parts.append("pilotInfo.aircraftRatings = :aircraftRatings")
            expression_values[":aircraftRatings"] = pilot_info['aircraftRatings']
        
        if 'medicalCertificateDate' in pilot_info and pilot_info['medicalCertificateDate'] is not None:
            update_expression_parts.append("pilotInfo.medicalCertificateDate = :medicalCertificateDate")
            expression_values[":medicalCertificateDate"] = pilot_info['medicalCertificateDate']
        
        if 'medicalCertificateClass' in pilot_info and pilot_info['medicalCertificateClass'] is not None:
            update_expression_parts.append("pilotInfo.medicalCertificateClass = :medicalCertificateClass")
            expression_values[":medicalCertificateClass"] = pilot_info['medicalCertificateClass']
        
        if 'dateOfBirth' in pilot_info and pilot_info['dateOfBirth'] is not None:
            update_expression_parts.append("pilotInfo.dateOfBirth = :dateOfBirth")
            expression_values[":dateOfBirth"] = pilot_info['dateOfBirth']
        
        # Instructor fields
        if 'instructorCertificates' in pilot_info and pilot_info['instructorCertificates'] is not None:
            update_expression_parts.append("pilotInfo.instructorCertificates = :instructorCertificates")
            expression_values[":instructorCertificates"] = pilot_info['instructorCertificates']
        
        if 'instructorCertificateNumber' in pilot_info and pilot_info['instructorCertificateNumber'] is not None:
            update_expression_parts.append("pilotInfo.instructorCertificateNumber = :instructorCertificateNumber")
            expression_values[":instructorCertificateNumber"] = pilot_info['instructorCertificateNumber']
        
        if 'instructorCertificateExpiration' in pilot_info and pilot_info['instructorCertificateExpiration'] is not None:
            update_expression_parts.append("pilotInfo.instructorCertificateExpiration = :instructorCertificateExpiration")
            expression_values[":instructorCertificateExpiration"] = pilot_info['instructorCertificateExpiration']
        
        # Auto-generate inviteCode if instructor certificates are set and inviteCode doesn't exist
        # Need to check if inviteCode already exists first
        users_table = get_users_table()
        user_response = users_table.get_item(Key={'userId': user_id})
        existing_user = user_response.get('Item', {})
        existing_pilot_info = existing_user.get('pilotInfo', {})
        existing_invite_code = existing_pilot_info.get('inviteCode')
        
        # If user is setting instructor certificates and doesn't have an invite code, generate one
        if 'instructorCertificates' in pilot_info and pilot_info['instructorCertificates'] and not existing_invite_code:
            new_invite_code = generate_invite_code()
            update_expression_parts.append("pilotInfo.inviteCode = :inviteCode")
            expression_values[":inviteCode"] = new_invite_code
    
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

