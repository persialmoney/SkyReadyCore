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

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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
                airport_value = preferences['defaultAirport']
                print(f"[UserUpdate] Updating defaultAirport to: {airport_value}")
                update_expression_parts.append("preferences.defaultAirport = :defaultAirport")
                expression_values[":defaultAirport"] = airport_value
            
            if 'defaultUnits' in preferences and preferences['defaultUnits'] is not None:
                update_expression_parts.append("preferences.defaultUnits = :defaultUnits")
                expression_values[":defaultUnits"] = preferences['defaultUnits']
            
            if 'notificationEnabled' in preferences and preferences['notificationEnabled'] is not None:
                update_expression_parts.append("preferences.notificationEnabled = :notificationEnabled")
                expression_values[":notificationEnabled"] = preferences['notificationEnabled']
            
            if 'criticalAlertThreshold' in preferences and preferences['criticalAlertThreshold'] is not None:
                update_expression_parts.append("preferences.criticalAlertThreshold = :criticalAlertThreshold")
                expression_values[":criticalAlertThreshold"] = preferences['criticalAlertThreshold']
        
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
        
        # Perform the update
        response = users_table.update_item(**update_params)
        
        # Get the updated item
        updated_item = response.get('Attributes', {})
        
        # Log the updated item to verify the save
        print(f"[UserUpdate] DynamoDB update response - Attributes: {json.dumps(updated_item, default=str)}")
        if 'preferences' in updated_item and 'defaultAirport' in updated_item.get('preferences', {}):
            print(f"[UserUpdate] Verified defaultAirport in DynamoDB: {updated_item['preferences']['defaultAirport']}")
        
        # Convert DynamoDB item to GraphQL format
        # Ensure 'id' field exists (GraphQL schema expects it)
        if 'id' not in updated_item:
            updated_item['id'] = updated_item.get('userId', user_id)
        
        # Convert sets to lists for JSON serialization (if any)
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
        
        user_data = convert_item(updated_item)
        
        print(f"Successfully updated user {user_id}")
        print(f"Updated fields: {', '.join(update_expression_parts)}")
        
        return user_data
        
    except Exception as e:
        error_message = f"Error updating user: {str(e)}"
        print(error_message)
        print(f"Event: {json.dumps(event, default=str)}")
        raise Exception(error_message)

