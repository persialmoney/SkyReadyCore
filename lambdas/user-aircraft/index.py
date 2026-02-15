"""
AppSync Lambda Resolver for user aircraft management.
Handles CRUD operations for user's personal aircraft library.
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
user_aircraft_table = dynamodb.Table(os.environ.get('USER_AIRCRAFT_TABLE', 'sky-ready-user-aircraft-dev'))


def convert_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively convert DynamoDB types to JSON-serializable types"""
    if item is None:
        return None
    if isinstance(item, dict):
        return {k: convert_item(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convert_item(v) for v in item]
    elif isinstance(item, Decimal):
        if item % 1 == 0:
            return int(item)
        return float(item)
    else:
        return item


def handler(event: Dict[str, Any], context: Any) -> Any:
    """
    AppSync Lambda resolver handler for user aircraft operations
    
    Event structure from AppSync:
    {
        "arguments": {...},
        "identity": {
            "sub": "user-id-uuid"
        },
        "info": {
            "fieldName": "getUserAircraft" | "saveUserAircraft" | "deleteUserAircraft"
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
            print(f"[UserAircraft] ERROR: {error_msg}")
            raise ValueError(error_msg)
        
        print(f"[UserAircraft] {field_name} for user: {user_id}")
        
        # Route to appropriate handler
        if field_name == "getUserAircraft":
            return handle_get_user_aircraft(user_id)
        elif field_name == "saveUserAircraft":
            return handle_save_user_aircraft(user_id, arguments)
        elif field_name == "deleteUserAircraft":
            return handle_delete_user_aircraft(user_id, arguments)
        else:
            raise ValueError(f"Unknown field name: {field_name}")
    
    except Exception as e:
        error_message = f"Error in user aircraft operation: {str(e)}"
        print(f"[UserAircraft] ERROR: {error_message}")
        print(f"[UserAircraft] fieldName: {event.get('info', {}).get('fieldName', 'UNKNOWN')}")
        print(f"[UserAircraft] userId: {event.get('identity', {}).get('sub', 'UNKNOWN')}")
        raise Exception(error_message)


def handle_get_user_aircraft(user_id: str) -> List[Dict[str, Any]]:
    """Get all aircraft for a user"""
    try:
        response = user_aircraft_table.query(
            KeyConditionExpression='userId = :userId',
            ExpressionAttributeValues={':userId': user_id}
        )
        
        items = response.get('Items', [])
        print(f"[UserAircraft] Found {len(items)} aircraft for user: {user_id}")
        
        return [convert_item(item) for item in items]
        
    except Exception as e:
        print(f"[UserAircraft] Error getting aircraft: {str(e)}")
        raise


def handle_save_user_aircraft(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create or update user aircraft
    
    If aircraftId is provided: UPDATE existing
    If aircraftId is NOT provided: CREATE new with generated UUID
    """
    try:
        input_data = arguments.get('input', {})
        
        if not input_data:
            raise ValueError("input is required")
        
        # Validate required fields
        if not input_data.get('tailNumber'):
            raise ValueError("tailNumber is required")
        
        # Get or generate aircraft ID
        aircraft_id = input_data.get('aircraftId')
        is_update = bool(aircraft_id)
        
        if not aircraft_id:
            aircraft_id = str(uuid.uuid4())
            print(f"[UserAircraft] Creating new aircraft: {aircraft_id}")
        else:
            print(f"[UserAircraft] Updating aircraft: {aircraft_id}")
        
        now = datetime.utcnow().isoformat()
        
        # Build item
        item = {
            'userId': user_id,
            'aircraftId': aircraft_id,
            'tailNumber': input_data['tailNumber'],
            'make': input_data.get('make', ''),
            'model': input_data.get('model', ''),
            'category': input_data.get('category', ''),
            'class': input_data.get('class', ''),
            'complex': input_data.get('complex', False),
            'highPerformance': input_data.get('highPerformance', False),
            'notes': input_data.get('notes', ''),
            'updatedAt': now,
        }
        
        # Add createdAt only for new records
        if not is_update:
            item['createdAt'] = now
        else:
            # For updates, fetch existing createdAt
            try:
                existing = user_aircraft_table.get_item(
                    Key={'userId': user_id, 'aircraftId': aircraft_id}
                )
                if 'Item' in existing:
                    item['createdAt'] = existing['Item'].get('createdAt', now)
                else:
                    item['createdAt'] = now
            except Exception:
                item['createdAt'] = now
        
        # Save to DynamoDB
        user_aircraft_table.put_item(Item=item)
        
        print(f"[UserAircraft] Saved aircraft: {aircraft_id} ({item['tailNumber']})")
        
        return convert_item(item)
        
    except Exception as e:
        print(f"[UserAircraft] Error saving aircraft: {str(e)}")
        raise


def handle_delete_user_aircraft(user_id: str, arguments: Dict[str, Any]) -> bool:
    """
    Permanently delete user aircraft (hard delete)
    """
    try:
        aircraft_id = arguments.get('aircraftId')
        
        if not aircraft_id:
            raise ValueError("aircraftId is required")
        
        # Hard delete from DynamoDB
        user_aircraft_table.delete_item(
            Key={
                'userId': user_id,
                'aircraftId': aircraft_id
            }
        )
        
        print(f"[UserAircraft] Deleted aircraft: {aircraft_id}")
        
        return True
        
    except Exception as e:
        print(f"[UserAircraft] Error deleting aircraft: {str(e)}")
        raise
