"""
Cognito Post-Confirmation Lambda Trigger.
Automatically creates user profile in DynamoDB when user confirms their email.
"""
import json
import os
import boto3
from datetime import datetime
from typing import Dict, Any

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Cognito Post-Confirmation trigger handler.
    
    Event structure from Cognito:
    {
        "version": "string",
        "region": "string",
        "userPoolId": "string",
        "userName": "string",
        "triggerSource": "PostConfirmation_ConfirmSignUp",
        "request": {
            "userAttributes": {
                "sub": "uuid",
                "email": "user@example.com",
                "email_verified": "true",
                "name": "User Name"
            }
        },
        "response": {}
    }
    """
    try:
        # Extract user information from Cognito event
        user_attributes = event.get('request', {}).get('userAttributes', {})
        user_id = user_attributes.get('sub')  # Cognito user ID (UUID)
        email = user_attributes.get('email', '')
        name = user_attributes.get('name') or user_attributes.get('given_name') or email.split('@')[0]
        
        if not user_id:
            print("Error: No user ID (sub) found in event")
            return event
        
        # Check if user already exists (idempotency)
        try:
            existing_user = users_table.get_item(
                Key={'userId': user_id}
            )
            if 'Item' in existing_user:
                print(f"User {user_id} already exists in DynamoDB, skipping creation")
                return event
        except Exception as e:
            print(f"Error checking existing user: {str(e)}")
            # Continue with creation even if check fails
        
        # Extract pilot-specific attributes from custom attributes
        # These should be set during sign-up via custom attributes
        pilot_license = user_attributes.get('custom:pilot_license', '')
        certificate_type = user_attributes.get('custom:certificate_type', '')
        aircraft_ratings = user_attributes.get('custom:aircraft_ratings', '')
        
        # Create user profile in DynamoDB with pilot information
        user_item = {
            'userId': user_id,
            'id': user_id,  # GraphQL schema expects 'id' field
            'name': name,
            'email': email,
            'createdAt': datetime.utcnow().isoformat(),
            'updatedAt': datetime.utcnow().isoformat(),
            'pilotInfo': {
                'licenseNumber': pilot_license,
                'certificateType': certificate_type,  # e.g., 'PPL', 'CPL', 'ATP'
                'aircraftRatings': aircraft_ratings.split(',') if aircraft_ratings else []
            },
            'preferences': {
                'defaultUnits': 'imperial',  # Default to imperial for aviation
                'notificationEnabled': True,
                'criticalAlertThreshold': 'moderate'
            }
        }
        
        users_table.put_item(Item=user_item)
        
        print(f"Successfully created user profile for {user_id} ({email})")
        
        return event
        
    except Exception as e:
        print(f"Error creating user profile: {str(e)}")
        print(f"Event: {json.dumps(event)}")
        # Don't fail the Cognito confirmation - return event to allow signup to complete
        return event
