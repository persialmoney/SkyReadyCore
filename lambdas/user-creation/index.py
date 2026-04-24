"""
Cognito Post-Confirmation Lambda Trigger.
Automatically creates user profile in DynamoDB when user confirms their email.
Includes default Personal Minimums profiles (VFR, Night VFR, IFR).
"""
import json
import os
import boto3
import uuid
from datetime import datetime
from typing import Dict, Any

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))


def create_default_minimums_profiles(user_id: str) -> list:
    """
    Create default personal minimums profiles matching the React Native app's DEFAULT_PROFILES.
    
    From: SkyReady/src/types/personalMinimums.ts
    
    Visibility is stored as tenths of statute miles (e.g., 3.0 SM = 30 tenths)
    """
    now = datetime.utcnow().isoformat()
    
    profiles = [
        {
            'id': str(uuid.uuid4()),
            'userId': user_id,
            'name': 'Default VFR',
            'kind': 'VFR',
            'isDefault': True,
            # Permissions
            'nightAllowed': True,
            'ifrAllowed': False,
            'passengersAllowed': True,
            # Recency
            'maxDaysSinceLastFlight': 30,
            # Weather limits
            'minCeilingFt': 2000,
            'minVisibilityTenthsSm': 30,  # 3.0 SM * 10
            'maxWindKt': 20,
            'maxCrosswindKt': 12,
            'maxGustSpreadKt': 10,
            # Comfort thresholds
            'comfortCrosswindKt': 8,
            'comfortGustSpreadKt': 6,
            # Metadata
            'createdAt': now,
            'updatedAt': now,
            'version': 0
        },
        {
            'id': str(uuid.uuid4()),
            'userId': user_id,
            'name': 'Night VFR',
            'kind': 'NIGHT_VFR',
            'isDefault': False,
            # Permissions
            'nightAllowed': True,
            'ifrAllowed': False,
            'passengersAllowed': True,
            # Recency
            'maxDaysSinceLastFlight': 21,
            # Weather limits
            'minCeilingFt': 3000,
            'minVisibilityTenthsSm': 50,  # 5.0 SM * 10
            'maxWindKt': 15,
            'maxCrosswindKt': 10,
            'maxGustSpreadKt': 8,
            # Comfort thresholds
            'comfortCrosswindKt': 7,
            'comfortGustSpreadKt': 5,
            # Metadata
            'createdAt': now,
            'updatedAt': now,
            'version': 0
        },
        {
            'id': str(uuid.uuid4()),
            'userId': user_id,
            'name': 'IFR Conservative',
            'kind': 'IFR',
            'isDefault': False,
            # Permissions
            'nightAllowed': True,
            'ifrAllowed': True,
            'passengersAllowed': True,
            # Recency
            'maxDaysSinceLastFlight': 30,
            # Weather limits
            'minCeilingFt': 1000,
            'minVisibilityTenthsSm': 20,  # 2.0 SM * 10
            'maxWindKt': 20,
            'maxCrosswindKt': 12,
            'maxGustSpreadKt': 10,
            # Comfort thresholds
            'comfortCrosswindKt': 8,
            'comfortGustSpreadKt': 6,
            # Metadata
            'createdAt': now,
            'updatedAt': now,
            'version': 0
        }
    ]
    
    return profiles


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
        
        # Create default personal minimums profiles
        default_profiles = create_default_minimums_profiles(user_id)
        
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
                'criticalAlertThreshold': 'moderate',
                'defaultAirport': 'KSFO',
                'enabledCurrencies': ['flight-review', 'medical', 'general-ASEL'],  # Default enabled currencies
                'createdAt': datetime.utcnow().isoformat(),
                'updatedAt': datetime.utcnow().isoformat()
            },
            'aircraft': [],  # Initialize empty aircraft list for sync
            'personalMinimumsProfiles': default_profiles,  # Pre-populated with VFR, Night VFR, IFR
            'subscription': {
                'plan': 'free',
                'status': 'active',
                'expiresAt': None,
                'rcUserId': user_id,
                'updatedAt': datetime.utcnow().isoformat(),
            }
        }
        
        users_table.put_item(Item=user_item)
        
        print(f"Successfully created user profile for {user_id} ({email}) with {len(default_profiles)} default personal minimums profiles")
        
        return event
        
    except Exception as e:
        print(f"Error creating user profile: {str(e)}")
        print(f"Event: {json.dumps(event)}")
        # Don't fail the Cognito confirmation - return event to allow signup to complete
        return event
