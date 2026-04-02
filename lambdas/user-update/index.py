"""
AppSync Lambda Resolver for updating user information.
Handles conditional updates to user profile and preferences.
Note: Aircraft management is now handled via sync protocol.
"""
import os
import boto3
import secrets
import string
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional

import email_templates

SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "noreply@skyready.app")
STAGE = os.environ.get("STAGE", "dev")

# Password-reset deep link — uses the universal link; works on both iOS and Android.
RESET_URL = "https://skyready.app/reset-password"

# Lazy initialization - DynamoDB resources created on first use to reduce cold start time
_dynamodb_resource = None
_users_table = None
_ses_client = None


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


def get_ses():
    """Get SES client with lazy initialization."""
    global _ses_client
    if _ses_client is None:
        _ses_client = boto3.client("ses")
    return _ses_client


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

    # Read the current user record upfront so we can:
    #   1. Detect whether the email is actually changing (for the security notice).
    #   2. Avoid a second get_item later when generating an instructor invite code.
    users_table = get_users_table()
    user_response = users_table.get_item(Key={'userId': user_id})
    existing_user = user_response.get('Item', {})
    existing_email = existing_user.get('email', '')
    
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
    new_email_value = None
    if 'email' in input_data and input_data['email'] is not None:
        new_email_value = input_data['email']
        update_expression_parts.append("email = :email")
        expression_values[":email"] = new_email_value
    
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

        if 'flyingStyles' in preferences and preferences['flyingStyles'] is not None:
            update_expression_parts.append("preferences.flyingStyles = :flyingStyles")
            expression_values[":flyingStyles"] = preferences['flyingStyles']
    
    # Conditionally update pilotInfo if provided.
    # Logbook entries (Postgres) are not modified here — signed rows keep snapshots/signatures.
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
            # Write the list unconditionally when the field is explicitly provided —
            # an empty [] means the caller is intentionally clearing CFI status.
            update_expression_parts.append("pilotInfo.instructorCertificates = :instructorCertificates")
            expression_values[":instructorCertificates"] = pilot_info['instructorCertificates']
        
        # For instructor fields that can be explicitly nulled (e.g. when disabling CFI role),
        # write the value whenever the key is present — null is a valid "clear" signal.
        if 'instructorCertificateNumber' in pilot_info:
            update_expression_parts.append("pilotInfo.instructorCertificateNumber = :instructorCertificateNumber")
            expression_values[":instructorCertificateNumber"] = pilot_info['instructorCertificateNumber']

        if 'instructorCertificateExpiration' in pilot_info:
            update_expression_parts.append("pilotInfo.instructorCertificateExpiration = :instructorCertificateExpiration")
            expression_values[":instructorCertificateExpiration"] = pilot_info['instructorCertificateExpiration']

        if 'instructorVerificationStatus' in pilot_info:
            update_expression_parts.append("pilotInfo.instructorVerificationStatus = :instructorVerificationStatus")
            expression_values[":instructorVerificationStatus"] = pilot_info['instructorVerificationStatus']

        if 'instructorVerifiedAt' in pilot_info:
            update_expression_parts.append("pilotInfo.instructorVerifiedAt = :instructorVerifiedAt")
            expression_values[":instructorVerifiedAt"] = pilot_info['instructorVerifiedAt']

        if 'instructorSnapshotDate' in pilot_info:
            update_expression_parts.append("pilotInfo.instructorSnapshotDate = :instructorSnapshotDate")
            expression_values[":instructorSnapshotDate"] = pilot_info['instructorSnapshotDate']

        # Pilot certificate FAA verification fields (written by pilot-verify Lambda)
        if 'pilotVerificationStatus' in pilot_info and pilot_info['pilotVerificationStatus'] is not None:
            update_expression_parts.append("pilotInfo.pilotVerificationStatus = :pilotVerificationStatus")
            expression_values[":pilotVerificationStatus"] = pilot_info['pilotVerificationStatus']

        if 'pilotVerifiedAt' in pilot_info and pilot_info['pilotVerifiedAt'] is not None:
            update_expression_parts.append("pilotInfo.pilotVerifiedAt = :pilotVerifiedAt")
            expression_values[":pilotVerifiedAt"] = pilot_info['pilotVerifiedAt']

        if 'pilotSnapshotDate' in pilot_info and pilot_info['pilotSnapshotDate'] is not None:
            update_expression_parts.append("pilotInfo.pilotSnapshotDate = :pilotSnapshotDate")
            expression_values[":pilotSnapshotDate"] = pilot_info['pilotSnapshotDate']

        # Instructor public profile enrichment
        if 'primaryAirport' in pilot_info and pilot_info['primaryAirport'] is not None:
            update_expression_parts.append("pilotInfo.primaryAirport = :primaryAirport")
            expression_values[":primaryAirport"] = pilot_info['primaryAirport']

        if 'bio' in pilot_info and pilot_info['bio'] is not None:
            update_expression_parts.append("pilotInfo.bio = :bio")
            expression_values[":bio"] = pilot_info['bio']

        if 'specializations' in pilot_info and pilot_info['specializations'] is not None:
            update_expression_parts.append("pilotInfo.specializations = :specializations")
            expression_values[":specializations"] = pilot_info['specializations']

        # Structured certificate profile blob (_v:1 JSON from CertificateProfile TypeScript type).
        # Stored as-is; client is responsible for versioning and parsing.
        if 'certificateProfile' in pilot_info and pilot_info['certificateProfile'] is not None:
            update_expression_parts.append("pilotInfo.certificateProfile = :certificateProfile")
            expression_values[":certificateProfile"] = pilot_info['certificateProfile']

        # Auto-generate inviteCode any time an instructor has certs but no code yet
        existing_pilot_info = existing_user.get('pilotInfo', {})
        existing_invite_code = existing_pilot_info.get('inviteCode')
        existing_instructor_certs = existing_pilot_info.get('instructorCertificates', [])

        # When the caller explicitly sends instructorCertificates (even as []) use
        # that value as the source of truth.  Only fall back to the existing certs
        # when the field was not included in the request at all.
        if 'instructorCertificates' in pilot_info:
            effective_certs = pilot_info['instructorCertificates'] or []
        else:
            effective_certs = existing_instructor_certs

        # Generate if: no existing code AND (incoming certs non-empty OR existing certs non-empty)
        if not existing_invite_code and effective_certs:
            new_invite_code = generate_invite_code()
            update_expression_parts.append("pilotInfo.inviteCode = :inviteCode")
            expression_values[":inviteCode"] = new_invite_code

        # Derive and persist isCfi: true iff the user has effective instructor certs.
        # This is always written so it stays in sync with the cert list even when
        # the caller does not explicitly pass isCfi.
        is_cfi = bool(effective_certs)
        update_expression_parts.append("pilotInfo.isCfi = :isCfi")
        expression_values[":isCfi"] = is_cfi
        print(f"[UserUpdate] isCfi derived as {is_cfi} for user {user_id} (effective_certs={effective_certs})")

    # Merge aircraft list (append or upsert by tailNumber) — onboarding + explicit updateUser
    aircraft_input = input_data.get('aircraft')
    if aircraft_input is not None and isinstance(aircraft_input, list) and len(aircraft_input) > 0:
        existing_aircraft = existing_user.get('aircraft') or []
        now_ms = Decimal(str(int(datetime.utcnow().timestamp() * 1000)))

        def _coerce_added_at(val) -> Decimal:
            """Normalize addedAt to a Decimal (ms timestamp).
            DynamoDB boto3 resource rejects Python float; Decimal is required.
            Handles legacy string ISO values stored before this fix."""
            if val is None:
                return now_ms
            if isinstance(val, Decimal):
                return val
            if isinstance(val, (int, float)):
                return Decimal(str(int(val)))
            # Legacy ISO string — parse and convert to ms
            try:
                from datetime import timezone
                s = str(val).rstrip('Z')
                dt = datetime.fromisoformat(s)
                return Decimal(str(int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)))
            except Exception:
                return now_ms

        # Migrate existing rows: coerce any string addedAt to float
        aircraft_list = []
        for x in existing_aircraft:
            row = dict(x)
            row['addedAt'] = _coerce_added_at(row.get('addedAt'))
            aircraft_list.append(row)

        for ac in aircraft_input:
            if not ac or not isinstance(ac, dict):
                continue
            raw_tail = ac.get('tailNumber')
            if not raw_tail or not str(raw_tail).strip():
                continue
            tail = str(raw_tail).strip().upper()
            # Use client-provided addedAt when it's a valid number; otherwise now
            new_entry = {
                'tailNumber': tail,
                'notes': ac.get('notes') if ac.get('notes') is not None else '',
                'make': ac.get('make') if ac.get('make') is not None else '',
                'model': ac.get('model') if ac.get('model') is not None else '',
                'category': ac.get('category') if ac.get('category') is not None else '',
                'class': ac.get('class') if ac.get('class') is not None else '',
                'complex': bool(ac.get('complex', False)),
                'highPerformance': bool(ac.get('highPerformance', False)),
                'tailwheel': bool(ac.get('tailwheel', False)),
                'isManual': bool(ac.get('isManual', True)),
                'usageCount': int(ac.get('usageCount', 0) or 0),
                'isArchived': bool(ac.get('isArchived', False)),
                'addedAt': _coerce_added_at(ac.get('addedAt')),
            }
            if ac.get('builderCertification') is not None:
                new_entry['builderCertification'] = ac.get('builderCertification')
            if ac.get('airworthinessDate') is not None:
                new_entry['airworthinessDate'] = ac.get('airworthinessDate')

            replaced = False
            for i, ex in enumerate(aircraft_list):
                if str(ex.get('tailNumber', '')).strip().upper() == tail:
                    merged = dict(ex)
                    merged.update(new_entry)
                    merged['usageCount'] = int(ex.get('usageCount', 0) or 0)
                    # Preserve original addedAt when updating an existing entry
                    merged['addedAt'] = _coerce_added_at(ex.get('addedAt'))
                    aircraft_list[i] = merged
                    replaced = True
                    break
            if not replaced:
                aircraft_list.append(new_entry)

        update_expression_parts.append("aircraft = :aircraft")
        expression_values[":aircraft"] = aircraft_list
        print(f"[UserUpdate] Merged aircraft list for user {user_id}, count={len(aircraft_list)}")
    
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
    
    # Convert DynamoDB item to GraphQL format
    # Ensure 'id' field exists (GraphQL schema expects it)
    if 'id' not in updated_item:
        updated_item['id'] = updated_item.get('userId', user_id)
    
    user_data = convert_item(updated_item)

    # DynamoDB omits list attributes entirely when they are empty (it does not
    # store []). Normalise here so AppSync always returns [] rather than null
    # for list fields — the client's PreferencesAdapter and usePilotInfo both
    # depend on [] (not null) to correctly derive isCfi = false.
    if user_data.get('pilotInfo') is not None:
        pi = user_data['pilotInfo']
        if not pi.get('instructorCertificates'):
            pi['instructorCertificates'] = []
        if not pi.get('aircraftRatings'):
            pi['aircraftRatings'] = []

    # Send a security notification to the OLD email address whenever the
    # sign-in email is changed. The old address remains active for sign-in
    # (Cognito keep_original is enabled) so the user can still recover via
    # password reset if this change was not made by them.
    if new_email_value and existing_email and new_email_value.lower() != existing_email.lower():
        try:
            get_ses().send_email(
                Source=SENDER_EMAIL,
                Destination={"ToAddresses": [existing_email]},
                Message=email_templates.email_change_notification(
                    old_email=existing_email,
                    new_email=new_email_value,
                    reset_url=RESET_URL,
                ),
            )
            print(f"[UserUpdate] Email-change security notice sent to {existing_email}")
        except Exception as ses_err:
            # Non-fatal — log and continue so the profile update is not blocked
            # by a transient SES failure.
            print(f"[UserUpdate] WARNING: Failed to send email-change notice: {ses_err}")

    return user_data

