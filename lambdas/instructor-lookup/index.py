"""
Instructor Lookup Lambda - Find CFI by invite code or certificate number
Returns public profile only (no private user data)
"""
import json
import os
import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ['USERS_TABLE_NAME'])

def handler(event, context):
    """
    Lookup instructor by invite code or certificate number
    Returns InstructorPublicProfile
    """
    print(f"[instructor-lookup] Processing request: {json.dumps(event, default=str)}")
    
    arguments = event['arguments']
    invite_code = arguments.get('inviteCode')
    certificate_number = arguments.get('certificateNumber')
    
    # Must provide at least one search criterion
    if not invite_code and not certificate_number:
        raise ValueError("Must provide either inviteCode or certificateNumber")
    
    try:
        # Build filter expression
        filter_expression = None
        
        if invite_code and certificate_number:
            # Both provided - search by both (AND)
            filter_expression = (
                Attr('pilotInfo.inviteCode').eq(invite_code) &
                Attr('pilotInfo.instructorCertificateNumber').eq(certificate_number)
            )
        elif invite_code:
            # Search by invite code only
            filter_expression = Attr('pilotInfo.inviteCode').eq(invite_code)
        else:
            # Search by certificate number only
            filter_expression = Attr('pilotInfo.instructorCertificateNumber').eq(certificate_number)
        
        # Scan users table with filter — paginate through all pages
        scan_kwargs = dict(
            FilterExpression=filter_expression,
            ProjectionExpression='userId, #name, pilotInfo, city, #state',
            ExpressionAttributeNames={
                '#name': 'name',
                '#state': 'state'
            }
        )
        items = []
        while True:
            response = users_table.scan(**scan_kwargs)
            items.extend(response.get('Items', []))
            # Stop as soon as we have a match — no need to keep scanning
            if items:
                break
            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break
            scan_kwargs['ExclusiveStartKey'] = last_key
        
        print(f"[instructor-lookup] Scan complete — found {len(items)} matching item(s)")
        
        if not items:
            # No instructor found
            return None
        
        if len(items) > 1:
            print(f"[instructor-lookup] WARNING: Multiple instructors found for search criteria")
        
        # Return first match (invite codes should be unique)
        user = items[0]
        pilot_info = user.get('pilotInfo', {})
        
        # Verify the user is actually a CFI. Primary signal is instructorCertificates;
        # fall back to isCfi flag for users whose certs may not be written yet.
        instructor_certs = pilot_info.get('instructorCertificates', [])
        is_cfi = bool(instructor_certs) or pilot_info.get('isCfi', False)
        if not is_cfi:
            print(f"[instructor-lookup] User {user['userId']} found but is not a CFI (no certs, isCfi=False)")
            return None
        
        # Build public profile (limited fields only)
        return {
            'userId': user['userId'],
            'name': user.get('name', ''),
            'instructorCertificates': instructor_certs,
            'instructorCertificateNumber': pilot_info.get('instructorCertificateNumber', ''),
            'instructorCertificateExpiration': pilot_info.get('instructorCertificateExpiration', ''),
            'primaryAirport': pilot_info.get('primaryAirport'),
            'city': user.get('city', ''),
            'state': user.get('state', '')
        }
    
    except Exception as e:
        print(f"[instructor-lookup] Error: {e}")
        raise e
