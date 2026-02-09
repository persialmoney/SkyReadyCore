"""
AppSync Lambda Resolver for currency operations.
Calculates pilot currency requirements based on logbook entries.

Returns RAW DATA only - frontend generates all UI messages from currencyMessages.ts
This follows industry standard: backend handles data, frontend handles presentation.
"""
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any
from psycopg2.extras import RealDictCursor

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'shared'))

from shared.db_utils import get_db_connection, return_db_connection, get_user_pilot_info


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AppSync Lambda resolver handler for currency operations.
    
    Event structure from AppSync:
    {
        "arguments": {...},  # Query arguments
        "identity": {
            "sub": "user-id-uuid"
        },
        "info": {
            "fieldName": "getCurrency",
            "parentTypeName": "Query"
        }
    }
    """
    try:
        # Extract common data with defensive null checks
        identity = event.get('identity') if event else {}
        if identity is None:
            identity = {}
        
        arguments = event.get('arguments') if event else {}
        if arguments is None:
            arguments = {}
        
        user_id = identity.get('sub')
        
        if not user_id:
            raise ValueError("User ID (identity.sub) is required but was missing")
        
        return handle_get_currency(user_id, arguments)
    
    except Exception as e:
        import traceback
        error_message = f"Error in currency operation: {str(e)}"
        print(f"[CurrencyOperations] ERROR: {error_message}")
        print(f"[CurrencyOperations] Full traceback: {traceback.format_exc()}")
        print(f"[CurrencyOperations] Event received: {json.dumps(event, default=str)}")
        raise Exception(error_message)


def handle_get_currency(user_id: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate pilot currency - uses ONLY existing tables!"""
    conn = None
    try:
        print(f"[getCurrency] Calculating currency for user: {user_id}")
        
        # Get medical data from DynamoDB (existing Users table)
        user_data = get_user_pilot_info(user_id)
        
        # Get PostgreSQL connection for logbook queries
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Calculate all currencies
        day_currency = calculate_day_currency(cursor, user_id)
        night_currency = calculate_night_currency(cursor, user_id)
        ifr_currency = calculate_ifr_currency(cursor, user_id)
        flight_review = calculate_flight_review(cursor, user_id)
        medical = calculate_medical_certificate(user_data)
        
        return {
            'dayCurrency': day_currency,
            'nightCurrency': night_currency,
            'ifrCurrency': ifr_currency,
            'flightReview': flight_review,
            'medicalCertificate': medical,
        }
    
    except Exception as e:
        print(f"[getCurrency] Error calculating currency: {str(e)}")
        import traceback
        print(f"[getCurrency] Traceback: {traceback.format_exc()}")
        raise
    
    finally:
        if conn:
            return_db_connection(conn)


def calculate_day_currency(cursor, user_id: str) -> Dict[str, Any]:
    """
    Calculate day currency from logbook entries (14 CFR 61.57(a))
    Returns RAW DATA only - frontend generates messages
    """
    query = """
        SELECT 
            COALESCE(SUM(day_landings), 0) as total_landings,
            MAX(date) as last_landing_date
        FROM logbook_entries
        WHERE user_id = %s 
          AND date >= CURRENT_DATE - INTERVAL '90 days'
          AND day_landings > 0
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    
    # Extract data
    total_landings = int(result['total_landings']) if result and result['total_landings'] is not None else 0
    last_landing_date = result['last_landing_date'] if result else None
    
    # Calculate status and expiration
    if total_landings >= 3 and last_landing_date is not None:
        expiration_date = last_landing_date + timedelta(days=90)
        days_remaining = (expiration_date - datetime.now().date()).days
        
        if days_remaining > 15:
            status = 'CURRENT'
        elif days_remaining > 0:
            status = 'EXPIRING'
        else:
            status = 'EXPIRED'
        
        return {
            'name': 'Day Currency',
            'status': status,
            'daysRemaining': days_remaining if days_remaining > 0 else 0,
            'validUntil': expiration_date.strftime('%b %d, %Y'),
            'totalLandings': total_landings,
            'lastLandingDate': last_landing_date.strftime('%Y-%m-%d') if last_landing_date else None,
        }
    else:
        return {
            'name': 'Day Currency',
            'status': 'EXPIRED',
            'daysRemaining': None,
            'validUntil': None,
            'totalLandings': total_landings,
            'lastLandingDate': last_landing_date.strftime('%Y-%m-%d') if last_landing_date else None,
        }


def calculate_night_currency(cursor, user_id: str) -> Dict[str, Any]:
    """
    Calculate night currency from logbook entries (14 CFR 61.57(b))
    Returns RAW DATA only - frontend generates messages
    """
    query = """
        SELECT 
            COALESCE(SUM(night_landings), 0) as night_landings,
            COALESCE(SUM(full_stop_landings), 0) as full_stops,
            MAX(date) as last_night_date
        FROM logbook_entries
        WHERE user_id = %s 
          AND date >= CURRENT_DATE - INTERVAL '90 days'
          AND night_landings > 0
          AND full_stop_landings > 0
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    
    # Extract data
    night_landings = int(result['night_landings']) if result and result['night_landings'] is not None else 0
    full_stops = int(result['full_stops']) if result and result['full_stops'] is not None else 0
    last_night_date = result['last_night_date'] if result else None
    
    # Night currency requires 3 night landings AND 3 full stops
    if night_landings >= 3 and full_stops >= 3 and last_night_date is not None:
        expiration_date = last_night_date + timedelta(days=90)
        days_remaining = (expiration_date - datetime.now().date()).days
        
        if days_remaining > 15:
            status = 'CURRENT'
        elif days_remaining > 0:
            status = 'EXPIRING'
        else:
            status = 'EXPIRED'
        
        return {
            'name': 'Night Currency',
            'status': status,
            'daysRemaining': days_remaining if days_remaining > 0 else 0,
            'validUntil': expiration_date.strftime('%b %d, %Y'),
            'nightLandings': night_landings,
            'fullStopLandings': full_stops,
            'lastLandingDate': last_night_date.strftime('%Y-%m-%d') if last_night_date else None,
        }
    else:
        return {
            'name': 'Night Currency',
            'status': 'EXPIRED',
            'daysRemaining': None,
            'validUntil': None,
            'nightLandings': night_landings,
            'fullStopLandings': full_stops,
            'lastLandingDate': last_night_date.strftime('%Y-%m-%d') if last_night_date else None,
        }


def calculate_ifr_currency(cursor, user_id: str) -> Dict[str, Any]:
    """
    Calculate IFR currency from logbook entries (14 CFR 61.57(c))
    Returns RAW DATA only - frontend generates messages
    """
    query = """
        SELECT 
            COALESCE(SUM(approaches), 0) as total_approaches,
            BOOL_OR(holds) as has_holds,
            BOOL_OR(tracking) as has_tracking,
            MAX(date) as last_ifr_date
        FROM logbook_entries
        WHERE user_id = %s 
          AND date >= CURRENT_DATE - INTERVAL '6 months'
          AND approaches > 0
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    
    # Extract data
    total_approaches = int(result['total_approaches']) if result and result['total_approaches'] is not None else 0
    has_holds = result['has_holds'] or False if result else False
    has_tracking = result['has_tracking'] or False if result else False
    last_ifr_date = result['last_ifr_date'] if result else None
    
    # IFR currency requires 6 approaches AND holds AND tracking
    if total_approaches >= 6 and has_holds and has_tracking and last_ifr_date is not None:
        expiration_date = last_ifr_date + timedelta(days=180)  # 6 months
        days_remaining = (expiration_date - datetime.now().date()).days
        
        if days_remaining > 15:
            status = 'CURRENT'
        elif days_remaining > 0:
            status = 'EXPIRING'
        else:
            status = 'EXPIRED'
        
        return {
            'name': 'IFR Currency',
            'status': status,
            'daysRemaining': days_remaining if days_remaining > 0 else 0,
            'validUntil': expiration_date.strftime('%b %d, %Y'),
            'totalApproaches': total_approaches,
            'hasHolds': has_holds,
            'hasTracking': has_tracking,
            'lastLandingDate': last_ifr_date.strftime('%Y-%m-%d') if last_ifr_date else None,
        }
    else:
        return {
            'name': 'IFR Currency',
            'status': 'EXPIRED',
            'daysRemaining': None,
            'validUntil': None,
            'totalApproaches': total_approaches,
            'hasHolds': has_holds,
            'hasTracking': has_tracking,
            'lastLandingDate': last_ifr_date.strftime('%Y-%m-%d') if last_ifr_date else None,
        }


def calculate_flight_review(cursor, user_id: str) -> Dict[str, Any]:
    """
    Calculate flight review from logbook entries with 'Flight Review' flight type (14 CFR 61.56)
    Returns RAW DATA only - frontend generates messages
    """
    query = """
        SELECT date
        FROM logbook_entries
        WHERE user_id = %s 
          AND 'Flight Review' = ANY(flight_types)
          AND status = 'SIGNED'
        ORDER BY date DESC
        LIMIT 1
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    
    # Handle empty result set or None date
    if result and result['date'] is not None:
        review_date = result['date']
        expiration_date = review_date + timedelta(days=730)  # 24 months
        days_remaining = (expiration_date - datetime.now().date()).days
        
        if days_remaining > 30:
            status = 'CURRENT'
        elif days_remaining > 0:
            status = 'EXPIRING'
        else:
            status = 'EXPIRED'
        
        return {
            'name': 'Flight Review',
            'status': status,
            'daysRemaining': days_remaining if days_remaining > 0 else 0,
            'validUntil': expiration_date.strftime('%b %d, %Y'),
            'lastReviewDate': review_date.strftime('%b %d, %Y'),
        }
    else:
        return {
            'name': 'Flight Review',
            'status': 'EXPIRED',
            'daysRemaining': None,
            'validUntil': None,
            'lastReviewDate': None,
        }


def calculate_medical_certificate(user_data: Dict) -> Dict[str, Any]:
    """
    Calculate medical certificate expiration from DynamoDB user data (14 CFR 61.23)
    Returns RAW DATA only - frontend generates messages
    """
    # Handle empty user_data or missing pilotInfo
    if not user_data:
        return {
            'name': 'Medical Certificate',
            'status': 'NOT_APPLICABLE',
            'daysRemaining': None,
            'validUntil': None,
            'medicalClass': None,
            'medicalExamDate': None,
        }
    
    pilot_info = user_data.get('pilotInfo', {})
    if not pilot_info:
        return {
            'name': 'Medical Certificate',
            'status': 'NOT_APPLICABLE',
            'daysRemaining': None,
            'validUntil': None,
            'medicalClass': None,
            'medicalExamDate': None,
        }
    
    medical_date_str = pilot_info.get('medicalCertificateDate')
    medical_class = pilot_info.get('medicalCertificateClass')
    dob_str = pilot_info.get('dateOfBirth')
    
    if not medical_date_str or not medical_class:
        return {
            'name': 'Medical Certificate',
            'status': 'NOT_APPLICABLE',
            'daysRemaining': None,
            'validUntil': None,
            'medicalClass': None,
            'medicalExamDate': None,
        }
    
    # Parse medical date with comprehensive error handling
    medical_date = None
    try:
        medical_date = datetime.fromisoformat(medical_date_str.replace('Z', '+00:00')).date()
    except Exception as e:
        print(f"[calculate_medical_certificate] Failed to parse ISO format date: {e}")
        try:
            medical_date = datetime.strptime(medical_date_str, '%Y-%m-%d').date()
        except Exception as e2:
            print(f"[calculate_medical_certificate] Failed to parse date string '{medical_date_str}': {e2}")
            return {
                'name': 'Medical Certificate',
                'status': 'NOT_APPLICABLE',
                'daysRemaining': None,
                'validUntil': None,
                'medicalClass': medical_class,
                'medicalExamDate': None,
            }
    
    # Additional safety check
    if medical_date is None:
        print(f"[calculate_medical_certificate] medical_date is None after parsing attempt")
        return {
            'name': 'Medical Certificate',
            'status': 'NOT_APPLICABLE',
            'daysRemaining': None,
            'validUntil': None,
            'medicalClass': medical_class,
            'medicalExamDate': None,
        }
    
    # Calculate expiration based on class and age
    age_at_exam = None
    if dob_str:
        try:
            dob = datetime.fromisoformat(dob_str.replace('Z', '+00:00')).date()
            age_at_exam = (medical_date - dob).days // 365
        except:
            pass
    
    # FAA medical expiration rules (simplified - assumes private operations)
    if medical_class == "1":
        expiration_months = 60 if age_at_exam and age_at_exam < 40 else 24
    elif medical_class == "2":
        expiration_months = 60 if age_at_exam and age_at_exam < 40 else 24
    elif medical_class == "3":
        expiration_months = 60 if age_at_exam and age_at_exam < 40 else 24
    else:
        expiration_months = 24  # Default
    
    expiration_date = medical_date + timedelta(days=expiration_months * 30)
    days_remaining = (expiration_date - datetime.now().date()).days
    
    if days_remaining > 30:
        status = 'CURRENT'
    elif days_remaining > 0:
        status = 'EXPIRING'
    else:
        status = 'EXPIRED'
    
    return {
        'name': 'Medical Certificate',
        'status': status,
        'daysRemaining': days_remaining if days_remaining > 0 else 0,
        'validUntil': expiration_date.strftime('%b %d, %Y'),
        'medicalClass': medical_class,
        'medicalExamDate': medical_date.strftime('%b %d, %Y'),
    }
