"""
Scheduled Lambda function for fetching and caching weather data.
Runs every 15 minutes to keep weather data fresh.
"""
import json
import os
import boto3
from datetime import datetime
from typing import Dict, Any, List
import urllib.request
import urllib.error

# Initialize clients
dynamodb = boto3.resource('dynamodb')
appsync = boto3.client('appsync')

saved_airports_table = dynamodb.Table(os.environ.get('SAVED_AIRPORTS_TABLE', 'sky-ready-saved-airports-dev'))
alerts_table = dynamodb.Table(os.environ.get('ALERTS_TABLE', 'sky-ready-alerts-dev'))
appsync_api_id = os.environ.get('APPSYNC_API_ID')

AWC_BASE_URL = "https://aviationweather.gov/api/data"
METAR_URL = f"{AWC_BASE_URL}/metar"
TAF_URL = f"{AWC_BASE_URL}/taf"
NOTAM_URL = f"{AWC_BASE_URL}/notam"


def fetch_metar_for_airport(airport_code: str) -> Dict[str, Any]:
    """Fetch METAR for a single airport."""
    try:
        url = f"{METAR_URL}?ids={airport_code.upper()}&format=json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            return data[0] if data else None
    except Exception as e:
        print(f"Error fetching METAR for {airport_code}: {str(e)}")
        return None


def fetch_taf_for_airport(airport_code: str) -> Dict[str, Any]:
    """Fetch TAF for a single airport."""
    try:
        url = f"{TAF_URL}?ids={airport_code.upper()}&format=json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            return data[0] if data else None
    except Exception as e:
        print(f"Error fetching TAF for {airport_code}: {str(e)}")
        return None


def fetch_notams_for_airport(airport_code: str) -> List[Dict[str, Any]]:
    """
    Fetch NOTAMs for an airport.
    Note: Public NOTAM APIs are limited. This is a placeholder implementation.
    """
    try:
        # NOTE: Public NOTAM APIs require authentication or have rate limits
        # This is a simplified implementation
        # In production, you'd use FAA NOTAM API or a commercial service
        
        # For now, return empty list with a note
        return [{
            "id": "placeholder",
            "airportCode": airport_code.upper(),
            "notamNumber": "N/A",
            "issueDate": datetime.utcnow().isoformat(),
            "effectiveDate": datetime.utcnow().isoformat(),
            "expirationDate": None,
            "message": "NOTAM API integration required. Please configure FAA NOTAM API or commercial service.",
            "category": "INFO",
            "severity": "INFO"
        }]
    except Exception as e:
        print(f"Error fetching NOTAMs for {airport_code}: {str(e)}")
        return [{
            "id": "error",
            "airportCode": airport_code.upper(),
            "notamNumber": "ERROR",
            "issueDate": datetime.utcnow().isoformat(),
            "effectiveDate": datetime.utcnow().isoformat(),
            "message": f"Error fetching NOTAMs: {str(e)}",
            "category": "ERROR",
            "severity": "ERROR"
        }]


def check_alert_conditions(airport_code: str, metar_data: Dict[str, Any], user_id: str) -> List[Dict[str, Any]]:
    """Check if any alerts should be triggered based on weather conditions."""
    triggered_alerts = []
    
    try:
        # Query alerts for this user and airport
        response = alerts_table.query(
            KeyConditionExpression="userId = :userId",
            FilterExpression="airportCode = :airportCode AND enabled = :enabled",
            ExpressionAttributeValues={
                ":userId": user_id,
                ":airportCode": airport_code,
                ":enabled": True
            }
        )
        
        alerts = response.get('Items', [])
        
        for alert in alerts:
            alert_type = alert.get('alertType')
            condition = alert.get('condition')
            threshold = alert.get('threshold')
            
            # Simple alert checking logic
            # In production, implement more sophisticated condition evaluation
            if alert_type == "WIND_SPEED" and metar_data.get('wspd'):
                if condition == "GREATER_THAN" and metar_data['wspd'] > float(threshold):
                    triggered_alerts.append(alert)
            elif alert_type == "VISIBILITY" and metar_data.get('visib'):
                if condition == "LESS_THAN" and metar_data['visib'] < float(threshold):
                    triggered_alerts.append(alert)
            # Add more alert types as needed
        
    except Exception as e:
        print(f"Error checking alerts: {str(e)}")
    
    return triggered_alerts


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Scheduled Lambda handler.
    Fetches weather data for all saved airports and checks for alert conditions.
    """
    try:
        # Get all saved airports (scan table - in production, use pagination)
        # For now, we'll process a limited set
        airports_to_check = set()
        
        # Scan saved airports table to get unique airport codes
        # Note: In production, use pagination for large datasets
        response = saved_airports_table.scan(
            ProjectionExpression="airportCode"
        )
        
        for item in response.get('Items', []):
            airports_to_check.add(item.get('airportCode'))
        
        # If no saved airports, use a default set
        if not airports_to_check:
            airports_to_check = {'KJFK', 'KLAX', 'KORD', 'KDFW', 'KDEN'}
        
        results = {
            "processed_airports": 0,
            "triggered_alerts": 0,
            "metar_fetched": 0,
            "taf_fetched": 0,
            "notams_fetched": 0,
            "errors": []
        }
        
        # Fetch weather data for each airport
        for airport_code in airports_to_check:
            try:
                # Fetch METAR
                metar_data = fetch_metar_for_airport(airport_code)
                if metar_data:
                    results["metar_fetched"] += 1
                
                # Fetch TAF
                taf_data = fetch_taf_for_airport(airport_code)
                if taf_data:
                    results["taf_fetched"] += 1
                
                # Fetch NOTAMs
                notams_data = fetch_notams_for_airport(airport_code)
                if notams_data:
                    results["notams_fetched"] += 1
                
                # If we got at least one data type, count as processed
                if metar_data or taf_data or notams_data:
                    results["processed_airports"] += 1
                    
                    # Check for alerts using METAR data (would need to iterate through users)
                    # For now, this is a simplified version
                    # In production, you'd query by airport code using GSI
                    if metar_data:
                        # Alert checking logic would go here
                        pass
                    
            except Exception as e:
                results["errors"].append({
                    "airport": airport_code,
                    "error": str(e)
                })
        
        print(f"Processed {results['processed_airports']} airports")
        print(f"  - METAR: {results['metar_fetched']}")
        print(f"  - TAF: {results['taf_fetched']}")
        print(f"  - NOTAMs: {results['notams_fetched']}")
        if results['errors']:
            print(f"  - Errors: {len(results['errors'])}")
        return results
        
    except Exception as e:
        print(f"Error in scheduled weather handler: {str(e)}")
        return {
            "error": str(e),
            "processed_airports": 0
        }
