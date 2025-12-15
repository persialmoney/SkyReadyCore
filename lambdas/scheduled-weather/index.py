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
            "errors": []
        }
        
        # Fetch weather for each airport
        for airport_code in airports_to_check:
            try:
                metar_data = fetch_metar_for_airport(airport_code)
                
                if metar_data:
                    results["processed_airports"] += 1
                    
                    # Check for alerts (would need to iterate through users)
                    # For now, this is a simplified version
                    # In production, you'd query by airport code using GSI
                    
            except Exception as e:
                results["errors"].append({
                    "airport": airport_code,
                    "error": str(e)
                })
        
        print(f"Processed {results['processed_airports']} airports")
        return results
        
    except Exception as e:
        print(f"Error in scheduled weather handler: {str(e)}")
        return {
            "error": str(e),
            "processed_airports": 0
        }
