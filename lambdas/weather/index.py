"""
Lambda function for fetching weather data (METAR, TAF, NOTAMs) from external APIs.
This function is used as an AppSync Lambda resolver.
"""
import json
import os
import boto3
from datetime import datetime
from typing import Dict, Any, Optional
import urllib.request
import urllib.error

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))
saved_airports_table = dynamodb.Table(os.environ.get('SAVED_AIRPORTS_TABLE', 'sky-ready-saved-airports-dev'))
alerts_table = dynamodb.Table(os.environ.get('ALERTS_TABLE', 'sky-ready-alerts-dev'))

# Weather API endpoints (using public Aviation Weather Center API)
AWC_BASE_URL = "https://aviationweather.gov/api/data"
METAR_URL = f"{AWC_BASE_URL}/metar"
TAF_URL = f"{AWC_BASE_URL}/taf"
NOTAM_URL = f"{AWC_BASE_URL}/notam"


def fetch_metar(airport_code: str) -> Dict[str, Any]:
    """
    Fetch METAR data for an airport.
    Uses Aviation Weather Center API.
    """
    try:
        url = f"{METAR_URL}?ids={airport_code.upper()}&format=json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
            if not data or len(data) == 0:
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "No data available",
                    "observationTime": datetime.utcnow().isoformat(),
                    "error": "No METAR data found for this airport"
                }
            
            # Parse the first METAR (most recent)
            metar = data[0]
            
            return {
                "airportCode": airport_code.upper(),
                "rawText": metar.get("rawOb", ""),
                "observationTime": metar.get("obsTime", datetime.utcnow().isoformat()),
                "temperature": metar.get("temp", None),
                "dewpoint": metar.get("dewp", None),
                "windDirection": metar.get("wdir", None),
                "windSpeed": metar.get("wspd", None),
                "windGust": metar.get("wspd", None),  # AWC doesn't always provide gust separately
                "visibility": metar.get("visib", None),
                "altimeter": metar.get("altim", None),
                "skyConditions": parse_sky_conditions(metar),
                "flightCategory": metar.get("flightCategory", None),
                "metarType": metar.get("metarType", None),
                "elevation": metar.get("elev", None)
            }
    except urllib.error.URLError as e:
        return {
            "airportCode": airport_code.upper(),
            "rawText": f"Error fetching METAR: {str(e)}",
            "observationTime": datetime.utcnow().isoformat(),
            "error": str(e)
        }
    except Exception as e:
        return {
            "airportCode": airport_code.upper(),
            "rawText": f"Error processing METAR: {str(e)}",
            "observationTime": datetime.utcnow().isoformat(),
            "error": str(e)
        }


def fetch_taf(airport_code: str) -> Dict[str, Any]:
    """
    Fetch TAF data for an airport.
    Uses Aviation Weather Center API.
    """
    try:
        url = f"{TAF_URL}?ids={airport_code.upper()}&format=json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
            if not data or len(data) == 0:
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "No data available",
                    "error": "No TAF data found for this airport"
                }
            
            taf = data[0]
            
            return {
                "airportCode": airport_code.upper(),
                "rawText": taf.get("rawTAF", ""),
                "issueTime": taf.get("issueTime", datetime.utcnow().isoformat()),
                "validTimeFrom": taf.get("validTimeFrom", ""),
                "validTimeTo": taf.get("validTimeTo", ""),
                "remarks": taf.get("remarks", ""),
                "forecast": parse_taf_forecast(taf)
            }
    except Exception as e:
        return {
            "airportCode": airport_code.upper(),
            "rawText": f"Error fetching TAF: {str(e)}",
            "error": str(e)
        }


def fetch_notams(airport_code: str) -> list:
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


def parse_sky_conditions(metar: Dict) -> list:
    """Parse sky conditions from METAR data."""
    sky_conditions = []
    
    # AWC API provides sky conditions in various formats
    # This is a simplified parser - adjust based on actual API response format
    if "skyc1" in metar and metar["skyc1"]:
        sky_conditions.append({
            "skyCover": metar.get("skyc1", ""),
            "cloudBase": metar.get("skyl1", None),
            "cloudType": None
        })
    
    return sky_conditions if sky_conditions else [{"skyCover": "CLR", "cloudBase": None, "cloudType": None}]


def parse_taf_forecast(taf: Dict) -> list:
    """Parse TAF forecast periods."""
    # TAF forecasts are complex - this is a placeholder
    # Adjust based on actual AWC API response format
    return [{
        "fcstTimeFrom": taf.get("validTimeFrom", ""),
        "fcstTimeTo": taf.get("validTimeTo", ""),
        "changeIndicator": None,
        "windDirection": taf.get("wdir", None),
        "windSpeed": taf.get("wspd", None),
        "windGust": None,
        "visibility": taf.get("visib", None),
        "skyConditions": [],
        "flightCategory": taf.get("flightCategory", None)
    }]


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for AppSync resolver.
    Event structure from AppSync:
    {
        "arguments": {
            "airportCode": "KJFK"
        },
        "info": {
            "fieldName": "getMETAR",
            "parentTypeName": "Query"
        },
        "identity": {...},
        "request": {...},
        "source": {...}
    }
    """
    field_name = event.get("info", {}).get("fieldName", "")
    arguments = event.get("arguments", {})
    
    try:
        if field_name == "getMETAR":
            airport_code = arguments.get("airportCode")
            if not airport_code:
                raise ValueError("airportCode is required")
            return fetch_metar(airport_code)
        
        elif field_name == "getTAF":
            airport_code = arguments.get("airportCode")
            if not airport_code:
                raise ValueError("airportCode is required")
            return fetch_taf(airport_code)
        
        elif field_name == "getNOTAMs":
            airport_code = arguments.get("airportCode")
            if not airport_code:
                raise ValueError("airportCode is required")
            return fetch_notams(airport_code)
        
        elif field_name == "getDistance":
            # Calculate distance between airports
            source = arguments.get("sourceAirport", "").upper()
            dest = arguments.get("destinationAirport", "").upper()
            
            # Simple airport database (in production, use DynamoDB or external API)
            airports = {
                'JFK': {'lat': 40.6413, 'lon': -73.7781},
                'LAX': {'lat': 33.9425, 'lon': -118.4081},
                # Add more airports as needed
            }
            
            if source not in airports or dest not in airports:
                raise ValueError(f"Airport not found: {source} or {dest}")
            
            # Haversine formula
            from math import radians, sin, cos, sqrt, atan2
            R = 3440.065  # Earth radius in nautical miles
            
            lat1, lon1 = radians(airports[source]['lat']), radians(airports[source]['lon'])
            lat2, lon2 = radians(airports[dest]['lat']), radians(airports[dest]['lon'])
            
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            distance = R * c
            
            return {
                "sourceAirport": source,
                "destinationAirport": dest,
                "distance": round(distance, 1),
                "unit": "nautical miles"
            }
        
        else:
            raise ValueError(f"Unknown field: {field_name}")
    
    except Exception as e:
        return {
            "error": str(e),
            "fieldName": field_name,
            "arguments": arguments
        }
