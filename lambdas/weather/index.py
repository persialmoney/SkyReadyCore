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
        # Use decoded format to get structured fields like skyc1, skyl1, etc.
        url = f"{METAR_URL}?ids={airport_code.upper()}&format=json&taf=false&hours=1"
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
            
            # Parse altimeter - AWC API provides altim_in_hg field for inHg directly
            # Fallback to altim field (which may be in hPa) if altim_in_hg is not available
            altim_inhg = None
            
            # First, try to get altim_in_hg (directly in inches of mercury)
            if "altim_in_hg" in metar:
                altim_inhg = metar.get("altim_in_hg")
            elif "altimInHg" in metar:
                # Try camelCase variant
                altim_inhg = metar.get("altimInHg")
            else:
                # Fallback: use altim field and convert if needed
                altim_value = metar.get("altim", None)
                if altim_value is not None:
                    # Check if value is in inHg range (typically 28-31) or hPa range (typically 950-1050)
                    if altim_value >= 28 and altim_value <= 31:
                        # Already in inHg range, use as-is
                        altim_inhg = altim_value
                    else:
                        # Assume it's in hPa/millibars, convert to inHg
                        # Conversion: inHg = hPa / 33.8639
                        altim_inhg = altim_value / 33.8639
            
            # Ensure observationTime is in ISO format with Z suffix
            obs_time = metar.get("obsTime", None)
            if obs_time:
                # AWC API returns ISO 8601 format (e.g., "2025-12-22T11:17:54Z")
                # Ensure it has Z suffix for UTC
                obs_time_str = str(obs_time)
                if not obs_time_str.endswith('Z') and not obs_time_str.endswith('+00:00'):
                    # Remove timezone offset if present and add Z
                    obs_time_str = obs_time_str.rstrip('+00:00').rstrip('-00:00')
                    if '+' in obs_time_str or (len(obs_time_str) > 10 and obs_time_str[10] == 'T'):
                        # Has time component, ensure Z suffix
                        obs_time = obs_time_str + 'Z' if not obs_time_str.endswith('Z') else obs_time_str
                    else:
                        obs_time = obs_time_str + 'Z'
                else:
                    obs_time = obs_time_str
            else:
                obs_time = datetime.utcnow().isoformat() + 'Z'
            
            return {
                "airportCode": airport_code.upper(),
                "rawText": metar.get("rawOb", ""),
                "observationTime": obs_time,
                "temperature": metar.get("temp", None),
                "dewpoint": metar.get("dewp", None),
                "windDirection": metar.get("wdir", None),
                "windSpeed": metar.get("wspd", None),
                "windGust": metar.get("wspd", None),  # AWC doesn't always provide gust separately
                "visibility": metar.get("visib", None),
                "altimeter": altim_inhg,
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
    
    # AWC API provides sky conditions with fields like:
    # skyc1, skyc2, skyc3, skyc4 (sky cover codes: FEW, SCT, BKN, OVC, CLR, etc.)
    # skyl1, skyl2, skyl3, skyl4 (cloud base levels in HUNDREDS of feet, e.g., 25 = 2500ft)
    # skyt1, skyt2, skyt3, skyt4 (cloud types, optional)
    
    # Debug: Log available sky condition fields and all metar keys
    sky_fields = {k: v for k, v in metar.items() if k.startswith('sky')}
    all_keys = list(metar.keys())
    print(f"DEBUG: All METAR keys: {all_keys[:20]}...")  # Show first 20 keys
    if sky_fields:
        print(f"DEBUG: Sky condition fields found: {sky_fields}")
    else:
        print(f"DEBUG: No sky condition fields found. Checking for alternative field names...")
        # Check for alternative field names
        alt_fields = {k: v for k, v in metar.items() if 'cloud' in k.lower() or 'sky' in k.lower()}
        if alt_fields:
            print(f"DEBUG: Alternative cloud/sky fields: {alt_fields}")
    
    # Parse up to 4 cloud layers
    for i in range(1, 5):
        skyc_key = f"skyc{i}"
        skyl_key = f"skyl{i}"
        skyt_key = f"skyt{i}"
        
        sky_cover = metar.get(skyc_key, None)
        
        # Handle both string and None values
        if sky_cover is not None and str(sky_cover).strip():
            sky_cover_str = str(sky_cover).strip().upper()
            
            # Skip empty strings and "///" (missing data indicator)
            if sky_cover_str and sky_cover_str != "///" and sky_cover_str != "":
                # Get cloud base - AWC API returns in actual feet (not hundreds)
                cloud_base_raw = metar.get(skyl_key, None)
                cloud_base = None
                if cloud_base_raw is not None:
                    try:
                        # API returns in feet directly (e.g., 18000 = 18,000 feet)
                        cloud_base = int(cloud_base_raw)
                    except (ValueError, TypeError):
                        cloud_base = None
                
                # Get cloud type (optional)
                cloud_type = metar.get(skyt_key, None)
                if cloud_type is not None and str(cloud_type).strip():
                    cloud_type = str(cloud_type).strip()
                else:
                    cloud_type = None
                
                # Handle CLR/SKC (clear skies) - only add if it's the first layer
                if sky_cover_str in ["CLR", "SKC"]:
                    if i == 1:
                        sky_conditions.append({
                            "skyCover": "CLR",
                            "cloudBase": None,
                            "cloudType": None
                        })
                        break  # CLR means no other layers
                else:
                    # Add cloud layer
                    sky_conditions.append({
                        "skyCover": sky_cover_str,
                        "cloudBase": cloud_base,
                        "cloudType": cloud_type
                    })
    
    # If no cloud layers found, try parsing from raw METAR text as fallback
    if not sky_conditions:
        raw_text = metar.get("rawOb", metar.get("rawText", ""))
        if raw_text:
            # Parse cloud layers from raw METAR (format: FEW025, SCT040, BKN200, etc.)
            import re
            # Pattern matches: FEW/SCT/BKN/OVC followed by 3 digits (cloud base in hundreds of feet)
            cloud_pattern = r'\b(FEW|SCT|BKN|OVC|CLR|SKC)(\d{3})?\b'
            matches = re.findall(cloud_pattern, raw_text)
            
            for cover, base_str in matches:
                if cover.upper() in ["CLR", "SKC"]:
                    # Clear skies - return immediately
                    return [{"skyCover": "CLR", "cloudBase": None, "cloudType": None}]
                elif cover.upper() in ["FEW", "SCT", "BKN", "OVC"]:
                    cloud_base = None
                    if base_str:
                        try:
                            # Raw METAR uses hundreds of feet (e.g., 025 = 2,500 feet)
                            cloud_base = int(base_str) * 100
                        except ValueError:
                            cloud_base = None
                    
                    sky_conditions.append({
                        "skyCover": cover.upper(),
                        "cloudBase": cloud_base,
                        "cloudType": None
                    })
        
        # If still no cloud layers found, return clear skies
        if not sky_conditions:
            return [{"skyCover": "CLR", "cloudBase": None, "cloudType": None}]
    
    return sky_conditions


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
