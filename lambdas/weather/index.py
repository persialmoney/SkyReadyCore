"""
Lambda function for fetching weather data (METAR, TAF, NOTAMs) from ElastiCache ValKey or external APIs.
This function is used as an AppSync Lambda resolver.
Uses cache-first strategy: checks ElastiCache, falls back to API if cache miss.
"""
import json
import os
import boto3
import re
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import urllib.request
import urllib.error
import redis  # ValKey is Redis-compatible, so we use the redis Python library
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ.get('USERS_TABLE', 'sky-ready-users-dev'))
saved_airports_table = dynamodb.Table(os.environ.get('SAVED_AIRPORTS_TABLE', 'sky-ready-saved-airports-dev'))
alerts_table = dynamodb.Table(os.environ.get('ALERTS_TABLE', 'sky-ready-alerts-dev'))

# ElastiCache configuration
ELASTICACHE_ENDPOINT = os.environ.get('ELASTICACHE_ENDPOINT')
ELASTICACHE_PORT = int(os.environ.get('ELASTICACHE_PORT', 6379))

# Weather API endpoints (using public Aviation Weather Center API)
AWC_BASE_URL = "https://aviationweather.gov/api/data"
METAR_URL = f"{AWC_BASE_URL}/metar"
TAF_URL = f"{AWC_BASE_URL}/taf"
NOTAM_URL = f"{AWC_BASE_URL}/notam"

# Redis client (lazy initialization)
redis_client = None


def get_redis_client() -> Optional[redis.Redis]:
    """Get or create Redis client connection with retry logic."""
    global redis_client
    if not ELASTICACHE_ENDPOINT:
        logger.warning("[ElastiCache] No ELASTICACHE_ENDPOINT configured, skipping cache")
        return None
    
    # Check if existing connection is still valid
    if redis_client is not None:
        try:
            redis_client.ping()
            return redis_client
        except Exception as e:
            logger.warning(f"[ElastiCache] Existing connection is stale: {str(e)}, creating new connection")
            redis_client = None
    
    # Retry connection with exponential backoff
    max_retries = 3
    base_delay = 0.1  # 100ms base delay
    
    for attempt in range(max_retries):
        try:
            logger.info(f"[ElastiCache] Connection attempt {attempt + 1}/{max_retries} to {ELASTICACHE_ENDPOINT}:{ELASTICACHE_PORT}")
            
            redis_client = redis.Redis(
                host=ELASTICACHE_ENDPOINT,
                port=ELASTICACHE_PORT,
                decode_responses=True,
                socket_connect_timeout=5,  # Increased from 2 to 5 seconds for VPC connections
                socket_timeout=5,  # Increased from 2 to 5 seconds
                retry_on_timeout=True,  # Enable retry on timeout
            )
            
            # Test connection with ping
            redis_client.ping()
            logger.info(f"[ElastiCache] Successfully connected to {ELASTICACHE_ENDPOINT}:{ELASTICACHE_PORT}")
            return redis_client
            
        except redis.TimeoutError as e:
            error_msg = f"Timeout connecting to ElastiCache: {str(e)}"
            logger.warning(f"[ElastiCache] {error_msg}")
            redis_client = None
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                logger.info(f"[ElastiCache] Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"[ElastiCache] All {max_retries} connection attempts failed, will use API fallback")
                
        except redis.ConnectionError as e:
            error_msg = f"Connection error to ElastiCache: {str(e)}"
            logger.warning(f"[ElastiCache] {error_msg}")
            redis_client = None
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"[ElastiCache] Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"[ElastiCache] All {max_retries} connection attempts failed, will use API fallback")
                
        except Exception as e:
            error_type = type(e).__name__
            error_msg = f"Unexpected error connecting to ElastiCache ({error_type}): {str(e)}"
            logger.error(f"[ElastiCache] {error_msg}")
            redis_client = None
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"[ElastiCache] Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"[ElastiCache] All {max_retries} connection attempts failed, will use API fallback")
    
    return None


def fetch_metar(airport_code: str) -> Dict[str, Any]:
    """
    Fetch METAR data for an airport.
    Cache-first strategy: checks ElastiCache, falls back to API if cache miss.
    """
    airport_code = airport_code.upper()
    cache_key = f"metar:{airport_code}"
    
    # Try to get from cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            logger.info(f"[METAR Cache] Checking cache for key: {cache_key}")
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info(f"[METAR Cache] ✅ Cache HIT for {airport_code} (key: {cache_key})")
                metar_data = json.loads(cached_data)
                # Transform to expected format
                return transform_metar_from_cache(metar_data, airport_code)
            else:
                logger.info(f"[METAR Cache] ❌ Cache MISS for {airport_code} - key '{cache_key}' not found in cache")
        except Exception as e:
            logger.warning(f"[METAR Cache] ⚠️ Cache read error for {airport_code} (key: {cache_key}): {str(e)}, falling back to API")
    else:
        logger.warning(f"[METAR Cache] ⚠️ Cannot check cache for {airport_code} - ElastiCache connection failed (key would be: {cache_key})")
    
    # Cache miss or error - fetch from API
    logger.info(f"[METAR Cache] Fetching METAR for {airport_code} from API (cache unavailable or miss)")
    try:
        # Use decoded format to get structured fields like skyc1, skyl1, etc.
        url = f"{METAR_URL}?ids={airport_code}&format=json&taf=false&hours=1"
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
            
            # Parse observation time - API returns obsTime as Unix timestamp (integer)
            # Per OpenAPI spec: obsTime is integer (UNIX timestamp)
            obs_time = metar.get("obsTime", None)
            if obs_time:
                # Check if it's a Unix timestamp (integer) from API
                if isinstance(obs_time, int):
                    # Convert Unix timestamp to ISO format
                    obs_time = datetime.utcfromtimestamp(obs_time).isoformat() + 'Z'
                elif isinstance(obs_time, (float, str)):
                    # Handle as string or float - might be ISO string or timestamp
                    obs_time_str = str(obs_time).strip()
                    if 'T' in obs_time_str:
                        # ISO format string
                        if not obs_time_str.endswith('Z') and not obs_time_str.endswith('+00:00'):
                            obs_time = obs_time_str + 'Z'
                        else:
                            obs_time = obs_time_str
                    else:
                        # Try parsing as timestamp
                        try:
                            ts = float(obs_time_str)
                            obs_time = datetime.utcfromtimestamp(ts).isoformat() + 'Z'
                        except (ValueError, OSError):
                            obs_time = datetime.utcnow().isoformat() + 'Z'
                else:
                    obs_time = datetime.utcnow().isoformat() + 'Z'
            else:
                obs_time = datetime.utcnow().isoformat() + 'Z'
            
            # Parse wind gust - check for wspdGust field, or parse from raw METAR if not available
            wind_gust = metar.get("wspdGust", None)
            if wind_gust is None:
                # Try alternative field names
                wind_gust = metar.get("gust", None)
            
            # Parse visibility - check for visib field, or parse from raw METAR if not available
            visibility = metar.get("visib", None)
            
            # If visibility is a string, try to parse it
            if isinstance(visibility, str):
                vis_str = visibility.strip()
                # Handle "+" suffix (means 10+ or 6+)
                if vis_str.endswith('+'):
                    try:
                        visibility = float(vis_str[:-1]) + 0.5
                    except ValueError:
                        visibility = None
                # Handle fractions like "3/4", "1 3/4"
                elif '/' in vis_str:
                    parts = vis_str.split()
                    if len(parts) == 2:  # "1 3/4" format
                        try:
                            whole = float(parts[0])
                            frac_parts = parts[1].split('/')
                            if len(frac_parts) == 2:
                                fraction = float(frac_parts[0]) / float(frac_parts[1])
                                visibility = whole + fraction
                            else:
                                visibility = None
                        except ValueError:
                            visibility = None
                    else:  # "3/4" format
                        frac_parts = vis_str.split('/')
                        if len(frac_parts) == 2:
                            try:
                                visibility = float(frac_parts[0]) / float(frac_parts[1])
                            except ValueError:
                                visibility = None
                        else:
                            visibility = None
                else:
                    try:
                        visibility = float(vis_str)
                    except (ValueError, TypeError):
                        visibility = None
            
            if visibility is None:
                # Try to parse from raw METAR text (format: 10SM, 1/2SM, M1/4SM, etc.)
                raw_text = metar.get("rawOb", "")
                if raw_text:
                    import re
                    # Pattern matches visibility: 10SM, 1/2SM, M1/4SM, etc.
                    vis_match = re.search(r'(\d+(?:/\d+)?|M?\d+/\d+)\s*SM', raw_text)
                    if vis_match:
                        vis_str = vis_match.group(1)
                        # Handle fractions like 1/2, M1/4
                        if '/' in vis_str:
                            if vis_str.startswith('M'):
                                # M1/4 means less than 1/4
                                parts = vis_str[1:].split('/')
                                if len(parts) == 2:
                                    visibility = float(parts[0]) / float(parts[1]) * 0.9  # Slightly less than the fraction
                            else:
                                parts = vis_str.split('/')
                                if len(parts) == 2:
                                    visibility = float(parts[0]) / float(parts[1])
                        else:
                            try:
                                visibility = float(vis_str)
                            except ValueError:
                                pass
            
            return {
                "airportCode": airport_code.upper(),
                "rawText": metar.get("rawOb", ""),
                "observationTime": obs_time,
                "temperature": metar.get("temp", None),
                "dewpoint": metar.get("dewp", None),
                "windDirection": metar.get("wdir", None),
                "windSpeed": metar.get("wspd", None),
                "windGust": wind_gust,
                "visibility": visibility,
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


def transform_metar_from_cache(metar_data: Dict[str, Any], airport_code: str) -> Dict[str, Any]:
    """Transform cached METAR data to expected format."""
    # Parse altimeter
    altim_inhg = None
    if "altim_in_hg" in metar_data:
        altim_inhg = metar_data.get("altim_in_hg")
    elif "altimInHg" in metar_data:
        altim_inhg = metar_data.get("altimInHg")
    elif "altim" in metar_data:
        altim_value = metar_data.get("altim")
        if altim_value is not None:
            if altim_value >= 28 and altim_value <= 31:
                altim_inhg = altim_value
            else:
                altim_inhg = altim_value / 33.8639
    
    # Parse observation time - handle both formats:
    # 1. CSV cache: "observation_time" as ISO string "2025-12-24T06:56:00.000Z"
    # 2. API JSON: "obsTime" as Unix timestamp integer
    obs_time = metar_data.get("obsTime", None)
    if not obs_time:
        # Try alternative field name from CSV
        obs_time = metar_data.get("observation_time", None)
    
    logger.info(f"Raw obs_time: {obs_time}, type: {type(obs_time)}")
    
    if obs_time:
        # Check if it's a Unix timestamp (integer) from API
        if isinstance(obs_time, int):
            # Convert Unix timestamp to ISO format
            obs_time = datetime.utcfromtimestamp(obs_time).isoformat() + 'Z'
        elif isinstance(obs_time, (float, str)):
            # It's a string or float - handle as ISO string
            obs_time_str = str(obs_time).strip()
            # CSV format is already "2025-12-24T06:56:00.000Z" - ensure it has Z
            if not obs_time_str.endswith('Z') and not obs_time_str.endswith('+00:00'):
                # Try to add Z if it looks like an ISO date
                if 'T' in obs_time_str:
                    obs_time = obs_time_str + 'Z'
                else:
                    # Might be a timestamp string, try to parse
                    try:
                        ts = float(obs_time_str)
                        obs_time = datetime.utcfromtimestamp(ts).isoformat() + 'Z'
                    except (ValueError, OSError):
                        obs_time = obs_time_str + 'Z'
            else:
                obs_time = obs_time_str
        else:
            obs_time = str(obs_time).strip()
    else:
        logger.warning(f"No observation time found for {airport_code}, using current time")
        obs_time = datetime.utcnow().isoformat() + 'Z'
    
    logger.info(f"Final obs_time: {obs_time}")
    
    # Parse visibility - handle both old and new field names
    visibility = metar_data.get("visib", None)
    if visibility is None:
        visibility = metar_data.get("visibility_statute_mi", None)
    
    # If visibility is a string with "+" suffix, convert it
    if isinstance(visibility, str):
        if visibility.endswith('+'):
            try:
                visibility = float(visibility[:-1]) + 0.5
            except ValueError:
                visibility = None
        elif '/' in visibility:
            # Handle fractions like "3/4", "1 3/4"
            parts = visibility.split()
            if len(parts) == 2:  # "1 3/4" format
                try:
                    whole = float(parts[0])
                    frac_parts = parts[1].split('/')
                    if len(frac_parts) == 2:
                        fraction = float(frac_parts[0]) / float(frac_parts[1])
                        visibility = whole + fraction
                    else:
                        visibility = None
                except ValueError:
                    visibility = None
            else:  # "3/4" format
                frac_parts = visibility.split('/')
                if len(frac_parts) == 2:
                    try:
                        visibility = float(frac_parts[0]) / float(frac_parts[1])
                    except ValueError:
                        visibility = None
                else:
                    visibility = None
        else:
            try:
                visibility = float(visibility)
            except (ValueError, TypeError):
                visibility = None
    
    # Parse wind gust - only include if different from wind speed
    wind_gust = metar_data.get("wspdGust", None)
    if wind_gust is None:
        wind_gust = metar_data.get("wind_gust_kt", None)
    
    # Only set wind gust if it's different from wind speed
    wind_speed = metar_data.get("wspd", None)
    if wind_gust is not None and wind_speed is not None:
        if wind_gust == wind_speed:
            wind_gust = None  # Don't show gusts if they're the same as wind speed
    
    # Ensure observationTime is always a non-empty string
    if not obs_time or not str(obs_time).strip():
        logger.warning(f"Invalid observation time for {airport_code}, using current time")
        obs_time = datetime.utcnow().isoformat() + 'Z'
    
    result = {
        "airportCode": airport_code,
        "rawText": metar_data.get("rawOb", metar_data.get("raw_text", "")),
        "observationTime": str(obs_time).strip(),  # Ensure it's always a string
        "temperature": metar_data.get("temp", None),
        "dewpoint": metar_data.get("dewp", None),
        "windDirection": metar_data.get("wdir", None),
        "windSpeed": metar_data.get("wspd", None),
        "windGust": wind_gust,
        "visibility": visibility,
        "altimeter": altim_inhg,
        "skyConditions": parse_sky_conditions(metar_data),
        "flightCategory": metar_data.get("flightCategory", None),
        "metarType": metar_data.get("metarType", None),
        "elevation": metar_data.get("elev", None)
    }
    
    logger.info(f"Transformed METAR result - visibility: {result.get('visibility')}, obsTime: {result.get('observationTime')}")
    
    return result


def fetch_taf(airport_code: str) -> Dict[str, Any]:
    """
    Fetch TAF data for an airport.
    Cache-first strategy: checks ElastiCache, falls back to API if cache miss.
    """
    airport_code = airport_code.upper()
    cache_key = f"taf:{airport_code}"
    
    # Try to get from cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            logger.info(f"[TAF Cache] Checking cache for key: {cache_key}")
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info(f"[TAF Cache] ✅ Cache HIT for {airport_code} (key: {cache_key})")
                taf_data = json.loads(cached_data)
                return transform_taf_from_cache(taf_data, airport_code)
            else:
                logger.info(f"[TAF Cache] ❌ Cache MISS for {airport_code} - key '{cache_key}' not found in cache")
        except Exception as e:
            logger.warning(f"[TAF Cache] ⚠️ Cache read error for {airport_code} (key: {cache_key}): {str(e)}, falling back to API")
    else:
        logger.warning(f"[TAF Cache] ⚠️ Cannot check cache for {airport_code} - ElastiCache connection failed (key would be: {cache_key})")
    
    # Cache miss or error - fetch from API
    logger.info(f"[TAF Cache] Fetching TAF for {airport_code} from API (cache unavailable or miss)")
    try:
        url = f"{TAF_URL}?ids={airport_code}&format=json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
            if not data or len(data) == 0:
                # Return valid TAF structure with all required non-nullable fields
                current_time = datetime.utcnow().isoformat()
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "No data available",
                    "issueTime": current_time,
                    "validTimeFrom": current_time,
                    "validTimeTo": current_time,
                    "remarks": "",
                    "forecast": []  # Empty list is valid for [TAFForecast!]!
                }
            
            taf = data[0]
            
            # Log the TAF structure for debugging
            logger.info(f"TAF data keys: {list(taf.keys())}")
            if "forecast" in taf:
                logger.info(f"Forecast type: {type(taf['forecast'])}, length: {len(taf['forecast']) if isinstance(taf['forecast'], list) else 'N/A'}")
            
            parsed_forecast = parse_taf_forecast(taf)
            logger.info(f"Parsed {len(parsed_forecast)} forecast periods")
            
            # Ensure all non-nullable fields have values (never None)
            current_time = datetime.utcnow().isoformat()
            result = {
                "airportCode": airport_code,
                "rawText": taf.get("rawTAF") or taf.get("rawText") or "",
                "issueTime": taf.get("issueTime") or current_time,
                "validTimeFrom": taf.get("validTimeFrom") or current_time,
                "validTimeTo": taf.get("validTimeTo") or current_time,
                "remarks": taf.get("remarks") or "",
                "forecast": parsed_forecast if parsed_forecast else []  # Ensure it's always a list
            }
            
            # Write-through: Store in cache for next request
            if redis_client:
                try:
                    cache_key = f"taf:{airport_code}"
                    redis_client.setex(cache_key, 900, json.dumps(taf))  # 15 minute TTL
                except Exception as e:
                    logger.warning(f"Failed to cache TAF for {airport_code}: {str(e)}")
            
            return result
    except urllib.error.URLError as e:
        logger.error(f"Network error fetching TAF for {airport_code}: {str(e)}")
        current_time = datetime.utcnow().isoformat()
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Error fetching TAF data",
            "issueTime": current_time,
            "validTimeFrom": current_time,
            "validTimeTo": current_time,
            "remarks": "",
            "forecast": []  # Empty list is valid for [TAFForecast!]!
        }
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for TAF {airport_code}: {str(e)}")
        current_time = datetime.utcnow().isoformat()
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Error parsing TAF data",
            "issueTime": current_time,
            "validTimeFrom": current_time,
            "validTimeTo": current_time,
            "remarks": "",
            "forecast": []  # Empty list is valid for [TAFForecast!]!
        }
    except Exception as e:
        logger.error(f"Unexpected error fetching TAF for {airport_code}: {str(e)}")
        current_time = datetime.utcnow().isoformat()
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Error fetching TAF data",
            "issueTime": current_time,
            "validTimeFrom": current_time,
            "validTimeTo": current_time,
            "remarks": "",
            "forecast": []  # Empty list is valid for [TAFForecast!]!
        }


def transform_taf_from_cache(taf_data: Dict[str, Any], airport_code: str) -> Dict[str, Any]:
    """Transform cached TAF data to expected format."""
    # Ensure all non-nullable fields have values (never None)
    current_time = datetime.utcnow().isoformat()
    parsed_forecast = parse_taf_forecast(taf_data)
    return {
        "airportCode": airport_code,
        "rawText": taf_data.get("rawTAF") or taf_data.get("rawText") or "",
        "issueTime": taf_data.get("issueTime") or current_time,
        "validTimeFrom": taf_data.get("validTimeFrom") or current_time,
        "validTimeTo": taf_data.get("validTimeTo") or current_time,
        "remarks": taf_data.get("remarks") or "",
        "forecast": parsed_forecast if parsed_forecast else []  # Ensure it's always a list
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
    """Parse TAF forecast periods from AWC API response or cached data."""
    forecasts = []
    
    # Check for structured forecast array first (from cache ingestion)
    if "forecast" in taf and isinstance(taf["forecast"], list) and len(taf["forecast"]) > 0:
        logger.info(f"Found {len(taf['forecast'])} forecast periods in structured format")
        for idx, fcst in enumerate(taf["forecast"]):
            logger.info(f"Forecast {idx} keys: {list(fcst.keys()) if isinstance(fcst, dict) else 'not a dict'}")
            
            # Extract sky conditions - check for structured format first, then fall back to parsing
            sky_conditions = []
            if "skyConditions" in fcst and isinstance(fcst["skyConditions"], list):
                # Use structured sky conditions from cache ingestion
                sky_conditions = fcst["skyConditions"]
                logger.info(f"Using structured skyConditions for forecast {idx}")
            else:
                # Parse from skyc1/skyl1 format or other formats
                sky_conditions = _parse_taf_sky_conditions(fcst)
            
            # Time fields - already in ISO format from cache ingestion, but handle both formats
            fcst_time_from = fcst.get("fcstTimeFrom", "")
            if not fcst_time_from:
                fcst_time_from = _convert_time_to_iso(fcst.get("validTimeFrom", fcst.get("timeFrom", "")))
            
            fcst_time_to = fcst.get("fcstTimeTo", "")
            if not fcst_time_to:
                fcst_time_to = _convert_time_to_iso(fcst.get("validTimeTo", fcst.get("timeTo", "")))
            
            forecast_period = {
                "fcstTimeFrom": fcst_time_from,
                "fcstTimeTo": fcst_time_to,
                "changeIndicator": fcst.get("changeIndicator", fcst.get("changeind", fcst.get("changeIndicator", None))),
                "windDirection": fcst.get("wdir", fcst.get("windDirection", None)),
                "windSpeed": fcst.get("wspd", fcst.get("windSpeed", None)),
                "windGust": fcst.get("wspdGust", fcst.get("windGust", None)),
                "visibility": fcst.get("visib", fcst.get("visibility", None)),
                "skyConditions": sky_conditions,
                "flightCategory": fcst.get("flightCategory", fcst.get("flightcat", None))
            }
            forecasts.append(forecast_period)
    
    # If no structured forecast, try to parse from raw TAF text
    if not forecasts:
        raw_taf = taf.get("rawTAF", taf.get("rawText", ""))
        if raw_taf:
            forecasts = _parse_taf_from_raw(raw_taf, taf)
    
    # Fallback: create a single forecast period from main TAF data
    if not forecasts:
        forecasts = _create_fallback_forecast(taf)
    
    return forecasts


def _convert_time_to_iso(time_str: Any) -> str:
    """Convert time string (Unix timestamp or ISO) to ISO format."""
    if not time_str:
        return ""
    
    # If it's already a string that looks like ISO, return it
    if isinstance(time_str, str) and ("T" in time_str or "-" in time_str):
        return time_str
    
    # Try to parse as Unix timestamp (seconds or milliseconds)
    try:
        if isinstance(time_str, (int, float)):
            timestamp = float(time_str)
        else:
            timestamp = float(str(time_str))
        
        # If timestamp is in seconds (less than year 2100 in seconds), convert to milliseconds
        if timestamp < 4102444800:  # Year 2100 in seconds
            timestamp = timestamp * 1000
        
        dt = datetime.utcfromtimestamp(timestamp / 1000)
        return dt.isoformat() + "Z"
    except (ValueError, TypeError, OSError):
        # If conversion fails, return as string
        return str(time_str)


def _parse_taf_sky_conditions(data: Dict) -> list:
    """Parse sky conditions from TAF data (similar to METAR parsing)."""
    sky_conditions = []
    
    # Parse up to 4 cloud layers (similar to METAR)
    for i in range(1, 5):
        skyc_key = f"skyc{i}"
        skyl_key = f"skyl{i}"
        skyt_key = f"skyt{i}"
        
        sky_cover = data.get(skyc_key, None)
        
        if sky_cover is not None and str(sky_cover).strip():
            sky_cover_str = str(sky_cover).strip().upper()
            
            if sky_cover_str and sky_cover_str != "///" and sky_cover_str != "":
                cloud_base_raw = data.get(skyl_key, None)
                cloud_base = None
                if cloud_base_raw is not None:
                    try:
                        cloud_base = int(cloud_base_raw)
                    except (ValueError, TypeError):
                        cloud_base = None
                
                cloud_type = data.get(skyt_key, None)
                if cloud_type is not None and str(cloud_type).strip():
                    cloud_type = str(cloud_type).strip()
                else:
                    cloud_type = None
                
                if sky_cover_str in ["CLR", "SKC"]:
                    if i == 1:
                        sky_conditions.append({
                            "skyCover": "CLR",
                            "cloudBase": None,
                            "cloudType": None
                        })
                        break
                else:
                    sky_conditions.append({
                        "skyCover": sky_cover_str,
                        "cloudBase": cloud_base,
                        "cloudType": cloud_type
                    })
    
    return sky_conditions if sky_conditions else []


def _parse_taf_from_raw(raw_taf: str, taf_data: Dict) -> list:
    """Parse TAF forecast periods from raw TAF text using AVWX library."""
    from avwx import Taf
    
    forecasts = []
    try:
        # Parse raw TAF using AVWX
        taf_obj = Taf.from_report(raw_taf)
        
        if not taf_obj.data or not taf_obj.data.forecast:
            logger.warning("AVWX parsed TAF but no forecast periods found")
            return _create_fallback_forecast(taf_data)
        
        # Extract values from AVWX objects to create JSON-serializable dicts
        for idx, line_data in enumerate(taf_obj.data.forecast):
            # Log the structure for debugging
            logger.info(f"Forecast period {idx}: type={line_data.type}, has_start_time={line_data.start_time is not None}, has_end_time={line_data.end_time is not None}")
            logger.info(f"  visibility={line_data.visibility}, clouds={line_data.clouds}, flight_rules={line_data.flight_rules}")
            
            # Extract timestamps (convert to ISO strings)
            fcst_time_from = ""
            if line_data.start_time:
                try:
                    # AVWX Timestamp objects have .dt property that returns datetime
                    if hasattr(line_data.start_time, 'dt') and line_data.start_time.dt:
                        dt = line_data.start_time.dt
                        # Ensure UTC timezone
                        if dt.tzinfo is None:
                            # If no timezone, assume UTC
                            dt = dt.replace(tzinfo=timezone.utc)
                        elif dt.tzinfo != timezone.utc:
                            # Convert to UTC if not already
                            dt = dt.astimezone(timezone.utc)
                        fcst_time_from = dt.isoformat().replace('+00:00', 'Z')
                    else:
                        # Fallback: try other methods
                        if hasattr(line_data.start_time, 'repr') and line_data.start_time.repr:
                            fcst_time_from = str(line_data.start_time.repr)
                        elif hasattr(line_data.start_time, 'value'):
                            fcst_time_from = _convert_time_to_iso(line_data.start_time.value)
                except Exception as e:
                    logger.warning(f"Error extracting start_time: {str(e)}")
            
            fcst_time_to = ""
            if line_data.end_time:
                try:
                    if hasattr(line_data.end_time, 'dt') and line_data.end_time.dt:
                        dt = line_data.end_time.dt
                        # Ensure UTC timezone
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        elif dt.tzinfo != timezone.utc:
                            dt = dt.astimezone(timezone.utc)
                        fcst_time_to = dt.isoformat().replace('+00:00', 'Z')
                    else:
                        if hasattr(line_data.end_time, 'repr') and line_data.end_time.repr:
                            fcst_time_to = str(line_data.end_time.repr)
                        elif hasattr(line_data.end_time, 'value'):
                            fcst_time_to = _convert_time_to_iso(line_data.end_time.value)
                except Exception as e:
                    logger.warning(f"Error extracting end_time: {str(e)}")
            
            # Extract wind values (Number objects have .value property)
            wind_dir = None
            if line_data.wind_direction:
                try:
                    if hasattr(line_data.wind_direction, 'value'):
                        wind_dir = line_data.wind_direction.value
                    elif isinstance(line_data.wind_direction, (int, float)):
                        wind_dir = line_data.wind_direction
                except:
                    pass
            
            wind_speed = None
            if line_data.wind_speed:
                try:
                    if hasattr(line_data.wind_speed, 'value'):
                        wind_speed = line_data.wind_speed.value
                    elif isinstance(line_data.wind_speed, (int, float)):
                        wind_speed = line_data.wind_speed
                except:
                    pass
            
            wind_gust = None
            if line_data.wind_gust:
                try:
                    if hasattr(line_data.wind_gust, 'value'):
                        wind_gust = line_data.wind_gust.value
                    elif isinstance(line_data.wind_gust, (int, float)):
                        wind_gust = line_data.wind_gust
                except:
                    pass
            
            # Extract visibility - handle different formats
            visibility = None
            if line_data.visibility:
                try:
                    # Try to get numeric value first
                    if hasattr(line_data.visibility, 'value'):
                        vis_value = line_data.visibility.value
                        if isinstance(vis_value, (int, float)):
                            visibility = float(vis_value)
                        elif isinstance(vis_value, str):
                            # Parse string like "6.0" or "P6SM"
                            visibility = _parse_visibility_string(vis_value)
                    elif isinstance(line_data.visibility, (int, float)):
                        visibility = float(line_data.visibility)
                    elif hasattr(line_data.visibility, 'repr'):
                        # Handle special cases like "P6SM" (greater than 6 SM)
                        vis_repr = str(line_data.visibility.repr)
                        visibility = _parse_visibility_string(vis_repr)
                    elif hasattr(line_data.visibility, '__str__'):
                        # Try string representation
                        vis_str = str(line_data.visibility)
                        visibility = _parse_visibility_string(vis_str)
                except Exception as e:
                    logger.warning(f"Error extracting visibility: {str(e)}")
            
            # Convert clouds to dicts - handle AVWX Cloud objects
            sky_conditions = []
            if line_data.clouds and len(line_data.clouds) > 0:
                try:
                    for cloud in line_data.clouds:
                        if cloud:
                            sky_cover = None
                            cloud_base = None
                            cloud_type = None
                            
                            # AVWX Cloud objects have repr, base, and type properties
                            # repr is the string representation (e.g., "FEW", "SCT", "BKN", "OVC")
                            if hasattr(cloud, 'repr') and cloud.repr:
                                sky_cover = str(cloud.repr).strip().upper()
                            elif hasattr(cloud, 'cover') and cloud.cover:
                                sky_cover = str(cloud.cover).strip().upper()
                            elif hasattr(cloud, '__str__'):
                                sky_cover = str(cloud).strip().upper()
                            
                            # base is the cloud base altitude in hundreds of feet (AVWX format)
                            # Need to convert to actual feet by multiplying by 100
                            if hasattr(cloud, 'base') and cloud.base is not None:
                                try:
                                    if isinstance(cloud.base, (int, float)):
                                        # AVWX returns base in hundreds of feet, convert to actual feet
                                        cloud_base = int(cloud.base * 100)
                                    else:
                                        cloud_base = int(float(cloud.base) * 100)
                                except (ValueError, TypeError):
                                    pass
                            
                            # type is optional cloud type (e.g., "CB", "TCU")
                            if hasattr(cloud, 'type') and cloud.type:
                                cloud_type = str(cloud.type).strip()
                            
                            # Only add if we have sky cover (skip None/empty)
                            if sky_cover and sky_cover not in ['', 'NONE', 'NULL']:
                                sky_conditions.append({
                                    "skyCover": sky_cover,
                                    "cloudBase": cloud_base,
                                    "cloudType": cloud_type
                                })
                except Exception as e:
                    logger.warning(f"Error extracting clouds: {str(e)}", exc_info=True)
            
            forecast_period = {
                "fcstTimeFrom": fcst_time_from,
                "fcstTimeTo": fcst_time_to,
                "changeIndicator": line_data.type if hasattr(line_data, 'type') else None,
                "windDirection": int(wind_dir) if wind_dir is not None else None,
                "windSpeed": int(wind_speed) if wind_speed is not None else None,
                "windGust": int(wind_gust) if wind_gust is not None else None,
                "visibility": float(visibility) if visibility is not None else None,
                "skyConditions": sky_conditions,
                "flightCategory": line_data.flight_rules if hasattr(line_data, 'flight_rules') else None
            }
            forecasts.append(forecast_period)
            
            # Log what we extracted
            logger.info(f"Extracted forecast {idx}: time={fcst_time_from} to {fcst_time_to}, vis={visibility}, clouds={len(sky_conditions)}")
            
        logger.info(f"AVWX parsed {len(forecasts)} forecast periods from raw TAF")
        
    except Exception as e:
        logger.error(f"Error parsing TAF with AVWX: {str(e)}", exc_info=True)
        return _create_fallback_forecast(taf_data)
    
    return forecasts if forecasts else _create_fallback_forecast(taf_data)


def _parse_visibility_string(vis_str: str) -> Optional[float]:
    """Parse visibility string from AVWX (e.g., 'P6SM', '6SM', '10SM', '1/2SM')."""
    if not vis_str:
        return None
    
    vis_str = str(vis_str).strip().upper()
    
    # Handle "P6SM" or "P10SM" (greater than X SM)
    if vis_str.startswith('P') and vis_str.endswith('SM'):
        # P6SM means >6 SM, return 6.1
        match = re.search(r'P(\d+\.?\d*)SM', vis_str)
        if match:
            return float(match.group(1)) + 0.1
    
    # Handle regular "6SM" or "10SM"
    if vis_str.endswith('SM'):
        match = re.search(r'(\d+\.?\d*)SM', vis_str)
        if match:
            return float(match.group(1))
    
    # Handle fractions like "1/2SM"
    if '/' in vis_str and vis_str.endswith('SM'):
        match = re.search(r'(\d+)/(\d+)SM', vis_str)
        if match:
            numerator = float(match.group(1))
            denominator = float(match.group(2))
            if denominator > 0:
                return numerator / denominator
    
    # Try to extract any number
    match = re.search(r'(\d+\.?\d*)', vis_str)
    if match:
        return float(match.group(1))
    
    return None


def _create_fallback_forecast(taf_data: Dict) -> list:
    """Create a single forecast period from main TAF data as fallback."""
    return [{
        "fcstTimeFrom": _convert_time_to_iso(taf_data.get("validTimeFrom", "")),
        "fcstTimeTo": _convert_time_to_iso(taf_data.get("validTimeTo", "")),
        "changeIndicator": None,
        "windDirection": taf_data.get("wdir", None),
        "windSpeed": taf_data.get("wspd", None),
        "windGust": taf_data.get("wspdGust", None),
        "visibility": taf_data.get("visib", None),
        "skyConditions": _parse_taf_sky_conditions(taf_data),
        "flightCategory": taf_data.get("flightCategory", taf_data.get("flightcat", None))
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
