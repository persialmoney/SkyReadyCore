"""
Lambda function for fetching weather data (METAR, TAF, NOTAMs) from ElastiCache ValKey or external APIs.
This function is used as an AppSync Lambda resolver.
Uses cache-first strategy: checks ElastiCache, falls back to API if cache miss.
"""
import json
import os
import boto3
from datetime import datetime
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
    """Get or create Redis client connection."""
    global redis_client
    if not ELASTICACHE_ENDPOINT:
        return None
    
    if redis_client is None:
        try:
            redis_client = redis.Redis(
                host=ELASTICACHE_ENDPOINT,
                port=ELASTICACHE_PORT,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
                retry_on_timeout=False,
            )
            redis_client.ping()
            logger.info(f"Connected to ElastiCache at {ELASTICACHE_ENDPOINT}:{ELASTICACHE_PORT}")
        except Exception as e:
            logger.warning(f"Failed to connect to ElastiCache: {str(e)}, will use API fallback")
            redis_client = None
    return redis_client


def fetch_metar(airport_code: str) -> Dict[str, Any]:
    """
    Fetch METAR data for an airport.
    Cache-first strategy: checks ElastiCache, falls back to API if cache miss.
    """
    airport_code = airport_code.upper()
    
    # Try to get from cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cache_key = f"metar:{airport_code}"
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info(f"Cache hit for METAR: {airport_code}")
                metar_data = json.loads(cached_data)
                # Transform to expected format
                return transform_metar_from_cache(metar_data, airport_code)
        except Exception as e:
            logger.warning(f"Cache read error for {airport_code}: {str(e)}, falling back to API")
    
    # Cache miss or error - fetch from API
    logger.info(f"Cache miss for METAR: {airport_code}, fetching from API")
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
    
    # Try to get from cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cache_key = f"taf:{airport_code}"
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info(f"Cache hit for TAF: {airport_code}")
                taf_data = json.loads(cached_data)
                return transform_taf_from_cache(taf_data, airport_code)
        except Exception as e:
            logger.warning(f"Cache read error for TAF {airport_code}: {str(e)}, falling back to API")
    
    # Cache miss or error - fetch from API
    logger.info(f"Cache miss for TAF: {airport_code}, fetching from API")
    try:
        url = f"{TAF_URL}?ids={airport_code}&format=json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
            if not data or len(data) == 0:
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "No data available",
                    "error": "No TAF data found for this airport"
                }
            
            taf = data[0]
            
            # Log the TAF structure for debugging
            logger.info(f"TAF data keys: {list(taf.keys())}")
            if "forecast" in taf:
                logger.info(f"Forecast type: {type(taf['forecast'])}, length: {len(taf['forecast']) if isinstance(taf['forecast'], list) else 'N/A'}")
            
            parsed_forecast = parse_taf_forecast(taf)
            logger.info(f"Parsed {len(parsed_forecast)} forecast periods")
            
            result = {
                "airportCode": airport_code,
                "rawText": taf.get("rawTAF", taf.get("rawText", "")),
                "issueTime": taf.get("issueTime", datetime.utcnow().isoformat()),
                "validTimeFrom": taf.get("validTimeFrom", ""),
                "validTimeTo": taf.get("validTimeTo", ""),
                "remarks": taf.get("remarks", ""),
                "forecast": parsed_forecast
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
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Error fetching TAF data",
            "error": f"Network error: {str(e)}"
        }
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for TAF {airport_code}: {str(e)}")
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Error parsing TAF data",
            "error": f"Parse error: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error fetching TAF for {airport_code}: {str(e)}")
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Error fetching TAF data",
            "error": f"Unexpected error: {str(e)}"
        }


def transform_taf_from_cache(taf_data: Dict[str, Any], airport_code: str) -> Dict[str, Any]:
    """Transform cached TAF data to expected format."""
    return {
        "airportCode": airport_code,
        "rawText": taf_data.get("rawTAF", ""),
        "issueTime": taf_data.get("issueTime", datetime.utcnow().isoformat()),
        "validTimeFrom": taf_data.get("validTimeFrom", ""),
        "validTimeTo": taf_data.get("validTimeTo", ""),
        "remarks": taf_data.get("remarks", ""),
        "forecast": parse_taf_forecast(taf_data)
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
    """Parse TAF forecast periods from AWC API response."""
    forecasts = []
    
    # AWC TAF API can return forecast periods in different formats
    # Check for structured forecast array first
    if "forecast" in taf and isinstance(taf["forecast"], list) and len(taf["forecast"]) > 0:
        logger.info(f"Found {len(taf['forecast'])} forecast periods in structured format")
        for idx, fcst in enumerate(taf["forecast"]):
            logger.info(f"Forecast {idx} keys: {list(fcst.keys()) if isinstance(fcst, dict) else 'not a dict'}")
            forecast_period = {
                "fcstTimeFrom": _convert_time_to_iso(fcst.get("fcstTimeFrom", fcst.get("validTimeFrom", fcst.get("timeFrom", "")))),
                "fcstTimeTo": _convert_time_to_iso(fcst.get("fcstTimeTo", fcst.get("validTimeTo", fcst.get("timeTo", "")))),
                "changeIndicator": fcst.get("changeIndicator", fcst.get("changeind", fcst.get("changeIndicator", None))),
                "windDirection": fcst.get("wdir", fcst.get("windDirection", None)),
                "windSpeed": fcst.get("wspd", fcst.get("windSpeed", None)),
                "windGust": fcst.get("wspdGust", fcst.get("windGust", None)),
                "visibility": fcst.get("visib", fcst.get("visibility", None)),
                "skyConditions": _parse_taf_sky_conditions(fcst),
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
        for line_data in taf_obj.data.forecast:
            # Extract timestamps (convert to ISO strings)
            fcst_time_from = ""
            if line_data.start_time and hasattr(line_data.start_time, 'dt') and line_data.start_time.dt:
                fcst_time_from = line_data.start_time.dt.isoformat() + "Z"
            
            fcst_time_to = ""
            if line_data.end_time and hasattr(line_data.end_time, 'dt') and line_data.end_time.dt:
                fcst_time_to = line_data.end_time.dt.isoformat() + "Z"
            
            # Extract wind values (Number objects have .value property)
            wind_dir = line_data.wind_direction.value if (line_data.wind_direction and hasattr(line_data.wind_direction, 'value')) else None
            wind_speed = line_data.wind_speed.value if (line_data.wind_speed and hasattr(line_data.wind_speed, 'value')) else None
            wind_gust = line_data.wind_gust.value if (line_data.wind_gust and hasattr(line_data.wind_gust, 'value')) else None
            
            # Extract visibility
            visibility = line_data.visibility.value if (line_data.visibility and hasattr(line_data.visibility, 'value')) else None
            
            # Convert clouds to dicts
            sky_conditions = []
            if line_data.clouds:
                for cloud in line_data.clouds:
                    if cloud:
                        sky_conditions.append({
                            "skyCover": str(cloud.repr) if hasattr(cloud, 'repr') and cloud.repr else None,
                            "cloudBase": int(cloud.base) if hasattr(cloud, 'base') and cloud.base is not None else None,
                            "cloudType": str(cloud.type) if hasattr(cloud, 'type') and cloud.type else None
                        })
            
            forecast_period = {
                "fcstTimeFrom": fcst_time_from,
                "fcstTimeTo": fcst_time_to,
                "changeIndicator": line_data.type,  # Already a string or None
                "windDirection": int(wind_dir) if wind_dir is not None else None,
                "windSpeed": int(wind_speed) if wind_speed is not None else None,
                "windGust": int(wind_gust) if wind_gust is not None else None,
                "visibility": float(visibility) if visibility is not None else None,
                "skyConditions": sky_conditions,
                "flightCategory": line_data.flight_rules  # Already a string
            }
            forecasts.append(forecast_period)
            
        logger.info(f"AVWX parsed {len(forecasts)} forecast periods from raw TAF")
        
    except Exception as e:
        logger.error(f"Error parsing TAF with AVWX: {str(e)}", exc_info=True)
        return _create_fallback_forecast(taf_data)
    
    return forecasts if forecasts else _create_fallback_forecast(taf_data)


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
