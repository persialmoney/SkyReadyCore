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
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import urllib.request
import urllib.error
from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
)
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

# AVWX API for AIR/SIGMET advisories
AVWX_BASE_URL = "https://avwx.rest/api"
_AVWX_SECRET_NAME = os.environ.get('AVWX_SECRET_NAME', 'sky-ready/avwx-token')
_avwx_token_cache: str = ''  # module-level cache — reused across warm invocations

def _get_avwx_token() -> str:
    """Fetch the AVWX token from Secrets Manager, caching it for the lifetime of the container."""
    global _avwx_token_cache
    if _avwx_token_cache:
        return _avwx_token_cache
    try:
        sm = boto3.client('secretsmanager')
        resp = sm.get_secret_value(SecretId=_AVWX_SECRET_NAME)
        _avwx_token_cache = resp.get('SecretString', '')
        return _avwx_token_cache
    except Exception as e:
        logger.error(f"[AIRMET] Failed to fetch AVWX token from Secrets Manager: {e}")
        return ''

# Glide client (lazy initialization)
glide_client = None


async def get_glide_client() -> Optional[GlideClusterClient]:
    """
    Get or create Glide cluster client connection.
    """
    global glide_client
    if not ELASTICACHE_ENDPOINT:
        logger.info("[ElastiCache] No endpoint configured")
        return None
    
    if glide_client is not None:
        logger.info("[ElastiCache] Checking existing connection")
        try:
            # Use 1-second timeout for ping to fail fast if connection is stale
            await asyncio.wait_for(glide_client.ping(), timeout=1.0)
            logger.info("[ElastiCache] Existing connection is valid")
            return glide_client
        except asyncio.TimeoutError:
            logger.warning("[ElastiCache] Ping timeout - connection is stale, closing and creating new")
            try:
                await glide_client.close()
            except:
                pass
            glide_client = None
        except Exception as e:
            logger.warning(f"[ElastiCache] Existing connection failed ping: {str(e)}, creating new")
            try:
                await glide_client.close()
            except:
                pass
            glide_client = None
    
    logger.info(f"[ElastiCache] Creating new connection to {ELASTICACHE_ENDPOINT}:{ELASTICACHE_PORT}")
    try:
        config = GlideClusterClientConfiguration(
            addresses=[NodeAddress(ELASTICACHE_ENDPOINT, ELASTICACHE_PORT)],
            use_tls=True,
            request_timeout=10000,
        )
        logger.info("[ElastiCache] Configuration created, initializing client")
        glide_client = await GlideClusterClient.create(config)
        logger.info("[ElastiCache] Client created successfully")
        return glide_client
    except Exception as e:
        logger.error(f"[ElastiCache] Connection failed: {str(e)}")
        import traceback
        logger.error(f"[ElastiCache] Traceback: {traceback.format_exc()}")
        glide_client = None
        return None


async def fetch_metar(airport_code: str) -> Dict[str, Any]:
    """
    Fetch METAR data for an airport.
    Cache-first strategy: checks ElastiCache, falls back to API if cache miss.
    """
    logger.info(f"[METAR] Starting fetch for {airport_code}")
    airport_code = airport_code.upper()
    cache_key = f"metar:{airport_code}"
    
    # Try to get from cache first
    logger.info(f"[METAR] Getting Glide client for {airport_code}")
    glide_client = await get_glide_client()
    if glide_client:
        logger.info(f"[METAR] Glide client obtained, checking cache for {airport_code}")
        try:
            cached_data = await glide_client.get(cache_key)
            if cached_data:
                logger.info(f"[METAR] Cache hit for {airport_code}, transforming data")
                if isinstance(cached_data, bytes):
                    cached_data = cached_data.decode('utf-8')
                metar_data = json.loads(cached_data)
                result = transform_metar_from_cache(metar_data, airport_code)
                logger.info(f"[METAR] Successfully returned cached METAR for {airport_code}")
                return result
            else:
                logger.info(f"[METAR] Cache miss for {airport_code}")
        except Exception as e:
            logger.warning(f"[METAR] Cache read error for {airport_code}: {str(e)}")
    else:
        logger.info(f"[METAR] No Glide client available for {airport_code}, fetching from API")
    
    # Cache miss or error - fetch from API
    logger.info(f"[METAR] Fetching from API for {airport_code}")
    try:
        # Use decoded format to get structured fields like skyc1, skyl1, etc.
        url = f"{METAR_URL}?ids={airport_code}&format=json&taf=false&hours=1"
        logger.info(f"[METAR] Making API request to {url}")
        with urllib.request.urlopen(url, timeout=10) as response:
            logger.info(f"[METAR] API response received for {airport_code}, parsing data")
            data = json.loads(response.read().decode())
            logger.info(f"[METAR] Parsed API response for {airport_code}, {len(data)} records")
            
            if not data or len(data) == 0:
                # METAR not found - return user-friendly message
                logger.info(f"METAR not found for {airport_code} - API returned empty data")
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "METAR not found for this airport",
                    "observationTime": datetime.utcnow().isoformat(),
                    "error": "No METAR data found for this airport"
                }
            
            # Parse the first METAR (most recent)
            metar = data[0]
            logger.info(f"[METAR] Processing METAR data for {airport_code}")
            
            # Check if METAR data is actually valid (has rawText)
            raw_text = metar.get("rawOb", "") or metar.get("rawText", "")
            if not raw_text or raw_text.strip() == "":
                # METAR data exists but has no content - treat as not found
                logger.info(f"METAR data for {airport_code} has no rawText - treating as not found")
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "METAR not found for this airport",
                    "observationTime": datetime.utcnow().isoformat(),
                    "error": "No METAR data found for this airport"
                }
            
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
            
            logger.info(f"[METAR] Parsing sky conditions for {airport_code}")
            try:
                sky_conditions = parse_sky_conditions(metar)
            except Exception as parse_error:
                # If parsing fails, log it but treat as "not found" rather than error
                logger.warning(f"Failed to parse METAR sky conditions for {airport_code}: {str(parse_error)}")
                logger.info(f"Treating parsing failure as METAR not found for {airport_code}")
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "METAR not found for this airport",
                    "observationTime": datetime.utcnow().isoformat(),
                    "error": "No METAR data found for this airport"
                }
            
            logger.info(f"[METAR] Sky conditions parsed for {airport_code}, returning result")
            return {
                "airportCode": airport_code.upper(),
                "rawText": raw_text,
                "observationTime": obs_time,
                "temperature": metar.get("temp", None),
                "dewpoint": metar.get("dewp", None),
                "windDirection": metar.get("wdir", None),
                "windSpeed": metar.get("wspd", None),
                "windGust": wind_gust,
                "visibility": visibility,
                "altimeter": altim_inhg,
                "skyConditions": sky_conditions,
                "flightCategory": metar.get("flightCategory", None),
                "metarType": metar.get("metarType", None),
                "elevation": metar.get("elev", None)
            }
    except urllib.error.URLError as e:
        logger.error(f"[METAR] URL error for {airport_code}: {str(e)}")
        import traceback
        logger.error(f"[METAR] Traceback: {traceback.format_exc()}")
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Unable to retrieve METAR data",
            "observationTime": datetime.utcnow().isoformat(),
            "error": str(e)
        }
    except Exception as e:
        logger.error(f"[METAR] Exception processing METAR for {airport_code}: {str(e)}")
        import traceback
        logger.error(f"[METAR] Traceback: {traceback.format_exc()}")
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Unable to retrieve METAR data",
            "observationTime": datetime.utcnow().isoformat(),
            "error": str(e)
        }


def transform_metar_from_cache(metar_data: Dict[str, Any], airport_code: str) -> Dict[str, Any]:
    """Transform cached METAR data to expected format."""
    # Check if this is an error state from cache
    raw_text = metar_data.get("rawOb", "") or metar_data.get("rawText", "") or ""
    
    # If rawText indicates an error state or is empty, preserve it and skip parsing
    if raw_text in ["METAR not found for this airport", "Unable to retrieve METAR data"] or not raw_text.strip():
        if not raw_text.strip():
            # Empty rawText in cache - treat as not found
            raw_text = "METAR not found for this airport"
        return {
            "airportCode": airport_code,
            "rawText": raw_text,
            "observationTime": datetime.utcnow().isoformat(),
            "temperature": None,
            "dewpoint": None,
            "windDirection": None,
            "windSpeed": None,
            "windGust": None,
            "visibility": None,
            "altimeter": None,
            "skyConditions": [],
            "flightCategory": None,
            "metarType": None,
            "elevation": None
        }
    
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
        obs_time = datetime.utcnow().isoformat() + 'Z'
    
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
    
    # Try to parse sky conditions, but handle parsing errors gracefully
    try:
        sky_conditions = parse_sky_conditions(metar_data)
    except Exception as parse_error:
        # If parsing fails, log it but treat as "not found" rather than error
        logger.warning(f"Failed to parse cached METAR sky conditions for {airport_code}: {str(parse_error)}")
        logger.info(f"Treating parsing failure as METAR not found for {airport_code}")
        return {
            "airportCode": airport_code,
            "rawText": "METAR not found for this airport",
            "observationTime": str(obs_time).strip(),
            "temperature": None,
            "dewpoint": None,
            "windDirection": None,
            "windSpeed": None,
            "windGust": None,
            "visibility": None,
            "altimeter": None,
            "skyConditions": [],
            "flightCategory": None,
            "metarType": None,
            "elevation": None
        }
    
    result = {
        "airportCode": airport_code,
        "rawText": raw_text if raw_text else metar_data.get("rawOb", metar_data.get("raw_text", "")),
        "observationTime": str(obs_time).strip(),  # Ensure it's always a string
        "temperature": metar_data.get("temp", None),
        "dewpoint": metar_data.get("dewp", None),
        "windDirection": metar_data.get("wdir", None),
        "windSpeed": metar_data.get("wspd", None),
        "windGust": wind_gust,
        "visibility": visibility,
        "altimeter": altim_inhg,
        "skyConditions": sky_conditions,
        "flightCategory": metar_data.get("flightCategory", None),
        "metarType": metar_data.get("metarType", None),
        "elevation": metar_data.get("elev", None)
    }
    
    logger.info(f"Transformed METAR result - visibility: {result.get('visibility')}, obsTime: {result.get('observationTime')}")
    
    return result


async def fetch_taf(airport_code: str) -> Dict[str, Any]:
    """
    Fetch TAF data for an airport.
    Cache-first strategy: checks ElastiCache, falls back to API if cache miss.
    """
    airport_code = airport_code.upper()
    cache_key = f"taf:{airport_code}"
    
    # Try to get from cache first
    glide_client = await get_glide_client()
    if glide_client:
        try:
            cached_data = await glide_client.get(cache_key)
            if cached_data:
                if isinstance(cached_data, bytes):
                    cached_data = cached_data.decode('utf-8')
                taf_data = json.loads(cached_data)
                return transform_taf_from_cache(taf_data, airport_code)
        except Exception:
            pass
    
    # Cache miss or error - fetch from API
    try:
        url = f"{TAF_URL}?ids={airport_code}&format=json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            
            if not data or len(data) == 0:
                # TAF not found - return user-friendly message
                logger.info(f"TAF not found for {airport_code} - API returned empty data")
                current_time = datetime.utcnow().isoformat()
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "TAF not found for this airport",
                    "issueTime": current_time,
                    "validTimeFrom": current_time,
                    "validTimeTo": current_time,
                    "remarks": "",
                    "forecast": []  # Empty list is valid for [TAFForecast!]!
                }
            
            taf = data[0]
            
            # Check if TAF data is actually valid (has rawText)
            raw_text = taf.get("rawTAF") or taf.get("rawText") or ""
            if not raw_text or raw_text.strip() == "":
                # TAF data exists but has no content - treat as not found
                logger.info(f"TAF data for {airport_code} has no rawText - treating as not found")
                current_time = datetime.utcnow().isoformat()
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "TAF not found for this airport",
                    "issueTime": current_time,
                    "validTimeFrom": current_time,
                    "validTimeTo": current_time,
                    "remarks": "",
                    "forecast": []  # Empty list is valid for [TAFForecast!]!
                }
            
            # Try to parse forecast, but handle parsing errors gracefully
            try:
                parsed_forecast = parse_taf_forecast(taf)
            except Exception as parse_error:
                # If parsing fails, log it but treat as "not found" rather than error
                logger.warning(f"Failed to parse TAF forecast for {airport_code}: {str(parse_error)}")
                logger.info(f"Treating parsing failure as TAF not found for {airport_code}")
                current_time = datetime.utcnow().isoformat()
                return {
                    "airportCode": airport_code.upper(),
                    "rawText": "TAF not found for this airport",
                    "issueTime": current_time,
                    "validTimeFrom": current_time,
                    "validTimeTo": current_time,
                    "remarks": "",
                    "forecast": []  # Empty list is valid for [TAFForecast!]!
                }
            
            # Ensure all non-nullable fields have values (never None)
            current_time = datetime.utcnow().isoformat()
            result = {
                "airportCode": airport_code,
                "rawText": raw_text,
                "issueTime": taf.get("issueTime") or current_time,
                "validTimeFrom": taf.get("validTimeFrom") or current_time,
                "validTimeTo": taf.get("validTimeTo") or current_time,
                "remarks": taf.get("remarks") or "",
                "forecast": parsed_forecast if parsed_forecast else []  # Ensure it's always a list
            }
            
            # Write-through: Store in cache for next request
            if glide_client:
                try:
                    cache_key = f"taf:{airport_code}"
                    await glide_client.set(cache_key, json.dumps(taf))
                    await glide_client.expire(cache_key, 3600)  # 1 hour
                except Exception:
                    pass
            
            return result
    except urllib.error.URLError as e:
        logger.error(f"Network error fetching TAF for {airport_code}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        current_time = datetime.utcnow().isoformat()
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Unable to retrieve TAF data",
            "issueTime": current_time,
            "validTimeFrom": current_time,
            "validTimeTo": current_time,
            "remarks": "",
            "forecast": []  # Empty list is valid for [TAFForecast!]!
        }
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for TAF {airport_code}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        current_time = datetime.utcnow().isoformat()
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Unable to retrieve TAF data",
            "issueTime": current_time,
            "validTimeFrom": current_time,
            "validTimeTo": current_time,
            "remarks": "",
            "forecast": []  # Empty list is valid for [TAFForecast!]!
        }
    except Exception as e:
        logger.error(f"Unexpected error fetching TAF for {airport_code}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        current_time = datetime.utcnow().isoformat()
        return {
            "airportCode": airport_code.upper(),
            "rawText": "Unable to retrieve TAF data",
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
    
    # Check if this is an error state from cache
    raw_text = taf_data.get("rawTAF") or taf_data.get("rawText") or ""
    
    # If rawText indicates an error state or is empty, preserve it and skip parsing
    if raw_text in ["TAF not found for this airport", "Unable to retrieve TAF data"] or not raw_text.strip():
        if not raw_text.strip():
            # Empty rawText in cache - treat as not found
            raw_text = "TAF not found for this airport"
        return {
            "airportCode": airport_code,
            "rawText": raw_text,
            "issueTime": taf_data.get("issueTime") or current_time,
            "validTimeFrom": taf_data.get("validTimeFrom") or current_time,
            "validTimeTo": taf_data.get("validTimeTo") or current_time,
            "remarks": taf_data.get("remarks") or "",
            "forecast": []  # Empty list for error/not found states
        }
    
    # Try to parse forecast, but handle parsing errors gracefully
    try:
        parsed_forecast = parse_taf_forecast(taf_data)
    except Exception as parse_error:
        # If parsing fails, log it but treat as "not found" rather than error
        logger.warning(f"Failed to parse cached TAF forecast for {airport_code}: {str(parse_error)}")
        logger.info(f"Treating parsing failure as TAF not found for {airport_code}")
        return {
            "airportCode": airport_code,
            "rawText": "TAF not found for this airport",
            "issueTime": taf_data.get("issueTime") or current_time,
            "validTimeFrom": taf_data.get("validTimeFrom") or current_time,
            "validTimeTo": taf_data.get("validTimeTo") or current_time,
            "remarks": taf_data.get("remarks") or "",
            "forecast": []  # Empty list for parsing failures
        }
    
    return {
        "airportCode": airport_code,
        "rawText": raw_text,
        "issueTime": taf_data.get("issueTime") or current_time,
        "validTimeFrom": taf_data.get("validTimeFrom") or current_time,
        "validTimeTo": taf_data.get("validTimeTo") or current_time,
        "remarks": taf_data.get("remarks") or "",
        "forecast": parsed_forecast if parsed_forecast else []  # Ensure it's always a list
    }


def _map_notam_category(report: dict) -> str:
    """Map AVWX NOTAM type/subject to a frontend category slug."""
    subject = (report.get("subject") or {}).get("value", "").lower()
    body = (report.get("body") or "").lower()
    if subject in ("runway", "rwy"):
        return "runway"
    if subject in ("taxiway", "twy"):
        return "taxiway"
    if "ils" in body or "glideslope" in body or "localizer" in body or "approach" in body or "nav" in subject:
        return "approach"
    if "light" in body or "papi" in body or "vasi" in body:
        return "lighting"
    if "crane" in body or "tower" in body or "obstacle" in body or "obst" in body:
        return "obstacle"
    if "procedure" in body or "chart" in body or "dp " in body or "star" in body:
        return "procedure"
    return "other"


def _derive_notam_severity(body: str) -> str:
    """Derive high/medium/low severity from NOTAM body text."""
    b = body.upper()
    if b.startswith("!") or "GFM" in b or "RUNWAY CLSD" in b or "AD CLSD" in b:
        return "high"
    if "ILS" in b or "GLIDESLOPE" in b or "LOCALIZER" in b or "PAPI" in b or "VASI" in b:
        return "medium"
    return "low"


def _derive_notam_title(report: dict) -> str:
    """Build a human-readable title when AVWX doesn't provide one."""
    subject = (report.get("subject") or {}).get("repr", "")
    condition = (report.get("condition") or {}).get("repr", "")
    station = report.get("station") or ""
    parts = [p for p in [station, subject, condition] if p]
    return " — ".join(parts) if parts else "NOTAM"


async def fetch_notams(airport_code: str) -> list:
    """
    Fetch active NOTAMs for an airport via the AVWX REST API.
    Returns an empty list on error or when no NOTAMs are active.
    """
    airport_code = airport_code.upper()
    token = _get_avwx_token()
    if not token:
        logger.warning("[NOTAM] AVWX token not available, skipping NOTAM fetch")
        return []

    url = f"{AVWX_BASE_URL}/notam/{airport_code}"
    req = urllib.request.Request(url, headers={"Authorization": f"Token {token}"})

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())

        raw_reports = data.get("reports") or data.get("results") or data.get("data") or []
        results = []
        skipped = 0
        for report in raw_reports:
            end_time = _dt(report.get("end_time"))
            if _is_expired(end_time):
                skipped += 1
                continue

            body = report.get("body") or report.get("raw") or ""
            results.append({
                "id": report.get("number") or report.get("id") or "",
                "title": report.get("title") or _derive_notam_title(report),
                "description": body,
                "category": _map_notam_category(report),
                "severity": _derive_notam_severity(body),
                "effectiveStart": _dt(report.get("start_time")),
                "effectiveEnd": end_time,
                "rawText": report.get("raw") or body,
                "whyShown": report.get("reason"),
            })

        logger.info(f"[NOTAM] Fetched {len(results)} active NOTAMs for {airport_code} (skipped {skipped} expired)")
        return results

    except urllib.error.HTTPError as e:
        if e.code in (404, 204):
            logger.info(f"[NOTAM] No active NOTAMs for {airport_code}")
            return []
        logger.error(f"[NOTAM] HTTP error for {airport_code}: {e.code} {e.reason}")
        return []
    except Exception as e:
        logger.error(f"[NOTAM] Error for {airport_code}: {str(e)}")
        return []


def _summarise_clouds(clouds: list) -> str | None:
    """Convert AVWX cloud list to a compact readable string."""
    if not clouds:
        return None
    parts = []
    for c in clouds:
        ctype = c.get("type", "")
        base = c.get("base")
        top = c.get("top")
        if base is not None:
            parts.append(f"{ctype}{base:03d}" + (f"-TOP{top:03d}" if top else ""))
        else:
            parts.append(ctype)
    return " ".join(parts) if parts else None


async def fetch_pireps(airport_code: str, radius: int = 100) -> list:
    """
    Fetch recent PIREPs near an airport via the AVWX REST API.
    Returns an empty list on error or when no PIREPs are available.
    """
    airport_code = airport_code.upper()
    token = _get_avwx_token()
    if not token:
        logger.warning("[PIREP] AVWX token not available, skipping PIREP fetch")
        return []

    url = f"{AVWX_BASE_URL}/pirep/{airport_code}?radius={radius}"
    req = urllib.request.Request(url, headers={"Authorization": f"Token {token}"})

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            raw = response.read()

        if not raw.strip():
            logger.info(f"[PIREP] No PIREPs near {airport_code}")
            return []

        data = json.loads(raw.decode())
        raw_reports = data.get("data") or data.get("reports") or data.get("results") or []

        results = []
        for report in raw_reports:
            time_obj = report.get("time") or {}
            turb = report.get("turbulence") or {}
            icing = report.get("icing") or {}
            temp_obj = report.get("temperature") or {}

            results.append({
                "raw": report.get("raw", ""),
                "reportType": report.get("type", "UA"),
                "time": time_obj.get("dt"),
                "location": (report.get("location") or {}).get("repr"),
                "altitude": report.get("altitude"),
                "aircraftType": (report.get("aircraft") or {}).get("code"),
                "turbulenceSeverity": turb.get("severity") if turb else None,
                "icingSeverity": icing.get("severity") if icing else None,
                "skyConditions": _summarise_clouds(report.get("clouds") or []),
                "temperature": temp_obj.get("value") if isinstance(temp_obj, dict) else None,
                "remarks": report.get("remarks"),
            })

        logger.info(f"[PIREP] Fetched {len(results)} PIREPs near {airport_code} (radius={radius}nm)")
        return results

    except urllib.error.HTTPError as e:
        if e.code in (404, 204):
            logger.info(f"[PIREP] No PIREPs near {airport_code}")
            return []
        logger.error(f"[PIREP] HTTP error for {airport_code}: {e.code} {e.reason}")
        return []
    except Exception as e:
        logger.error(f"[PIREP] Error for {airport_code}: {str(e)}")
        return []


def parse_sky_conditions(metar: Dict) -> list:
    """Parse sky conditions from METAR data."""
    logger.info("[METAR] Starting parse_sky_conditions")
    sky_conditions = []
    
    # AWC API provides sky conditions with fields like:
    # skyc1, skyc2, skyc3, skyc4 (sky cover codes: FEW, SCT, BKN, OVC, CLR, etc.)
    # skyl1, skyl2, skyl3, skyl4 (cloud base levels in HUNDREDS of feet, e.g., 25 = 2500ft)
    # skyt1, skyt2, skyt3, skyt4 (cloud types, optional)
    
    sky_fields = {k: v for k, v in metar.items() if k.startswith('sky')}
    logger.info(f"[METAR] Found {len(sky_fields)} sky condition fields")
    
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
            logger.info("[METAR] No sky conditions found, returning CLR")
            return [{"skyCover": "CLR", "cloudBase": None, "cloudType": None}]
    
    logger.info(f"[METAR] parse_sky_conditions completed, returning {len(sky_conditions)} conditions")
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


async def fetch_airmets(airport_code: str) -> list:
    """
    Fetch active AIRMET and SIGMET advisories that contain the given airport
    using the AVWX REST API /airsigmet/{location} endpoint.
    Returns an empty list on error or when no advisories are active.
    """
    airport_code = airport_code.upper()

    token = _get_avwx_token()
    if not token:
        logger.warning("[AIRMET] AVWX token not available, skipping advisory fetch")
        return []

    url = f"{AVWX_BASE_URL}/airsigmet/{airport_code}"
    req = urllib.request.Request(url, headers={"Authorization": f"Token {token}"})

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())

        def _dt(obj):
            """Safely pull the ISO datetime string from a time object."""
            if not obj:
                return None
            return obj.get("dt")

        def _alt(obj):
            """Extract altitude value from either an int or an {repr, value} object."""
            if obj is None:
                return None
            if isinstance(obj, dict):
                return obj.get("value")
            return int(obj)

        def _is_expired(end_time_str):
            """Return True if the advisory end time is in the past."""
            if not end_time_str:
                return False  # no end time — assume still active
            try:
                end_dt = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
                return end_dt < datetime.now(timezone.utc)
            except Exception:
                return False

        # API returns either "reports" or "results" depending on the response shape
        raw_reports = data.get("reports") or data.get("results") or []
        results = []
        skipped = 0
        for report in raw_reports:
            bulletin = report.get("bulletin") or {}
            report_type = (bulletin.get("type") or {}).get("value", "")

            # start_time / end_time at the report level are often null;
            # prefer the observation window, fall back to forecast window
            obs = report.get("observation") or {}
            forecast = report.get("forecast") or {}

            end_time = (
                _dt(report.get("end_time"))
                or _dt(obs.get("end_time"))
                or _dt(forecast.get("end_time"))
            )

            # Skip expired advisories — no point surfacing stale data
            if _is_expired(end_time):
                skipped += 1
                continue

            start_time = (
                _dt(report.get("start_time"))
                or _dt(obs.get("start_time"))
                or _dt(forecast.get("start_time"))
            )

            floor_val = _alt(obs.get("floor")) if obs.get("floor") is not None else _alt((forecast or {}).get("floor"))
            ceiling_val = _alt(obs.get("ceiling")) if obs.get("ceiling") is not None else _alt((forecast or {}).get("ceiling"))
            obs_type = (obs.get("type") or {}).get("value") or (forecast.get("type") or {}).get("value")

            results.append({
                "reportType": report_type,
                "type": report.get("type", ""),
                "area": report.get("area"),
                "startTime": start_time,
                "endTime": end_time,
                "observationType": obs_type,
                "floor": floor_val,
                "ceiling": ceiling_val,
                "raw": report.get("raw", ""),
            })

        logger.info(f"[AIRMET] Fetched {len(results)} active advisories for {airport_code} (skipped {skipped} expired)")
        return results

    except urllib.error.HTTPError as e:
        if e.code == 404:
            # No advisories found for this airport — normal, not an error
            logger.info(f"[AIRMET] No active advisories for {airport_code} (404)")
            return []
        logger.error(f"[AIRMET] HTTP error fetching for {airport_code}: {e.code} {e.reason}")
        return []
    except Exception as e:
        logger.error(f"[AIRMET] Error fetching for {airport_code}: {str(e)}")
        return []


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
    
    async def async_handler():
        import time
        start_time = time.time()
        logger.info(f"[Handler] Processing {field_name} request")
        try:
            if field_name == "getMETAR":
                airport_code = arguments.get("airportCode")
                if not airport_code:
                    raise ValueError("airportCode is required")
                logger.info(f"[Handler] Calling fetch_metar for {airport_code}")
                result = await fetch_metar(airport_code)
                elapsed = time.time() - start_time
                logger.info(f"[Handler] getMETAR completed for {airport_code} in {elapsed:.2f}s")
                return result
            
            elif field_name == "getTAF":
                airport_code = arguments.get("airportCode")
                if not airport_code:
                    raise ValueError("airportCode is required")
                return await fetch_taf(airport_code)
            
            elif field_name == "getNOTAMs":
                airport_code = arguments.get("airportCode")
                if not airport_code:
                    raise ValueError("airportCode is required")
                return await fetch_notams(airport_code)

            elif field_name == "getPireps":
                airport_code = arguments.get("airportCode")
                if not airport_code:
                    raise ValueError("airportCode is required")
                radius = arguments.get("radius", 100)
                return await fetch_pireps(airport_code, radius)

            elif field_name == "getAirSigmets":
                airport_code = arguments.get("airportCode")
                if not airport_code:
                    raise ValueError("airportCode is required")
                return await fetch_airmets(airport_code)

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
        finally:
            # Cleanup: close Glide client connection after handler completes
            # This prevents stale connections from persisting across Lambda invocations
            global glide_client
            if glide_client is not None:
                try:
                    logger.info("[Handler] Closing Glide client connection")
                    await glide_client.close()
                    logger.info("[Handler] Glide client connection closed successfully")
                except Exception as e:
                    logger.warning(f"[Handler] Error closing Glide client: {str(e)}")
                finally:
                    glide_client = None
    
    # Run async handler
    return asyncio.run(async_handler())
