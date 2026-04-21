"""
Lambda function for ingesting aviation weather data from AWC cache API into ElastiCache ValKey.
Downloads cache files, parses them, and stores data in ValKey with appropriate TTLs.
"""
import json
import os
import gzip
import csv
import hashlib
import xml.etree.ElementTree as ET
import urllib.request
import urllib.error
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional
from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    ExpirySet,
    ExpiryType,
)
import boto3
import logging

# Initialize CloudWatch client for custom metrics
cloudwatch_client = boto3.client('cloudwatch')

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

# Get environment variables
ELASTICACHE_ENDPOINT = os.environ.get('ELASTICACHE_ENDPOINT')
ELASTICACHE_PORT = int(os.environ.get('ELASTICACHE_PORT', 6379))
CACHE_FILES_BUCKET = os.environ.get('CACHE_FILES_BUCKET')
STAGE = os.environ.get('STAGE', 'dev')

# TTL values (in seconds)
TTL_METAR = 3600  # 1 hour
TTL_TAF = 3600  # 1 hour
TTL_SIGMET = 120  # 2 minutes (per-record; OPS-bar path tolerates churn)
TTL_AIRMET = 120  # 2 minutes (per-record; OPS-bar path tolerates churn)
# Bundle keys are read by the AdvisoryMap screen and must outlive a missed ingest
# cycle (ingest runs every 5 min; allow ~2 missed cycles before going dark).
TTL_BUNDLE = 900  # 15 minutes
TTL_PIREP = 120  # 2 minutes
TTL_STATION = 90000  # 25 hours

# Glide client (reused across invocations)
glide_client = None


async def get_glide_client():
    """
    Get or create Glide cluster client connection.
    Resets connection on failure to prevent stale connections.
    """
    global glide_client
    if glide_client is None:
        try:
            config = GlideClusterClientConfiguration(
                addresses=[NodeAddress(ELASTICACHE_ENDPOINT, ELASTICACHE_PORT)],
                use_tls=True,
                request_timeout=10000,
            )
            glide_client = await GlideClusterClient.create(config)
            logger.info("Successfully connected to ElastiCache")
        except Exception as e:
            logger.error(f"Failed to connect to ElastiCache: {str(e)}")
            glide_client = None  # Reset on failure
            raise
    return glide_client


def reset_glide_client():
    """
    Reset the global Glide client connection.
    Used when connection errors occur to force reconnection on next invocation.
    """
    global glide_client
    if glide_client is not None:
        logger.warning("Resetting Glide client connection due to error")
        glide_client = None


async def execute_operations_with_error_logging(operations: List, operation_name: str):
    """
    Execute async operations and log any exceptions instead of silently swallowing them.
    
    Args:
        operations: List of async operations to execute
        operation_name: Name of the operation type for logging context
    """
    if not operations:
        return
    
    results = await asyncio.gather(*operations, return_exceptions=True)
    
    # Log any exceptions that occurred
    error_count = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            error_count += 1
            logger.error(f"[{operation_name}] Operation {i} failed: {type(result).__name__}: {str(result)}")
    
    if error_count > 0:
        logger.warning(f"[{operation_name}] {error_count} out of {len(operations)} operations failed")


def download_and_decompress(url: str) -> bytes:
    """Download and optionally decompress file from URL.
    
    Handles both gzipped (.gz URLs) and plain text/JSON responses.
    """
    try:
        logger.info(f"Downloading {url}")
        with urllib.request.urlopen(url, timeout=30) as response:
            data = response.read()
        
        # Check if data is gzipped (starts with magic number 0x1f8b)
        if len(data) >= 2 and data[0] == 0x1f and data[1] == 0x8b:
            logger.info(f"Decompressing {len(data)} bytes")
            decompressed_data = gzip.decompress(data)
            logger.info(f"Decompressed to {len(decompressed_data)} bytes")
            return decompressed_data
        else:
            logger.info(f"Downloaded {len(data)} bytes (not gzipped)")
            return data
            
    except urllib.error.URLError as e:
        logger.error(f"Error downloading {url}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing {url}: {str(e)}")
        raise


def save_to_s3(data: bytes, key: str):
    """Save raw cache file to S3 for backup."""
    if not CACHE_FILES_BUCKET:
        return
    
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d/%H%M%S")
        s3_key = f"cache-files/{timestamp}/{key}"
        s3_client.put_object(
            Bucket=CACHE_FILES_BUCKET,
            Key=s3_key,
            Body=data,
            ContentType='application/gzip'
        )
        logger.info(f"Saved cache file to S3: s3://{CACHE_FILES_BUCKET}/{s3_key}")
    except Exception as e:
        logger.warning(f"Failed to save to S3: {str(e)}")


def parse_csv_metar(data: bytes) -> List[Dict[str, Any]]:
    """Parse METAR CSV data."""
    records = []
    try:
        text = data.decode('utf-8')
        lines = text.splitlines()
        if not lines:
            return records
        
        # Parse header to get column indices (handles duplicate column names)
        header_reader = csv.reader([lines[0]])
        header = next(header_reader)
        column_indices = {}
        for i, col in enumerate(header):
            col = col.strip('"')
            if col not in column_indices:
                column_indices[col] = []
            column_indices[col].append(i)
        
        # Parse data rows
        reader = csv.reader(lines[1:])
        for row in reader:
            # Map new CSV field names to expected format
            record = {}
            
            # Helper function to get value by column name
            def get_col(name, default=''):
                if name in column_indices and column_indices[name]:
                    idx = column_indices[name][0]
                    if idx < len(row):
                        val = row[idx].strip('"')
                        return val if val else default
                return default
            
            # Map raw text
            record['rawOb'] = get_col('raw_text', '')
            
            # Map station ID
            station_id = get_col('station_id', '')
            if not station_id:
                logger.warning(f"[Cache Ingest] Skipping METAR record - missing station_id. Row data: {row[:5] if len(row) > 5 else row}")
                continue  # Skip records without station ID
            
            # Map station_id to expected field names (like parse_xml_taf does)
            record['icaoId'] = station_id.upper()
            record['stationId'] = station_id.upper()
            
            # Map observation time
            obs_time = get_col('observation_time', '')
            if obs_time:
                # Ensure it ends with Z
                if not obs_time.endswith('Z') and not obs_time.endswith('+00:00'):
                    obs_time = obs_time.rstrip('+00:00').rstrip('-00:00')
                    if '+' in obs_time or (len(obs_time) > 10 and obs_time[10] == 'T'):
                        obs_time = obs_time + 'Z' if not obs_time.endswith('Z') else obs_time
                    else:
                        obs_time = obs_time + 'Z'
            record['obsTime'] = obs_time
            
            # Map temperature and dewpoint
            temp_c = get_col('temp_c', '')
            if temp_c and temp_c.strip():
                try:
                    record['temp'] = float(temp_c)
                except ValueError:
                    record['temp'] = None
            else:
                record['temp'] = None
            
            dewpoint_c = get_col('dewpoint_c', '')
            if dewpoint_c and dewpoint_c.strip():
                try:
                    record['dewp'] = float(dewpoint_c)
                except ValueError:
                    record['dewp'] = None
            else:
                record['dewp'] = None
            
            # Map wind data
            wind_dir = get_col('wind_dir_degrees', '')
            if wind_dir and wind_dir.strip():
                try:
                    record['wdir'] = int(float(wind_dir))
                except ValueError:
                    record['wdir'] = None
            else:
                record['wdir'] = None
            
            wind_speed = get_col('wind_speed_kt', '')
            if wind_speed and wind_speed.strip():
                try:
                    record['wspd'] = int(float(wind_speed))
                except ValueError:
                    record['wspd'] = None
            else:
                record['wspd'] = None
            
            wind_gust = get_col('wind_gust_kt', '')
            if wind_gust and wind_gust.strip():
                try:
                    record['wspdGust'] = int(float(wind_gust))
                except ValueError:
                    record['wspdGust'] = None
            else:
                record['wspdGust'] = None
            
            # Map visibility - handle values like "10+", "6+", "2.5", "1 3/4", "3/4", etc.
            vis_mi = get_col('visibility_statute_mi', '')
            if vis_mi and vis_mi.strip():
                vis_str = vis_mi.strip()
                # Handle "+" suffix (means 10+ or 6+)
                if vis_str.endswith('+'):
                    try:
                        record['visib'] = float(vis_str[:-1]) + 0.5  # Add 0.5 to indicate "or more"
                    except ValueError:
                        record['visib'] = None
                # Handle fractions like "3/4", "1/2", "1 3/4", "2 1/2"
                elif '/' in vis_str:
                    parts = vis_str.split()
                    if len(parts) == 2:  # "1 3/4" format
                        try:
                            whole = float(parts[0])
                            frac_parts = parts[1].split('/')
                            if len(frac_parts) == 2:
                                fraction = float(frac_parts[0]) / float(frac_parts[1])
                                record['visib'] = whole + fraction
                            else:
                                record['visib'] = None
                        except ValueError:
                            record['visib'] = None
                    else:  # "3/4" format
                        frac_parts = vis_str.split('/')
                        if len(frac_parts) == 2:
                            try:
                                record['visib'] = float(frac_parts[0]) / float(frac_parts[1])
                            except ValueError:
                                record['visib'] = None
                        else:
                            record['visib'] = None
                else:
                    try:
                        record['visib'] = float(vis_str)
                    except ValueError:
                        record['visib'] = None
            else:
                record['visib'] = None
            
            # Map altimeter
            altim = get_col('altim_in_hg', '')
            if altim and altim.strip():
                try:
                    record['altim_in_hg'] = float(altim)
                except ValueError:
                    record['altim_in_hg'] = None
            else:
                record['altim_in_hg'] = None
            
            # Map sea level pressure
            slp = get_col('sea_level_pressure_mb', '')
            if slp and slp.strip():
                try:
                    record['slp'] = float(slp)
                except ValueError:
                    record['slp'] = None
            else:
                record['slp'] = None
            
            # Map elevation
            elev = get_col('elevation_m', '')
            if elev and elev.strip():
                try:
                    # Convert meters to feet
                    record['elev'] = float(elev) * 3.28084
                except ValueError:
                    record['elev'] = None
            else:
                record['elev'] = None
            
            # Map flight category
            record['flightCategory'] = get_col('flight_category', '')
            
            # Map METAR type
            record['metarType'] = get_col('metar_type', '')
            
            # Parse sky conditions - handle duplicate column names by using all indices
            sky_conditions = []
            if 'sky_cover' in column_indices and 'cloud_base_ft_agl' in column_indices:
                sky_cover_indices = column_indices['sky_cover']
                cloud_base_indices = column_indices['cloud_base_ft_agl']
                
                # Process up to 4 cloud layers
                for i in range(min(4, len(sky_cover_indices), len(cloud_base_indices))):
                    sky_idx = sky_cover_indices[i]
                    base_idx = cloud_base_indices[i]
                    
                    if sky_idx < len(row) and base_idx < len(row):
                        sky_cover = row[sky_idx].strip('"')
                        cloud_base = row[base_idx].strip('"')
                        
                        if sky_cover and sky_cover.strip() and sky_cover.upper() not in ['', 'NIL', 'NCD']:
                            sky_cover_upper = sky_cover.strip().upper()
                            if sky_cover_upper in ['CLR', 'SKC', 'FEW', 'SCT', 'BKN', 'OVC', 'OVX', 'VV']:
                                cloud_layer = {
                                    'skyCover': sky_cover_upper,
                                    'cloudBase': None,
                                    'cloudType': None
                                }
                                
                                if cloud_base and cloud_base.strip():
                                    try:
                                        cloud_layer['cloudBase'] = int(float(cloud_base))
                                    except ValueError:
                                        pass
                                
                                sky_conditions.append(cloud_layer)
                
                # Check if we have a cloud layer
                if sky_cover and sky_cover.strip() and sky_cover.upper() not in ['', 'NIL', 'NCD']:
                    sky_cover_upper = sky_cover.strip().upper()
                    if sky_cover_upper in ['CLR', 'SKC', 'FEW', 'SCT', 'BKN', 'OVC', 'OVX', 'VV']:
                        cloud_layer = {
                            'skyCover': sky_cover_upper,
                            'cloudBase': None,
                            'cloudType': None
                        }
                        
                        if cloud_base and cloud_base.strip():
                            try:
                                cloud_layer['cloudBase'] = int(float(cloud_base))
                            except ValueError:
                                pass
                        
                        sky_conditions.append(cloud_layer)
            
            # Store sky conditions in the format expected by parse_sky_conditions
            # Also store individual skyc1, skyl1, etc. fields for compatibility
            if sky_conditions:
                record['clouds'] = sky_conditions
                for i, layer in enumerate(sky_conditions[:4], 1):
                    record[f'skyc{i}'] = layer.get('skyCover')
                    record[f'skyl{i}'] = layer.get('cloudBase')
            else:
                record['clouds'] = []
            
            # Map weather string
            record['wxString'] = get_col('wx_string', '')
            
            records.append(record)
        logger.info(f"Parsed {len(records)} METAR records from CSV")
    except Exception as e:
        logger.error(f"Error parsing METAR CSV: {str(e)}")
        raise
    return records


def parse_xml_taf(data: bytes) -> List[Dict[str, Any]]:
    """Parse TAF XML data."""
    records = []
    try:
        root = ET.fromstring(data)
        # TAF XML structure: <response><data><TAF>...</TAF></data></response>
        for taf_elem in root.findall('.//TAF'):
            record = {}
            
            # Extract top-level TAF fields
            for child in taf_elem:
                # Skip forecast elements - we'll parse those separately
                if child.tag == 'forecast':
                    continue
                    
                tag = child.tag
                text = child.text
                
                # Handle CDATA sections (like raw_text)
                if text:
                    # Convert numeric fields
                    if tag in ['temp', 'dewp', 'wdir', 'wspd', 'visib', 'altim', 'elev']:
                        try:
                            record[tag] = float(text) if '.' in text else int(text)
                        except ValueError:
                            record[tag] = text
                    else:
                        record[tag] = text
                else:
                    record[tag] = None
            
            # Map station_id to expected field names
            station_id = record.get('station_id', None)
            if station_id:
                record['icaoId'] = station_id.upper()
                record['stationId'] = station_id.upper()
                logger.debug(f"[Cache Ingest] TAF record - extracted station_id: {station_id}, mapped to icaoId: {record['icaoId']}, stationId: {record['stationId']}")
            else:
                logger.warning(f"[Cache Ingest] TAF record missing station_id. Record keys: {list(record.keys())[:10]}")
            
            # Extract raw TAF text - handle both raw_text and rawTAF
            if 'rawTAF' not in record:
                raw_text = record.get('raw_text', '')
                if raw_text:
                    record['rawTAF'] = raw_text
                else:
                    record['rawTAF'] = taf_elem.findtext('rawTAF', '')
            
            # Map time fields to expected names
            if 'issue_time' in record:
                record['issueTime'] = record['issue_time']
            if 'valid_time_from' in record:
                record['validTimeFrom'] = record['valid_time_from']
            if 'valid_time_to' in record:
                record['validTimeTo'] = record['valid_time_to']
            
            # Parse nested forecast elements
            forecasts = []
            for forecast_elem in taf_elem.findall('forecast'):
                forecast = {}
                
                # Extract time fields
                fcst_time_from = forecast_elem.findtext('fcst_time_from', '')
                fcst_time_to = forecast_elem.findtext('fcst_time_to', '')
                if fcst_time_from:
                    forecast['fcstTimeFrom'] = fcst_time_from
                if fcst_time_to:
                    forecast['fcstTimeTo'] = fcst_time_to
                
                # Extract change indicator
                change_indicator = forecast_elem.findtext('change_indicator', None)
                if change_indicator:
                    forecast['changeIndicator'] = change_indicator
                
                # Extract wind data
                wind_dir = forecast_elem.findtext('wind_dir_degrees', None)
                if wind_dir and wind_dir.strip() and wind_dir.upper() != 'VRB':
                    try:
                        forecast['wdir'] = int(float(wind_dir))
                    except (ValueError, TypeError):
                        forecast['wdir'] = None
                else:
                    forecast['wdir'] = None
                
                wind_speed = forecast_elem.findtext('wind_speed_kt', None)
                if wind_speed:
                    try:
                        forecast['wspd'] = int(float(wind_speed))
                    except (ValueError, TypeError):
                        forecast['wspd'] = None
                else:
                    forecast['wspd'] = None
                
                wind_gust = forecast_elem.findtext('wind_gust_kt', None)
                if wind_gust:
                    try:
                        forecast['wspdGust'] = int(float(wind_gust))
                    except (ValueError, TypeError):
                        forecast['wspdGust'] = None
                else:
                    forecast['wspdGust'] = None
                
                # Extract visibility
                visibility = forecast_elem.findtext('visibility_statute_mi', None)
                if visibility:
                    vis_str = visibility.strip()
                    # Handle "+" suffix (means 10+ or 6+)
                    if vis_str.endswith('+'):
                        try:
                            forecast['visib'] = float(vis_str[:-1]) + 0.5
                        except ValueError:
                            forecast['visib'] = None
                    # Handle fractions like "3/4", "1 3/4"
                    elif '/' in vis_str:
                        parts = vis_str.split()
                        if len(parts) == 2:  # "1 3/4" format
                            try:
                                whole = float(parts[0])
                                frac_parts = parts[1].split('/')
                                if len(frac_parts) == 2:
                                    fraction = float(frac_parts[0]) / float(frac_parts[1])
                                    forecast['visib'] = whole + fraction
                                else:
                                    forecast['visib'] = None
                            except ValueError:
                                forecast['visib'] = None
                        else:  # "3/4" format
                            frac_parts = vis_str.split('/')
                            if len(frac_parts) == 2:
                                try:
                                    forecast['visib'] = float(frac_parts[0]) / float(frac_parts[1])
                                except ValueError:
                                    forecast['visib'] = None
                            else:
                                forecast['visib'] = None
                    else:
                        try:
                            forecast['visib'] = float(vis_str)
                        except ValueError:
                            forecast['visib'] = None
                else:
                    forecast['visib'] = None
                
                # Extract sky conditions - handle multiple sky_condition elements
                sky_conditions = forecast_elem.findall('sky_condition')
                sky_cover_list = []
                cloud_base_list = []
                cloud_type_list = []
                
                for sky_cond in sky_conditions:
                    # Extract attributes (sky_cover and cloud_base_ft_agl are attributes)
                    sky_cover = sky_cond.get('sky_cover', None)
                    cloud_base_ft_agl = sky_cond.get('cloud_base_ft_agl', None)
                    cloud_type = sky_cond.get('cloud_type', None)
                    
                    if sky_cover:
                        sky_cover_list.append(sky_cover.strip().upper())
                        # cloud_base_ft_agl is already in feet (not hundreds)
                        if cloud_base_ft_agl:
                            try:
                                cloud_base_list.append(int(float(cloud_base_ft_agl)))
                            except (ValueError, TypeError):
                                cloud_base_list.append(None)
                        else:
                            cloud_base_list.append(None)
                        
                        if cloud_type:
                            cloud_type_list.append(cloud_type.strip())
                        else:
                            cloud_type_list.append(None)
                
                # Map sky conditions to skyc1/skyl1/skyt1 format (for compatibility with weather lambda)
                # Also store as structured array
                structured_sky_conditions = []
                for i, (sky_cover, cloud_base) in enumerate(zip(sky_cover_list[:4], cloud_base_list[:4]), 1):
                    forecast[f'skyc{i}'] = sky_cover
                    forecast[f'skyl{i}'] = cloud_base
                    if i <= len(cloud_type_list):
                        forecast[f'skyt{i}'] = cloud_type_list[i-1] if cloud_type_list[i-1] else None
                    
                    # Also create structured format
                    structured_sky_conditions.append({
                        'skyCover': sky_cover,
                        'cloudBase': cloud_base,
                        'cloudType': cloud_type_list[i-1] if i <= len(cloud_type_list) and cloud_type_list[i-1] else None
                    })
                
                # Store structured sky conditions
                if structured_sky_conditions:
                    forecast['skyConditions'] = structured_sky_conditions
                
                forecasts.append(forecast)
            
            # Store forecasts array
            if forecasts:
                record['forecast'] = forecasts
            
            records.append(record)
        
        # Log summary of parsed records
        records_with_station_id = sum(1 for r in records if r.get('icaoId') or r.get('stationId'))
        records_without_station_id = len(records) - records_with_station_id
        logger.info(f"[Cache Ingest] Parsed {len(records)} TAF records from XML - {records_with_station_id} with station_id, {records_without_station_id} without")
        if records_without_station_id > 0:
            logger.warning(f"[Cache Ingest] {records_without_station_id} TAF records will be skipped during storage due to missing station_id")
    except Exception as e:
        logger.error(f"Error parsing TAF XML: {str(e)}")
        raise
    return records


def parse_json_sigmet(data: bytes) -> List[Dict[str, Any]]:
    """Parse SIGMET JSON from AWC API.
    
    Provides better structured data than CSV:
    - altitudeHi1/Hi2 and altitudeLow1/Low2 for altitude ranges
    - coords array with lat/lon objects
    - validTimeFrom/To as unix timestamps
    - seriesId for better identification
    """
    records = []
    try:
        text = data.decode('utf-8')
        items = json.loads(text)
        if not isinstance(items, list):
            items = [items]
        
        for item in items:
            sigmet_id = item.get('seriesId', '')
            if not sigmet_id:
                # Fallback to hash if no seriesId
                raw = item.get('rawAirSigmet', '')
                sigmet_id = hashlib.md5(raw.encode()).hexdigest()[:16]
            
            # Extract polygon coordinates
            polygon: List[List[float]] = []
            coords = item.get('coords', [])
            for pt in coords:
                try:
                    lon = float(pt.get('lon', 0))
                    lat = float(pt.get('lat', 0))
                    polygon.append([lon, lat])
                except (ValueError, AttributeError, TypeError):
                    pass
            
            # Calculate floor and ceiling from altitude ranges
            # altitudeLow1/Low2 are floor range, altitudeHi1/Hi2 are ceiling range
            altitude_low1 = item.get('altitudeLow1')
            altitude_low2 = item.get('altitudeLow2')
            altitude_hi1 = item.get('altitudeHi1')
            altitude_hi2 = item.get('altitudeHi2')
            
            # Use the lower of the two lows for floor, higher of the two highs for ceiling
            floor_ft = None
            if altitude_low1 is not None or altitude_low2 is not None:
                lows = [x for x in [altitude_low1, altitude_low2] if x is not None]
                floor_ft = min(lows) if lows else None
            
            ceiling_ft = None
            if altitude_hi1 is not None or altitude_hi2 is not None:
                highs = [x for x in [altitude_hi1, altitude_hi2] if x is not None]
                ceiling_ft = max(highs) if highs else None
            
            # Convert timestamps from epoch to ISO format
            valid_from = item.get('validTimeFrom')
            valid_to = item.get('validTimeTo')
            
            valid_time_from = ''
            if valid_from:
                try:
                    valid_time_from = datetime.utcfromtimestamp(int(valid_from)).isoformat() + 'Z'
                except (ValueError, TypeError):
                    pass
            
            valid_time_to = ''
            if valid_to:
                try:
                    valid_time_to = datetime.utcfromtimestamp(int(valid_to)).isoformat() + 'Z'
                except (ValueError, TypeError):
                    pass
            
            records.append({
                'airsigmetId': sigmet_id,
                'raw_text': item.get('rawAirSigmet', ''),
                'valid_time_from': valid_time_from,
                'valid_time_to': valid_time_to,
                'floor': floor_ft,
                'ceiling': ceiling_ft,
                'hazard': item.get('hazard', ''),
                'severity': item.get('severity', ''),
                'airsigmet_type': item.get('airSigmetType', 'SIGMET'),
                'movement_dir': item.get('movementDir'),
                'movement_spd': item.get('movementSpd'),
                'polygon': polygon,
            })
        logger.info(f"Parsed {len(records)} SIGMET records from JSON")
    except Exception as e:
        logger.error(f"Error parsing SIGMET JSON: {str(e)}")
        raise
    return records


def parse_csv_sigmet(data: bytes) -> List[Dict[str, Any]]:
    """Parse SIGMET CSV from AWC bulk cache file.

    Columns: raw_text, valid_time_from, valid_time_to, lon:lat points,
             min_ft_msl, max_ft_msl, movement_dir_degrees, movement_speed_kt,
             hazard, severity, airsigmet_type
    """
    records = []
    try:
        text = data.decode('utf-8')
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            raw_text = row.get('raw_text', '')
            # Stable ID: content hash so re-ingests don't create duplicate keys
            sigmet_id = hashlib.md5(raw_text.encode()).hexdigest()[:16]

            # Parse polygon from "lon:lat points" (semicolon-separated "lon:lat" pairs)
            polygon: List[List[float]] = []
            pts_str = row.get('lon:lat points', '')
            for pt in pts_str.split(';'):
                pt = pt.strip()
                if ':' in pt:
                    lon_s, lat_s = pt.split(':', 1)
                    try:
                        polygon.append([float(lon_s), float(lat_s)])
                    except ValueError:
                        pass

            records.append({
                'airsigmetId': sigmet_id,
                'raw_text': raw_text,
                'valid_time_from': row.get('valid_time_from'),
                'valid_time_to': row.get('valid_time_to'),
                'min_ft_msl': row.get('min_ft_msl', ''),
                'max_ft_msl': row.get('max_ft_msl', ''),
                'hazard': row.get('hazard', ''),
                'severity': row.get('severity', ''),
                'airsigmet_type': row.get('airsigmet_type', 'SIGMET'),
                'polygon': polygon,
            })
        logger.info(f"Parsed {len(records)} SIGMET records from CSV")
    except Exception as e:
        logger.error(f"Error parsing SIGMET CSV: {str(e)}")
        raise
    return records


def parse_xml_airmet(data: bytes) -> List[Dict[str, Any]]:
    """Parse G-AIRMET XML from AWC bulk cache file.

    Each <GAIRMET> element contains scalar fields plus a nested
    <area><point><longitude>...</longitude><latitude>...</latitude></point></area>
    block for the polygon.  The old implementation read only top-level child.text
    and completely missed the nested polygon.
    """
    records = []
    try:
        root = ET.fromstring(data)
        for elem in root.findall('.//GAIRMET'):
            product    = elem.findtext('product', '')
            tag        = elem.findtext('tag', '')
            valid_time = elem.findtext('valid_time', '')
            expire_time = elem.findtext('expire_time', '')
            issue_time  = elem.findtext('issue_time', '')
            due_to     = elem.findtext('due_to', '')

            hazard_elem = elem.find('hazard')
            hazard_type = hazard_elem.get('type', '') if hazard_elem is not None else ''

            # Stable ID from product + tag + valid_time (survives re-ingests)
            ts = valid_time.replace(':', '').replace('-', '').replace('Z', '')
            airmet_id = f"{product}-{tag}-{ts}"

            # Parse nested <area><point> polygon
            polygon: List[List[float]] = []
            for pt in elem.findall('.//area/point'):
                try:
                    lon = float(pt.findtext('longitude', '0'))
                    lat = float(pt.findtext('latitude', '0'))
                    polygon.append([lon, lat])
                except ValueError:
                    pass

            records.append({
                'forecastId': airmet_id,
                'product': product,
                'tag': tag,
                'valid_time': valid_time,
                'expire_time': expire_time,
                'issue_time': issue_time,
                'due_to': due_to,
                'hazard_type': hazard_type,
                'polygon': polygon,
            })
        logger.info(f"Parsed {len(records)} G-AIRMET records from XML")
    except Exception as e:
        logger.error(f"Error parsing G-AIRMET XML: {str(e)}")
        raise
    return records


def parse_altitude(alt_str: str) -> Optional[int]:
    """Parse altitude string to feet MSL.
    
    Examples:
      "100" -> 10000 (flight level to feet)
      "240" -> 24000
      "SFC" -> 0 (surface)
      "FZL" -> 8000 (freezing level, typical assumption)
      "" -> None
    """
    if not alt_str or not isinstance(alt_str, str):
        return None
    
    alt_str = alt_str.strip().upper()
    
    if alt_str == 'SFC':
        return 0
    
    if alt_str == 'FZL':
        # Freezing level varies, but 8000 ft is a reasonable default
        # TODO: Could look up actual freezing level from FZLVL G-AIRMETs
        return 8000
    
    # Try to parse as flight level (numeric string)
    try:
        fl = int(alt_str)
        return fl * 100  # FL100 = 10,000 ft
    except ValueError:
        logger.warning(f"Unknown altitude format: {alt_str}")
        return None


def parse_json_airmet(data: bytes) -> List[Dict[str, Any]]:
    """Parse G-AIRMET JSON from AWC API.
    
    JSON structure:
    {
      "tag": "1E",
      "forecastHour": 3,
      "validTime": "2026-04-21T06:00:00.000Z",
      "hazard": "TURB-HI",
      "base": "100",      # Flight level or "SFC" or "FZL"
      "top": "240",       # Flight level
      "severity": "MOD",
      "product": "TANGO",
      "coords": [{"lat": "34.50", "lon": "-101.67"}, ...]
    }
    """
    records = []
    try:
        text = data.decode('utf-8')
        items = json.loads(text)
        
        if not isinstance(items, list):
            logger.warning("G-AIRMET JSON is not a list")
            return []
        
        for item in items:
            product = item.get('product', '')
            tag = item.get('tag', '')
            valid_time = item.get('validTime', '')
            hazard_type = item.get('hazard', '')
            
            # Stable ID from product + tag + valid_time
            ts = valid_time.replace(':', '').replace('-', '').replace('Z', '').replace('.', '')
            airmet_id = f"{product}-{tag}-{ts}"
            
            # Parse polygon from coords array
            polygon: List[List[float]] = []
            coords = item.get('coords', [])
            for pt in coords:
                try:
                    lon = float(pt.get('lon', 0))
                    lat = float(pt.get('lat', 0))
                    polygon.append([lon, lat])
                except (ValueError, AttributeError):
                    pass
            
            # Parse altitude fields
            base_str = item.get('base', '')
            top_str = item.get('top', '')
            
            floor_ft = parse_altitude(base_str)
            ceiling_ft = parse_altitude(top_str)
            
            # Get timestamps - convert epoch seconds to ISO format if needed
            issue_time = item.get('issueTime', '')
            if issue_time and isinstance(issue_time, (int, float)):
                issue_time = datetime.utcfromtimestamp(issue_time).isoformat() + 'Z'
            
            expire_time = item.get('expireTime', '')
            if expire_time and isinstance(expire_time, (int, float)):
                expire_time = datetime.utcfromtimestamp(expire_time).isoformat() + 'Z'
            
            records.append({
                'forecastId': airmet_id,
                'product': product,
                'tag': tag,
                'valid_time': valid_time,
                'expire_time': expire_time,
                'issue_time': issue_time,
                'due_to': item.get('due_to', ''),
                'hazard_type': hazard_type,
                'severity': item.get('severity', ''),
                'floor': floor_ft,
                'ceiling': ceiling_ft,
                'polygon': polygon,
            })
        logger.info(f"Parsed {len(records)} G-AIRMET records from JSON")
    except Exception as e:
        logger.error(f"Error parsing G-AIRMET JSON: {str(e)}")
        raise
    return records


def parse_csv_pirep(data: bytes) -> List[Dict[str, Any]]:
    """Parse PIREP CSV data."""
    records = []
    try:
        text = data.decode('utf-8')
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            record = dict(row)
            # Generate ID if not present
            if 'aircraftReportId' not in record or not record['aircraftReportId']:
                record['aircraftReportId'] = f"AWC-PIREP-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{len(records)}"
            records.append(record)
        logger.info(f"Parsed {len(records)} PIREP records from CSV")
    except Exception as e:
        logger.error(f"Error parsing PIREP CSV: {str(e)}")
        raise
    return records


def parse_json_stations(data: bytes) -> List[Dict[str, Any]]:
    """Parse Stations JSON data."""
    try:
        text = data.decode('utf-8')
        stations = json.loads(text)
        if isinstance(stations, list):
            logger.info(f"Parsed {len(stations)} station records from JSON")
            return stations
        elif isinstance(stations, dict) and 'features' in stations:
            # GeoJSON format
            records = []
            for feature in stations['features']:
                record = feature.get('properties', {})
                if 'geometry' in feature:
                    coords = feature['geometry'].get('coordinates', [])
                    if coords:
                        record['longitude'] = coords[0]
                        record['latitude'] = coords[1]
                records.append(record)
            logger.info(f"Parsed {len(records)} station records from GeoJSON")
            return records
        else:
            logger.warning("Unexpected JSON structure for stations")
            return []
    except Exception as e:
        logger.error(f"Error parsing Stations JSON: {str(e)}")
        raise


async def store_metar(glide_client: GlideClusterClient, records: List[Dict[str, Any]]):
    """Store METAR records in ValKey."""
    station_ids = set()
    current_time = int(datetime.utcnow().timestamp())
    skipped_count = 0
    
    logger.info(f"[Cache Store] Storing {len(records)} METAR records")
    
    # Batch operations for better performance
    operations = []
    
    for record in records:
        # Log raw values for debugging
        icao_id = record.get('icaoId')
        station_id_field = record.get('stationId')
        raw_station_id = record.get('station_id')  # From CSV/XML parsing
        
        station_id = icao_id or station_id_field
        if not station_id:
            skipped_count += 1
            logger.warning(f"[Cache Store] Skipping METAR record - missing icaoId and stationId. Raw station_id: {raw_station_id}, record keys: {list(record.keys())[:10]}")
            continue
        
        station_id = station_id.upper()
        key = f"metar:{station_id}"
        
        # Log key being stored (sample first 5 for brevity)
        if len(station_ids) < 5:
            logger.info(f"[Cache Store] Storing METAR - station_id: {station_id}, icaoId: {icao_id}, stationId: {station_id_field}, key: {key}")
        
        # Store METAR data with TTL atomically using SET with expiry
        operations.append(
            glide_client.set(
                key,
                json.dumps(record),
                expiry=ExpirySet(ExpiryType.SEC, TTL_METAR)
            )
        )
        station_ids.add(station_id)
    
    # Execute all operations concurrently with error logging
    await execute_operations_with_error_logging(operations, "METAR")
    
    # Update station set and updated ZSET with atomic TTL
    if station_ids:
        try:
            # Delete old indexes
            await glide_client.delete(["metar:stations", "metar:updated"])
            
            # Add all members to the sets in batch
            index_operations = []
            for station_id in station_ids:
                index_operations.append(glide_client.sadd("metar:stations", [station_id]))
                index_operations.append(glide_client.zadd("metar:updated", {station_id: current_time}))
            
            # Execute index operations
            await execute_operations_with_error_logging(index_operations, "METAR indexes")
            
            # Set TTL on both indexes after they're populated
            await glide_client.expire("metar:stations", TTL_METAR)
            await glide_client.expire("metar:updated", TTL_METAR)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update METAR indexes: {type(error).__name__}: {str(error)}")
    
    logger.info(f"[Cache Store] Stored {len(station_ids)} METAR records, skipped {skipped_count} records")
    if station_ids:
        sample_keys = list(station_ids)[:5]
        logger.info(f"[Cache Store] Sample METAR keys stored: {sample_keys}")


async def store_taf(glide_client: GlideClusterClient, records: List[Dict[str, Any]]):
    """Store TAF records in ValKey."""
    station_ids = set()
    current_time = int(datetime.utcnow().timestamp())
    skipped_count = 0
    
    logger.info(f"[Cache Store] Storing {len(records)} TAF records")
    
    operations = []
    
    for record in records:
        # Log raw values for debugging
        icao_id = record.get('icaoId')
        station_id_field = record.get('stationId')
        raw_station_id = record.get('station_id')  # From XML parsing
        
        station_id = icao_id or station_id_field
        if not station_id:
            skipped_count += 1
            logger.warning(f"[Cache Store] Skipping TAF record - missing icaoId and stationId. Raw station_id: {raw_station_id}, record keys: {list(record.keys())[:10]}")
            continue
        
        station_id = station_id.upper()
        key = f"taf:{station_id}"
        
        # Log key being stored (sample first record for brevity)
        if len(station_ids) == 0:
            logger.info(f"[Cache Store] Storing TAF - station_id: {station_id}, icaoId: {icao_id}, stationId: {station_id_field}, key: {key}")
        
        # Log forecast data before storing (from earlier implementation)
        if 'forecast' in record and isinstance(record['forecast'], list):
            if len(station_ids) == 0:  # Only log for first record to avoid spam
                logger.info(f"[Cache Store] TAF {station_id} has {len(record['forecast'])} forecast periods")
                if len(record['forecast']) > 0:
                    first_fcst = record['forecast'][0]
                    logger.info(f"[Cache Store] First forecast for {station_id}: skyc1={first_fcst.get('skyc1')}, skyl1={first_fcst.get('skyl1')}")
        
        # Store TAF data with TTL atomically using SET with expiry
        operations.append(
            glide_client.set(
                key,
                json.dumps(record),
                expiry=ExpirySet(ExpiryType.SEC, TTL_TAF)
            )
        )
        station_ids.add(station_id)
    
    # Execute all operations concurrently with error logging
    await execute_operations_with_error_logging(operations, "TAF")
    
    if station_ids:
        try:
            # Delete old indexes
            await glide_client.delete(["taf:stations", "taf:updated"])
            
            # Add all members to the sets in batch
            index_operations = []
            for station_id in station_ids:
                index_operations.append(glide_client.sadd("taf:stations", [station_id]))
                index_operations.append(glide_client.zadd("taf:updated", {station_id: current_time}))
            
            # Execute index operations
            await execute_operations_with_error_logging(index_operations, "TAF indexes")
            
            # Set TTL on both indexes after they're populated
            await glide_client.expire("taf:stations", TTL_TAF)
            await glide_client.expire("taf:updated", TTL_TAF)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update TAF indexes: {type(error).__name__}: {str(error)}")
    
    logger.info(f"[Cache Store] Stored {len(station_ids)} TAF records, skipped {skipped_count} records")
    if station_ids:
        sample_keys = list(station_ids)[:5]
        logger.info(f"[Cache Store] Sample TAF keys stored: {sample_keys}")


async def store_sigmet(glide_client: GlideClusterClient, records: List[Dict[str, Any]]):
    """Store SIGMET records in ValKey."""
    sigmet_ids = set()
    hazard_types = {}
    operations = []
    
    for record in records:
        sigmet_id = record.get('airsigmetId') or record.get('id')
        if not sigmet_id:
            continue
        
        key = f"sigmet:{sigmet_id}"
        # Store SIGMET data with TTL atomically
        operations.append(
            glide_client.set(
                key,
                json.dumps(record),
                expiry=ExpirySet(ExpiryType.SEC, TTL_SIGMET)
            )
        )
        sigmet_ids.add(sigmet_id)
        
        # Index by hazard type
        hazard = record.get('hazard', 'UNKNOWN')
        if hazard not in hazard_types:
            hazard_types[hazard] = set()
        hazard_types[hazard].add(sigmet_id)
    
    # Execute all operations concurrently with error logging
    await execute_operations_with_error_logging(operations, "SIGMET")
    
    # Update indexes
    if sigmet_ids:
        try:
            # Delete old index
            await glide_client.delete(["sigmet:all"])
            
            # Add all members in batch
            index_operations = []
            for sigmet_id in sigmet_ids:
                index_operations.append(glide_client.sadd("sigmet:all", [sigmet_id]))
            
            # Execute index operations
            await execute_operations_with_error_logging(index_operations, "SIGMET indexes")
            
            # Set TTL on index key
            await glide_client.expire("sigmet:all", TTL_SIGMET)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update SIGMET indexes: {type(error).__name__}: {str(error)}")
    
    # Update hazard type indexes
    for hazard, ids in hazard_types.items():
        try:
            hazard_key = f"sigmet:hazard:{hazard}"
            await glide_client.delete([hazard_key])
            
            # Add all members for this hazard type
            hazard_operations = []
            for sigmet_id in ids:
                hazard_operations.append(glide_client.sadd(hazard_key, [sigmet_id]))
            
            # Execute hazard operations
            await execute_operations_with_error_logging(hazard_operations, f"SIGMET hazard:{hazard}")
            
            # Set TTL on hazard index key
            await glide_client.expire(hazard_key, TTL_SIGMET)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update SIGMET hazard index {hazard}: {type(error).__name__}: {str(error)}")

    # Pre-aggregated bundle for the AdvisoryMap screen: a single JSON blob of all
    # real SIGMETs (skipping AIRMET/OUTLOOK rows from the same combined feed) so
    # the map query is a single GET — no SMEMBERS+MGET fan-out.
    try:
        bundle_records = []
        for record in records:
            kind = (record.get('airsigmet_type') or '').upper()
            if kind not in ('SIGMET', 'INTL_SIGMET'):
                continue
            polygon = record.get('polygon') or []
            if not polygon:
                continue
            bundle_records.append({
                'airsigmetId': record.get('airsigmetId'),
                'polygon': polygon,
                'raw_text': record.get('raw_text', ''),
                'valid_time_from': record.get('valid_time_from'),
                'valid_time_to': record.get('valid_time_to'),
                'floor': record.get('floor') or record.get('min_ft_msl'),  # JSON or CSV fallback
                'ceiling': record.get('ceiling') or record.get('max_ft_msl'),  # JSON or CSV fallback
                'hazard': record.get('hazard', ''),
                'severity': record.get('severity', ''),
                'airsigmet_type': kind,
            })
        await glide_client.set(
            "sigmet:bundle",
            json.dumps(bundle_records),
            expiry=ExpirySet(ExpiryType.SEC, TTL_BUNDLE),
        )
        logger.info(f"[Cache Store] Wrote sigmet:bundle with {len(bundle_records)} records")
    except Exception as error:
        logger.error(f"[Cache Store] Failed to write sigmet:bundle: {type(error).__name__}: {str(error)}")

    logger.info(f"Stored {len(sigmet_ids)} SIGMET records")


async def store_airmet(glide_client: GlideClusterClient, records: List[Dict[str, Any]]):
    """Store G-AIRMET records in ValKey."""
    airmet_ids = set()
    hazard_types = {}
    operations = []
    
    for record in records:
        airmet_id = record.get('forecastId') or record.get('id')
        if not airmet_id:
            continue
        
        key = f"airmet:{airmet_id}"
        # Store AIRMET data with TTL atomically
        operations.append(
            glide_client.set(
                key,
                json.dumps(record),
                expiry=ExpirySet(ExpiryType.SEC, TTL_AIRMET)
            )
        )
        airmet_ids.add(airmet_id)
        
        hazard = record.get('hazard', 'UNKNOWN')
        if hazard not in hazard_types:
            hazard_types[hazard] = set()
        hazard_types[hazard].add(airmet_id)
    
    # Execute all operations concurrently with error logging
    await execute_operations_with_error_logging(operations, "AIRMET")
    
    if airmet_ids:
        try:
            # Delete old index
            await glide_client.delete(["airmet:all"])
            
            # Add all members in batch
            index_operations = []
            for airmet_id in airmet_ids:
                index_operations.append(glide_client.sadd("airmet:all", [airmet_id]))
            
            # Execute index operations
            await execute_operations_with_error_logging(index_operations, "AIRMET indexes")
            
            # Set TTL on index key
            await glide_client.expire("airmet:all", TTL_AIRMET)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update AIRMET indexes: {type(error).__name__}: {str(error)}")
    
    # Update hazard type indexes
    for hazard, ids in hazard_types.items():
        try:
            hazard_key = f"airmet:hazard:{hazard}"
            await glide_client.delete([hazard_key])
            
            # Add all members for this hazard type
            hazard_operations = []
            for airmet_id in ids:
                hazard_operations.append(glide_client.sadd(hazard_key, [airmet_id]))
            
            # Execute hazard operations
            await execute_operations_with_error_logging(hazard_operations, f"AIRMET hazard:{hazard}")
            
            # Set TTL on hazard index key
            await glide_client.expire(hazard_key, TTL_AIRMET)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update AIRMET hazard index {hazard}: {type(error).__name__}: {str(error)}")

    # Pre-aggregated bundle for the AdvisoryMap screen: a single JSON blob of all
    # G-AIRMETs with polygons so the map query is a single GET — no SMEMBERS+MGET fan-out.
    try:
        bundle_records = []
        for record in records:
            polygon = record.get('polygon') or []
            if not polygon:
                continue
            bundle_records.append({
                'forecastId': record.get('forecastId'),
                'polygon': polygon,
                'product': record.get('product', ''),
                'hazard_type': record.get('hazard_type', ''),
                'valid_time': record.get('valid_time'),
                'expire_time': record.get('expire_time'),
                'due_to': record.get('due_to', ''),
                'floor': record.get('floor'),
                'ceiling': record.get('ceiling'),
            })
        await glide_client.set(
            "airmet:bundle",
            json.dumps(bundle_records),
            expiry=ExpirySet(ExpiryType.SEC, TTL_BUNDLE),
        )
        logger.info(f"[Cache Store] Wrote airmet:bundle with {len(bundle_records)} records")
    except Exception as error:
        logger.error(f"[Cache Store] Failed to write airmet:bundle: {type(error).__name__}: {str(error)}")

    logger.info(f"Stored {len(airmet_ids)} G-AIRMET records")


async def store_pirep(glide_client: GlideClusterClient, records: List[Dict[str, Any]]):
    """Store PIREP records in ValKey."""
    pirep_ids = set()
    current_time = int(datetime.utcnow().timestamp())
    operations = []
    
    # CRITICAL FIX: Ensure pirep:recent exists with TTL before any ZADD operations
    # This prevents unbounded growth of the sorted set
    try:
        pirep_recent_exists = await glide_client.exists(["pirep:recent"])
        if not pirep_recent_exists:
            # Create the key with a dummy entry and TTL
            await glide_client.zadd("pirep:recent", {"__init__": 0})
            await glide_client.expire("pirep:recent", TTL_PIREP)
            logger.info("[Cache Store] Initialized pirep:recent with TTL")
    except Exception as e:
        logger.error(f"[Cache Store] Failed to initialize pirep:recent: {str(e)}")
    
    for record in records:
        pirep_id = record.get('aircraftReportId') or record.get('id')
        if not pirep_id:
            continue
        
        key = f"pirep:{pirep_id}"
        # Store PIREP data with TTL atomically
        operations.append(
            glide_client.set(
                key,
                json.dumps(record),
                expiry=ExpirySet(ExpiryType.SEC, TTL_PIREP)
            )
        )
        pirep_ids.add(pirep_id)
        
        # Add to recent sorted set (key now has TTL from initialization above)
        operations.append(glide_client.zadd("pirep:recent", {pirep_id: current_time}))
    
    # Execute all operations concurrently with error logging
    await execute_operations_with_error_logging(operations, "PIREP")
    
    if pirep_ids:
        try:
            # Delete and recreate pirep:all index
            await glide_client.delete(["pirep:all"])
            
            # Add all members in batch
            index_operations = []
            for pirep_id in pirep_ids:
                index_operations.append(glide_client.sadd("pirep:all", [pirep_id]))
            
            # Execute index operations
            await execute_operations_with_error_logging(index_operations, "PIREP indexes")
            
            # Set TTL on index
            await glide_client.expire("pirep:all", TTL_PIREP)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update PIREP indexes: {type(error).__name__}: {str(error)}")
    
    # Keep only last 1000 PIREPs in recent set
    try:
        await glide_client.zremrangebyrank("pirep:recent", 0, -1001)
        # Refresh TTL on recent sorted set
        await glide_client.expire("pirep:recent", TTL_PIREP)
    except Exception as e:
        logger.error(f"[Cache Store] Failed to trim pirep:recent: {str(e)}")
    
    logger.info(f"Stored {len(pirep_ids)} PIREP records")


async def store_stations(glide_client: GlideClusterClient, records: List[Dict[str, Any]]):
    """Store Station records in ValKey."""
    station_codes = set()
    name_index = {}
    iata_index = {}
    operations = []
    
    for record in records:
        station_code = record.get('icaoId') or record.get('id')
        if not station_code:
            continue
        
        station_code = station_code.upper()
        key = f"station:{station_code}"
        # Store station data with TTL atomically
        operations.append(
            glide_client.set(
                key,
                json.dumps(record),
                expiry=ExpirySet(ExpiryType.SEC, TTL_STATION)
            )
        )
        station_codes.add(station_code)
        
        # Index by name
        name = record.get('name', '')
        if name:
            name_lower = name.lower()
            if name_lower not in name_index:
                name_index[name_lower] = set()
            name_index[name_lower].add(station_code)
        
        # Index by IATA code
        iata = record.get('iataId', '')
        if iata:
            iata_index[iata.upper()] = station_code
    
    # Execute all operations concurrently with error logging
    await execute_operations_with_error_logging(operations, "STATIONS")
    
    # Update indexes
    if station_codes:
        try:
            # Delete old station:all index
            await glide_client.delete(["station:all"])
            
            # Add all station codes in batch
            index_operations = []
            for station_code in station_codes:
                index_operations.append(glide_client.sadd("station:all", [station_code]))
            
            # Execute index operations
            await execute_operations_with_error_logging(index_operations, "STATION indexes")
            
            # Set TTL on index key
            await glide_client.expire("station:all", TTL_STATION)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update station:all index: {type(error).__name__}: {str(error)}")
    
    # Update name index
    for name, codes in name_index.items():
        try:
            name_key = f"station:name:{name}"
            await glide_client.delete([name_key])
            
            # Add all codes for this name
            name_operations = []
            for code in codes:
                name_operations.append(glide_client.sadd(name_key, [code]))
            
            # Execute name operations
            await execute_operations_with_error_logging(name_operations, f"STATION name:{name}")
            
            # Set TTL on name index key
            await glide_client.expire(name_key, TTL_STATION)
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update station name index {name}: {type(error).__name__}: {str(error)}")
    
    # Update IATA index
    for iata, icao in iata_index.items():
        try:
            iata_key = f"station:iata:{iata}"
            # Store IATA->ICAO mapping with TTL atomically
            await glide_client.set(
                iata_key,
                icao,
                expiry=ExpirySet(ExpiryType.SEC, TTL_STATION)
            )
        except Exception as error:
            logger.error(f"[Cache Store] Failed to update station IATA index {iata}: {type(error).__name__}: {str(error)}")
    
    logger.info(f"Stored {len(station_codes)} station records")


async def process_cache_file(data_type: str, source_url: str) -> Dict[str, Any]:
    """Process a cache file and store data in ValKey.
    
    For G-AIRMET JSON API, fetches all forecast hours (0, 3, 6, 9, 12) and combines them.
    """
    start_time = datetime.utcnow()
    records_processed = 0
    errors = []
    
    try:
        # Special handling for G-AIRMET JSON API: fetch all forecast hours
        if data_type == "airmet" and 'api/data/gairmet' in source_url and 'format=json' in source_url:
            logger.info("[G-AIRMET] Fetching all forecast hours (0, 3, 6, 9, 12)")
            all_records = []
            
            for fore_hour in [0, 3, 6, 9, 12]:
                try:
                    # Add fore parameter to URL
                    url_with_fore = source_url
                    if '?' in url_with_fore:
                        url_with_fore += f'&fore={fore_hour}'
                    else:
                        url_with_fore += f'?fore={fore_hour}'
                    
                    logger.info(f"[G-AIRMET] Fetching fore={fore_hour}")
                    data = download_and_decompress(url_with_fore)
                    records = parse_json_airmet(data)
                    all_records.extend(records)
                    logger.info(f"[G-AIRMET] fore={fore_hour}: {len(records)} records")
                except Exception as e:
                    logger.error(f"[G-AIRMET] Failed to fetch fore={fore_hour}: {str(e)}")
                    # Continue with other forecast hours
            
            records = all_records
            logger.info(f"[G-AIRMET] Combined total: {len(records)} records from all forecast hours")
        else:
            # Standard single-file processing
            # Download and decompress
            data = download_and_decompress(source_url)
            
            # Save to S3 for backup
            filename = source_url.split('/')[-1].split('?')[0]
            save_to_s3(data, filename)
            
            # Parse based on data type
            records = []
            if data_type == "metar":
                records = parse_csv_metar(data)
            elif data_type == "taf":
                records = parse_xml_taf(data)
            elif data_type == "sigmet":
                # Check if source is JSON API or CSV cache file
                if source_url.endswith('.json') or 'format=json' in source_url:
                    records = parse_json_sigmet(data)
                else:
                    records = parse_csv_sigmet(data)  # Keep CSV as fallback
            elif data_type == "airmet":
                # Check if source is JSON API or XML cache file
                if source_url.endswith('.json') or 'format=json' in source_url:
                    records = parse_json_airmet(data)
                else:
                    records = parse_xml_airmet(data)  # Keep XML as fallback
            elif data_type == "pirep":
                records = parse_csv_pirep(data)
            elif data_type == "station":
                records = parse_json_stations(data)
            else:
                raise ValueError(f"Unknown data type: {data_type}")
        
        # Connect to Glide
        try:
            glide_client = await get_glide_client()
        except Exception as e:
            # Reset connection on failure
            reset_glide_client()
            raise
        
        # Store records
        try:
            if data_type == "metar":
                await store_metar(glide_client, records)
            elif data_type == "taf":
                await store_taf(glide_client, records)
            elif data_type == "sigmet":
                await store_sigmet(glide_client, records)
            elif data_type == "airmet":
                await store_airmet(glide_client, records)
            elif data_type == "pirep":
                await store_pirep(glide_client, records)
            elif data_type == "station":
                await store_stations(glide_client, records)
        except Exception as e:
            # Reset connection if storage fails (might be connection issue)
            reset_glide_client()
            raise
        
        records_processed = len(records)
        
    except Exception as e:
        error_msg = f"Error processing {data_type} from {source_url}: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)
        raise
    
    duration = (datetime.utcnow() - start_time).total_seconds()
    
    return {
        "dataType": data_type,
        "source": source_url,
        "recordsProcessed": records_processed,
        "durationSeconds": duration,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for cache ingestion.
    
    Expected event structure:
    {
        "dataType": "metar|taf|sigmet|airmet|pirep|station",
        "source": "https://aviationweather.gov/data/cache/..."
    }
    """
    async def async_handler():
        try:
            data_type = event.get('dataType', '').lower()
            source_url = event.get('source', '')
            
            if not data_type or not source_url:
                raise ValueError("dataType and source are required in event")
            
            logger.info(f"Processing {data_type} cache from {source_url}")
            
            result = await process_cache_file(data_type, source_url)
            
            logger.info(f"Successfully processed {result['recordsProcessed']} {data_type} records in {result['durationSeconds']:.2f}s")
            
            # Publish success metrics to CloudWatch
            try:
                cloudwatch_client.put_metric_data(
                    Namespace=f"WeatherCache/{STAGE}",
                    MetricData=[
                        {
                            'MetricName': f'{data_type.title()}RecordsProcessed',
                            'Value': result['recordsProcessed'],
                            'Unit': 'Count',
                            'Dimensions': [
                                {'Name': 'DataType', 'Value': data_type},
                                {'Name': 'Stage', 'Value': STAGE}
                            ]
                        },
                        {
                            'MetricName': f'{data_type.title()}Duration',
                            'Value': result['durationSeconds'],
                            'Unit': 'Seconds',
                            'Dimensions': [
                                {'Name': 'DataType', 'Value': data_type},
                                {'Name': 'Stage', 'Value': STAGE}
                            ]
                        },
                        {
                            'MetricName': f'{data_type.title()}Success',
                            'Value': 1,
                            'Unit': 'Count',
                            'Dimensions': [
                                {'Name': 'DataType', 'Value': data_type},
                                {'Name': 'Stage', 'Value': STAGE}
                            ]
                        }
                    ]
                )
            except Exception as metric_error:
                # Don't fail the handler if metrics fail
                logger.warning(f"Failed to publish metrics: {str(metric_error)}")
            
            return {
                "statusCode": 200,
                "body": json.dumps(result)
            }
            
        except Exception as e:
            logger.error(f"Handler error: {str(e)}", exc_info=True)
            
            # Publish error metrics to CloudWatch
            data_type = event.get('dataType', 'unknown')
            try:
                cloudwatch_client.put_metric_data(
                    Namespace=f"WeatherCache/{STAGE}",
                    MetricData=[
                        {
                            'MetricName': f'{data_type.title()}Errors',
                            'Value': 1,
                            'Unit': 'Count',
                            'Dimensions': [
                                {'Name': 'DataType', 'Value': data_type},
                                {'Name': 'Stage', 'Value': STAGE},
                                {'Name': 'ErrorType', 'Value': type(e).__name__}
                            ]
                        }
                    ]
                )
            except Exception as metric_error:
                # Don't fail the handler if metrics fail
                logger.warning(f"Failed to publish error metrics: {str(metric_error)}")
            
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": str(e),
                    "dataType": data_type,
                    "timestamp": datetime.utcnow().isoformat() + 'Z'
                })
            }
    
    # Run async handler
    return asyncio.run(async_handler())
