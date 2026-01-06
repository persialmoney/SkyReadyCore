"""
Lambda function for ingesting aviation weather data from AWC cache API into ElastiCache ValKey.
Downloads cache files, parses them, and stores data in ValKey with appropriate TTLs.
"""
import json
import os
import gzip
import csv
import xml.etree.ElementTree as ET
import urllib.request
import urllib.error
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional
from glide import (
    ClosingError,
    ConnectionError as GlideConnectionError,
    GlideClusterClient,
    GlideClusterClientConfiguration,
    Logger as GlideLogger,
    LogLevel,
    NodeAddress,
    RequestError,
    TimeoutError as GlideTimeoutError,
)
import boto3
import logging

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
TTL_METAR = 120  # 2 minutes
TTL_TAF = 900  # 15 minutes
TTL_SIGMET = 120  # 2 minutes
TTL_AIRMET = 120  # 2 minutes
TTL_PIREP = 120  # 2 minutes
TTL_STATION = 90000  # 25 hours

# Glide client (reused across invocations)
glide_client = None


async def get_glide_client():
    """
    Get or create Glide client connection.
    Following AWS tutorial: https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/LambdaRedis.html
    Using valkey-glide library for ElastiCache Serverless access.
    """
    global glide_client
    if glide_client is None:
        try:
            # Set Glide logger configuration
            GlideLogger.set_logger_config(LogLevel.INFO)
            
            # Configure Glide client for VPC connections
            # ElastiCache Serverless doesn't require TLS unless in-transit encryption is enabled
            addresses = [
                NodeAddress(ELASTICACHE_ENDPOINT, ELASTICACHE_PORT)
            ]
            config = GlideClusterClientConfiguration(
                addresses=addresses,
                use_tls=False,  # Set to True if in-transit encryption is enabled
            )
            
            # Create the client
            glide_client = await GlideClusterClient.create(config)
            
            # Test connection
            await glide_client.ping()
            logger.info(f"Connected to ElastiCache at {ELASTICACHE_ENDPOINT}:{ELASTICACHE_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to ElastiCache: {str(e)}")
            raise
    return glide_client


def download_and_decompress(url: str) -> bytes:
    """Download and decompress gzipped file from URL."""
    try:
        logger.info(f"Downloading {url}")
        with urllib.request.urlopen(url, timeout=30) as response:
            compressed_data = response.read()
        
        logger.info(f"Decompressing {len(compressed_data)} bytes")
        decompressed_data = gzip.decompress(compressed_data)
        logger.info(f"Decompressed to {len(decompressed_data)} bytes")
        
        return decompressed_data
    except urllib.error.URLError as e:
        logger.error(f"Error downloading {url}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error decompressing {url}: {str(e)}")
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


def parse_csv_sigmet(data: bytes) -> List[Dict[str, Any]]:
    """Parse SIGMET CSV data."""
    records = []
    try:
        text = data.decode('utf-8')
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            record = dict(row)
            # Generate unique ID if not present
            if 'airsigmetId' not in record or not record['airsigmetId']:
                record['airsigmetId'] = f"AWC-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{len(records)}"
            records.append(record)
        logger.info(f"Parsed {len(records)} SIGMET records from CSV")
    except Exception as e:
        logger.error(f"Error parsing SIGMET CSV: {str(e)}")
        raise
    return records


def parse_xml_airmet(data: bytes) -> List[Dict[str, Any]]:
    """Parse G-AIRMET XML data."""
    records = []
    try:
        root = ET.fromstring(data)
        for airmet_elem in root.findall('.//G-AIRMET'):
            record = {}
            for child in airmet_elem:
                tag = child.tag
                text = child.text
                record[tag] = text if text else None
            
            # Generate ID if not present
            if 'forecastId' not in record or not record['forecastId']:
                record['forecastId'] = f"AWC-GAIRMET-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{len(records)}"
            
            records.append(record)
        logger.info(f"Parsed {len(records)} G-AIRMET records from XML")
    except Exception as e:
        logger.error(f"Error parsing G-AIRMET XML: {str(e)}")
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
        
        # Store METAR data with TTL
        operations.append(glide_client.set(key, json.dumps(record), ex=TTL_METAR))
        station_ids.add(station_id)
        
        # Update sorted set with timestamp
        operations.append(glide_client.zadd("metar:updated", {station_id: current_time}))
    
    # Execute all operations concurrently
    if operations:
        await asyncio.gather(*operations, return_exceptions=True)
    
    # Update station set
    if station_ids:
        await glide_client.delete("metar:stations")
        # SADD with multiple members
        for station_id in station_ids:
            await glide_client.sadd("metar:stations", station_id)
    
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
        
        operations.append(glide_client.set(key, json.dumps(record), ex=TTL_TAF))
        station_ids.add(station_id)
        operations.append(glide_client.zadd("taf:updated", {station_id: current_time}))
    
    if operations:
        await asyncio.gather(*operations, return_exceptions=True)
    
    if station_ids:
        await glide_client.delete("taf:stations")
        for station_id in station_ids:
            await glide_client.sadd("taf:stations", station_id)
    
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
        operations.append(glide_client.set(key, json.dumps(record), ex=TTL_SIGMET))
        sigmet_ids.add(sigmet_id)
        
        # Index by hazard type
        hazard = record.get('hazard', 'UNKNOWN')
        if hazard not in hazard_types:
            hazard_types[hazard] = set()
        hazard_types[hazard].add(sigmet_id)
    
    if operations:
        await asyncio.gather(*operations, return_exceptions=True)
    
    # Update indexes
    if sigmet_ids:
        await glide_client.delete("sigmet:all")
        for sigmet_id in sigmet_ids:
            await glide_client.sadd("sigmet:all", sigmet_id)
    
    for hazard, ids in hazard_types.items():
        hazard_key = f"sigmet:hazard:{hazard}"
        await glide_client.delete(hazard_key)
        for sigmet_id in ids:
            await glide_client.sadd(hazard_key, sigmet_id)
    
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
        operations.append(glide_client.set(key, json.dumps(record), ex=TTL_AIRMET))
        airmet_ids.add(airmet_id)
        
        hazard = record.get('hazard', 'UNKNOWN')
        if hazard not in hazard_types:
            hazard_types[hazard] = set()
        hazard_types[hazard].add(airmet_id)
    
    if operations:
        await asyncio.gather(*operations, return_exceptions=True)
    
    if airmet_ids:
        await glide_client.delete("airmet:all")
        for airmet_id in airmet_ids:
            await glide_client.sadd("airmet:all", airmet_id)
    
    for hazard, ids in hazard_types.items():
        hazard_key = f"airmet:hazard:{hazard}"
        await glide_client.delete(hazard_key)
        for airmet_id in ids:
            await glide_client.sadd(hazard_key, airmet_id)
    
    logger.info(f"Stored {len(airmet_ids)} G-AIRMET records")


async def store_pirep(glide_client: GlideClusterClient, records: List[Dict[str, Any]]):
    """Store PIREP records in ValKey."""
    pirep_ids = set()
    current_time = int(datetime.utcnow().timestamp())
    operations = []
    
    for record in records:
        pirep_id = record.get('aircraftReportId') or record.get('id')
        if not pirep_id:
            continue
        
        key = f"pirep:{pirep_id}"
        operations.append(glide_client.set(key, json.dumps(record), ex=TTL_PIREP))
        pirep_ids.add(pirep_id)
        
        # Add to recent sorted set
        operations.append(glide_client.zadd("pirep:recent", {pirep_id: current_time}))
    
    if operations:
        await asyncio.gather(*operations, return_exceptions=True)
    
    if pirep_ids:
        await glide_client.delete("pirep:all")
        for pirep_id in pirep_ids:
            await glide_client.sadd("pirep:all", pirep_id)
    
    # Keep only last 1000 PIREPs in recent set
    await glide_client.zremrangebyrank("pirep:recent", 0, -1001)
    
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
        operations.append(glide_client.set(key, json.dumps(record), ex=TTL_STATION))
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
    
    if operations:
        await asyncio.gather(*operations, return_exceptions=True)
    
    # Update indexes
    if station_codes:
        await glide_client.delete("station:all")
        for station_code in station_codes:
            await glide_client.sadd("station:all", station_code)
    
    # Update name index
    for name, codes in name_index.items():
        name_key = f"station:name:{name}"
        await glide_client.delete(name_key)
        for code in codes:
            await glide_client.sadd(name_key, code)
    
    # Update IATA index
    for iata, icao in iata_index.items():
        await glide_client.set(f"station:iata:{iata}", icao)
    
    logger.info(f"Stored {len(station_codes)} station records")


async def process_cache_file(data_type: str, source_url: str) -> Dict[str, Any]:
    """Process a cache file and store data in ValKey."""
    start_time = datetime.utcnow()
    records_processed = 0
    errors = []
    
    try:
        # Download and decompress
        data = download_and_decompress(source_url)
        
        # Save to S3 for backup
        filename = source_url.split('/')[-1]
        save_to_s3(data, filename)
        
        # Parse based on data type
        records = []
        if data_type == "metar":
            records = parse_csv_metar(data)
        elif data_type == "taf":
            records = parse_xml_taf(data)
        elif data_type == "sigmet":
            records = parse_csv_sigmet(data)
        elif data_type == "airmet":
            records = parse_xml_airmet(data)
        elif data_type == "pirep":
            records = parse_csv_pirep(data)
        elif data_type == "station":
            records = parse_json_stations(data)
        else:
            raise ValueError(f"Unknown data type: {data_type}")
        
        # Connect to Glide
        glide_client = await get_glide_client()
        
        # Store records
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
            
            return {
                "statusCode": 200,
                "body": json.dumps(result)
            }
            
        except Exception as e:
            logger.error(f"Handler error: {str(e)}", exc_info=True)
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": str(e),
                    "dataType": event.get('dataType', 'unknown'),
                    "timestamp": datetime.utcnow().isoformat() + 'Z'
                })
            }
    
    # Run async handler
    return asyncio.run(async_handler())
