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
from datetime import datetime
from typing import Dict, Any, List, Optional
import redis  # ValKey is Redis-compatible, so we use the redis Python library
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

# Redis connection pool (reused across invocations)
redis_client = None


def get_redis_client():
    """Get or create Redis client connection."""
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.Redis(
                host=ELASTICACHE_ENDPOINT,
                port=ELASTICACHE_PORT,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            # Test connection
            redis_client.ping()
            logger.info(f"Connected to ElastiCache at {ELASTICACHE_ENDPOINT}:{ELASTICACHE_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to ElastiCache: {str(e)}")
            raise
    return redis_client


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
                continue  # Skip records without station ID
            
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
            record['wxString'] = row.get('wx_string', '')
            
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
            # Extract fields from XML
            for child in taf_elem:
                tag = child.tag
                text = child.text
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
            
            # Extract raw TAF text
            if 'rawTAF' not in record:
                record['rawTAF'] = taf_elem.findtext('rawTAF', '')
            
            records.append(record)
        logger.info(f"Parsed {len(records)} TAF records from XML")
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


def store_metar(redis_client: redis.Redis, records: List[Dict[str, Any]]):
    """Store METAR records in ValKey."""
    pipeline = redis_client.pipeline()
    station_ids = set()
    current_time = int(datetime.utcnow().timestamp())
    
    for record in records:
        station_id = record.get('icaoId') or record.get('stationId')
        if not station_id:
            continue
        
        station_id = station_id.upper()
        key = f"metar:{station_id}"
        
        # Store METAR data
        pipeline.setex(key, TTL_METAR, json.dumps(record))
        station_ids.add(station_id)
        
        # Update sorted set with timestamp
        pipeline.zadd("metar:updated", {station_id: current_time})
    
    # Update station set
    if station_ids:
        pipeline.delete("metar:stations")
        pipeline.sadd("metar:stations", *station_ids)
    
    pipeline.execute()
    logger.info(f"Stored {len(station_ids)} METAR records")


def store_taf(redis_client: redis.Redis, records: List[Dict[str, Any]]):
    """Store TAF records in ValKey."""
    pipeline = redis_client.pipeline()
    station_ids = set()
    current_time = int(datetime.utcnow().timestamp())
    
    for record in records:
        station_id = record.get('icaoId') or record.get('stationId')
        if not station_id:
            continue
        
        station_id = station_id.upper()
        key = f"taf:{station_id}"
        
        pipeline.setex(key, TTL_TAF, json.dumps(record))
        station_ids.add(station_id)
        pipeline.zadd("taf:updated", {station_id: current_time})
    
    if station_ids:
        pipeline.delete("taf:stations")
        pipeline.sadd("taf:stations", *station_ids)
    
    pipeline.execute()
    logger.info(f"Stored {len(station_ids)} TAF records")


def store_sigmet(redis_client: redis.Redis, records: List[Dict[str, Any]]):
    """Store SIGMET records in ValKey."""
    pipeline = redis_client.pipeline()
    sigmet_ids = set()
    hazard_types = {}
    
    for record in records:
        sigmet_id = record.get('airsigmetId') or record.get('id')
        if not sigmet_id:
            continue
        
        key = f"sigmet:{sigmet_id}"
        pipeline.setex(key, TTL_SIGMET, json.dumps(record))
        sigmet_ids.add(sigmet_id)
        
        # Index by hazard type
        hazard = record.get('hazard', 'UNKNOWN')
        if hazard not in hazard_types:
            hazard_types[hazard] = set()
        hazard_types[hazard].add(sigmet_id)
    
    # Update indexes
    if sigmet_ids:
        pipeline.delete("sigmet:all")
        pipeline.sadd("sigmet:all", *sigmet_ids)
    
    for hazard, ids in hazard_types.items():
        hazard_key = f"sigmet:hazard:{hazard}"
        pipeline.delete(hazard_key)
        pipeline.sadd(hazard_key, *ids)
    
    pipeline.execute()
    logger.info(f"Stored {len(sigmet_ids)} SIGMET records")


def store_airmet(redis_client: redis.Redis, records: List[Dict[str, Any]]):
    """Store G-AIRMET records in ValKey."""
    pipeline = redis_client.pipeline()
    airmet_ids = set()
    hazard_types = {}
    
    for record in records:
        airmet_id = record.get('forecastId') or record.get('id')
        if not airmet_id:
            continue
        
        key = f"airmet:{airmet_id}"
        pipeline.setex(key, TTL_AIRMET, json.dumps(record))
        airmet_ids.add(airmet_id)
        
        hazard = record.get('hazard', 'UNKNOWN')
        if hazard not in hazard_types:
            hazard_types[hazard] = set()
        hazard_types[hazard].add(airmet_id)
    
    if airmet_ids:
        pipeline.delete("airmet:all")
        pipeline.sadd("airmet:all", *airmet_ids)
    
    for hazard, ids in hazard_types.items():
        hazard_key = f"airmet:hazard:{hazard}"
        pipeline.delete(hazard_key)
        pipeline.sadd(hazard_key, *ids)
    
    pipeline.execute()
    logger.info(f"Stored {len(airmet_ids)} G-AIRMET records")


def store_pirep(redis_client: redis.Redis, records: List[Dict[str, Any]]):
    """Store PIREP records in ValKey."""
    pipeline = redis_client.pipeline()
    pirep_ids = set()
    current_time = int(datetime.utcnow().timestamp())
    
    for record in records:
        pirep_id = record.get('aircraftReportId') or record.get('id')
        if not pirep_id:
            continue
        
        key = f"pirep:{pirep_id}"
        pipeline.setex(key, TTL_PIREP, json.dumps(record))
        pirep_ids.add(pirep_id)
        
        # Add to recent sorted set
        pipeline.zadd("pirep:recent", {pirep_id: current_time})
    
    if pirep_ids:
        pipeline.delete("pirep:all")
        pipeline.sadd("pirep:all", *pirep_ids)
    
    # Keep only last 1000 PIREPs in recent set
    pipeline.zremrangebyrank("pirep:recent", 0, -1001)
    
    pipeline.execute()
    logger.info(f"Stored {len(pirep_ids)} PIREP records")


def store_stations(redis_client: redis.Redis, records: List[Dict[str, Any]]):
    """Store Station records in ValKey."""
    pipeline = redis_client.pipeline()
    station_codes = set()
    name_index = {}
    iata_index = {}
    
    for record in records:
        station_code = record.get('icaoId') or record.get('id')
        if not station_code:
            continue
        
        station_code = station_code.upper()
        key = f"station:{station_code}"
        pipeline.setex(key, TTL_STATION, json.dumps(record))
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
    
    # Update indexes
    if station_codes:
        pipeline.delete("station:all")
        pipeline.sadd("station:all", *station_codes)
    
    # Update name index
    for name, codes in name_index.items():
        name_key = f"station:name:{name}"
        pipeline.delete(name_key)
        pipeline.sadd(name_key, *codes)
    
    # Update IATA index
    for iata, icao in iata_index.items():
        pipeline.set(f"station:iata:{iata}", icao)
    
    pipeline.execute()
    logger.info(f"Stored {len(station_codes)} station records")


def process_cache_file(data_type: str, source_url: str) -> Dict[str, Any]:
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
        
        # Connect to Redis
        redis_client = get_redis_client()
        
        # Store records
        if data_type == "metar":
            store_metar(redis_client, records)
        elif data_type == "taf":
            store_taf(redis_client, records)
        elif data_type == "sigmet":
            store_sigmet(redis_client, records)
        elif data_type == "airmet":
            store_airmet(redis_client, records)
        elif data_type == "pirep":
            store_pirep(redis_client, records)
        elif data_type == "station":
            store_stations(redis_client, records)
        
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
    try:
        data_type = event.get('dataType', '').lower()
        source_url = event.get('source', '')
        
        if not data_type or not source_url:
            raise ValueError("dataType and source are required in event")
        
        logger.info(f"Processing {data_type} cache from {source_url}")
        
        result = process_cache_file(data_type, source_url)
        
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
