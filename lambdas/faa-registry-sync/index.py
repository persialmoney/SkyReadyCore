"""
FAA Aircraft Registry Sync Lambda

Downloads the FAA's daily aircraft registration database (ReleasableAircraft.zip),
parses the CSV data, and populates DynamoDB aircraft cache.

Data source: https://registry.faa.gov/database/ReleasableAircraft.zip
Updates daily at 11:30 PM CT, this Lambda runs at 12:30 AM CT

CSV Files:
- MASTER.txt: Aircraft registration master file (fixed-width format)
- ACFTREF.txt: Aircraft reference (MFR MDL CODE -> Make/Model mapping)
"""

import json
import os
import boto3
import time
import urllib.request
import zipfile
import io
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from decimal import Decimal

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Environment variables
STAGE = os.environ.get('STAGE', 'dev')
AIRCRAFT_CACHE_TABLE = os.environ.get('AIRCRAFT_CACHE_TABLE', f'sky-ready-aircraft-registry-cache-{STAGE}')
CACHE_FILES_BUCKET = os.environ.get('CACHE_FILES_BUCKET', f'sky-ready-weather-cache-files-{STAGE}')

# FAA data source
FAA_ZIP_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"

# Cache TTL: 365 days (FAA data is relatively stable)
TTL_DAYS = 365


class FAAMasterParser:
    """
    Parser for FAA MASTER.txt file (fixed-width CSV format)
    
    Field positions per FAA documentation:
    https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/media/ardata.pdf
    """
    
    # Field definitions: (start_pos - 1, end_pos) - positions are 1-indexed in docs
    FIELDS = {
        'N_NUMBER': (0, 5),              # N-Number (without 'N' prefix)
        'SERIAL_NUMBER': (5, 35),        # Serial Number
        'MFR_MDL_CODE': (35, 42),        # Manufacturer Model Code
        'ENG_MFR_MDL': (42, 47),         # Engine Manufacturer Model
        'YEAR_MFR': (47, 51),            # Year Manufactured
        'TYPE_REGISTRANT': (51, 52),     # Type Registrant (1=Individual, 2=Partnership, etc)
        'NAME': (52, 102),               # Owner Name
        'TYPE_AIRCRAFT': (102, 103),     # Type Aircraft (1=Glider, 2=Balloon, 3=Blimp/Dirigible, 
                                          # 4=Fixed wing single engine, 5=Fixed wing multi engine, 
                                          # 6=Rotorcraft, 7=Weight-shift-control, 8=Powered Parachute, 9=Gyroplane)
        'TYPE_ENGINE': (103, 105),       # Type Engine (0=None, 1=Reciprocating, 2=Turbo-prop, 
                                          # 3=Turbo-shaft, 4=Turbo-jet, 5=Turbo-fan, 6=Ramjet, 
                                          # 7=2 Cycle, 8=4 Cycle, 10=Electric, 11=Rotary)
        'STATUS_CODE': (105, 106),       # Status Code (V=Valid, blank=others)
    }
    
    # Aircraft type mappings
    AIRCRAFT_TYPE_MAP = {
        '1': ('Glider', ''),
        '2': ('Lighter-than-Air', 'Balloon'),
        '3': ('Lighter-than-Air', 'Airship'),
        '4': ('Airplane', 'Single-Engine Land'),
        '5': ('Airplane', 'Multi-Engine Land'),
        '6': ('Rotorcraft', 'Helicopter'),
        '7': ('Weight-Shift Control', ''),
        '8': ('Powered Parachute', ''),
        '9': ('Rotorcraft', 'Gyroplane'),
    }
    
    # Engine type mappings
    ENGINE_TYPE_MAP = {
        '0': 'None',
        '1': 'Reciprocating',
        '2': 'Turbo-prop',
        '3': 'Turbo-shaft',
        '4': 'Turbo-jet',
        '5': 'Turbo-fan',
        '6': 'Ramjet',
        '7': '2 Cycle',
        '8': '4 Cycle',
        '10': 'Electric',
        '11': 'Rotary',
    }
    
    @staticmethod
    def parse_line(line: str) -> Optional[Dict[str, str]]:
        """Parse a single line from MASTER.txt"""
        if len(line) < 106:
            return None
        
        data = {}
        for field, (start, end) in FAAMasterParser.FIELDS.items():
            value = line[start:end].strip()
            data[field] = value
        
        # Only process valid registrations
        if data['STATUS_CODE'] != 'V':
            return None
        
        # Must have N-Number
        if not data['N_NUMBER']:
            return None
        
        return data


class ACFTREFParser:
    """
    Parser for ACFTREF.txt file (aircraft reference)
    Maps MFR_MDL_CODE to Make/Model names
    """
    
    @staticmethod
    def parse_file(content: str) -> Dict[str, Dict[str, str]]:
        """
        Parse ACFTREF.txt and return mapping of MFR_MDL_CODE -> {make, model}
        
        Format: Fixed-width CSV
        CODE (0-7), MFR (8-38), MODEL (39-59), TYPE-ACFT (60-61), TYPE-ENG (62-63), AC-CAT (64-64), ...
        """
        mapping = {}
        
        for line in content.split('\n'):
            if len(line) < 60:
                continue
            
            code = line[0:7].strip()
            mfr = line[8:38].strip()
            model = line[39:59].strip()
            
            if code and (mfr or model):
                mapping[code] = {
                    'make': mfr,
                    'model': model
                }
        
        return mapping


def download_faa_zip() -> bytes:
    """Download FAA aircraft registry ZIP file"""
    print(f"[FAA-Sync] Downloading from {FAA_ZIP_URL}")
    
    req = urllib.request.Request(
        FAA_ZIP_URL,
        headers={
            'User-Agent': 'SkyReady/1.0 (Flight Training App)',
            'Accept': 'application/zip'
        }
    )
    
    with urllib.request.urlopen(req, timeout=120) as response:
        zip_data = response.read()
    
    print(f"[FAA-Sync] Downloaded {len(zip_data)} bytes")
    return zip_data


def extract_files(zip_data: bytes) -> Dict[str, str]:
    """Extract MASTER.txt and ACFTREF.txt from ZIP"""
    print("[FAA-Sync] Extracting CSV files from ZIP")
    
    files = {}
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
        for filename in ['MASTER.txt', 'ACFTREF.txt']:
            if filename in zip_file.namelist():
                content = zip_file.read(filename).decode('utf-8', errors='ignore')
                files[filename] = content
                print(f"[FAA-Sync] Extracted {filename}: {len(content)} bytes")
            else:
                print(f"[FAA-Sync] WARNING: {filename} not found in ZIP")
    
    return files


def parse_and_store(master_content: str, acftref_content: str) -> Dict[str, int]:
    """
    Parse CSV files and store in DynamoDB
    
    Returns stats: {total, valid, stored, errors}
    """
    print("[FAA-Sync] Parsing CSV data")
    
    # Parse aircraft reference first (for make/model mapping)
    acftref_mapping = ACFTREFParser.parse_file(acftref_content)
    print(f"[FAA-Sync] Loaded {len(acftref_mapping)} aircraft reference records")
    
    # Get DynamoDB table
    table = dynamodb.Table(AIRCRAFT_CACHE_TABLE)
    
    # Parse MASTER.txt and batch write to DynamoDB
    lines = master_content.split('\n')
    total_lines = len(lines)
    
    stats = {
        'total': total_lines,
        'valid': 0,
        'stored': 0,
        'errors': 0
    }
    
    batch_items = []
    batch_size = 25  # DynamoDB BatchWriteItem limit
    
    now = int(time.time())
    ttl = now + (TTL_DAYS * 24 * 60 * 60)
    
    for i, line in enumerate(lines):
        if i % 10000 == 0:
            print(f"[FAA-Sync] Processing line {i}/{total_lines} ({stats['valid']} valid, {stats['stored']} stored)")
        
        # Parse line
        data = FAAMasterParser.parse_line(line)
        if not data:
            continue
        
        stats['valid'] += 1
        
        try:
            # Get make/model from reference file
            mfr_mdl_code = data['MFR_MDL_CODE']
            make = ''
            model = ''
            
            if mfr_mdl_code in acftref_mapping:
                make = acftref_mapping[mfr_mdl_code].get('make', '')
                model = acftref_mapping[mfr_mdl_code].get('model', '')
            
            # Map aircraft type and engine type
            aircraft_type_code = data['TYPE_AIRCRAFT']
            category, aircraft_class = FAAMasterParser.AIRCRAFT_TYPE_MAP.get(
                aircraft_type_code, 
                ('Unknown', '')
            )
            
            engine_type_code = data['TYPE_ENGINE']
            engine_type = FAAMasterParser.ENGINE_TYPE_MAP.get(engine_type_code, 'Unknown')
            
            # Build DynamoDB item
            item = {
                'tailNumber': f"N{data['N_NUMBER']}",  # Add N prefix
                'make': make,
                'model': model,
                'year': data['YEAR_MFR'] if data['YEAR_MFR'] else None,
                'category': category,
                'class': aircraft_class,
                'serialNumber': data['SERIAL_NUMBER'],
                'engineType': engine_type,
                'cachedAt': now,
                'ttl': ttl
            }
            
            batch_items.append(item)
            
            # Batch write when we reach batch size
            if len(batch_items) >= batch_size:
                batch_write_items(table, batch_items)
                stats['stored'] += len(batch_items)
                batch_items = []
        
        except Exception as e:
            print(f"[FAA-Sync] Error processing line {i}: {str(e)}")
            stats['errors'] += 1
    
    # Write remaining items
    if batch_items:
        batch_write_items(table, batch_items)
        stats['stored'] += len(batch_items)
    
    print(f"[FAA-Sync] Parsing complete: {stats}")
    return stats


def batch_write_items(table, items: List[Dict[str, Any]]) -> None:
    """Batch write items to DynamoDB with retry logic"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            with table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            return
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"[FAA-Sync] Batch write failed (attempt {attempt + 1}), retrying in {wait_time}s: {str(e)}")
                time.sleep(wait_time)
            else:
                print(f"[FAA-Sync] Batch write failed after {max_retries} attempts: {str(e)}")
                raise


def archive_zip_to_s3(zip_data: bytes) -> str:
    """Archive ZIP file to S3 for audit trail"""
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    key = f"faa-registry-archive/ReleasableAircraft-{timestamp}.zip"
    
    print(f"[FAA-Sync] Archiving ZIP to s3://{CACHE_FILES_BUCKET}/{key}")
    
    s3_client.put_object(
        Bucket=CACHE_FILES_BUCKET,
        Key=key,
        Body=zip_data,
        ContentType='application/zip'
    )
    
    return key


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for FAA aircraft registry sync
    
    Triggered daily by EventBridge at 12:30 AM CT
    """
    start_time = time.time()
    
    try:
        print(f"[FAA-Sync] Starting FAA aircraft registry sync for stage: {STAGE}")
        print(f"[FAA-Sync] Target table: {AIRCRAFT_CACHE_TABLE}")
        
        # Step 1: Download ZIP
        zip_data = download_faa_zip()
        
        # Step 2: Archive to S3
        s3_key = archive_zip_to_s3(zip_data)
        
        # Step 3: Extract files
        files = extract_files(zip_data)
        
        if 'MASTER.txt' not in files or 'ACFTREF.txt' not in files:
            raise ValueError("Required CSV files not found in ZIP")
        
        # Step 4: Parse and store
        stats = parse_and_store(files['MASTER.txt'], files['ACFTREF.txt'])
        
        elapsed_time = time.time() - start_time
        
        result = {
            'statusCode': 200,
            'message': 'FAA aircraft registry sync completed successfully',
            'stats': stats,
            's3Archive': s3_key,
            'elapsedSeconds': round(elapsed_time, 2)
        }
        
        print(f"[FAA-Sync] Sync completed in {elapsed_time:.2f}s: {json.dumps(result)}")
        return result
        
    except Exception as e:
        error_msg = f"FAA aircraft registry sync failed: {str(e)}"
        print(f"[FAA-Sync] ERROR: {error_msg}")
        
        return {
            'statusCode': 500,
            'error': error_msg,
            'elapsedSeconds': round(time.time() - start_time, 2)
        }
