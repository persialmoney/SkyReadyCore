"""
AppSync Lambda Resolver for aircraft registry lookup.
Queries FAA aircraft registry API and caches results in DynamoDB.
"""
import json
import os
import boto3
import time
import urllib.request
import urllib.parse
from html.parser import HTMLParser
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, Optional

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
aircraft_cache_table = dynamodb.Table(os.environ.get('AIRCRAFT_CACHE_TABLE', 'sky-ready-aircraft-registry-cache-dev'))

# FAA Registry API endpoint
FAA_REGISTRY_URL = "https://registry.faa.gov/aircraftinquiry/Search/NNumberResult"

# TTL: 90 days in seconds
TTL_SECONDS = 90 * 24 * 60 * 60


class FAARegistryParser(HTMLParser):
    """Parse FAA aircraft registry HTML response"""
    
    def __init__(self):
        super().__init__()
        self.data = {}
        self.current_label = None
        self.in_data_label = False
        self.in_data_content = False
        
    def handle_starttag(self, tag, attrs):
        # Look for data labels and content in the FAA response
        attrs_dict = dict(attrs)
        if 'class' in attrs_dict:
            if 'label' in attrs_dict['class']:
                self.in_data_label = True
            elif 'data' in attrs_dict['class'] or 'value' in attrs_dict['class']:
                self.in_data_content = True
    
    def handle_data(self, data):
        data = data.strip()
        if not data:
            return
            
        if self.in_data_label:
            self.current_label = data
        elif self.in_data_content and self.current_label:
            self.data[self.current_label] = data
            self.current_label = None
            
    def handle_endtag(self, tag):
        self.in_data_label = False
        self.in_data_content = False


def normalize_tail_number(tail_number: str) -> str:
    """Normalize tail number to uppercase, remove spaces and dashes"""
    if not tail_number:
        return ''
    
    # Remove 'N' prefix if present (FAA API expects it without)
    normalized = tail_number.upper().replace(' ', '').replace('-', '')
    if normalized.startswith('N'):
        normalized = normalized[1:]
    
    return normalized


def query_faa_registry(tail_number: str) -> Dict[str, Any]:
    """
    Query FAA aircraft registry API
    
    Args:
        tail_number: Aircraft N-number (without 'N' prefix)
    
    Returns:
        Dict with aircraft data or empty dict if not found
    """
    try:
        print(f"[AircraftLookup] Querying FAA registry for: {tail_number}")
        
        # Build URL with query parameters
        params = urllib.parse.urlencode({'nNumberTxt': tail_number})
        url = f"{FAA_REGISTRY_URL}?{params}"
        
        # Make request
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': 'SkyReady/1.0 (Flight Training App)',
                'Accept': 'text/html'
            }
        )
        
        with urllib.request.urlopen(req, timeout=10) as response:
            html = response.read().decode('utf-8')
        
        # Parse HTML response
        parser = FAARegistryParser()
        parser.feed(html)
        
        # Check if aircraft was found
        if not parser.data or 'not found' in html.lower():
            print(f"[AircraftLookup] Aircraft not found in FAA registry: {tail_number}")
            return {}
        
        print(f"[AircraftLookup] Found aircraft data: {parser.data}")
        return parser.data
        
    except Exception as e:
        print(f"[AircraftLookup] Error querying FAA registry: {str(e)}")
        return {}


def parse_aircraft_data(faa_data: Dict[str, str], tail_number: str) -> Dict[str, Any]:
    """
    Parse FAA data into standardized aircraft format
    
    Args:
        faa_data: Raw data from FAA parser
        tail_number: Original tail number query
    
    Returns:
        Standardized aircraft data dict
    """
    # Extract fields (FAA field names may vary)
    make = faa_data.get('Manufacturer Name', faa_data.get('Make', ''))
    model = faa_data.get('Model', '')
    year = faa_data.get('Year Manufactured', faa_data.get('Year', ''))
    serial_number = faa_data.get('Serial Number', '')
    engine_type = faa_data.get('Engine Type', faa_data.get('Type Engine', ''))
    
    # Derive category and class from aircraft type
    aircraft_category = faa_data.get('Aircraft Category', faa_data.get('Type Aircraft', ''))
    
    # Map FAA categories to our standard categories
    category = 'Airplane'  # Default
    aircraft_class = 'Single-Engine Land'  # Default
    
    if aircraft_category:
        category_lower = aircraft_category.lower()
        if 'rotorcraft' in category_lower or 'helicopter' in category_lower:
            category = 'Rotorcraft'
            aircraft_class = 'Helicopter'
        elif 'glider' in category_lower:
            category = 'Glider'
            aircraft_class = ''
        elif 'balloon' in category_lower or 'airship' in category_lower:
            category = 'Lighter-than-Air'
            aircraft_class = ''
    
    # Determine engine configuration for airplanes
    if category == 'Airplane':
        engine_count = faa_data.get('Number of Engines', '1')
        if '2' in engine_count or 'multi' in engine_type.lower():
            aircraft_class = 'Multi-Engine Land'
        else:
            aircraft_class = 'Single-Engine Land'
    
    return {
        'tailNumber': f"N{tail_number}",  # Add N prefix back
        'make': make,
        'model': model,
        'year': year,
        'category': category,
        'class': aircraft_class,
        'serialNumber': serial_number,
        'engineType': engine_type,
    }


def get_from_cache(tail_number: str) -> Optional[Dict[str, Any]]:
    """
    Get aircraft data from DynamoDB cache
    
    Args:
        tail_number: Normalized tail number (without 'N')
    
    Returns:
        Cached aircraft data or None if not found/expired
    """
    try:
        response = aircraft_cache_table.get_item(
            Key={'tailNumber': f"N{tail_number}"}
        )
        
        item = response.get('Item')
        if not item:
            return None
        
        # Check if expired (extra check beyond DynamoDB TTL)
        cached_at = item.get('cachedAt', 0)
        now = int(time.time())
        if now - cached_at > TTL_SECONDS:
            print(f"[AircraftLookup] Cache expired for: {tail_number}")
            return None
        
        print(f"[AircraftLookup] Cache hit for: {tail_number}")
        return convert_item(item)
        
    except Exception as e:
        print(f"[AircraftLookup] Error reading from cache: {str(e)}")
        return None


def cache_aircraft(aircraft_data: Dict[str, Any]) -> None:
    """
    Store aircraft data in DynamoDB cache with 90-day TTL
    
    Args:
        aircraft_data: Aircraft data to cache
    """
    try:
        now = int(time.time())
        ttl = now + TTL_SECONDS
        
        item = {
            **aircraft_data,
            'cachedAt': now,
            'ttl': ttl  # DynamoDB TTL attribute
        }
        
        aircraft_cache_table.put_item(Item=item)
        print(f"[AircraftLookup] Cached aircraft: {aircraft_data.get('tailNumber')}")
        
    except Exception as e:
        print(f"[AircraftLookup] Error caching aircraft: {str(e)}")


def convert_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Convert DynamoDB types to JSON-serializable types"""
    if item is None:
        return None
    if isinstance(item, dict):
        return {k: convert_item(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convert_item(v) for v in item]
    elif isinstance(item, Decimal):
        if item % 1 == 0:
            return int(item)
        return float(item)
    else:
        return item


def handler(event: Dict[str, Any], context: Any) -> Optional[Dict[str, Any]]:
    """
    AppSync Lambda resolver handler for aircraft lookup
    
    Event structure from AppSync:
    {
        "arguments": {
            "tailNumber": "N12345"
        },
        "info": {
            "fieldName": "lookupAircraft"
        }
    }
    """
    try:
        arguments = event.get('arguments', {})
        tail_number = arguments.get('tailNumber', '')
        
        if not tail_number:
            raise ValueError("tailNumber is required")
        
        # Normalize tail number
        normalized = normalize_tail_number(tail_number)
        
        print(f"[AircraftLookup] Looking up aircraft: {tail_number} (normalized: {normalized})")
        
        # 1. Check cache
        cached_data = get_from_cache(normalized)
        if cached_data:
            return cached_data
        
        # 2. Query FAA registry
        faa_data = query_faa_registry(normalized)
        if not faa_data:
            print(f"[AircraftLookup] Aircraft not found: {tail_number}")
            return None
        
        # 3. Parse and standardize data
        aircraft_data = parse_aircraft_data(faa_data, normalized)
        
        # 4. Cache for future lookups
        cache_aircraft(aircraft_data)
        
        return aircraft_data
        
    except Exception as e:
        error_message = f"Error looking up aircraft: {str(e)}"
        print(f"[AircraftLookup] ERROR: {error_message}")
        raise Exception(error_message)
