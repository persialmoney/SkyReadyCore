"""
AppSync Lambda Resolver for aircraft registry lookup.
Queries DynamoDB aircraft cache populated by faa-registry-sync Lambda.
"""
import json
import os
import boto3
from decimal import Decimal
from typing import Dict, Any, Optional

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
aircraft_cache_table = dynamodb.Table(os.environ.get('AIRCRAFT_CACHE_TABLE', 'sky-ready-aircraft-registry-cache-dev'))


def normalize_tail_number(tail_number: str) -> str:
    """Normalize tail number to uppercase with N prefix"""
    if not tail_number:
        return ''
    
    normalized = tail_number.upper().replace(' ', '').replace('-', '')
    
    # Ensure N prefix
    if not normalized.startswith('N'):
        normalized = f"N{normalized}"
    
    return normalized


def get_from_cache(tail_number: str) -> Optional[Dict[str, Any]]:
    """
    Get aircraft data from DynamoDB cache
    
    Args:
        tail_number: Normalized tail number (with 'N' prefix)
    
    Returns:
        Cached aircraft data or None if not found
    """
    try:
        response = aircraft_cache_table.get_item(
            Key={'tailNumber': tail_number}
        )
        
        item = response.get('Item')
        if not item:
            print(f"[AircraftLookup] Aircraft not found in cache: {tail_number}")
            return None
        
        print(f"[AircraftLookup] Found aircraft in cache: {tail_number}")
        return convert_item(item)
        
    except Exception as e:
        print(f"[AircraftLookup] Error reading from cache: {str(e)}")
        return None


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
        
        # Query DynamoDB cache
        aircraft_data = get_from_cache(normalized)
        
        if not aircraft_data:
            print(f"[AircraftLookup] Aircraft not found: {tail_number}")
            return None
        
        # Remove internal fields
        aircraft_data.pop('cachedAt', None)
        aircraft_data.pop('ttl', None)
        
        return aircraft_data
        
    except Exception as e:
        error_message = f"Error looking up aircraft: {str(e)}"
        print(f"[AircraftLookup] ERROR: {error_message}")
        raise Exception(error_message)
