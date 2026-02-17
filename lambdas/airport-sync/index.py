"""
Airport data sync Lambda - Scheduled daily via EventBridge
Downloads OurAirports CSV and syncs to Valkey cache
"""
import asyncio
import csv
import json
import logging
import os
from typing import List, Dict, Any
from datetime import datetime
from glide import GlideClusterClient, GlideClusterClientConfiguration
import httpx

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AIRPORTS_CSV_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
VALKEY_ENDPOINT = os.environ.get('VALKEY_ENDPOINT', '')
TTL_AIRPORTS = 86400 * 7  # 7 days

async def download_airports() -> List[Dict[str, Any]]:
    """Download and parse OurAirports CSV"""
    logger.info(f"[Airport Sync] Starting download from {AIRPORTS_CSV_URL}")
    
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.get(AIRPORTS_CSV_URL)
        response.raise_for_status()
        
        # Parse CSV
        lines = response.text.splitlines()
        reader = csv.DictReader(lines)
        
        airports = []
        for row in reader:
            # ident is the ICAO code
            icao = row.get('ident', '').upper().strip()
            if not icao:
                continue
            
            airport = {
                'icao': icao,  # ident IS the ICAO code
                'name': row.get('name', ''),
                'type': row.get('type', ''),
                'latitude': float(row.get('latitude_deg', 0)) if row.get('latitude_deg') else None,
                'longitude': float(row.get('longitude_deg', 0)) if row.get('longitude_deg') else None,
                'elevation': int(float(row.get('elevation_ft', 0))) if row.get('elevation_ft') else None,
                'municipality': row.get('municipality', ''),
                'region': row.get('iso_region', ''),
                'country': row.get('iso_country', ''),
            }
            airports.append(airport)
        
        logger.info(f"[Airport Sync] Downloaded {len(airports)} airports from OurAirports")
        return airports

async def sync_to_valkey(airports: List[Dict[str, Any]]):
    """Sync airports to Valkey cache"""
    logger.info(f"[Airport Sync] Connecting to Valkey at {VALKEY_ENDPOINT}")
    
    config = GlideClusterClientConfiguration(
        addresses=[VALKEY_ENDPOINT],
        use_tls=True,
    )
    client = await GlideClusterClient.create(config)
    
    try:
        operations = []
        skipped = 0
        updated = 0
        
        logger.info(f"[Airport Sync] Starting sync of {len(airports)} airports")
        
        for airport in airports:
            icao = airport['icao']  # ident is ICAO
            key = f"airport:{icao}"
            
            # Check if key already exists (avoid unnecessary overwrites)
            existing = await client.get(key)
            if existing:
                # Key exists - skip or compare and update if changed
                try:
                    existing_data = json.loads(existing)
                    # Simple comparison - if data is identical, skip
                    if existing_data == airport:
                        skipped += 1
                        continue
                except json.JSONDecodeError:
                    # Corrupted data, overwrite
                    logger.warning(f"[Airport Sync] Corrupted data for {icao}, overwriting")
                    pass
            
            # Store or update full data under ICAO code
            operations.append(client.set(key, json.dumps(airport)))
            operations.append(client.expire(key, TTL_AIRPORTS))
            updated += 1
        
        # Execute all operations in batches to avoid overwhelming Valkey
        batch_size = 1000
        total_batches = (len(operations) + batch_size - 1) // batch_size
        
        logger.info(f"[Airport Sync] Processing {len(operations)} operations in {total_batches} batches")
        
        for i in range(0, len(operations), batch_size):
            batch = operations[i:i + batch_size]
            await asyncio.gather(*batch)
            batch_num = i // batch_size + 1
            logger.info(f"[Airport Sync] Processed batch {batch_num}/{total_batches}")
        
        # Store metadata
        metadata = {
            'total_airports': len(airports),
            'updated': updated,
            'skipped': skipped,
            'last_sync': datetime.utcnow().isoformat(),
        }
        await client.set('airport:metadata', json.dumps(metadata))
        await client.expire('airport:metadata', TTL_AIRPORTS)
        
        logger.info(f"[Airport Sync] Complete - Updated: {updated}, Skipped: {skipped}, Total: {len(airports)}")
        
        return {
            'total': len(airports),
            'updated': updated,
            'skipped': skipped,
        }
        
    finally:
        await client.close()
        logger.info("[Airport Sync] Valkey connection closed")

def lambda_handler(event, context):
    """EventBridge scheduled handler"""
    async def async_handler():
        try:
            logger.info("[Airport Sync] Starting airport data sync")
            
            airports = await download_airports()
            sync_result = await sync_to_valkey(airports)
            
            logger.info("[Airport Sync] Airport data sync completed successfully")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Airport sync completed',
                    'airports_total': sync_result['total'],
                    'airports_updated': sync_result['updated'],
                    'airports_skipped': sync_result['skipped'],
                })
            }
        except Exception as e:
            logger.error(f"[Airport Sync] Error: {str(e)}", exc_info=True)
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Airport sync failed',
                    'error': str(e)
                })
            }
    
    return asyncio.run(async_handler())
