"""
Airport lookup Lambda - Fast Valkey-only lookups
Handles lookupAirport GraphQL query
"""
import asyncio
import json
import logging
import os
from typing import Optional, Dict, Any
from glide import GlideClusterClient, GlideClusterClientConfiguration

logger = logging.getLogger()
logger.setLevel(logging.INFO)

VALKEY_ENDPOINT = os.environ.get('VALKEY_ENDPOINT', '')

# Glide client (reused across invocations)
glide_client = None

async def get_glide_client() -> Optional[GlideClusterClient]:
    """Get or create Glide cluster client connection."""
    global glide_client
    
    if glide_client is not None:
        try:
            await asyncio.wait_for(glide_client.ping(), timeout=1.0)
            return glide_client
        except Exception:
            try:
                await glide_client.close()
            except Exception:
                pass
            glide_client = None
    
    try:
        config = GlideClusterClientConfiguration(
            addresses=[VALKEY_ENDPOINT],
            use_tls=True,
        )
        glide_client = await GlideClusterClient.create(config)
        logger.info("[Airport Lookup] Glide client connected")
        return glide_client
    except Exception as e:
        logger.error(f"[Airport Lookup] Failed to create Glide client: {str(e)}")
        glide_client = None
        return None

async def lookup_airport(code: str) -> Optional[Dict[str, Any]]:
    """
    Lookup airport from Valkey cache.
    Returns airport data if found, None otherwise.
    """
    if not code:
        return None
    
    code = code.strip().upper()
    
    glide_client = await get_glide_client()
    if not glide_client:
        logger.error("[Airport Lookup] Glide client unavailable")
        return None
    
    try:
        # Direct lookup by ICAO code
        cache_key = f"airport:{code}"
        cached_data = await glide_client.get(cache_key)
        
        if not cached_data:
            logger.info(f"[Airport Lookup] Not found: {code}")
            return None
        
        # Parse and return airport data
        airport = json.loads(cached_data)
        logger.info(f"[Airport Lookup] Found: {code} -> {airport.get('name')}")
        return {
            "code": code,
            "icao": airport.get("icao"),
            "name": airport.get("name"),
            "municipality": airport.get("municipality"),
            "country": airport.get("country"),
            "latitude": airport.get("latitude"),
            "longitude": airport.get("longitude"),
            "elevation": airport.get("elevation"),
            "type": airport.get("type"),
        }
                
    except Exception as e:
        logger.error(f"[Airport Lookup] Error looking up {code}: {str(e)}")
        return None

def lambda_handler(event, context):
    """
    AppSync direct Lambda resolver handler.
    Event structure from AppSync:
    {
        "arguments": {"code": "KSFO"},
        "identity": {...},
        "request": {...}
    }
    """
    async def async_handler():
        try:
            logger.info(f"[Airport Lookup] Event: {json.dumps(event)}")
            
            # Extract code from AppSync event
            arguments = event.get('arguments', {})
            code = arguments.get('code')
            
            if not code:
                logger.error("[Airport Lookup] Missing code argument")
                return None
            
            result = await lookup_airport(code)
            return result
            
        except Exception as e:
            logger.error(f"[Airport Lookup] Handler error: {str(e)}", exc_info=True)
            return None
        finally:
            # Cleanup: close Glide client connection
            global glide_client
            if glide_client is not None:
                try:
                    await glide_client.close()
                except Exception as e:
                    logger.warning(f"[Airport Lookup] Error closing Glide client: {str(e)}")
                finally:
                    glide_client = None
    
    return asyncio.run(async_handler())
