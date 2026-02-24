"""
Lambda function for inspecting ElastiCache ValKey to identify keys without TTL
and report cache health statistics.
"""
import json
import os
import asyncio
from typing import Dict, Any, List
from glide import (
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
)
import boto3
import logging

# Initialize CloudWatch client for custom metrics
cloudwatch_client = boto3.client('cloudwatch')

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get environment variables
ELASTICACHE_ENDPOINT = os.environ.get('ELASTICACHE_ENDPOINT')
ELASTICACHE_PORT = int(os.environ.get('ELASTICACHE_PORT', 6379))
STAGE = os.environ.get('STAGE', 'dev')

# Glide client (reused across invocations)
glide_client = None


async def get_glide_client():
    """Get or create Glide cluster client connection."""
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
            glide_client = None
            raise
    return glide_client


async def scan_keys_by_pattern(client: GlideClusterClient, pattern: str) -> List[str]:
    """Scan for keys matching the given pattern."""
    keys = []
    cursor = "0"
    
    try:
        # Use SCAN to iterate through keys matching the pattern
        while True:
            # SCAN returns [new_cursor, [keys]]
            result = await client.scan(cursor, match=pattern, count=1000)
            cursor = result[0]
            keys.extend(result[1])
            
            if cursor == "0":
                break
                
    except Exception as e:
        logger.error(f"Error scanning keys with pattern {pattern}: {str(e)}")
    
    return keys


async def check_key_ttl(client: GlideClusterClient, key: str) -> int:
    """
    Check the TTL of a key.
    Returns:
      - positive number: TTL in seconds
      - -1: key exists but has no expiration
      - -2: key does not exist
    """
    try:
        return await client.ttl(key)
    except Exception as e:
        logger.error(f"Error checking TTL for key {key}: {str(e)}")
        return -2


async def inspect_cache_pattern(client: GlideClusterClient, pattern: str, pattern_name: str) -> Dict[str, Any]:
    """Inspect cache keys for a specific pattern."""
    logger.info(f"Inspecting pattern: {pattern}")
    
    keys = await scan_keys_by_pattern(client, pattern)
    total_keys = len(keys)
    keys_without_ttl = 0
    keys_with_ttl = 0
    keys_nonexistent = 0
    sample_keys_without_ttl = []
    
    # Sample up to 100 keys to check TTL
    sample_size = min(100, total_keys)
    sample_keys = keys[:sample_size] if sample_size > 0 else []
    
    for key in sample_keys:
        ttl = await check_key_ttl(client, key)
        if ttl == -1:
            keys_without_ttl += 1
            if len(sample_keys_without_ttl) < 10:
                sample_keys_without_ttl.append(key)
        elif ttl == -2:
            keys_nonexistent += 1
        else:
            keys_with_ttl += 1
    
    result = {
        "pattern": pattern,
        "pattern_name": pattern_name,
        "total_keys": total_keys,
        "sample_size": sample_size,
        "keys_with_ttl": keys_with_ttl,
        "keys_without_ttl": keys_without_ttl,
        "keys_nonexistent": keys_nonexistent,
        "sample_keys_without_ttl": sample_keys_without_ttl,
    }
    
    logger.info(f"Pattern {pattern_name}: {total_keys} total, {keys_without_ttl}/{sample_size} sampled without TTL")
    
    return result


async def get_cache_memory_usage(client: GlideClusterClient) -> Dict[str, Any]:
    """Get memory usage statistics from Valkey."""
    try:
        info = await client.info(["memory"])
        
        # Parse the INFO output
        memory_stats = {}
        for line in info.split('\n'):
            if ':' in line and not line.startswith('#'):
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                if key in ['used_memory', 'used_memory_human', 'used_memory_rss', 
                          'used_memory_peak', 'used_memory_peak_human']:
                    memory_stats[key] = value
        
        return memory_stats
    except Exception as e:
        logger.error(f"Error getting memory usage: {str(e)}")
        return {}


async def inspect_cache():
    """Main inspection function."""
    client = await get_glide_client()
    
    # Patterns to inspect
    patterns = [
        ("metar:*", "METAR"),
        ("taf:*", "TAF"),
        ("pirep:*", "PIREP"),
        ("sigmet:*", "SIGMET"),
        ("airmet:*", "AIRMET"),
        ("station:*", "STATION"),
    ]
    
    results = []
    for pattern, name in patterns:
        result = await inspect_cache_pattern(client, pattern, name)
        results.append(result)
    
    # Get memory usage
    memory_stats = await get_cache_memory_usage(client)
    
    # Calculate summary statistics
    total_keys = sum(r["total_keys"] for r in results)
    total_without_ttl = sum(r["keys_without_ttl"] for r in results)
    total_sampled = sum(r["sample_size"] for r in results)
    
    summary = {
        "total_keys": total_keys,
        "total_sampled": total_sampled,
        "total_without_ttl": total_without_ttl,
        "memory_stats": memory_stats,
        "patterns": results,
    }
    
    # Publish metrics to CloudWatch
    try:
        metric_data = [
            {
                'MetricName': 'TotalCacheKeys',
                'Value': total_keys,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Stage', 'Value': STAGE}
                ]
            },
            {
                'MetricName': 'KeysWithoutTTL',
                'Value': total_without_ttl,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Stage', 'Value': STAGE}
                ]
            },
        ]
        
        # Add per-pattern metrics
        for result in results:
            metric_data.append({
                'MetricName': f'{result["pattern_name"]}KeyCount',
                'Value': result["total_keys"],
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Stage', 'Value': STAGE},
                    {'Name': 'Pattern', 'Value': result["pattern_name"]}
                ]
            })
            
            if result["sample_size"] > 0:
                metric_data.append({
                    'MetricName': f'{result["pattern_name"]}KeysWithoutTTL',
                    'Value': result["keys_without_ttl"],
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'Stage', 'Value': STAGE},
                        {'Name': 'Pattern', 'Value': result["pattern_name"]}
                    ]
                })
        
        cloudwatch_client.put_metric_data(
            Namespace=f"ValKeyCache/{STAGE}",
            MetricData=metric_data
        )
        logger.info("Published cache inspection metrics to CloudWatch")
    except Exception as e:
        logger.error(f"Failed to publish metrics: {str(e)}")
    
    return summary


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for cache inspection."""
    async def async_handler():
        try:
            logger.info("Starting cache inspection")
            result = await inspect_cache()
            logger.info(f"Cache inspection complete: {result['total_keys']} keys, {result['total_without_ttl']} without TTL")
            
            return {
                "statusCode": 200,
                "body": json.dumps(result, default=str)
            }
        except Exception as e:
            logger.error(f"Handler error: {str(e)}", exc_info=True)
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": str(e),
                    "errorType": type(e).__name__
                })
            }
    
    return asyncio.run(async_handler())
