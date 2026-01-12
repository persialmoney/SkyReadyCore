# Weather Cache System Documentation

## Overview

SkyReady uses AWS ElastiCache ValKey to cache aviation weather data from the Aviation Weather Center (AWC) cache API. This provides sub-millisecond latency for weather lookups while reducing external API calls and costs.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Aviation Weather Center Cache API                          │
│  - METARs: /data/cache/metars.cache.csv.gz (1 min)         │
│  - TAFs: /data/cache/tafs.cache.xml.gz (10 min)            │
│  - SIGMETs: /data/cache/airsigmets.cache.csv.gz (1 min)    │
│  - G-AIRMETs: /data/cache/gairmets.cache.xml.gz (1 min)   │
│  - PIREPs: /data/cache/aircraftreports.cache.csv.gz (1 min)│
│  - Stations: /data/cache/stations.cache.json.gz (daily)    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  Cache Ingest Lambda (weather-cache-ingest)                 │
│  Triggered by EventBridge:                                  │
│  - METAR/SIGMET/AIRMET/PIREP: Every 5 minutes               │
│  - TAF: Every 10 minutes                                    │
│  - Stations: Daily at midnight UTC                          │
│                                                              │
│  Process:                                                    │
│  1. Download gzipped cache file                             │
│  2. Decompress and parse (CSV/XML/JSON)                    │
│  3. Store in ElastiCache ValKey with TTLs                  │
│  4. Update index sets for efficient queries                 │
│  5. Backup raw file to S3                                   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  AWS ElastiCache ValKey                                      │
│  - Sub-millisecond latency                                  │
│  - Automatic TTL expiration                                 │
│  - Indexed for fast lookups                                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  Weather Lambda (AppSync Resolver)                           │
│  - Cache-first lookup                                       │
│  - API fallback if cache miss                               │
│  - Write-through caching                                    │
└─────────────────────────────────────────────────────────────┘
```

## Components

### 1. Cache Ingest Lambda (`lambdas/weather-cache-ingest/`)

**Purpose**: Downloads bulk cache files from AWC and stores parsed data in ElastiCache ValKey.

**Trigger**: EventBridge rules with different schedules:
- **METAR, SIGMET, G-AIRMET, PIREP**: Every 5 minutes
- **TAF**: Every 10 minutes
- **Stations**: Daily at 00:00 UTC

**Process Flow**:

1. **Download**: Fetches gzipped cache file from AWC API
2. **Decompress**: Extracts compressed data
3. **Parse**: Converts CSV/XML/JSON to structured Python dictionaries
4. **Store**: Writes to ValKey with appropriate TTLs
5. **Index**: Updates index sets (e.g., `metar:stations`, `sigmet:all`)
6. **Backup**: Saves raw file to S3 for debugging/recovery

**Key Functions**:
- `download_and_decompress()`: Downloads and decompresses gzipped files
- `parse_csv_metar()`, `parse_xml_taf()`, etc.: Parse different data formats
- `store_metar()`, `store_taf()`, etc.: Store data in ValKey with TTLs
- `save_to_s3()`: Backup raw files to S3

**Error Handling**:
- Graceful degradation: Continues processing if individual records fail
- Logging: Comprehensive error logging for debugging
- Retries: Built into Redis client for transient failures

### 2. Weather Lambda (`lambdas/weather/`)

**Purpose**: AppSync resolver that fetches weather data for GraphQL queries.

**Cache-First Strategy**:

1. **Check Cache**: Attempts to read from ElastiCache ValKey
   - Key format: `metar:{airportCode}` or `taf:{airportCode}`
   - Returns immediately if found (<1ms latency)

2. **API Fallback**: If cache miss or error:
   - Fetches from AWC REST API
   - Transforms to expected format
   - **Write-through**: Stores in cache for next request

3. **Return Data**: Returns formatted weather data to AppSync

**Key Functions**:
- `get_redis_client()`: Lazy initialization of Redis client
- `fetch_metar()`: Cache-first METAR retrieval
- `fetch_taf()`: Cache-first TAF retrieval
- `transform_metar_from_cache()`: Transforms cached data to API format
- `transform_taf_from_cache()`: Transforms cached TAF data

**Performance**:
- Cache hit: <1ms latency
- Cache miss: ~50-200ms (API call + cache write)
- Cache hit rate: Expected >95% after warm-up period

## Data Schema

See `lambdas/weather-cache-ingest/SCHEMA.md` for complete schema documentation.

### Key Structure

All keys use lowercase with colon separators:

```
metar:{stationId}        → JSON string with METAR data
taf:{stationId}          → JSON string with TAF data
sigmet:{id}              → JSON string with SIGMET data
airmet:{id}              → JSON string with G-AIRMET data
pirep:{id}               → JSON string with PIREP data
station:{code}           → JSON string with station info
```

### Index Keys

For efficient queries:

```
metar:stations           → SET of all station IDs with METARs
metar:updated            → ZSET of station IDs by update timestamp
sigmet:all               → SET of all active SIGMET IDs
sigmet:hazard:{type}     → SET of SIGMET IDs for specific hazard
station:all              → SET of all station codes
station:name:{name}      → SET of station codes matching name
station:iata:{iata}      → String mapping IATA to ICAO code
```

### TTL Values

Time-to-live values ensure data freshness:

- **METAR**: 120 seconds (2 minutes) - buffer beyond 1-minute updates
- **TAF**: 900 seconds (15 minutes) - buffer beyond 10-minute updates
- **SIGMET/AIRMET**: 120 seconds (2 minutes)
- **PIREP**: 120 seconds (2 minutes)
- **Stations**: 90000 seconds (25 hours) - buffer beyond daily updates

## Cache Ingest Process

### Step-by-Step Flow

1. **EventBridge Trigger**
   - EventBridge rule fires based on schedule
   - Passes event with `dataType` and `source` URL

2. **Download Cache File**
   ```python
   data = download_and_decompress(source_url)
   ```
   - Downloads gzipped file from AWC
   - Decompresses using `gzip.decompress()`
   - Returns raw bytes

3. **Parse Data**
   ```python
   if data_type == "metar":
       records = parse_csv_metar(data)
   elif data_type == "taf":
       records = parse_xml_taf(data)
   # ... etc
   ```
   - Parses based on format (CSV/XML/JSON)
   - Normalizes field names and types
   - Returns list of dictionaries

4. **Store in ValKey**
   ```python
   store_metar(redis_client, records)
   ```
   - Uses Redis pipeline for batch operations
   - Sets TTL for automatic expiration
   - Updates index sets for queries

5. **Backup to S3**
   ```python
   save_to_s3(data, filename)
   ```
   - Saves raw cache file for debugging
   - Lifecycle policy deletes after 7 days

### Example: METAR Cache Update

```python
# EventBridge event
{
    "dataType": "metar",
    "source": "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
}

# Process:
# 1. Download ~500KB gzipped file
# 2. Decompress to ~5MB CSV
# 3. Parse ~10,000 METAR records
# 4. Store each as: metar:{ICAO} → JSON
# 5. Update metar:stations SET
# 6. Update metar:updated ZSET
# Duration: ~2-5 seconds
```

## Cache Retrieval Process

### Step-by-Step Flow

1. **AppSync Query**
   ```graphql
   query {
     getMETAR(airportCode: "KJFK") {
       rawText
       temperature
       windSpeed
       # ...
     }
   }
   ```

2. **Lambda Handler**
   ```python
   weather_lambda.handler(event, context)
   # Calls fetch_metar("KJFK")
   ```

3. **Cache Check**
   ```python
   cache_key = f"metar:{airport_code}"
   cached_data = redis_client.get(cache_key)
   if cached_data:
       return transform_metar_from_cache(json.loads(cached_data))
   ```

4. **API Fallback** (if cache miss)
   ```python
   # Fetch from AWC API
   metar_data = fetch_from_api(airport_code)
   
   # Write-through: Store in cache
   redis_client.setex(cache_key, 120, json.dumps(metar_data))
   
   return transform_metar(metar_data)
   ```

### Performance Comparison

| Scenario | Latency | Cost per Request |
|----------|---------|------------------|
| Cache Hit | <1ms | $0 (included in ElastiCache cost) |
| Cache Miss | 50-200ms | ~$0.0002 (Lambda + API) |
| Direct API (old) | 50-500ms | ~$0.0002 |

**Expected Cache Hit Rate**: >95% after initial warm-up period

## Data Formats

### METAR (CSV)
```csv
icaoId,obsTime,rawOb,temp,dewp,wdir,wspd,visib,altim,...
KJFK,2025-12-23T11:51:00Z,METAR KJFK 231151Z...,12.2,2.8,280,15,10.0,30.12,...
```

### TAF (XML)
```xml
<response>
  <data>
    <TAF>
      <icaoId>KJFK</icaoId>
      <rawTAF>TAF KJFK 231100Z...</rawTAF>
      <issueTime>2025-12-23T11:00:00Z</issueTime>
      ...
    </TAF>
  </data>
</response>
```

### Stations (JSON)
```json
[
  {
    "icaoId": "KJFK",
    "iataId": "JFK",
    "name": "John F Kennedy International Airport",
    "latitude": 40.6413,
    "longitude": -73.7781,
    ...
  }
]
```

## Error Handling

### Cache Ingest Lambda

- **Download Failures**: Logs error and returns failure status
- **Parse Errors**: Logs and continues with remaining records
- **Redis Errors**: Logs and retries (built into Redis client)
- **S3 Backup Failures**: Logs warning but doesn't fail ingestion

### Weather Lambda

- **Cache Unavailable**: Falls back to API automatically
- **Cache Read Errors**: Logs warning and uses API
- **API Failures**: Returns error response to AppSync
- **Connection Timeouts**: 2-second timeout, then API fallback

## Monitoring

### Key Metrics

1. **Cache Hit Rate**: Percentage of requests served from cache
2. **Ingestion Success Rate**: Percentage of successful cache updates
3. **Latency**: P50, P95, P99 latencies for weather queries
4. **Error Rate**: Failed cache reads/writes and API calls

### CloudWatch Logs

- Cache ingest Lambda: Logs records processed, errors, timing
- Weather Lambda: Logs cache hits/misses, API fallbacks

### Example Logs

```
# Cache Ingest
INFO: Downloading https://aviationweather.gov/data/cache/metars.cache.csv.gz
INFO: Decompressed to 5242880 bytes
INFO: Parsed 10234 METAR records from CSV
INFO: Stored 10234 METAR records

# Weather Lambda
INFO: Cache hit for METAR: KJFK
INFO: Cache miss for METAR: KXYZ, fetching from API
```

## Troubleshooting

### Cache Not Populating

1. Check EventBridge rules are enabled
2. Verify Lambda has VPC access to ElastiCache
3. Check Lambda logs for download/parse errors
4. Verify ElastiCache endpoint in environment variables

### High Cache Miss Rate

1. Check cache-ingest Lambda is running successfully
2. Verify TTL values aren't too short
3. Check for Redis connection errors
4. Monitor cache memory usage

### Slow Performance

1. Check cache hit rate (should be >95%)
2. Verify ElastiCache is in same region as Lambda
3. Check VPC configuration (private subnets)
4. Monitor Lambda cold starts

## Cost Optimization

### Current Costs (Estimated)

- **ElastiCache ValKey Serverless**: $6-12/month (base cost)
- **Lambda Executions**: ~43,200/month (cache ingest)
  - Cost: ~$0.86/month
- **S3 Storage**: ~$0.50/month (cache file backups)
- **Total**: ~$7-13/month

### vs. Direct API Calls

- **1M API calls/month**: ~$200/month (Lambda invocations)
- **Savings**: ~$187/month (93% cost reduction)

### Optimization Tips

1. **Monitor Cache Hit Rate**: Aim for >95%
2. **Adjust TTLs**: Balance freshness vs. hit rate
3. **S3 Lifecycle**: Delete backups after 7 days
4. **Lambda Memory**: Tune based on actual usage

## Future Enhancements

1. **Proactive Alerting**: Add Lambda to check cached weather and trigger alerts
2. **Cache Warming**: Pre-populate cache for popular airports
3. **Multi-Region**: Replicate cache across regions for global users
4. **Metrics Dashboard**: CloudWatch dashboard for cache performance
5. **Cache Invalidation**: Manual cache refresh API endpoint

## References

- [Aviation Weather Center Cache API](https://aviationweather.gov/data/api/#cache)
- [ElastiCache ValKey Documentation](https://docs.aws.amazon.com/AmazonElastiCache/latest/valkey-ug/)
- [Redis Python Client](https://redis-py.readthedocs.io/)
- Schema Documentation: `lambdas/weather-cache-ingest/SCHEMA.md`

