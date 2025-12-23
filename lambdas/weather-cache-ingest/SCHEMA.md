# ElastiCache ValKey Schema for Aviation Weather Data

This document defines the schema for storing aviation weather data from the Aviation Weather Center cache API in AWS ElastiCache ValKey.

## Cache File Sources

Based on [Aviation Weather Center Cache API](https://aviationweather.gov/data/api/#cache):

- **METARs**: `/data/cache/metars.cache.csv.gz` or `/data/cache/metars.cache.xml.gz` (updated every 1 minute)
- **TAFs**: `/data/cache/tafs.cache.xml.gz` (updated every 10 minutes)
- **SIGMETs**: `/data/cache/airsigmets.cache.csv.gz` or `/data/cache/airsigmets.cache.xml.gz` (updated every 1 minute)
- **G-AIRMETs**: `/data/cache/gairmets.cache.xml.gz` (updated every 1 minute)
- **PIREPs**: `/data/cache/aircraftreports.cache.csv.gz` or `/data/cache/aircraftreports.cache.xml.gz` (updated every 1 minute)
- **Stations**: `/data/cache/stations.cache.json.gz` or `/data/cache/stations.cache.xml.gz` (updated once daily)

## Key Naming Conventions

### Pattern: `{type}:{identifier}`

- All keys use lowercase
- Colons (`:`) separate namespace components
- Identifiers are uppercase (airport codes, IDs)

## 1. METAR Data

### Key Format
```
metar:{stationId}
```

### Value Structure (JSON string)
```json
{
  "rawOb": "METAR KJFK 231151Z 28015G25KT 10SM FEW250 12/03 A3012 RMK AO2 SLP201 T01220028",
  "obsTime": "2025-12-23T11:51:00Z",
  "reportTime": "2025-12-23T11:51:00Z",
  "temp": 12.2,
  "dewp": 2.8,
  "wdir": 280,
  "wspd": 15,
  "wspdGust": 25,
  "visib": 10.0,
  "altim": 30.12,
  "altimInHg": 30.12,
  "slp": 1020.1,
  "flightCategory": "VFR",
  "metarType": "METAR",
  "elev": 13,
  "qcField": "V",
  "wxString": null,
  "presTend": null,
  "maxT": null,
  "minT": null,
  "maxT24": null,
  "minT24": null,
  "precip": null,
  "pcp3hr": null,
  "pcp6hr": null,
  "pcp24hr": null,
  "snow": null,
  "vertVis": null,
  "clouds": [
    {
      "cover": "FEW",
      "base": 25000,
      "type": null
    }
  ],
  "skyc1": "FEW",
  "skyl1": 25000,
  "skyc2": null,
  "skyl2": null,
  "skyc3": null,
  "skyl3": null,
  "skyc4": null,
  "skyl4": null
}
```

### Index Keys
```
metar:stations → SET of all station IDs with current METARs
metar:updated → Sorted SET (ZSET) of station IDs by update timestamp
```

### TTL
- **120 seconds** (2 minutes) - provides buffer beyond 1-minute update frequency

### Example Keys
```
metar:KJFK → {JSON string}
metar:KLAX → {JSON string}
metar:stations → SET{KJFK, KLAX, KORD, ...}
metar:updated → ZSET{KJFK:1734954660, KLAX:1734954661, ...}
```

---

## 2. TAF Data

### Key Format
```
taf:{stationId}
```

### Value Structure (JSON string)
```json
{
  "rawTAF": "TAF KJFK 231100Z 2312/2412 28015G25KT P6SM FEW250 FM231800 27012KT P6SM SCT250",
  "issueTime": "2025-12-23T11:00:00Z",
  "bulletinTime": "2025-12-23T11:00:00Z",
  "validTimeFrom": "2025-12-23T12:00:00Z",
  "validTimeTo": "2025-12-24T12:00:00Z",
  "remarks": null,
  "latitude": 40.6413,
  "longitude": -73.7781,
  "elevation": 13,
  "winds": [
    {
      "timeFrom": "2025-12-23T12:00:00Z",
      "timeTo": "2025-12-23T18:00:00Z",
      "wdir": 280,
      "wspd": 15,
      "wspdGust": 25
    },
    {
      "timeFrom": "2025-12-23T18:00:00Z",
      "timeTo": "2025-12-24T12:00:00Z",
      "wdir": 270,
      "wspd": 12,
      "wspdGust": null
    }
  ],
  "forecast": [
    {
      "fcstTimeFrom": "2025-12-23T12:00:00Z",
      "fcstTimeTo": "2025-12-23T18:00:00Z",
      "changeIndicator": null,
      "wdir": 280,
      "wspd": 15,
      "wspdGust": 25,
      "visibility": 6.0,
      "skyConditions": [
        {
          "skyCover": "FEW",
          "cloudBase": 25000
        }
      ],
      "flightCategory": "VFR"
    }
  ]
}
```

### Index Keys
```
taf:stations → SET of all station IDs with current TAFs
taf:updated → Sorted SET (ZSET) of station IDs by update timestamp
```

### TTL
- **900 seconds** (15 minutes) - provides buffer beyond 10-minute update frequency

### Example Keys
```
taf:KJFK → {JSON string}
taf:KLAX → {JSON string}
taf:stations → SET{KJFK, KLAX, KORD, ...}
```

---

## 3. SIGMET Data

### Key Format
```
sigmet:{id}
```

Where `id` is a unique identifier (typically from `airsigmetId` field or combination of fields).

### Value Structure (JSON string)
```json
{
  "id": "AWC-2025-12-23-1151-001",
  "airsigmetId": "AWC-2025-12-23-1151-001",
  "rawAirsigmet": "WSUS31 KKCI 231151 TURB SIGMET ALPHA 1 VALID UNTIL 231451",
  "validTimeFrom": "2025-12-23T11:51:00Z",
  "validTimeTo": "2025-12-23T14:51:00Z",
  "issueTime": "2025-12-23T11:51:00Z",
  "hazard": "TURB",
  "severity": "MOD",
  "airsigmetType": "SIGMET",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[-100.0, 40.0], [-95.0, 40.0], [-95.0, 45.0], [-100.0, 45.0], [-100.0, 40.0]]]
  },
  "altitude": {
    "min": 20000,
    "max": 40000
  },
  "movement": {
    "dir": 270,
    "spd": 30
  }
}
```

### Index Keys
```
sigmet:all → SET of all active SIGMET IDs
sigmet:active → Sorted SET (ZSET) of active SIGMET IDs by validTimeTo
sigmet:hazard:{hazardType} → SET of SIGMET IDs for specific hazard (TURB, ICE, CONVECTIVE, etc.)
sigmet:region:{region} → SET of SIGMET IDs for specific region
```

### TTL
- **120 seconds** (2 minutes) - provides buffer beyond 1-minute update frequency

### Example Keys
```
sigmet:AWC-2025-12-23-1151-001 → {JSON string}
sigmet:all → SET{AWC-2025-12-23-1151-001, AWC-2025-12-23-1152-002, ...}
sigmet:hazard:TURB → SET{AWC-2025-12-23-1151-001, ...}
```

---

## 4. G-AIRMET Data

### Key Format
```
airmet:{id}
```

Where `id` is a unique identifier (typically from `forecastId` or combination of fields).

### Value Structure (JSON string)
```json
{
  "id": "AWC-GAIRMET-2025-12-23-1151-001",
  "forecastId": "AWC-GAIRMET-2025-12-23-1151-001",
  "validTimeFrom": "2025-12-23T12:00:00Z",
  "validTimeTo": "2025-12-23T18:00:00Z",
  "issueTime": "2025-12-23T11:51:00Z",
  "hazard": "IFR",
  "severity": "MOD",
  "airsigmetType": "G-AIRMET",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[-100.0, 40.0], [-95.0, 40.0], [-95.0, 45.0], [-100.0, 45.0], [-100.0, 40.0]]]
  },
  "altitude": {
    "min": 0,
    "max": 10000
  },
  "movement": {
    "dir": 270,
    "spd": 25
  }
}
```

### Index Keys
```
airmet:all → SET of all active G-AIRMET IDs
airmet:active → Sorted SET (ZSET) of active G-AIRMET IDs by validTimeTo
airmet:hazard:{hazardType} → SET of G-AIRMET IDs for specific hazard (IFR, MTN_OBSC, TURB, ICE, etc.)
```

### TTL
- **120 seconds** (2 minutes) - provides buffer beyond 1-minute update frequency

### Example Keys
```
airmet:AWC-GAIRMET-2025-12-23-1151-001 → {JSON string}
airmet:all → SET{AWC-GAIRMET-2025-12-23-1151-001, ...}
airmet:hazard:IFR → SET{AWC-GAIRMET-2025-12-23-1151-001, ...}
```

---

## 5. PIREP Data

### Key Format
```
pirep:{id}
```

Where `id` is a unique identifier (typically from `aircraftReportId` or combination of fields).

### Value Structure (JSON string)
```json
{
  "id": "AWC-PIREP-2025-12-23-1151-001",
  "aircraftReportId": "AWC-PIREP-2025-12-23-1151-001",
  "rawAircraftReport": "UA /OV KJFK123045 /TM 1151 /FL350 /TP B737 /SK FEW250 /TB MOD",
  "obsTime": "2025-12-23T11:51:00Z",
  "reportTime": "2025-12-23T11:51:00Z",
  "latitude": 40.6413,
  "longitude": -73.7781,
  "altitude": 35000,
  "aircraftRef": "B737",
  "flightPhase": "CRUISE",
  "skyCondition": {
    "cover": "FEW",
    "base": 25000
  },
  "turbulence": {
    "intensity": "MOD",
    "frequency": null,
    "inCloud": false
  },
  "icing": null,
  "visibility": null,
  "weather": null,
  "wind": {
    "wdir": 280,
    "wspd": 45
  },
  "temperature": -45
}
```

### Index Keys
```
pirep:all → SET of all recent PIREP IDs
pirep:recent → Sorted SET (ZSET) of PIREP IDs by obsTime (most recent first)
pirep:location:{lat}:{lon} → Sorted SET (ZSET) of PIREP IDs near location
pirep:aircraft:{aircraftType} → SET of PIREP IDs for specific aircraft type
```

### TTL
- **120 seconds** (2 minutes) - provides buffer beyond 1-minute update frequency

### Example Keys
```
pirep:AWC-PIREP-2025-12-23-1151-001 → {JSON string}
pirep:recent → ZSET{AWC-PIREP-2025-12-23-1151-001:1734954660, ...}
```

---

## 6. Station Data

### Key Format
```
station:{code}
```

Where `code` is the ICAO station identifier (e.g., KJFK, KLAX).

### Value Structure (JSON string)
```json
{
  "icaoId": "KJFK",
  "iataId": "JFK",
  "synopId": "74486",
  "name": "John F Kennedy International Airport",
  "latitude": 40.6413,
  "longitude": -73.7781,
  "elevation": 13,
  "country": "US",
  "region": "NY",
  "city": "New York",
  "state": "NY",
  "wmoRegion": "4",
  "stationType": "AIRPORT",
  "priority": 1,
  "timezone": "America/New_York"
}
```

### Index Keys
```
station:all → SET of all station codes
station:name:{lowercase_name} → SET of station codes matching name (for search)
station:country:{countryCode} → SET of station codes in country
station:region:{regionCode} → SET of station codes in region
station:iata:{iataCode} → String mapping IATA code to ICAO code
```

### TTL
- **90000 seconds** (25 hours) - provides buffer beyond daily update frequency

### Example Keys
```
station:KJFK → {JSON string}
station:all → SET{KJFK, KLAX, KORD, ...}
station:name:john f kennedy → SET{KJFK}
station:iata:JFK → "KJFK"
```

---

## Data Access Patterns

### Lookup by Airport Code
```python
# METAR
metar_data = redis.get(f"metar:{airport_code}")

# TAF
taf_data = redis.get(f"taf:{airport_code}")

# Station info
station_data = redis.get(f"station:{airport_code}")
```

### Get All Active SIGMETs
```python
# Get all SIGMET IDs
sigmet_ids = redis.smembers("sigmet:all")

# Get each SIGMET
sigmet_data = [redis.get(f"sigmet:{id}") for id in sigmet_ids]
```

### Search Stations by Name
```python
# Search for stations matching name
name_key = f"station:name:{name.lower()}"
station_codes = redis.smembers(name_key)

# Get station details
stations = [redis.get(f"station:{code}") for code in station_codes]
```

### Get Recent PIREPs
```python
# Get most recent 10 PIREPs
pirep_ids = redis.zrevrange("pirep:recent", 0, 9)

# Get each PIREP
pirep_data = [redis.get(f"pirep:{id}") for id in pirep_ids]
```

## Cache Update Strategy

### Update Process
1. Download gzipped cache file from AWC
2. Decompress file
3. Parse CSV/XML/JSON format
4. For each record:
   - Generate unique key
   - Serialize data to JSON string
   - Store in ValKey with `SETEX` (set with TTL)
   - Update index sets/lists
5. Clean up expired entries from indexes

### Atomic Operations
- Use Redis transactions (MULTI/EXEC) for index updates
- Use `SETEX` for setting values with TTL
- Use `ZADD` for sorted sets with scores
- Use `SADD` for set additions

## Memory Considerations

### Estimated Sizes
- **METAR**: ~500 bytes per record × ~10,000 stations = ~5 MB
- **TAF**: ~2 KB per record × ~5,000 stations = ~10 MB
- **SIGMET**: ~1 KB per record × ~50 active = ~50 KB
- **G-AIRMET**: ~1 KB per record × ~100 active = ~100 KB
- **PIREP**: ~800 bytes per record × ~1,000 recent = ~800 KB
- **Stations**: ~300 bytes per record × ~20,000 stations = ~6 MB

**Total Estimated**: ~22 MB (well within ElastiCache Serverless limits)

## Notes

- All JSON strings are stored as UTF-8 encoded strings
- Timestamps are stored in ISO 8601 format (UTC)
- Coordinates are stored as decimal degrees (WGS84)
- Altitudes are stored in feet
- Speeds are stored in knots
- Distances are stored in statute miles or nautical miles as appropriate

