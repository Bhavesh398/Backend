# Cache Data System

## Overview
The cache system provides automatic persistence of all Supabase tables to JSON files. This ensures the application can continue operating even when Supabase is offline.

## How It Works

### Automatic Sync on Startup
When the backend starts, it automatically syncs all tables from Supabase to JSON cache files in the `Backend/cache_data/` directory.

### Online Mode (Supabase Available)
- Data is fetched from Supabase
- Results are automatically cached to JSON files
- Cache files are updated with latest data

### Offline Mode (Supabase Unavailable)
- Data is loaded from JSON cache files
- Application continues to function with cached data
- No data loss or service interruption

## Cache Directory Structure
```
Backend/
├── cache_data/
│   ├── _metadata.json          # Sync timestamps and record counts
│   ├── satellites.json         # All satellite data
│   ├── debris.json             # All debris data
│   ├── collision_events.json   # Collision event data
│   ├── alerts.json             # Alert data
│   └── maneuvers.json          # Maneuver data
```

## API Endpoints

### Get Cache Status
```http
GET /api/cache/status
```
Returns information about cached tables, last sync times, record counts, and file sizes.

**Response:**
```json
{
  "success": true,
  "data": {
    "cache_dir": "Backend/cache_data",
    "last_sync": "2025-11-23T10:30:00",
    "supabase_status": "online",
    "tables": {
      "satellites": {
        "cache_exists": true,
        "record_count": 13304,
        "last_sync": "2025-11-23T10:30:00",
        "file_size_kb": 2456.78
      },
      "debris": {
        "cache_exists": true,
        "record_count": 2571,
        "last_sync": "2025-11-23T10:30:00",
        "file_size_kb": 512.34
      }
    }
  }
}
```

### Sync All Tables
```http
POST /api/cache/sync
```
Manually trigger a full sync of all tables from Supabase to cache.

**Response:**
```json
{
  "success": true,
  "message": "Successfully synced 15875 total records",
  "data": {
    "satellites": 13304,
    "debris": 2571,
    "collision_events": 0,
    "alerts": 0,
    "maneuvers": 0
  }
}
```

### Sync Specific Table
```http
POST /api/cache/sync/{table}
```
Sync a specific table (satellites, debris, collision_events, alerts, maneuvers).

**Example:**
```http
POST /api/cache/sync/satellites
```

### Load Cached Table
```http
GET /api/cache/load/{table}
```
Load data from cache file for a specific table.

## Implementation Details

### Services Integration
Both `satellite_service.py` and `debris_service.py` automatically:
1. Try to fetch from Supabase
2. Cache successful responses
3. Fall back to cache on errors

### Cache Manager Functions
- `save_to_cache(table, data)` - Save table data to JSON
- `load_from_cache(table)` - Load table data from JSON
- `sync_table(client, table)` - Sync single table from Supabase
- `sync_all_tables(client)` - Sync all tables from Supabase
- `get_cache_status()` - Get metadata about cached data

### Metadata Tracking
The `_metadata.json` file tracks:
- Last sync timestamp
- Supabase connection status
- Per-table record counts
- Per-table sync timestamps

## Benefits

1. **Offline Resilience**: Application works without Supabase connection
2. **Fast Startup**: Pre-cached data loads instantly
3. **Automatic Updates**: Cache syncs on every startup
4. **Manual Control**: API endpoints for manual sync
5. **Transparent**: Services automatically handle online/offline modes
6. **No Code Changes**: Existing API endpoints work identically

## Usage Examples

### Check Cache Health
```bash
curl http://localhost:8000/api/cache/status
```

### Force Data Refresh
```bash
curl -X POST http://localhost:8000/api/cache/sync
```

### Sync Only Satellites
```bash
curl -X POST http://localhost:8000/api/cache/sync/satellites
```

## Monitoring

Check backend logs for cache operations:
- `✅ Cached N records from {table}` - Successful cache write
- `📦 Loaded N records from cache for {table}` - Cache read
- `⚠️ Supabase unavailable. Loading from cache...` - Offline mode activated
- `🔄 Starting pagination for table={table}` - Full table sync started

## Error Handling

If cache files are missing or corrupted:
1. Application attempts Supabase connection
2. If successful, rebuilds cache automatically
3. If both fail, returns empty data with warning log

## Performance

- **Satellites**: ~13,000 records (~2.5MB JSON)
- **Debris**: ~2,500 records (~500KB JSON)
- **Load Time**: <100ms from cache
- **Sync Time**: ~6-8 seconds full sync from Supabase

## Maintenance

Cache files are automatically managed. No manual maintenance required.

To clear cache:
```bash
rm -rf Backend/cache_data/*.json
```
Cache will rebuild on next startup or manual sync.
