"""
Cache Manager for Supabase Data Persistence

This module handles automatic caching of all Supabase tables to JSON files.
When Supabase is online, it fetches and caches data.
When Supabase is offline, it loads data from cache files.
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent / "cache_data"
CACHE_DIR.mkdir(exist_ok=True)


class CacheManager:
    """Manages persistent JSON cache files for all Supabase tables"""
    
    TABLES = [
        "satellites",
        "debris",
        "collision_events",
        "alerts",
        "maneuvers"
    ]
    
    def __init__(self):
        self.cache_dir = CACHE_DIR
        self.metadata_file = self.cache_dir / "_metadata.json"
        
    def get_cache_path(self, table: str) -> Path:
        """Get cache file path for a table"""
        return self.cache_dir / f"{table}.json"
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get cache metadata (last sync times, record counts, etc.)"""
        if not self.metadata_file.exists():
            return {
                "last_sync": None,
                "tables": {},
                "supabase_status": "unknown"
            }
        try:
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading metadata: {e}")
            return {"last_sync": None, "tables": {}, "supabase_status": "unknown"}
    
    def update_metadata(self, table: str, record_count: int, status: str = "online"):
        """Update metadata after syncing a table"""
        metadata = self.get_metadata()
        metadata["last_sync"] = datetime.utcnow().isoformat()
        metadata["supabase_status"] = status
        if "tables" not in metadata:
            metadata["tables"] = {}
        metadata["tables"][table] = {
            "record_count": record_count,
            "last_sync": datetime.utcnow().isoformat()
        }
        
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Error writing metadata: {e}")
    
    def save_to_cache(self, table: str, data: List[Dict]) -> bool:
        """Save table data to JSON cache file"""
        cache_path = self.get_cache_path(table)
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"✅ Cached {len(data)} records from {table} to {cache_path.name}")
            self.update_metadata(table, len(data), "online")
            return True
        except Exception as e:
            logger.error(f"❌ Error caching {table}: {e}")
            return False
    
    def load_from_cache(self, table: str) -> Optional[List[Dict]]:
        """Load table data from JSON cache file"""
        cache_path = self.get_cache_path(table)
        if not cache_path.exists():
            logger.warning(f"⚠️ No cache file found for {table}")
            return None
        
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
            logger.info(f"📦 Loaded {len(data)} records from cache for {table}")
            return data
        except Exception as e:
            logger.error(f"❌ Error loading cache for {table}: {e}")
            return None
    
    async def sync_table(self, supabase_client, table: str) -> int:
        """
        Sync a single table from Supabase to cache
        
        Args:
            supabase_client: SupabaseClient instance
            table: Table name
            
        Returns:
            Number of records synced (0 if failed)
        """
        try:
            logger.info(f"🔄 Syncing {table} from Supabase...")
            # Fetch all records (pagination handled by client)
            data = await supabase_client.select(table, limit=None)
            
            if data:
                self.save_to_cache(table, data)
                logger.info(f"✅ Synced {len(data)} records for {table}")
                return len(data)
            else:
                logger.warning(f"⚠️ No data returned from Supabase for {table}")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Error syncing {table}: {e}")
            self.update_metadata(table, 0, "offline")
            return 0
    
    async def sync_all_tables(self, supabase_client) -> Dict[str, int]:
        """
        Sync all tables from Supabase to cache files
        
        Args:
            supabase_client: SupabaseClient instance
            
        Returns:
            Dict mapping table names to record counts
        """
        results = {}
        logger.info("🚀 Starting full cache sync from Supabase...")
        
        for table in self.TABLES:
            count = await self.sync_table(supabase_client, table)
            results[table] = count
        
        total_records = sum(results.values())
        logger.info(f"✅ Cache sync complete! Total records: {total_records}")
        logger.info(f"📊 Breakdown: {results}")
        
        return results
    
    def load_all_tables(self) -> Dict[str, List[Dict]]:
        """
        Load all tables from cache files
        
        Returns:
            Dict mapping table names to data lists
        """
        results = {}
        for table in self.TABLES:
            data = self.load_from_cache(table)
            results[table] = data if data is not None else []
        return results
    
    def get_cache_status(self) -> Dict[str, Any]:
        """Get status information about cached data"""
        metadata = self.get_metadata()
        status = {
            "cache_dir": str(self.cache_dir),
            "last_sync": metadata.get("last_sync"),
            "supabase_status": metadata.get("supabase_status", "unknown"),
            "tables": {}
        }
        
        for table in self.TABLES:
            cache_path = self.get_cache_path(table)
            table_meta = metadata.get("tables", {}).get(table, {})
            status["tables"][table] = {
                "cache_exists": cache_path.exists(),
                "record_count": table_meta.get("record_count", 0),
                "last_sync": table_meta.get("last_sync"),
                "file_size_kb": round(cache_path.stat().st_size / 1024, 2) if cache_path.exists() else 0
            }
        
        return status


# Global cache manager instance
cache_manager = CacheManager()
