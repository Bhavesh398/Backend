"""
Cache Management API Endpoints

Provides endpoints to check cache status, trigger sync, and manage cached data
"""
from fastapi import APIRouter, HTTPException
from config.cache_manager import cache_manager
from config.supabase_client import supabase_client
from typing import Dict, Any

router = APIRouter()


@router.get("/cache/status")
async def get_cache_status() -> Dict[str, Any]:
    """
    Get current cache status
    
    Returns information about cached tables, last sync times, and file sizes
    """
    try:
        status = cache_manager.get_cache_status()
        return {
            "success": True,
            "data": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting cache status: {str(e)}")


@router.post("/cache/sync")
async def sync_cache() -> Dict[str, Any]:
    """
    Manually trigger cache sync from Supabase
    
    Fetches all data from Supabase and saves to JSON cache files
    """
    try:
        results = await cache_manager.sync_all_tables(supabase_client)
        total_records = sum(results.values())
        
        return {
            "success": True,
            "message": f"Successfully synced {total_records} total records",
            "data": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error syncing cache: {str(e)}")


@router.post("/cache/sync/{table}")
async def sync_table(table: str) -> Dict[str, Any]:
    """
    Sync a specific table from Supabase to cache
    
    Args:
        table: Table name (satellites, debris, collision_events, alerts, maneuvers)
    """
    if table not in cache_manager.TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid table name. Must be one of: {', '.join(cache_manager.TABLES)}"
        )
    
    try:
        count = await cache_manager.sync_table(supabase_client, table)
        return {
            "success": True,
            "message": f"Successfully synced {count} records for {table}",
            "data": {
                "table": table,
                "record_count": count
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error syncing {table}: {str(e)}")


@router.get("/cache/load/{table}")
async def load_cached_table(table: str) -> Dict[str, Any]:
    """
    Load data from cache file for a specific table
    
    Args:
        table: Table name
    """
    if table not in cache_manager.TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid table name. Must be one of: {', '.join(cache_manager.TABLES)}"
        )
    
    try:
        data = cache_manager.load_from_cache(table)
        if data is None:
            raise HTTPException(status_code=404, detail=f"No cache file found for {table}")
        
        return {
            "success": True,
            "data": {
                "table": table,
                "record_count": len(data),
                "records": data
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading cache for {table}: {str(e)}")
