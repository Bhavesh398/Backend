"""Cache synchronization utility.
Attempts to pull latest data from Supabase (if available) and seed/update local SQLite cache.
Run manually or schedule (e.g., cron / Windows Task Scheduler).
"""
from config.supabase_client import supabase_client, SupabaseUnavailable
from config.local_cache import local_cache
import logging

logger = logging.getLogger(__name__)

TABLES = ["satellites", "debris", "alerts"]

async def sync_table(table: str):
    try:
        records = await supabase_client.select(table, limit=1000)
        if not records:
            logger.info(f"No remote records for table {table}")
            return
        for r in records:
            local_cache.upsert(table, r)
        logger.info(f"Synced {len(records)} records for {table}")
    except SupabaseUnavailable:
        logger.warning("Supabase unavailable - skipping sync")
    except Exception as e:
        logger.error(f"Error syncing {table}: {e}")

async def run_sync():
    for t in TABLES:
        await sync_table(t)

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_sync())
    print("Cache sync complete.")
