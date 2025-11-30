"""
Supabase Client Configuration
Async database operations wrapper
"""
from supabase import create_client, Client
from config.settings import settings
from typing import Optional, Dict, List, Any
import time
import logging
from .local_cache import local_cache

class SupabaseUnavailable(Exception):
    """Raised when Supabase cannot be initialized or queried."""
    pass

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Async wrapper for Supabase operations"""
    
    def __init__(self):
        self.url = settings.SUPABASE_URL
        self.key = settings.SUPABASE_KEY
        self._client: Optional[Client] = None
        self._last_error_time: Optional[float] = None
        
    def reset_client(self):
        """Reset client to force reconnection"""
        self._client = None
        logger.info("Supabase client reset - will reconnect on next request")
        
    @property
    def client(self) -> Client:
        """Lazy initialization of Supabase client with retry on each access"""
        # Always try to reconnect if client not initialized
        if self._client is None:
            if not self.url or not self.key:
                logger.warning("Supabase credentials missing – entering cache-only mode.")
                raise SupabaseUnavailable("Supabase credentials missing")
            try:
                # Create client without proxy parameter to avoid version compatibility issues
                self._client = create_client(
                    supabase_url=self.url,
                    supabase_key=self.key
                )
                logger.info("✅ Supabase client initialized successfully")
            except Exception as e:
                logger.warning(f"❌ Supabase initialization failed: {e}. Using local cache.")
                raise SupabaseUnavailable(f"Supabase init failed: {e}")
        return self._client
    
    async def select(self, table: str, filters: Optional[Dict] = None, limit: Optional[int] = None, columns: Optional[str] = None) -> List[Dict]:
        """
        Select records from table with automatic pagination for unlimited queries
        
        Args:
            table: Table name
            filters: Dict of column: value filters
            limit: Maximum number of records (None = fetch all via pagination)
            
        Returns:
            List of records
        """
        start = time.time()
        try:
            select_cols = columns if columns else "*"
            
            # If limit is None, use pagination to fetch ALL records
            if limit is None:
                # Full pagination: continue until a short page returned.
                all_data: List[Dict] = []
                page_size = 1000  # Supabase hard cap per request
                start_idx = 0
                safety_cap = 50000  # absolute hard stop to avoid runaway
                page_num = 1
                print(f"🔄 Starting pagination for table={table}")
                while start_idx < safety_cap:
                    end_idx = start_idx + page_size - 1
                    query = self.client.table(table).select(select_cols).range(start_idx, end_idx)
                    if filters:
                        for key, value in filters.items():
                            query = query.eq(key, value)
                    response = query.execute()
                    batch = response.data or []
                    batch_len = len(batch)
                    if batch_len == 0:
                        print(f"✅ Pagination complete - empty batch at start={start_idx}")
                        break
                    all_data.extend(batch)
                    print(f"📄 Page {page_num}: fetched {batch_len}, total={len(all_data)}")
                    # Supabase returns max 999 per page (not 1000). Continue if we got exactly 999.
                    if batch_len < 999:
                        print(f"✅ Pagination complete - last page had {batch_len} records")
                        break
                    start_idx += batch_len  # Use actual batch length, not page_size
                    page_num += 1
                took_ms = int((time.time() - start) * 1000)
                print(f"✅ Final count: {len(all_data)} records in {took_ms}ms")
                logger.info(f"Supabase select paginated complete table={table} final_count={len(all_data)} cols={select_cols} ({took_ms}ms)")
                return all_data
            else:
                # Regular query with limit
                query = self.client.table(table).select(select_cols)
                
                if filters:
                    for key, value in filters.items():
                        query = query.eq(key, value)
                
                query = query.limit(limit)
                response = query.execute()
                took_ms = int((time.time() - start) * 1000)
                
                if not response.data:
                    logger.warning(f"Supabase select returned empty set table={table} cols={select_cols} filters={filters} limit={limit} ({took_ms}ms)")
                    return []
                    
                logger.info(f"Supabase select ok table={table} count={len(response.data)} cols={select_cols} ({took_ms}ms)")
                return response.data
            
        except SupabaseUnavailable:
            # Fallback to local cache
            print(f"⚠️ Supabase unavailable, falling back to cache")
            return local_cache.get_all(table, limit=limit or 100)
        except Exception as e:
            took_ms = int((time.time() - start) * 1000)
            print(f"❌ Supabase error: {type(e).__name__}: {e}")
            logger.error(f"Supabase select error table={table} cols={columns or '*'} filters={filters} limit={limit} ({took_ms}ms): {e}. Falling back to cache")
            return local_cache.get_all(table, limit=limit or 100)
    
    async def select_by_id(self, table: str, record_id: str) -> Optional[Dict]:
        """Get single record by ID"""
        try:
            response = self.client.table(table).select("*").eq("id", record_id).execute()
            return response.data[0] if response.data else None
        except SupabaseUnavailable:
            return local_cache.get_by_id(table, record_id)
        except Exception as e:
            logger.error(f"Supabase select_by_id error: {e}. Falling back to cache")
            return local_cache.get_by_id(table, record_id)
    
    async def insert(self, table: str, data: Dict) -> Optional[Dict]:
        """
        Insert record into table
        
        Args:
            table: Table name
            data: Record data
            
        Returns:
            Inserted record
        """
        try:
            response = self.client.table(table).insert(data).execute()
            inserted = response.data[0] if response.data else None
            if inserted:
                # write-through to cache
                local_cache.upsert(table, inserted)
            return inserted
        except SupabaseUnavailable:
            local_cache.upsert(table, data)
            return data
        except Exception as e:
            logger.error(f"Supabase insert error: {e}. Using cache only")
            local_cache.upsert(table, data)
            return data
    
    async def update(self, table: str, record_id: str, data: Dict) -> Optional[Dict]:
        """
        Update record by ID
        
        Args:
            table: Table name
            record_id: Record ID
            data: Updated data
            
        Returns:
            Updated record
        """
        try:
            response = self.client.table(table).update(data).eq("id", record_id).execute()
            updated = response.data[0] if response.data else None
            if updated:
                local_cache.upsert(table, updated)
            return updated
        except SupabaseUnavailable:
            # update cache directly
            local_cache.upsert(table, {"id": record_id, **data})
            return {"id": record_id, **data}
        except Exception as e:
            logger.error(f"Supabase update error: {e}. Updating cache only")
            local_cache.upsert(table, {"id": record_id, **data})
            return {"id": record_id, **data}
    
    async def delete(self, table: str, record_id: str) -> bool:
        """
        Delete record by ID
        
        Args:
            table: Table name
            record_id: Record ID
            
        Returns:
            Success status
        """
        try:
            self.client.table(table).delete().eq("id", record_id).execute()
            local_cache.delete(table, record_id)
            return True
        except SupabaseUnavailable:
            return local_cache.delete(table, record_id)
        except Exception as e:
            logger.error(f"Supabase delete error: {e}. Deleting in cache only")
            return local_cache.delete(table, record_id)


# Global client instance
supabase_client = SupabaseClient()
