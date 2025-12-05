"""
Supabase Client Configuration
Uses raw httpx REST API instead of supabase SDK to avoid proxy initialization bug
"""
import httpx
from config.settings import settings
from typing import Optional, Dict, List, Any
import time
import logging
import json
from .local_cache import local_cache

class SupabaseUnavailable(Exception):
    """Raised when Supabase cannot be initialized or queried."""
    pass

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Raw httpx wrapper for Supabase REST API - avoids SDK proxy bug"""
    
    def __init__(self):
        self.url = settings.SUPABASE_URL
        self.key = settings.SUPABASE_KEY
        self._client: Optional[httpx.Client] = None
        
    def reset_client(self):
        """Reset client to force reconnection"""
        if self._client:
            self._client.close()
        self._client = None
        logger.info("Supabase client reset - will reconnect on next request")
        
    @property
    def client(self) -> httpx.Client:
        """Lazy initialization of httpx client"""
        if self._client is None:
            if not self.url or not self.key:
                logger.warning("Supabase credentials missing – entering cache-only mode.")
                raise SupabaseUnavailable("Supabase credentials missing")
            try:
                # Use raw httpx without any proxy parameters
                self._client = httpx.Client(
                    base_url=self.url,
                    headers={
                        "Authorization": f"Bearer {self.key}",
                        "Content-Type": "application/json",
                        "apikey": self.key
                    }
                )
                logger.info("✅ Supabase httpx client initialized successfully")
            except Exception as e:
                logger.warning(f"❌ Supabase client initialization failed: {e}. Using local cache.")
                raise SupabaseUnavailable(f"Supabase init failed: {e}")
        return self._client
    
    async def select(self, table: str, filters: Optional[Dict] = None, limit: Optional[int] = None, columns: Optional[str] = None) -> List[Dict]:
        """
        Select records from table using REST API
        
        Args:
            table: Table name
            filters: Dict of column: value filters
            limit: Maximum number of records (None = fetch all via pagination)
            columns: Columns to select
            
        Returns:
            List of records
        """
        start = time.time()
        try:
            select_cols = columns if columns else "*"
            all_data: List[Dict] = []
            
            if limit is None:
                # Paginate through all records
                page_size = 1000
                start_idx = 0
                safety_cap = 50000
                page_num = 1
                
                print(f"🔄 Starting pagination for table={table}")
                while start_idx < safety_cap:
                    try:
                        # Build query string
                        params = {
                            "select": select_cols,
                            "offset": start_idx,
                            "limit": page_size
                        }
                        
                        # Add filters if provided
                        if filters:
                            for key, value in filters.items():
                                params[f"{key}=eq.{value}"] = True
                        
                        response = self.client.get(f"/rest/v1/{table}", params=params)
                        response.raise_for_status()
                        batch = response.json() or []
                        batch_len = len(batch)
                        
                        if batch_len == 0:
                            print(f"✅ Pagination complete - empty batch at start={start_idx}")
                            break
                        
                        all_data.extend(batch)
                        print(f"📄 Page {page_num}: fetched {batch_len}, total={len(all_data)}")
                        
                        if batch_len < 1000:
                            print(f"✅ Pagination complete - last page had {batch_len} records")
                            break
                        
                        start_idx += batch_len
                        page_num += 1
                    except Exception as e:
                        print(f"⚠️ Pagination error at page {page_num}: {e}")
                        break
                
                took_ms = int((time.time() - start) * 1000)
                print(f"✅ Final count: {len(all_data)} records in {took_ms}ms")
                logger.info(f"Supabase select complete table={table} count={len(all_data)} ({took_ms}ms)")
                return all_data
            else:
                # Single request with limit
                params = {
                    "select": select_cols,
                    "limit": limit
                }
                
                if filters:
                    for key, value in filters.items():
                        params[f"{key}=eq.{value}"] = True
                
                response = self.client.get(f"/rest/v1/{table}", params=params)
                response.raise_for_status()
                data = response.json() or []
                
                took_ms = int((time.time() - start) * 1000)
                logger.info(f"Supabase select ok table={table} count={len(data)} ({took_ms}ms)")
                return data
                
        except SupabaseUnavailable:
            raise
        except Exception as e:
            logger.warning(f"❌ Supabase query failed: {e}. Falling back to cache.")
            try:
                cached = await local_cache.load_from_cache(table)
                if cached:
                    logger.info(f"Loaded {len(cached)} records from cache for {table}")
                    return cached
            except:
                pass
            return []
            
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
