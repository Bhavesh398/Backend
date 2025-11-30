"""
Debris Service
Business logic for space debris tracking
"""
from typing import Optional, List, Dict
from datetime import datetime
from config.supabase_client import supabase_client
from config.local_cache import local_cache
from config.cache_manager import cache_manager
from config.sql_loader import load_debris_from_sql
import uuid
import random


class DebrisService:
    """Service for debris CRUD and tracking operations"""
    
    TABLE_NAME = "debris"
    
    async def get_all_debris(self, limit: Optional[int] = 100) -> List[Dict]:
        """Get all debris from Supabase or cache"""
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Fetching debris with limit={limit}")
        
        try:
            # Request only necessary columns to mitigate PostgREST JSON generation errors
            debris_list = await supabase_client.select(
                self.TABLE_NAME,
                limit=limit,
                columns="id,deb_id,deb_name,deb_x,deb_y,deb_z,deb_vx,deb_vy,deb_vz,altitude,size_estimate,mass_estimate,source,status"
            )
            logger.info(f"Retrieved {len(debris_list)} debris from Supabase")
            
            # Cache data if we fetched all records
            if debris_list and limit is None:
                cache_manager.save_to_cache(self.TABLE_NAME, debris_list)
                logger.info(f"💾 Cached {len(debris_list)} debris")
                
        except Exception as e:
            logger.warning(f"⚠️ Supabase unavailable: {e}. Loading from cache...")
            debris_list = cache_manager.load_from_cache(self.TABLE_NAME) or []
            logger.info(f"Retrieved {len(debris_list)} debris from cache")

        # Fallback to file loader if still empty
        if not debris_list:
            logger.warning("No debris found in Supabase or cache – attempting local file load fallback")
            file_debris = load_debris_from_sql()
            if file_debris:
                logger.info(f"Loaded {len(file_debris)} debris from local files; seeding cache")
                for d in file_debris:
                    if not d.get('id'):
                        d['id'] = str(uuid.uuid4())
                    local_cache.upsert(self.TABLE_NAME, d)
                debris_list = file_debris
            else:
                logger.warning("Local debris loader also returned empty; returning []")
                return []
        
        # Enrich with derived position (x,y,z) and velocity vector if available
        enriched = []
        for deb in debris_list:
            # Map Supabase coordinate fields (deb_x, deb_y, etc.) to expected fields (x, y, etc.)
            if 'x' not in deb and 'deb_x' in deb:
                deb['x'] = deb['deb_x']
            if 'y' not in deb and 'deb_y' in deb:
                deb['y'] = deb['deb_y']
            if 'z' not in deb and 'deb_z' in deb:
                deb['z'] = deb['deb_z']
            if 'vx' not in deb and 'deb_vx' in deb:
                deb['vx'] = deb['deb_vx']
            if 'vy' not in deb and 'deb_vy' in deb:
                deb['vy'] = deb['deb_vy']
            if 'vz' not in deb and 'deb_vz' in deb:
                deb['vz'] = deb['deb_vz']
            # Map size_estimate to size_estimate_m for compatibility
            if 'size_estimate_m' not in deb and 'size_estimate' in deb:
                deb['size_estimate_m'] = deb['size_estimate']
            
            try:
                # If we have deb_x/y/z but no lat/lon, calculate lat/lon from Cartesian
                if (deb.get('latitude') is None or deb.get('longitude') is None) and deb.get('deb_x') is not None:
                    import math
                    x = float(deb['deb_x'])
                    y = float(deb['deb_y'])
                    z = float(deb['deb_z'])
                    
                    # Convert Cartesian to spherical (lat/lon/alt)
                    r = math.sqrt(x*x + y*y + z*z)
                    lat_rad = math.asin(y / r) if r > 0 else 0
                    lon_rad = math.atan2(z, x) if r > 0 else 0
                    
                    deb['latitude'] = math.degrees(lat_rad)
                    deb['longitude'] = math.degrees(lon_rad)
                    if deb.get('altitude_km') is None:
                        deb['altitude_km'] = r - 6371.0  # Earth radius
                
                # Now if we have lat/lon, ensure we also have x/y/z in the expected format
                lat = deb.get("latitude")
                lon = deb.get("longitude")
                alt = deb.get("altitude_km") or deb.get("altitude")
                if lat is not None and lon is not None and alt is not None:
                    import math
                    r = 6371.0 + float(alt)
                    lat_r = math.radians(float(lat))
                    lon_r = math.radians(float(lon))
                    # Only set x/y/z if not already set from deb_x/y/z
                    if deb.get("x") is None:
                        deb["x"] = r * math.cos(lat_r) * math.cos(lon_r)
                        deb["y"] = r * math.sin(lat_r)
                        deb["z"] = r * math.cos(lat_r) * math.sin(lon_r)
                    v_mag = deb.get("velocity_kmps") or deb.get("velocity") or random.uniform(7.2, 7.8)
                    # Simple tangential velocity approximation
                    if deb.get("vx") is None:
                        deb["vx"] = -float(v_mag) * math.sin(lon_r)
                        deb["vy"] = float(v_mag) * math.cos(lon_r)
                        deb["vz"] = 0.0
            except Exception as e:
                import logging
                logging.warning(f"Failed to calculate coordinates for debris {deb.get('id')}: {e}")
                pass
            enriched.append(deb)
        return enriched
    
    async def get_debris_by_id(self, debris_id: str) -> Optional[Dict]:
        """Get debris by ID from database"""
        debris = await supabase_client.select_by_id(self.TABLE_NAME, debris_id)
        if not debris:
            debris = local_cache.get_by_id(self.TABLE_NAME, debris_id)
            if not debris:
                return None
        
        # Map Supabase coordinate fields (deb_x, deb_y, etc.) to expected fields (x, y, etc.)
        if 'x' not in debris and 'deb_x' in debris:
            debris['x'] = debris['deb_x']
        if 'y' not in debris and 'deb_y' in debris:
            debris['y'] = debris['deb_y']
        if 'z' not in debris and 'deb_z' in debris:
            debris['z'] = debris['deb_z']
        if 'vx' not in debris and 'deb_vx' in debris:
            debris['vx'] = debris['deb_vx']
        if 'vy' not in debris and 'deb_vy' in debris:
            debris['vy'] = debris['deb_vy']
        if 'vz' not in debris and 'deb_vz' in debris:
            debris['vz'] = debris['deb_vz']
        
        # Enrich single record
        try:
            lat = debris.get("latitude")
            lon = debris.get("longitude")
            alt = debris.get("altitude_km") or debris.get("altitude")
            if lat is not None and lon is not None and alt is not None:
                import math
                r = 6371.0 + float(alt)
                lat_r = math.radians(float(lat))
                lon_r = math.radians(float(lon))
                debris["x"] = r * math.cos(lat_r) * math.cos(lon_r)
                debris["y"] = r * math.sin(lat_r)
                debris["z"] = r * math.cos(lat_r) * math.sin(lon_r)
                v_mag = debris.get("velocity_kmps") or debris.get("velocity") or random.uniform(7.2, 7.8)
                debris["vx"] = -float(v_mag) * math.sin(lon_r)
                debris["vy"] = float(v_mag) * math.cos(lon_r)
                debris["vz"] = 0.0
        except Exception:
            pass
        return debris
    
    async def create_debris(self, data: Dict) -> Dict:
        """Create new debris entry"""
        debris_data = {
            **data,
            "id": str(uuid.uuid4()),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = await supabase_client.insert(self.TABLE_NAME, debris_data)
        local_cache.upsert_debris(debris_data)
        return result or debris_data
    
    async def update_debris(self, debris_id: str, data: Dict) -> Optional[Dict]:
        """Update debris entry"""
        update_data = {
            **data,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = await supabase_client.update(self.TABLE_NAME, debris_id, update_data)
        local_cache.upsert_debris({"id": debris_id, **update_data})
        return result or {"id": debris_id, **update_data}
    
    async def delete_debris(self, debris_id: str) -> bool:
        """Delete debris entry"""
        deleted = await supabase_client.delete(self.TABLE_NAME, debris_id)
        if deleted:
            local_cache.delete(self.TABLE_NAME, debris_id)
        return deleted
    
    def _get_mock_debris(self) -> List[Dict]:
        """Generate mock debris data"""
        current_time = datetime.utcnow().isoformat()
        
        debris_list = []
        for i in range(1, 11):
            debris_list.append({
                "id": f"debris-{str(i).zfill(3)}",
                "name": f"Debris-{str(i).zfill(3)}",
                "object_type": random.choice(["rocket_body", "payload", "debris", "unknown"]),
                "altitude_km": random.uniform(400, 600),
                "latitude": random.uniform(-90, 90),
                "longitude": random.uniform(-180, 180),
                "velocity_kmps": random.uniform(7.0, 8.0),
                "size_estimate_m": random.uniform(0.1, 5.0),
                "created_at": current_time,
                "updated_at": current_time
            })
        
        return debris_list


debris_service = DebrisService()
