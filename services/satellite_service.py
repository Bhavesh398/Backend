"""
Satellite Service
Business logic for satellite operations
"""
from typing import Optional, List, Dict
from datetime import datetime
from config.supabase_client import supabase_client
from config.local_cache import local_cache
from config.cache_manager import cache_manager
from config.sql_loader import load_satellites_from_sql
from core.orbital.propagate_tle import tle_to_position
from core.orbital.vector_math import compute_distance
import uuid


class SatelliteService:
    """Service for satellite CRUD and tracking operations"""
    
    TABLE_NAME = "satellites"
    
    async def get_all_satellites(self, limit: Optional[int] = 100) -> List[Dict]:
        """Get all satellites from Supabase or cache"""
        import logging
        logger = logging.getLogger(__name__)
        print(f"🔍 TRACE: get_all_satellites called with limit={limit} type={type(limit)}")
        
        try:
            satellites = await supabase_client.select(self.TABLE_NAME, limit=limit)
            print(f"📦 TRACE: Retrieved {len(satellites)} satellites from Supabase")
            
            # If we got data, cache it for offline use
            if satellites and limit is None:
                cache_manager.save_to_cache(self.TABLE_NAME, satellites)
                logger.info(f"💾 Cached {len(satellites)} satellites")
            
        except Exception as e:
            logger.warning(f"⚠️ Supabase unavailable: {e}. Loading from cache...")
            satellites = cache_manager.load_from_cache(self.TABLE_NAME) or []
            print(f"📦 TRACE: Retrieved {len(satellites)} satellites from cache")
        
        # If still no data, return empty list
        if not satellites:
            logger.warning("No satellites found in Supabase or cache")
            return []
        
        # Update positions for each satellite
        for sat in satellites:
            # Normalize naming: map Supabase 'sat_name' to 'name' for frontend
            if 'name' not in sat and 'sat_name' in sat:
                sat['name'] = sat['sat_name']

            # Map Supabase coordinate fields (sat_x, sat_y, etc.) to expected fields (x, y, etc.)
            if 'x' not in sat and 'sat_x' in sat:
                sat['x'] = sat['sat_x']
            if 'y' not in sat and 'sat_y' in sat:
                sat['y'] = sat['sat_y']
            if 'z' not in sat and 'sat_z' in sat:
                sat['z'] = sat['sat_z']
            if 'vx' not in sat and 'sat_vx' in sat:
                sat['vx'] = sat['sat_vx']
            if 'vy' not in sat and 'sat_vy' in sat:
                sat['vy'] = sat['sat_vy']
            if 'vz' not in sat and 'sat_vz' in sat:
                sat['vz'] = sat['sat_vz']

            # Derive latitude/longitude from Cartesian if missing
            if (sat.get('latitude') is None or sat.get('longitude') is None) and sat.get('x') is not None:
                try:
                    import math
                    x = float(sat['x']); y = float(sat['y']); z = float(sat['z'])
                    r = math.sqrt(x*x + y*y + z*z)
                    if r > 0:
                        lat_rad = math.asin(y / r)
                        lon_rad = math.atan2(z, x)
                        sat['latitude'] = math.degrees(lat_rad)
                        sat['longitude'] = math.degrees(lon_rad)
                        if sat.get('altitude_km') is None:
                            sat['altitude_km'] = r - 6371.0
                except Exception:
                    pass

            # Map 'altitude' to 'altitude_km' if present
            if sat.get('altitude_km') is None and sat.get('altitude') is not None:
                try:
                    sat['altitude_km'] = float(sat['altitude'])
                except Exception:
                    pass

            # If still missing lat/lon but we have a NORAD identifier, fall back to TLE propagation
            norad_id = sat.get('norad_id') or sat.get('sat_name') or sat.get('name')
            if (sat.get('latitude') is None or sat.get('longitude') is None) and norad_id:
                try:
                    lat, lon, alt = tle_to_position(str(norad_id))
                    sat['latitude'] = lat
                    sat['longitude'] = lon
                    sat['altitude_km'] = alt
                    if 'norad_id' not in sat:
                        sat['norad_id'] = norad_id
                except Exception:
                    pass

            # Ensure x,y,z computed if we have lat/lon/alt but not Cartesian
            lat = sat.get('latitude'); lon = sat.get('longitude'); alt = sat.get('altitude_km') or sat.get('altitude')
            if (sat.get('x') is None or sat.get('y') is None or sat.get('z') is None) and lat is not None and lon is not None and alt is not None:
                try:
                    import math
                    r = 6371.0 + float(alt)
                    lat_r = math.radians(float(lat)); lon_r = math.radians(float(lon))
                    sat['x'] = r * math.cos(lat_r) * math.cos(lon_r)
                    sat['y'] = r * math.sin(lat_r)
                    sat['z'] = r * math.cos(lat_r) * math.sin(lon_r)
                except Exception:
                    pass

            # Provide basic velocity defaults if missing
            if sat.get('vx') is None or sat.get('vy') is None or sat.get('vz') is None:
                try:
                    import math
                    lon = sat.get('longitude')
                    if lon is not None:
                        lon_r = math.radians(float(lon))
                        v_mag = sat.get('velocity_kmps') or sat.get('velocity') or 7.5
                        sat.setdefault('vx', -float(v_mag) * math.sin(lon_r))
                        sat.setdefault('vy', float(v_mag) * math.cos(lon_r))
                        sat.setdefault('vz', 0.0)
                except Exception:
                    pass
        
        return satellites
    
    async def get_satellite_by_id(self, satellite_id: str) -> Optional[Dict]:
        """Get satellite by ID from database"""
        satellite = await supabase_client.select_by_id(self.TABLE_NAME, satellite_id)
        if not satellite:
            satellite = local_cache.get_by_id(self.TABLE_NAME, satellite_id)
            if not satellite:
                return None
        
        # Map Supabase coordinate fields (sat_x, sat_y, etc.) to expected fields (x, y, etc.)
        if 'x' not in satellite and 'sat_x' in satellite:
            satellite['x'] = satellite['sat_x']
        if 'y' not in satellite and 'sat_y' in satellite:
            satellite['y'] = satellite['sat_y']
        if 'z' not in satellite and 'sat_z' in satellite:
            satellite['z'] = satellite['sat_z']
        if 'vx' not in satellite and 'sat_vx' in satellite:
            satellite['vx'] = satellite['sat_vx']
        if 'vy' not in satellite and 'sat_vy' in satellite:
            satellite['vy'] = satellite['sat_vy']
        if 'vz' not in satellite and 'sat_vz' in satellite:
            satellite['vz'] = satellite['sat_vz']
        
        # Derive latitude/longitude from Cartesian if missing
        if (satellite.get('latitude') is None or satellite.get('longitude') is None) and satellite.get('x') is not None:
            try:
                import math
                x = float(satellite['x']); y = float(satellite['y']); z = float(satellite['z'])
                r = math.sqrt(x*x + y*y + z*z)
                if r > 0:
                    lat_rad = math.asin(y / r)
                    lon_rad = math.atan2(z, x)
                    satellite['latitude'] = math.degrees(lat_rad)
                    satellite['longitude'] = math.degrees(lon_rad)
                    if satellite.get('altitude_km') is None:
                        satellite['altitude_km'] = r - 6371.0
            except Exception:
                pass

        # Map 'altitude' to 'altitude_km'
        if satellite.get('altitude_km') is None and satellite.get('altitude') is not None:
            try:
                satellite['altitude_km'] = float(satellite['altitude'])
            except Exception:
                pass

        # Fallback to TLE only if still missing lat/lon
        norad_id = satellite.get('norad_id') or satellite.get('sat_name') or satellite.get('name')
        if (satellite.get('latitude') is None or satellite.get('longitude') is None) and norad_id:
            try:
                lat, lon, alt = tle_to_position(str(norad_id))
                satellite['latitude'] = lat
                satellite['longitude'] = lon
                satellite['altitude_km'] = alt
                if 'norad_id' not in satellite:
                    satellite['norad_id'] = norad_id
            except Exception:
                pass

        # Ensure Cartesian from lat/lon/alt if missing
        lat = satellite.get('latitude'); lon = satellite.get('longitude'); alt = satellite.get('altitude_km') or satellite.get('altitude')
        if (satellite.get('x') is None or satellite.get('y') is None or satellite.get('z') is None) and lat is not None and lon is not None and alt is not None:
            try:
                import math
                r = 6371.0 + float(alt)
                lat_r = math.radians(float(lat)); lon_r = math.radians(float(lon))
                satellite['x'] = r * math.cos(lat_r) * math.cos(lon_r)
                satellite['y'] = r * math.sin(lat_r)
                satellite['z'] = r * math.cos(lat_r) * math.sin(lon_r)
            except Exception:
                pass

        # Provide velocity defaults if missing
        if satellite.get('vx') is None or satellite.get('vy') is None or satellite.get('vz') is None:
            try:
                import math
                lon = satellite.get('longitude')
                if lon is not None:
                    lon_r = math.radians(float(lon))
                    v_mag = satellite.get('velocity_kmps') or satellite.get('velocity') or 7.5
                    satellite.setdefault('vx', -float(v_mag) * math.sin(lon_r))
                    satellite.setdefault('vy', float(v_mag) * math.cos(lon_r))
                    satellite.setdefault('vz', 0.0)
            except Exception:
                pass
                satellite.setdefault("vx", 0.0)
                satellite.setdefault("vy", 0.0)
                satellite.setdefault("vz", 0.0)
        
        return satellite
    
    async def create_satellite(self, data: Dict) -> Dict:
        """Create new satellite"""
        satellite_data = {
            **data,
            "id": str(uuid.uuid4()),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = await supabase_client.insert(self.TABLE_NAME, satellite_data)
        # Local cache write-through already handled in client; ensure present
        local_cache.upsert_satellite(satellite_data)
        return result or satellite_data
    
    async def update_satellite(self, satellite_id: str, data: Dict) -> Optional[Dict]:
        """Update satellite"""
        update_data = {
            **data,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = await supabase_client.update(self.TABLE_NAME, satellite_id, update_data)
        local_cache.upsert_satellite({"id": satellite_id, **update_data})
        return result or {"id": satellite_id, **update_data}
    
    async def delete_satellite(self, satellite_id: str) -> bool:
        """Delete satellite"""
        deleted = await supabase_client.delete(self.TABLE_NAME, satellite_id)
        if deleted:
            local_cache.delete(self.TABLE_NAME, satellite_id)
        return deleted
    
    def _get_mock_satellites(self) -> List[Dict]:
        """Generate mock satellite data"""
        current_time = datetime.utcnow().isoformat()
        
        return [
            {
                "id": "sat-001",
                "name": "Sat-01",
                "norad_id": "25544",
                "altitude_km": 450.0,
                "inclination_deg": 51.6,
                "latitude": 20.5,
                "longitude": -45.3,
                "velocity_kmps": 7.66,
                "status": "active",
                "created_at": current_time,
                "updated_at": current_time
            },
            {
                "id": "sat-002",
                "name": "Sat-02",
                "norad_id": "25545",
                "altitude_km": 470.0,
                "inclination_deg": 52.3,
                "latitude": 30.2,
                "longitude": -120.5,
                "velocity_kmps": 7.62,
                "status": "active",
                "created_at": current_time,
                "updated_at": current_time
            },
            {
                "id": "sat-003",
                "name": "Sat-03",
                "norad_id": "25546",
                "altitude_km": 500.0,
                "inclination_deg": 53.1,
                "latitude": -15.8,
                "longitude": 78.2,
                "velocity_kmps": 7.58,
                "status": "active",
                "created_at": current_time,
                "updated_at": current_time
            },
            {
                "id": "sat-004",
                "name": "Sat-04",
                "norad_id": "25547",
                "altitude_km": 520.0,
                "inclination_deg": 54.0,
                "latitude": -25.3,
                "longitude": 150.7,
                "velocity_kmps": 7.55,
                "status": "active",
                "created_at": current_time,
                "updated_at": current_time
            }
        ]


satellite_service = SatelliteService()
