"""
Alert Service
Business logic for alert management
"""
from typing import List, Dict, Optional
from datetime import datetime
from config.supabase_client import supabase_client
from config.local_cache import local_cache
import uuid


class AlertService:
    """Service for alert generation and management"""
    
    TABLE_NAME = "alerts"
    
    async def create_alert(self, alert_data: Dict) -> Dict:
        """Create new alert"""
        alert = {
            **alert_data,
            "id": str(uuid.uuid4()),
            "acknowledged": False,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = await supabase_client.insert(self.TABLE_NAME, alert)
        # Cache write-through
        local_cache.upsert(self.TABLE_NAME, alert)
        return result or alert
    
    async def get_all_alerts(
        self,
        severity: Optional[str] = None,
        acknowledged: Optional[bool] = None,
        limit: int = 50
    ) -> List[Dict]:
        """Get alerts with optional filtering"""
        filters = {}
        
        if severity:
            filters["severity"] = severity
        
        if acknowledged is not None:
            filters["acknowledged"] = acknowledged
        
        alerts = await supabase_client.select(self.TABLE_NAME, filters=filters, limit=limit)
        if not alerts:
            alerts = local_cache.get_all(self.TABLE_NAME, limit=limit)
        return alerts
    
    async def get_alert_by_id(self, alert_id: str) -> Optional[Dict]:
        """Get specific alert by ID from database"""
        alert = await supabase_client.select_by_id(self.TABLE_NAME, alert_id)
        if not alert:
            alert = local_cache.get_by_id(self.TABLE_NAME, alert_id)
            if not alert:
                return None
        return alert
    
    async def acknowledge_alert(self, alert_id: str) -> Dict:
        """Mark alert as acknowledged"""
        update_data = {
            "acknowledged": True,
            "acknowledged_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = await supabase_client.update(self.TABLE_NAME, alert_id, update_data)
        local_cache.upsert(self.TABLE_NAME, {"id": alert_id, **update_data})
        return result or {"id": alert_id, **update_data}
    
    async def generate_collision_alert(self, collision_event: Dict) -> Dict:
        """Generate alert from collision event"""
        severity_map = {
            0: "low",
            1: "low",
            2: "medium",
            3: "high"
        }
        
        risk_level = collision_event.get("risk_level", 0)
        
        alert_data = {
            "alert_type": "collision",
            "severity": severity_map.get(risk_level, "medium"),
            "title": f"{collision_event.get('risk_label', 'Risk')} Warning",
            "message": f"Collision risk detected between {collision_event.get('satellite_name')} and {collision_event.get('debris_name')}. Distance: {collision_event.get('distance_km')}km",
            "satellite_id": collision_event.get("satellite_id"),
            "metadata": {
                "collision_event_id": collision_event.get("id"),
                "distance_km": collision_event.get("distance_km"),
                "probability": collision_event.get("collision_probability"),
                "time_to_closest_approach_sec": collision_event.get("time_to_closest_approach_sec")
            }
        }
        
        return await self.create_alert(alert_data)
    
    def _get_mock_alerts(self) -> List[Dict]:
        """Generate mock alert data"""
        current_time = datetime.utcnow().isoformat()
        
        return [
            {
                "id": "alert-001",
                "alert_type": "collision",
                "severity": "high",
                "title": "HIGH RISK WARNING!",
                "message": "Potential collision predicted in 18 minutes.",
                "satellite_id": "sat-003",
                "acknowledged": False,
                "metadata": {
                    "distance_km": 4.5,
                    "deltaV": "0.21 m/s",
                    "pathChange": "+3° orbital altitude",
                    "timeToCollision": 18,
                    "probability": 0.87
                },
                "created_at": current_time,
                "updated_at": current_time
            },
            {
                "id": "alert-002",
                "alert_type": "debris",
                "severity": "medium",
                "title": "Space Debris Alert",
                "message": "Debris detected in orbital path of Sat-02.",
                "satellite_id": "sat-002",
                "acknowledged": False,
                "metadata": {
                    "distance_km": 7.2,
                    "deltaV": "0.15 m/s",
                    "timeToCollision": 45
                },
                "created_at": current_time,
                "updated_at": current_time
            },
            {
                "id": "alert-003",
                "alert_type": "maneuver",
                "severity": "low",
                "title": "Maneuver Scheduled",
                "message": "Station-keeping maneuver scheduled for Sat-01 in 2 hours.",
                "satellite_id": "sat-001",
                "acknowledged": True,
                "metadata": {
                    "maneuver_type": "station_keeping",
                    "scheduled_time": current_time
                },
                "created_at": current_time,
                "updated_at": current_time
            }
        ]


alert_service = AlertService()
