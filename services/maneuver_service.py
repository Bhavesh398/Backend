"""
Maneuver Service
Business logic for maneuver planning using RL agent
"""
from typing import List, Dict
from datetime import datetime
from config.supabase_client import supabase_client
from core.ai.rl_maneuver_agent import suggest_maneuver, suggest_multi_burn_maneuver, evaluate_maneuver_safety
import uuid


class ManeuverService:
    """Service for AI-powered maneuver planning"""
    
    TABLE_NAME = "maneuvers"
    
    async def plan_maneuver(self, satellite_id: str, threat_data: Dict) -> Dict:
        """
        Plan collision avoidance maneuver using RL agent
        
        Args:
            satellite_id: ID of satellite requiring maneuver
            threat_data: Dict with threat information
            
        Returns:
            Maneuver plan
        """
        # Prepare satellite state for RL agent
        satellite_state = {
            "satellite_id": satellite_id,
            "position": threat_data.get("satellite_position", {"x": 0, "y": 0, "z": 400}),
            "velocity": threat_data.get("satellite_velocity", {"vx": 7.5, "vy": 0, "vz": 0}),
            "threat_distance_km": threat_data.get("distance_km", 10.0),
            "threat_direction": threat_data.get("threat_direction", {"x": 1, "y": 0, "z": 0})
        }
        
        # Use RL agent to suggest maneuver
        maneuver = suggest_maneuver(satellite_state)
        
        # Evaluate maneuver safety
        safety_eval = evaluate_maneuver_safety(maneuver, satellite_state)
        
        # Create full maneuver plan
        maneuver_plan = {
            "id": str(uuid.uuid4()),
            "satellite_id": satellite_id,
            **maneuver,
            "safety_evaluation": safety_eval,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat()
        }
        
        # Store in database
        await supabase_client.insert(self.TABLE_NAME, maneuver_plan)
        
        return maneuver_plan
    
    async def plan_multi_burn_maneuver(self, satellite_id: str, threat_data: Dict) -> List[Dict]:
        """
        Plan multi-burn maneuver sequence
        More fuel-efficient for non-urgent situations
        """
        satellite_state = {
            "satellite_id": satellite_id,
            "position": threat_data.get("satellite_position", {"x": 0, "y": 0, "z": 400}),
            "velocity": threat_data.get("satellite_velocity", {"vx": 7.5, "vy": 0, "vz": 0}),
            "threat_distance_km": threat_data.get("distance_km", 10.0),
            "threat_direction": threat_data.get("threat_direction", {"x": 1, "y": 0, "z": 0})
        }
        
        maneuvers = suggest_multi_burn_maneuver(satellite_state, num_burns=3)
        
        maneuver_plans = []
        for i, maneuver in enumerate(maneuvers):
            plan = {
                "id": str(uuid.uuid4()),
                "satellite_id": satellite_id,
                "sequence_number": i + 1,
                **maneuver,
                "status": "pending",
                "created_at": datetime.utcnow().isoformat()
            }
            await supabase_client.insert(self.TABLE_NAME, plan)
            maneuver_plans.append(plan)
        
        return maneuver_plans
    
    async def get_maneuvers_for_satellite(self, satellite_id: str) -> List[Dict]:
        """Get all maneuvers for a specific satellite"""
        maneuvers = await supabase_client.select(
            self.TABLE_NAME,
            filters={"satellite_id": satellite_id}
        )
        return maneuvers or []
    
    async def update_maneuver_status(self, maneuver_id: str, status: str) -> Dict:
        """Update maneuver execution status"""
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = await supabase_client.update(self.TABLE_NAME, maneuver_id, update_data)
        return result


maneuver_service = ManeuverService()
