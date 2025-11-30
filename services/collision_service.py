"""
Collision Service
Business logic for collision detection and risk assessment
"""
from typing import List, Dict
from datetime import datetime
from config.supabase_client import supabase_client
from services.satellite_service import satellite_service
from services.debris_service import debris_service
from core.orbital.collision_detection import detect_collision_simple, compute_collision_probability
from core.orbital.vector_math import compute_distance, compute_relative_velocity, compute_closest_approach
from core.ai.model1_risk_predictor import predict_risk
from core.ai.model2_risk_classifier import classify_risk_advanced
import uuid


class CollisionService:
    """Service for collision event detection and management"""
    
    TABLE_NAME = "collision_events"
    
    async def calculate_collision_risks(self) -> List[Dict]:
        """
        Calculate collision risks for all satellites against all debris
        Uses AI models for risk prediction and classification
        """
        satellites = await satellite_service.get_all_satellites()
        debris_list = await debris_service.get_all_debris()
        
        collision_events = []
        
        for satellite in satellites:
            sat_pos = {
                "x": satellite.get("longitude", 0) * 100,
                "y": satellite.get("latitude", 0) * 100,
                "z": satellite.get("altitude_km", 400)
            }
            sat_vel = {
                "vx": satellite.get("velocity_kmps", 7.5),
                "vy": 0,
                "vz": 0
            }
            
            for debris in debris_list:
                debris_pos = {
                    "x": debris.get("longitude", 0) * 100,
                    "y": debris.get("latitude", 0) * 100,
                    "z": debris.get("altitude_km", 400)
                }
                debris_vel = {
                    "vx": debris.get("velocity_kmps", 7.5),
                    "vy": 0,
                    "vz": 0
                }
                
                # Calculate distance
                distance = compute_distance(sat_pos, debris_pos)
                
                # Only process if within monitoring threshold
                if distance < 50:  # 50 km threshold
                    # Calculate relative velocity
                    rel_velocity = compute_relative_velocity(sat_vel, debris_vel)
                    
                    # Calculate altitude difference
                    alt_diff = abs(satellite.get("altitude_km", 0) - debris.get("altitude_km", 0))
                    
                    # AI Model 1: Predict risk score
                    risk_score = predict_risk(
                        distance_km=distance,
                        relative_velocity_kmps=rel_velocity,
                        angle_deg=90,  # Simplified
                        altitude_diff_km=alt_diff
                    )
                    
                    # AI Model 2: Classify risk level
                    risk_classification = classify_risk_advanced(
                        distance_km=distance,
                        relative_velocity_kmps=rel_velocity,
                        time_to_approach_sec=3600
                    )
                    
                    # Compute closest approach
                    time_to_ca, min_distance = compute_closest_approach(
                        sat_pos, sat_vel,
                        debris_pos, debris_vel
                    )
                    
                    # Compute collision probability
                    collision_prob = compute_collision_probability(
                        distance_km=distance,
                        relative_velocity_kmps=rel_velocity
                    )
                    
                    event = {
                        "id": str(uuid.uuid4()),
                        "satellite_id": satellite["id"],
                        "satellite_name": satellite["name"],
                        "debris_id": debris.get("deb_id", debris["id"]),
                        "debris_name": debris.get("deb_name", debris.get("name", "Unknown")),
                        "distance_km": round(distance, 3),
                        "relative_velocity_kmps": round(rel_velocity, 3),
                        "altitude_diff_km": round(alt_diff, 3),
                        "risk_score": round(risk_score, 4),
                        "risk_level": risk_classification["risk_level"],
                        "risk_label": risk_classification["risk_label"],
                        "time_to_closest_approach_sec": round(time_to_ca, 1),
                        "minimum_distance_km": round(min_distance, 3),
                        "collision_probability": round(collision_prob, 4),
                        "recommended_action": risk_classification["recommended_action"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    collision_events.append(event)
        
        # Sort by risk level (highest first)
        collision_events.sort(key=lambda x: x["risk_level"], reverse=True)
        
        return collision_events
    
    async def get_collision_event(self, event_id: str) -> Dict:
        """Get specific collision event by ID"""
        event = await supabase_client.select_by_id(self.TABLE_NAME, event_id)
        return event
    
    async def log_collision_event(self, event_data: Dict) -> Dict:
        """Log a collision event to database"""
        event = {
            **event_data,
            "id": str(uuid.uuid4()),
            "created_at": datetime.utcnow().isoformat()
        }
        
        result = await supabase_client.insert(self.TABLE_NAME, event)
        return result or event


collision_service = CollisionService()
