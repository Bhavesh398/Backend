"""
Collision Detection Algorithms
Simple and complex collision detection for space objects
"""
import math
from typing import Dict, Tuple, List
from core.orbital.vector_math import compute_distance, compute_closest_approach
from config.settings import settings


def detect_collision_simple(
    sat_position: Dict,
    debris_position: Dict,
    threshold_km: float = None
) -> Tuple[bool, float]:
    """
    Simple collision detection based on distance threshold
    
    Args:
        sat_position: Satellite position dict
        debris_position: Debris position dict
        threshold_km: Distance threshold for collision warning
        
    Returns:
        Tuple of (is_collision_risk, distance_km)
    """
    if threshold_km is None:
        threshold_km = settings.COLLISION_THRESHOLD_KM
    
    distance = compute_distance(sat_position, debris_position)
    is_risk = distance < threshold_km
    
    return (is_risk, distance)


def detect_conjunction(
    sat_state: Dict,
    debris_state: Dict,
    time_window_sec: int = 3600,
    distance_threshold_km: float = None
) -> Dict:
    """
    Detect potential conjunction (close approach) event
    
    Args:
        sat_state: Satellite state (position + velocity)
        debris_state: Debris state (position + velocity)
        time_window_sec: Time window to check (seconds)
        distance_threshold_km: Threshold for conjunction
        
    Returns:
        Conjunction data dict
    """
    if distance_threshold_km is None:
        distance_threshold_km = settings.COLLISION_THRESHOLD_KM
    
    sat_pos = {k: sat_state[k] for k in ["x", "y", "z"] if k in sat_state}
    sat_vel = {k: sat_state[k] for k in ["vx", "vy", "vz"] if k in sat_state}
    
    debris_pos = {k: debris_state[k] for k in ["x", "y", "z"] if k in debris_state}
    debris_vel = {k: debris_state[k] for k in ["vx", "vy", "vz"] if k in debris_state}
    
    # Current distance
    current_distance = compute_distance(sat_pos, debris_pos)
    
    # Closest approach
    time_to_ca, min_distance = compute_closest_approach(
        sat_pos, sat_vel,
        debris_pos, debris_vel,
        time_window_sec
    )
    
    is_conjunction = min_distance < distance_threshold_km
    
    return {
        "is_conjunction": is_conjunction,
        "current_distance_km": current_distance,
        "time_to_closest_approach_sec": time_to_ca,
        "minimum_distance_km": min_distance,
        "threshold_km": distance_threshold_km
    }


def compute_collision_probability(
    distance_km: float,
    relative_velocity_kmps: float,
    satellite_size_m: float = 5.0,
    debris_size_m: float = 1.0
) -> float:
    """
    Compute collision probability (DUMMY/SIMPLIFIED)
    
    In production, this would use proper conjunction analysis
    
    Args:
        distance_km: Distance between objects
        relative_velocity_kmps: Relative velocity
        satellite_size_m: Satellite size estimate
        debris_size_m: Debris size estimate
        
    Returns:
        Probability between 0 and 1
    """
    # Combined radius in km
    combined_radius_km = (satellite_size_m + debris_size_m) / 2000.0
    
    # Simple exponential decay model
    if distance_km <= combined_radius_km:
        return 0.99
    
    # Scale factor based on relative velocity
    velocity_factor = min(relative_velocity_kmps / 15.0, 1.0)
    
    # Exponential decay with distance
    decay_rate = 0.5
    probability = math.exp(-decay_rate * (distance_km - combined_radius_km)) * velocity_factor
    
    return min(max(probability, 0.0), 0.99)


def batch_collision_check(
    satellite: Dict,
    debris_list: List[Dict],
    threshold_km: float = None
) -> List[Dict]:
    """
    Check collision risk for satellite against multiple debris objects
    
    Args:
        satellite: Satellite state dict
        debris_list: List of debris state dicts
        threshold_km: Collision threshold
        
    Returns:
        List of collision events
    """
    if threshold_km is None:
        threshold_km = settings.COLLISION_THRESHOLD_KM
    
    collisions = []

    sat_pos = {k: satellite.get(k, 0) for k in ["x", "y", "z"]}

    # Coarse spatial hashing prefilter: reduce candidate debris set before precise distance checks
    cell_size = 50.0  # km bucket size
    def cell(point: Dict) -> Tuple[int, int, int]:
        return (
            int(point.get("x", 0) // cell_size),
            int(point.get("y", 0) // cell_size),
            int(point.get("z", 0) // cell_size)
        )
    sat_cell = cell(sat_pos)

    # Collect debris in neighboring cells (Manhattan distance <= 1)
    candidate_debris: List[Dict] = []
    for debris in debris_list:
        d_pos = {k: debris.get(k, 0) for k in ["x", "y", "z"]}
        if sum(abs(a - b) for a, b in zip(sat_cell, cell(d_pos))) <= 1:
            candidate_debris.append(debris)

    for debris in candidate_debris:
        debris_pos = {k: debris.get(k, 0) for k in ["x", "y", "z"]}
        is_risk, distance = detect_collision_simple(sat_pos, debris_pos, threshold_km)
        if is_risk:
            collisions.append({
                "satellite_id": satellite.get("id"),
                "debris_id": debris.get("id"),
                "distance_km": distance,
                "is_high_risk": distance < settings.HIGH_RISK_THRESHOLD_KM
            })

    return collisions
