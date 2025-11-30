"""
Vector Mathematics for Orbital Mechanics
3D vector operations and distance calculations
"""
import math
from typing import Tuple, Dict


def compute_distance(pos1: Dict, pos2: Dict) -> float:
    """
    Compute Euclidean distance between two positions
    
    Args:
        pos1: Position dict with x, y, z keys (km)
        pos2: Position dict with x, y, z keys (km)
        
    Returns:
        Distance in km
    """
    dx = pos2.get("x", 0) - pos1.get("x", 0)
    dy = pos2.get("y", 0) - pos1.get("y", 0)
    dz = pos2.get("z", 0) - pos1.get("z", 0)
    
    return math.sqrt(dx**2 + dy**2 + dz**2)


def compute_relative_velocity(vel1: Dict, vel2: Dict) -> float:
    """
    Compute magnitude of relative velocity
    
    Args:
        vel1: Velocity dict with vx, vy, vz keys (km/s)
        vel2: Velocity dict with vx, vy, vz keys (km/s)
        
    Returns:
        Relative velocity magnitude in km/s
    """
    dvx = vel2.get("vx", 0) - vel1.get("vx", 0)
    dvy = vel2.get("vy", 0) - vel1.get("vy", 0)
    dvz = vel2.get("vz", 0) - vel1.get("vz", 0)
    
    return math.sqrt(dvx**2 + dvy**2 + dvz**2)


def compute_altitude_diff(alt1: float, alt2: float) -> float:
    """
    Compute altitude difference
    
    Args:
        alt1: Altitude 1 in km
        alt2: Altitude 2 in km
        
    Returns:
        Absolute altitude difference in km
    """
    return abs(alt2 - alt1)


def compute_closest_approach(
    pos1: Dict, vel1: Dict,
    pos2: Dict, vel2: Dict,
    time_window_sec: int = 3600
) -> Tuple[float, float]:
    """
    Compute time and distance of closest approach
    
    Args:
        pos1: Position of object 1
        vel1: Velocity of object 1
        pos2: Position of object 2
        vel2: Velocity of object 2
        time_window_sec: Time window to search (seconds)
        
    Returns:
        Tuple of (time_to_closest_approach_sec, minimum_distance_km)
    """
    # Simplified linear approximation
    current_distance = compute_distance(pos1, pos2)
    rel_velocity = compute_relative_velocity(vel1, vel2)
    
    # Relative position vector
    dx = pos2.get("x", 0) - pos1.get("x", 0)
    dy = pos2.get("y", 0) - pos1.get("y", 0)
    dz = pos2.get("z", 0) - pos1.get("z", 0)
    
    # Relative velocity vector
    dvx = vel2.get("vx", 0) - vel1.get("vx", 0)
    dvy = vel2.get("vy", 0) - vel1.get("vy", 0)
    dvz = vel2.get("vz", 0) - vel1.get("vz", 0)
    
    # Time to closest approach (dot product)
    numerator = -(dx*dvx + dy*dvy + dz*dvz)
    denominator = dvx**2 + dvy**2 + dvz**2
    
    if denominator < 1e-10:
        # Objects moving in parallel
        return (0.0, current_distance)
    
    time_to_closest = numerator / denominator
    
    # Clamp to time window
    if time_to_closest < 0:
        time_to_closest = 0
    elif time_to_closest > time_window_sec:
        time_to_closest = time_window_sec
    
    # Compute position at closest approach
    x1_future = pos1.get("x", 0) + vel1.get("vx", 0) * time_to_closest
    y1_future = pos1.get("y", 0) + vel1.get("vy", 0) * time_to_closest
    z1_future = pos1.get("z", 0) + vel1.get("vz", 0) * time_to_closest
    
    x2_future = pos2.get("x", 0) + vel2.get("vx", 0) * time_to_closest
    y2_future = pos2.get("y", 0) + vel2.get("vy", 0) * time_to_closest
    z2_future = pos2.get("z", 0) + vel2.get("vz", 0) * time_to_closest
    
    min_distance = math.sqrt(
        (x2_future - x1_future)**2 +
        (y2_future - y1_future)**2 +
        (z2_future - z1_future)**2
    )
    
    return (time_to_closest, min_distance)


def vector_magnitude(vec: Dict) -> float:
    """
    Compute magnitude of a 3D vector
    
    Args:
        vec: Vector dict with x, y, z keys
        
    Returns:
        Magnitude
    """
    x = vec.get("x", vec.get("vx", 0))
    y = vec.get("y", vec.get("vy", 0))
    z = vec.get("z", vec.get("vz", 0))
    
    return math.sqrt(x**2 + y**2 + z**2)


def normalize_vector(vec: Dict) -> Dict:
    """
    Normalize a 3D vector to unit length
    
    Args:
        vec: Vector dict
        
    Returns:
        Normalized vector dict
    """
    mag = vector_magnitude(vec)
    
    if mag < 1e-10:
        return {"x": 0, "y": 0, "z": 1}
    
    return {
        "x": vec.get("x", 0) / mag,
        "y": vec.get("y", 0) / mag,
        "z": vec.get("z", 0) / mag
    }
