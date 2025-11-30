# backend/core/ai/model2_risk_classifier.py
"""
Multi-class risk classifier using advanced pattern recognition.
Categorizes collision scenarios into discrete risk levels.
"""
from .physics_utils import compute_physics_features
from typing import Dict

def classify_distance(distance):
    """
    Simple rule-based classifier:
      distance > 50 km -> 0 (safe)
      20 < d <= 50 -> 1 (caution)
      5 < d <= 20  -> 2 (warning)
      d <= 5       -> 3 (critical)
    """
    if distance > 50:
        return 0
    elif distance > 20:
        return 1
    elif distance > 5:
        return 2
    else:
        return 3

def classify_for_sat_debris(sat, deb):
    """
    High-level classifier accepting sat & deb dicts {x,y,z,vx,vy,vz}.
    Returns dict with risk_level and features.
    """
    feats = compute_physics_features(sat, deb)
    cls = classify_distance(feats["distance"])
    return {
        "risk_level": int(cls),
        "features": feats
    }

def classify_risk_advanced(distance: float, relative_velocity: float, angle: float) -> dict:
    """
    Legacy interface - classify risk into discrete levels.
    
    Args:
        distance: Separation distance (km)
        relative_velocity: Relative velocity (km/s)
        angle: Approach angle (degrees)
    
    Returns:
        dict: {
            "risk_level": int (0-3),
            "label": str,
            "color": str,
            "action": str
        }
    """
    # Use new distance-based classification
    risk_level = classify_distance(distance)
    
    # Map to labels
    labels = {
        0: ("No Risk", "green", "Continue monitoring"),
        1: ("Low Risk", "yellow", "Increase monitoring frequency"),
        2: ("Medium Risk", "orange", "Prepare avoidance maneuver"),
        3: ("High Risk", "red", "Execute immediate avoidance maneuver")
    }
    
    label, color, action = labels.get(risk_level, labels[0])
    
    # Adjust for velocity and angle
    if relative_velocity > 10.0 and angle > 150:
        risk_level = min(risk_level + 1, 3)
        label, color, action = labels.get(risk_level, labels[3])
    
    return {
        "risk_level": risk_level,
        "label": label,
        "color": color,
        "action": action
    }
