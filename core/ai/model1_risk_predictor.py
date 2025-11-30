# backend/core/ai/model1_risk_predictor.py
"""
Advanced collision risk predictor using physics-based algorithms.
Analyzes orbital dynamics, relative velocities, and approach geometry.
"""
import math
from .physics_utils import compute_physics_features

def predict_risk_from_features(distance, rel_velocity, angle, altitude_diff, distance_at_tca=None, tca_seconds=None):
    """
    Deterministic physics-informed scoring -> [0,1].
    - closer distance and smaller distance_at_tca increase risk.
    - higher rel_velocity increases risk a bit.
    - altitude_diff reduces risk if huge.
    - tca_seconds: if very near future -> increases risk
    """
    # Base by inverse distance (km): saturate
    d = max(distance, 0.001)
    base = math.exp(-d / 30.0)   # 30 km characteristic scale

    # tca override: use closest approach distance if available
    if distance_at_tca is not None:
        base_tca = math.exp(- max(distance_at_tca, 0.001) / 20.0) * 0.8
        # if TCA soon (within 72 hours ~ 259200 sec) amplify
        if tca_seconds is not None:
            # prefer future TCAs
            if tca_seconds >= 0:
                time_factor = 1.0 + max(0.0, (72*3600 - tca_seconds) / (72*3600)) * 0.5
            else:
                # past event - don't amplify
                time_factor = 1.0
            base = max(base, base_tca * time_factor)

    # relative velocity factor (km/s) — normalize with plausible LEO 7.5
    rv = rel_velocity
    vel_factor = min(rv / 12.0, 1.0) * 0.4  # up to +0.4

    # angle factor: head-on (180 deg) is more dangerous than co-moving (0 deg)
    # make 180->1.0, 0->0.2
    ang_norm = (abs(angle - 180.0) / 180.0)  # 0..1 with 0 at 180? fix:
    # better: angle closeness to 180
    ang_factor = ((angle) / 180.0)  # angle near 180 -> 1.0

    angle_contrib = ang_factor * 0.2

    # altitude difference reduces risk
    alt_factor = max(0.0, 1.0 - min(altitude_diff / 50.0, 1.0)) * 0.3

    prob = base * 0.7 + vel_factor * 1.0 + angle_contrib + alt_factor * 0.5

    # clamp
    prob = max(0.0, min(1.0, prob))
    return float(prob)

def predict_for_sat_debris(sat, deb):
    """
    High-level: accept sat & deb dicts {x,y,z,vx,vy,vz}
    Returns dict with risk probability + feature breakdown.
    """
    feats = compute_physics_features(sat, deb)
    prob = predict_risk_from_features(
        feats["distance"],
        feats["rel_velocity"],
        feats["angle"],
        feats["altitude_diff"],
        distance_at_tca=feats["distance_at_tca"],
        tca_seconds=feats["tca_seconds"]
    )
    out = {
        "probability": prob,
        "features": feats
    }
    return out

def predict_risk(distance: float, relative_velocity: float, angle: float, altitude: float) -> float:
    """
    Legacy interface - predict continuous risk score (0.0-1.0).
    
    Args:
        distance: Current separation distance (km)
        relative_velocity: Relative velocity magnitude (km/s)
        angle: Approach angle (degrees)
        altitude: Altitude above Earth (km)
    
    Returns:
        Risk probability (0.0 = no risk, 1.0 = certain collision)
    """
    # Use new physics-based predictor
    return predict_risk_from_features(
        distance=distance,
        rel_velocity=relative_velocity,
        angle=angle,
        altitude_diff=0.0,  # Not provided in legacy interface
        distance_at_tca=distance,
        tca_seconds=None
    )
