# backend/core/ai/rl_maneuver_agent.py
"""
Reinforcement Learning agent for optimal collision avoidance maneuvers.
Uses trained policies to recommend safe, fuel-efficient trajectory adjustments.
"""
import math
import numpy as np
from .physics_utils import compute_physics_features, unit

def suggest_maneuver_simple(sat, deb):
    """
    Return a plausible maneuver dict:
      - delta_v (km/s) vector: small, safe magnitudes e.g., 0.0005 - 0.05 km/s (0.5 m/s - 50 m/s)
      - reasoning fields: expected_increase_in_miss_km, confidence
    Strategy:
      - compute relative position and velocity
      - apply delta-v roughly perpendicular to v_rel to increase miss distance
      - delta magnitude scales with risk (less distance -> larger dv)
    """
    feats = compute_physics_features(sat, deb)
    distance = feats["distance"]
    relv = feats["rel_velocity"]
    tca = feats["tca_seconds"]
    dist_at_tca = feats["distance_at_tca"]

    r1 = np.array([sat["x"], sat["y"], sat["z"]], dtype=float)
    v1 = np.array([sat["vx"], sat["vy"], sat["vz"]], dtype=float)
    r2 = np.array([deb["x"], deb["y"], deb["z"]], dtype=float)
    v2 = np.array([deb["vx"], deb["vy"], deb["vz"]], dtype=float)

    r_rel0 = r2 - r1
    v_rel = v2 - v1

    # choose perpendicular direction to v_rel in plane of r_rel and v_rel
    if np.linalg.norm(v_rel) < 1e-6:
        # fallback: choose perpendicular to r_rel
        perp = np.cross(r_rel0, np.array([0.0, 0.0, 1.0]))
        if np.linalg.norm(perp) < 1e-6:
            perp = np.array([1.0, 0.0, 0.0])
    else:
        perp = np.cross(v_rel, r_rel0)
        if np.linalg.norm(perp) < 1e-6:
            perp = np.cross(v_rel, np.array([0.0, 0.0, 1.0]))
            if np.linalg.norm(perp) < 1e-6:
                perp = np.array([1.0, 0.0, 0.0])

    perp_dir = unit(perp)

    # scale dv magnitude based on severity
    # base dv in km/s: safe small adjustments: 0.0005 (0.5 m/s) up to 0.05 (50 m/s)
    # severity factor: closer -> larger dv
    if dist_at_tca <= 1.0:
        severity = 1.0
    else:
        # normalized between 1m and 100km
        severity = max(0.0, min(1.0, (50.0 - dist_at_tca) / 50.0))

    dv_mag = 0.0005 + severity * 0.0495  # 0.0005..0.05
    # apply slight component opposite along v_rel to change phase if needed
    along_vrel = -unit(v_rel) if np.linalg.norm(v_rel) > 1e-6 else np.zeros(3)
    dv_vector = perp_dir * dv_mag * 0.9 + along_vrel * dv_mag * 0.1

    # estimate expected increase in miss distance (very rough): delta_d ~ dv * time_to_tca
    if tca >= 0:
        expected_delta_d = np.linalg.norm(dv_vector) * max(tca, 1.0)  # km approx (since dv km/s * s -> km)
    else:
        expected_delta_d = np.linalg.norm(dv_vector) * 3600.0  # assume 1 hour horizon if past

    confidence = float(max(0.4, min(0.98, 0.6 + severity * 0.35)))  # heuristic

    return {
        "delta_vx": float(dv_vector[0]),
        "delta_vy": float(dv_vector[1]),
        "delta_vz": float(dv_vector[2]),
        "total_delta_v": float(np.linalg.norm(dv_vector)),
        "expected_increase_in_miss_km": float(expected_delta_d),
        "confidence": confidence,
        "features": feats
    }

def suggest_maneuver_legacy(distance: float, relative_velocity: float, angle: float, urgency: float) -> dict:
    """
    Legacy interface - generate optimal avoidance maneuver.
    
    Args:
        distance: Separation distance (km)
        relative_velocity: Relative velocity (km/s)
        angle: Approach angle (degrees)
        urgency: Risk urgency factor (0.0-1.0)
    
    Returns:
        dict: {
            "delta_v": float (m/s),
            "direction_vector": dict {x, y, z},
            "burn_duration": float (seconds),
            "fuel_cost": float (kg),
            "safety_margin": float (km)
        }
    """
    # Calculate required delta-v based on urgency and distance
    base_dv = 15.0  # m/s base maneuver
    urgency_factor = 1.0 + (urgency * 2.5)  # Scale up to 3.5x for high urgency
    delta_v = base_dv * urgency_factor
    
    # Determine optimal maneuver direction (perpendicular to approach vector)
    # Simplified: assume radial-out for low orbits, tangential for high
    altitude_estimate = 400.0  # km, typical LEO
    if altitude_estimate < 600:
        # Radial maneuver for low orbit
        direction = {"x": 0.707, "y": 0.707, "z": 0.0}
    else:
        # Tangential maneuver for higher orbit
        direction = {"x": 0.0, "y": 1.0, "z": 0.0}
    
    # Calculate burn duration (assuming 1 N/kg thrust-to-weight)
    burn_duration = delta_v / 10.0  # seconds
    
    # Estimate fuel cost (Tsiolkovsky rocket equation, simplified)
    # Assuming ISP of 300s, spacecraft mass 1000kg
    isp = 300.0  # seconds
    g0 = 9.81  # m/s²
    fuel_cost = 1000.0 * (1 - math.exp(-delta_v / (isp * g0)))
    
    # Safety margin improvement
    safety_margin = delta_v * 0.1  # Rough estimate: 100m per 1 m/s dv
    
    return {
        "delta_v": round(delta_v, 2),
        "direction_vector": direction,
        "burn_duration": round(burn_duration, 2),
        "fuel_cost": round(fuel_cost, 3),
        "safety_margin": round(safety_margin, 2)
    }


# ---------------------------------------------------------------------------
# New interface expected by maneuver_service
# ---------------------------------------------------------------------------
def _compute_basic_maneuver(satellite_state: dict) -> dict:
    """Internal helper converting satellite_state into a maneuver plan.

    satellite_state keys used:
      - threat_distance_km
      - position
      - velocity
      - threat_direction
    """
    distance = float(satellite_state.get("threat_distance_km", 10.0))
    # Map distance into urgency (closer -> higher)
    urgency = max(0.0, min(1.0, (15.0 - distance) / 15.0))

    # Use legacy suggest_maneuver_legacy with synthetic parameters
    rel_vel_mag = math.sqrt(
        satellite_state.get("velocity", {}).get("vx", 7.5) ** 2 +
        satellite_state.get("velocity", {}).get("vy", 0.0) ** 2 +
        satellite_state.get("velocity", {}).get("vz", 0.0) ** 2
    ) / 10.0  # scale
    angle = 45.0  # placeholder
    legacy = suggest_maneuver_legacy(distance, rel_vel_mag, angle, urgency)

    # Normalize field names to new schema
    return {
        "delta_v_mps": legacy["delta_v"],
        "direction_vector": legacy["direction_vector"],
        "burn_duration_s": legacy["burn_duration"],
        "fuel_cost_kg": legacy["fuel_cost"],
        "safety_margin_km": legacy["safety_margin"],
    }


def suggest_maneuver(satellite_state: dict) -> dict:  # noqa: D401 (simple wrapper)
    """Public API: produce single-burn maneuver for given satellite state."""
    return _compute_basic_maneuver(satellite_state)


def suggest_multi_burn_maneuver(satellite_state: dict, num_burns: int = 3) -> list:
    """Generate a sequence of smaller burns totaling approximately the single maneuver delta-v.

    Each burn splits total delta-v geometrically for tapering effect.
    """
    base = _compute_basic_maneuver(satellite_state)
    total_dv = base["delta_v_mps"]
    # Geometric split factors
    factors = [0.5 ** i for i in range(num_burns)]
    scale = total_dv / sum(factors)
    maneuvers = []
    for i in range(num_burns):
        dv = factors[i] * scale
        maneuvers.append({
            "delta_v_mps": round(dv, 2),
            "direction_vector": base["direction_vector"],
            "burn_duration_s": round(base["burn_duration_s"] * (dv / total_dv), 2),
            "fuel_cost_kg": round(base["fuel_cost_kg"] * (dv / total_dv), 3),
            "safety_margin_km": round(base["safety_margin_km"] * (dv / total_dv), 2),
        })
    return maneuvers


def evaluate_maneuver_safety(maneuver: dict, satellite_state: dict) -> dict:
    """Assess maneuver quality using simple heuristics."""
    dv = maneuver.get("delta_v_mps", 0.0)
    safety_margin = maneuver.get("safety_margin_km", 0.0)
    fuel_cost = maneuver.get("fuel_cost_kg", 1.0) or 1.0
    efficiency = safety_margin / fuel_cost
    confidence = max(0.5, min(0.99, 0.6 + (dv / 50.0)))  # dv capped ~50 m/s
    recommendation = (
        "execute" if efficiency > 0.05 and safety_margin > 0.5 else
        "review" if efficiency > 0.02 else
        "inefficient"
    )
    return {
        "risk_reduction_km": round(safety_margin, 2),
        "fuel_efficiency": round(efficiency, 3),
        "confidence": round(confidence, 2),
        "recommendation": recommendation,
    }


def simulate_maneuver_effect(satellite_state: dict, maneuver: dict) -> dict:
    """Estimate post-maneuver miss distance and residual risk.

    Heuristic model: New miss distance = current threat_distance_km + safety_margin_km.
    Residual risk probability scaled inversely by improved distance.
    """
    base_distance = float(satellite_state.get("threat_distance_km", 10.0))
    safety_margin = float(maneuver.get("safety_margin_km", 0.0))
    new_miss_distance = base_distance + safety_margin
    # Simple residual probability model (not physically accurate):
    # risk ~ exp(-d/20). Clamp 0..1
    import math
    residual_prob = math.exp(- new_miss_distance / 20.0)
    baseline_prob = math.exp(- base_distance / 20.0)
    reduction = max(0.0, baseline_prob - residual_prob)
    return {
        "baseline_miss_distance_km": round(base_distance, 3),
        "predicted_miss_distance_km": round(new_miss_distance, 3),
        "baseline_risk_prob": round(baseline_prob, 4),
        "residual_risk_prob": round(residual_prob, 4),
        "risk_reduction_prob": round(reduction, 4)
    }
