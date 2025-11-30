# backend/core/ai/physics_utils.py
import numpy as np

EARTH_RADIUS_KM = 6371.0

def norm(vec):
    return float(np.linalg.norm(vec))

def unit(vec):
    n = np.linalg.norm(vec)
    if n == 0:
        return np.zeros_like(vec)
    return vec / n

def compute_distance(r1, r2):
    """Euclidean distance (km) between position vectors r1, r2 (arrays of length 3)."""
    r1 = np.asarray(r1, dtype=float)
    r2 = np.asarray(r2, dtype=float)
    return float(np.linalg.norm(r1 - r2))

def compute_relative_velocity(v1, v2):
    """Relative speed (km/s)"""
    v1 = np.asarray(v1, dtype=float)
    v2 = np.asarray(v2, dtype=float)
    return float(np.linalg.norm(v1 - v2))

def compute_angle_between(v1, v2):
    """Angle between velocity vectors in degrees (0..180)."""
    v1 = np.asarray(v1, dtype=float)
    v2 = np.asarray(v2, dtype=float)
    dv = np.dot(v1, v2)
    n1 = np.linalg.norm(v1)
    n2 = np.linalg.norm(v2)
    if n1 == 0 or n2 == 0:
        return 0.0
    cosang = np.clip(dv / (n1 * n2), -1.0, 1.0)
    return float(np.degrees(np.arccos(cosang)))

def altitude_from_radius(r_vec):
    """Return altitude above Earth surface in km, given ECI-like radius vector (km)."""
    r = np.linalg.norm(r_vec)
    return float(r - EARTH_RADIUS_KM)

def time_of_closest_approach(r1, v1, r2, v2):
    """
    Compute time of closest approach t* (seconds) assuming linear motion:
      r_rel(t) = (r2 - r1) + (v2 - v1) * t
    Minimize |r_rel(t)|^2 -> t* = - (r_rel0 · v_rel) / |v_rel|^2

    Returns:
      tca_seconds (float) -- can be negative (past), or positive (future),
      distance_at_tca_km (float).
    """
    r1 = np.asarray(r1, dtype=float)
    v1 = np.asarray(v1, dtype=float)
    r2 = np.asarray(r2, dtype=float)
    v2 = np.asarray(v2, dtype=float)

    r_rel0 = r2 - r1
    v_rel = v2 - v1
    v_rel_sq = np.dot(v_rel, v_rel)

    # If relative velocity is extremely small, tca is now
    if v_rel_sq < 1e-12:
        tstar = 0.0
    else:
        tstar = - float(np.dot(r_rel0, v_rel) / v_rel_sq)

    # compute distance at tstar
    r_rel_t = r_rel0 + v_rel * tstar
    dist = float(np.linalg.norm(r_rel_t))

    return tstar, dist

def time_until_tca_seconds(tca_seconds, now_seconds=0.0):
    """
    If you have tca relative to now=0 (as returned from time_of_closest_approach),
    this is simply tca_seconds (but here for naming clarity).
    """
    return float(tca_seconds)

# Small convenience to package feature computation
def compute_physics_features(sat, deb):
    """
    sat, deb: dict-like with keys x,y,z,vx,vy,vz (units: km and km/s)
    Returns dict:
      distance, rel_velocity, angle, altitude_diff, tca_seconds, dist_at_tca
    """
    r1 = np.array([sat["x"], sat["y"], sat["z"]], dtype=float)
    v1 = np.array([sat["vx"], sat["vy"], sat["vz"]], dtype=float)
    r2 = np.array([deb["x"], deb["y"], deb["z"]], dtype=float)
    v2 = np.array([deb["vx"], deb["vy"], deb["vz"]], dtype=float)

    distance = compute_distance(r1, r2)
    rel_velocity = compute_relative_velocity(v1, v2)
    angle = compute_angle_between(v1, v2)
    alt_diff = abs(altitude_from_radius(r1) - altitude_from_radius(r2))
    tca_seconds, dist_at_tca = time_of_closest_approach(r1, v1, r2, v2)

    return {
        "distance": distance,
        "rel_velocity": rel_velocity,
        "angle": angle,
        "altitude_diff": alt_diff,
        "tca_seconds": tca_seconds,
        "distance_at_tca": dist_at_tca
    }
