# backend/core/ai/on_click_handler.py
from .physics_utils import compute_physics_features
from .model1_risk_predictor import predict_for_sat_debris
from .model2_risk_classifier import classify_for_sat_debris
from .rl_maneuver_agent import suggest_maneuver_simple

def handle_satellite_click(sat, debris_list, top_n=1, include_maneuver=True):
    """
    sat: dict with keys x,y,z,vx,vy,vz and metadata
    debris_list: iterable of debris dicts each with x,y,z,vx,vy,vz and metadata
    top_n: number of nearest debris to return predictions for (default 1)
    Returns:
      {
        "sat": sat,
        "nearest": [
          {
            "debris": deb,
            "features": ...,
            "model1": { probability, features },
            "model2": { risk_level, features },
            "maneuver": {...}  # optional
          }, ...
        ]
      }
    """
    # find nearest debris by current distance
    dists = []
    for d in debris_list:
        feats = compute_physics_features(sat, d)
        dists.append((feats["distance"], d, feats))

    # sort ascending
    dists.sort(key=lambda x: x[0])

    out_items = []
    for i in range(min(top_n, len(dists))):
        dist, deb, feats = dists[i]
        m1 = predict_for_sat_debris(sat, deb)
        m2 = classify_for_sat_debris(sat, deb)
        man = suggest_maneuver_simple(sat, deb) if include_maneuver else None

        out_items.append({
            "debris": deb,
            "distance_now_km": float(dist),
            "features": feats,
            "model1_risk": m1,
            "model2_class": m2,
            "maneuver": man
        })

    return {
        "sat": sat,
        "nearest": out_items
    }
