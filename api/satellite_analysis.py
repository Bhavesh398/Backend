# backend/api/satellite_analysis.py
"""
API endpoints for satellite click analysis using physics-based AI models.
Provides comprehensive collision risk assessment and maneuver recommendations.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime, timedelta
import uuid
from core.utils.response import success_response, error_response
from core.ai.on_click_handler import handle_satellite_click
from services.satellite_service import SatelliteService
from services.debris_service import DebrisService
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

satellite_service = SatelliteService()
debris_service = DebrisService()


@router.get("/analyze/{satellite_id}")
async def analyze_satellite(
    satellite_id: str,
    top_n: int = Query(default=5, ge=1, le=20, description="Number of nearest debris to analyze"),
    include_maneuver: bool = Query(default=True, description="Include maneuver recommendations")
):
    """
    Comprehensive analysis of satellite collision risks.
    
    Returns:
    - Nearest debris objects
    - Physics-based risk assessment (Model 1: probability)
    - Risk classification (Model 2: discrete levels)
    - Optimal avoidance maneuvers (RL Agent)
    """
    try:
        # Get satellite data
        sat = await satellite_service.get_satellite_by_id(satellite_id)
        if not sat:
            raise HTTPException(status_code=404, detail=f"Satellite {satellite_id} not found")
        
        # Debug: Check if satellite has required coordinates
        required_fields = ['x', 'y', 'z', 'vx', 'vy', 'vz']
        missing_fields = [f for f in required_fields if f not in sat]
        if missing_fields:
            logger.error(f"Satellite {satellite_id} missing fields: {missing_fields}")
            logger.error(f"Satellite data: lat={sat.get('latitude')}, lon={sat.get('longitude')}, alt={sat.get('altitude_km')}")
        
        # Get all debris
        debris_list = await debris_service.get_all_debris(limit=1000)
        if not debris_list:
            return success_response({
                "sat": sat,
                "nearest": [],
                "message": "No debris data available"
            })
        
        # Check debris also have coordinates
        debris_with_coords = [d for d in debris_list if all(f in d for f in required_fields)]
        logger.info(f"Found {len(debris_with_coords)} debris with coordinates out of {len(debris_list)} total")
        
        if not debris_with_coords:
            return success_response({
                "sat": sat,
                "nearest": [],
                "message": "No debris with valid coordinates"
            })
        
        # Run AI analysis
        result = handle_satellite_click(
            sat=sat,
            debris_list=debris_with_coords,
            top_n=top_n,
            include_maneuver=include_maneuver
        )

        # Persist nearest debris collision event for first (most critical) threat
        if result.get("nearest"):
            try:
                primary = result["nearest"][0]
                # Calculate time to closest approach (TCA) from tca_seconds
                tca_seconds = primary.get("features", {}).get("tca_seconds")
                tca_timestamp = None
                time_until_tca_hours = None
                if tca_seconds:
                    tca_timestamp = (datetime.utcnow() + timedelta(seconds=tca_seconds)).isoformat()
                    time_until_tca_hours = tca_seconds / 3600.0
                
                # Get proper IDs - prefer string IDs over UUIDs for foreign key refs
                sat_ref_id = sat.get("sat_id") or sat.get("norad_id") or str(sat.get("id", ""))
                deb_ref_id = primary["debris"].get("deb_id") or str(primary["debris"].get("id", ""))
                
                collision_record = {
                    "sat_id": sat_ref_id,
                    "deb_id": deb_ref_id,
                    "distance": primary.get("distance_now_km"),
                    "rel_velocity": primary.get("features", {}).get("relative_speed") or 7.5,
                    "angle": primary.get("features", {}).get("approach_angle"),
                    "altitude_diff": abs((sat.get("altitude") or 0) - (primary["debris"].get("altitude") or 0)),
                    "collision_probability": primary.get("model1_risk", {}).get("probability"),
                    "risk_level": primary.get("model2_class", {}).get("risk_level"),
                    "tca": tca_timestamp,
                    "time_until_tca": time_until_tca_hours,
                    "status": "monitoring"
                }
                
                # Try inserting into Supabase (non-blocking)
                from config.supabase_client import supabase_client
                try:
                    await supabase_client.insert("collision_events", collision_record)
                    logger.info(f"✅ Collision event saved to Supabase for sat={sat_ref_id}, deb={deb_ref_id}")
                except Exception as insert_err:
                    logger.warning(f"⚠️ Supabase collision_events insert failed (foreign key constraint or schema issue): {insert_err}")
                    # Store in cache as fallback
                    from config.local_cache import local_cache
                    collision_record["id"] = str(uuid.uuid4())
                    local_cache.upsert("collision_events", collision_record)
                    logger.info(f"💾 Collision event saved to cache instead for sat={sat_ref_id}")
            except Exception as persist_err:
                logger.error(f"❌ Collision event persistence completely failed for satellite {satellite_id}: {persist_err}")
        
        logger.info(f"Analysis completed for satellite {satellite_id}: {len(result['nearest'])} threats identified")
        return success_response(result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing satellite {satellite_id}: {str(e)}")
        return error_response(f"Analysis failed: {str(e)}")


@router.post("/analyze/batch")
async def analyze_multiple_satellites(
    satellite_ids: list[str],
    top_n: int = Query(default=3, ge=1, le=10),
    include_maneuver: bool = Query(default=False)
):
    """
    Batch analysis for multiple satellites.
    Returns comprehensive risk assessment for each satellite.
    """
    try:
        # Get all debris once for efficiency
        debris_list = await debris_service.get_all_debris(limit=1000)
        if not debris_list:
            return success_response({
                "results": [],
                "message": "No debris data available"
            })
        
        results = []
        for sat_id in satellite_ids:
            sat = await satellite_service.get_satellite_by_id(sat_id)
            if sat:
                analysis = handle_satellite_click(
                    sat=sat,
                    debris_list=debris_list,
                    top_n=top_n,
                    include_maneuver=include_maneuver
                )
                results.append(analysis)
                # Persist first threat for each satellite
                if analysis.get("nearest"):
                    try:
                        primary = analysis["nearest"][0]
                        # Calculate TCA
                        tca_seconds = primary.get("features", {}).get("tca_seconds")
                        tca_timestamp = None
                        time_until_tca_hours = None
                        if tca_seconds:
                            tca_timestamp = (datetime.utcnow() + timedelta(seconds=tca_seconds)).isoformat()
                            time_until_tca_hours = tca_seconds / 3600.0
                        
                        # Get proper IDs
                        sat_ref_id = sat.get("sat_id") or sat.get("norad_id") or str(sat.get("id", ""))
                        deb_ref_id = primary["debris"].get("deb_id") or str(primary["debris"].get("id", ""))
                        
                        collision_record = {
                            "sat_id": sat_ref_id,
                            "deb_id": deb_ref_id,
                            "distance": primary.get("distance_now_km"),
                            "rel_velocity": primary.get("features", {}).get("relative_speed") or 7.5,
                            "angle": primary.get("features", {}).get("approach_angle"),
                            "altitude_diff": abs((sat.get("altitude") or 0) - (primary["debris"].get("altitude") or 0)),
                            "collision_probability": primary.get("model1_risk", {}).get("probability"),
                            "risk_level": primary.get("model2_class", {}).get("risk_level"),
                            "tca": tca_timestamp,
                            "time_until_tca": time_until_tca_hours,
                            "status": "monitoring"
                        }
                        from config.supabase_client import supabase_client
                        from config.local_cache import local_cache
                        try:
                            await supabase_client.insert("collision_events", collision_record)
                            logger.info(f"✅ Collision event saved to Supabase for sat={sat_ref_id}, deb={deb_ref_id}")
                        except Exception as insert_err:
                            logger.warning(f"⚠️ Supabase collision_events insert failed (foreign key constraint): {insert_err}")
                            # Fallback to cache
                            collision_record["id"] = str(uuid.uuid4())
                            local_cache.upsert("collision_events", collision_record)
                            logger.info(f"💾 Collision event saved to cache instead for sat={sat_ref_id}")
                    except Exception as persist_err:
                        logger.error(f"❌ Batch collision event persistence failed for satellite {sat_id}: {persist_err}")
            else:
                results.append({
                    "sat": {"id": sat_id},
                    "nearest": [],
                    "error": "Satellite not found"
                })
        
        logger.info(f"Batch analysis completed for {len(results)} satellites")
        return success_response({"results": results})
        
    except Exception as e:
        logger.error(f"Error in batch analysis: {str(e)}")
        return error_response(f"Batch analysis failed: {str(e)}")


@router.get("/high-risk")
async def get_high_risk_satellites(
    risk_threshold: float = Query(default=0.7, ge=0.0, le=1.0),
    limit: int = Query(default=10, ge=1, le=50)
):
    """
    Identify all satellites with high collision risk.
    Scans entire satellite catalog and returns high-risk items.
    """
    try:
        satellites = await satellite_service.get_all_satellites(limit=100)
        debris_list = await debris_service.get_all_debris(limit=1000)
        
        if not satellites or not debris_list:
            return success_response({
                "high_risk_satellites": [],
                "message": "Insufficient data for analysis"
            })
        
        high_risk = []
        for sat in satellites:
            # Quick analysis with top 1 threat only
            analysis = handle_satellite_click(
                sat=sat,
                debris_list=debris_list,
                top_n=1,
                include_maneuver=False
            )
            
            if analysis["nearest"]:
                nearest = analysis["nearest"][0]
                risk_prob = nearest["model1_risk"]["probability"]
                
                if risk_prob >= risk_threshold:
                    high_risk.append({
                        "satellite": sat,
                        "risk_probability": risk_prob,
                        "risk_level": nearest["model2_class"]["risk_level"],
                        "nearest_debris": nearest["debris"],
                        "distance_km": nearest["distance_now_km"],
                        "tca_seconds": nearest["features"]["tca_seconds"]
                    })
        
        # Sort by risk probability descending
        high_risk.sort(key=lambda x: x["risk_probability"], reverse=True)
        high_risk = high_risk[:limit]
        
        logger.info(f"High-risk scan complete: {len(high_risk)} satellites exceed threshold {risk_threshold}")
        return success_response({
            "high_risk_satellites": high_risk,
            "total_scanned": len(satellites),
            "threshold": risk_threshold
        })
        
    except Exception as e:
        logger.error(f"Error in high-risk scan: {str(e)}")
        return error_response(f"High-risk scan failed: {str(e)}")


@router.post("/analyze-sandbox")
async def analyze_sandbox_scenario(
    satellite_id: str,
    altitude_km: float | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    top_n: int = 3,
    include_maneuver: bool = False
):
    """Analyze hypothetical scenario by overriding satellite position parameters.

    Allows UI sandbox to explore 'what-if' risk outcomes.
    """
    try:
        sat = await satellite_service.get_satellite_by_id(satellite_id)
        if not sat:
            raise HTTPException(status_code=404, detail=f"Satellite {satellite_id} not found")

        # Clone and override
        sat_mod = {**sat}
        if altitude_km is not None:
            sat_mod["altitude_km"] = altitude_km
        if latitude is not None:
            sat_mod["latitude"] = latitude
        if longitude is not None:
            sat_mod["longitude"] = longitude

        # Recompute Cartesian from modified lat/lon/alt if provided
        if any(v is not None for v in [altitude_km, latitude, longitude]):
            try:
                import math
                r = 6371.0 + float(sat_mod.get("altitude_km", 0))
                lat_r = math.radians(float(sat_mod.get("latitude", 0)))
                lon_r = math.radians(float(sat_mod.get("longitude", 0)))
                sat_mod["x"] = r * math.cos(lat_r) * math.cos(lon_r)
                sat_mod["y"] = r * math.sin(lat_r)
                sat_mod["z"] = r * math.cos(lat_r) * math.sin(lon_r)
            except Exception:
                pass

        debris_list = await debris_service.get_all_debris(limit=1000)
        debris_with_coords = [d for d in debris_list if all(f in d for f in ["x","y","z","vx","vy","vz"])]
        if not debris_with_coords:
            return success_response({"scenario": sat_mod, "nearest": [], "message": "No debris coordinates"})

        analysis = handle_satellite_click(
            sat=sat_mod,
            debris_list=debris_with_coords,
            top_n=top_n,
            include_maneuver=include_maneuver
        )

        return success_response({"scenario": {"original": sat, "modified": sat_mod}, **analysis})
    except HTTPException:
        raise
    except Exception as e:
        return error_response(f"Sandbox analysis failed: {e}")


@router.get("/risk-horizon/{satellite_id}")
async def get_risk_horizon(
    satellite_id: str,
    hours: int = 24,
    step_hours: int = 2
):
    """Generate simple multi-horizon risk curve for a satellite.

    Uses current nearest debris risk as baseline then applies exponential decay over time.
    """
    try:
        sat = await satellite_service.get_satellite_by_id(satellite_id)
        if not sat:
            raise HTTPException(status_code=404, detail="Satellite not found")
        debris_list = await debris_service.get_all_debris(limit=1000)
        debris_with_coords = [d for d in debris_list if all(f in d for f in ["x","y","z","vx","vy","vz"])]
        if not debris_with_coords:
            return success_response({"curve": [], "message": "No debris coordinates"})
        analysis = handle_satellite_click(sat=sat, debris_list=debris_with_coords, top_n=1, include_maneuver=False)
        base_prob = 0.0
        base_distance = None
        if analysis.get("nearest"):
            base_prob = analysis["nearest"][0]["model1_risk"]["probability"]
            base_distance = analysis["nearest"][0]["distance_now_km"]
        curve = []
        decay_scale = 36.0  # hours characteristic
        for h in range(0, hours + 1, step_hours):
            # Simple model: probability decays with time; include slight oscillation
            import math
            prob = base_prob * math.exp(- h / decay_scale) * (1.0 + 0.05 * math.sin(h / 3.0))
            curve.append({"hour": h, "probability": round(max(prob, 0.0), 4)})
        return success_response({"curve": curve, "baseline_probability": base_prob, "baseline_distance_km": base_distance})
    except HTTPException:
        raise
    except Exception as e:
        return error_response(f"Risk horizon failed: {e}")
