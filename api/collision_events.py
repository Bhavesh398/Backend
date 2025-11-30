"""
Collision Events API Endpoints
AI-powered collision risk calculation and event management
"""
from fastapi import APIRouter, HTTPException, Query
from services.collision_service import collision_service
from core.utils.validators import CollisionEventBase
from core.utils.response import success_response
from config.supabase_client import supabase_client
from config.local_cache import local_cache
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/calculate")
async def calculate_collision_risks():
    """
    Calculate collision risks for all satellites
    
    Uses AI models to:
    1. Predict continuous risk scores (Model 1)
    2. Classify risk levels (Model 2)
    3. Compute closest approach times
    4. Calculate collision probabilities
    
    Returns list of all detected collision events sorted by risk level
    """
    collision_events = await collision_service.calculate_collision_risks()
    
    # Separate by risk level
    high_risk = [e for e in collision_events if e["risk_level"] == 3]
    medium_risk = [e for e in collision_events if e["risk_level"] == 2]
    low_risk = [e for e in collision_events if e["risk_level"] == 1]
    
    return success_response(
        data=collision_events,
        message=f"Calculated {len(collision_events)} collision events",
        meta={
            "total_events": len(collision_events),
            "high_risk_count": len(high_risk),
            "medium_risk_count": len(medium_risk),
            "low_risk_count": len(low_risk)
        }
    )


@router.get("/{event_id}")
async def get_collision_event(event_id: str):
    """
    Get specific collision event by ID
    
    Path Parameters:
        - event_id: Unique event identifier
    """
    event = await collision_service.get_collision_event(event_id)
    
    if not event:
        raise HTTPException(status_code=404, detail="Collision event not found")
    
    return success_response(
        data=event,
        message="Collision event retrieved successfully"
    )


@router.post("/log")
async def log_collision_event(event: CollisionEventBase):
    """
    Manually log a collision event
    
    Request Body:
        - satellite_id: Satellite involved (required)
        - debris_id: Debris object involved (required)
        - risk_level: Risk level 0-3 (required)
        - distance_km: Distance between objects (required)
        - relative_velocity_kmps: Relative velocity (required)
        - time_to_closest_approach_sec: Time to CA (optional)
        - probability: Collision probability (required)
    """
    result = await collision_service.log_collision_event(event.dict())
    
    return success_response(
        data=result,
        message="Collision event logged successfully"
    )


@router.get("/satellite/{satellite_id}")
async def get_satellite_collision_risks(satellite_id: str):
    """
    Get all collision risks for a specific satellite
    
    Path Parameters:
        - satellite_id: Unique satellite identifier
    """
    all_events = await collision_service.calculate_collision_risks()
    satellite_events = [e for e in all_events if e["satellite_id"] == satellite_id]
    
    if not satellite_events:
        return success_response(
            data=[],
            message=f"No collision risks found for satellite {satellite_id}"
        )
    
    return success_response(
        data=satellite_events,
        message=f"Found {len(satellite_events)} collision risks",
        meta={
            "satellite_id": satellite_id,
            "risk_count": len(satellite_events),
            "highest_risk": max(e["risk_level"] for e in satellite_events)
        }
    )


@router.get("/high-risk/list")
async def list_high_risk_events():
    """
    Get all high-risk collision events (risk_level = 3)
    
    Returns events requiring immediate attention
    """
    all_events = await collision_service.calculate_collision_risks()
    high_risk_events = [e for e in all_events if e["risk_level"] == 3]
    
    return success_response(
        data=high_risk_events,
        message=f"Found {len(high_risk_events)} high-risk events",
        meta={
            "count": len(high_risk_events),
            "severity": "critical"
        }
    )


@router.get("/for-simulator")
async def get_collision_events_for_simulator(
    sat_id: str = Query(..., description="Satellite ID"),
    deb_id: str = Query(None, description="Optional debris ID filter")
):
    """
    Get collision event data for CollisionSimulator component.
    Returns stored collision events from Supabase or cache.
    
    Query Parameters:
    - sat_id: Satellite ID (required)
    - deb_id: Debris ID (optional, filters to specific debris)
    
    Returns collision event with all fields needed for simulator visualization
    """
    try:
        # Try Supabase first
        filters = {"sat_id": sat_id}
        if deb_id:
            filters["deb_id"] = deb_id
        
        events = await supabase_client.select("collision_events", filters=filters, limit=10)
        
        if not events:
            # Fallback to cache
            logger.info(f"No Supabase collision_events for sat={sat_id}, checking cache...")
            cache_events = local_cache.get_all("collision_events")
            events = [e for e in cache_events if e.get("sat_id") == sat_id or e.get("satellite_id") == sat_id]
            if deb_id:
                events = [e for e in events if e.get("deb_id") == deb_id or e.get("debris_id") == deb_id]
        
        if not events:
            return success_response(
                data=[],
                message=f"No collision events found for satellite {sat_id}"
            )
        
        # Sort by risk level (descending) and collision probability (descending)
        events.sort(key=lambda x: (x.get("risk_level", 0), x.get("collision_probability", 0)), reverse=True)
        
        return success_response(
            data=events,
            message=f"Found {len(events)} collision event(s)",
            meta={
                "satellite_id": sat_id,
                "debris_id": deb_id,
                "event_count": len(events),
                "highest_risk": max((e.get("risk_level", 0) for e in events), default=0)
            }
        )
    except Exception as e:
        logger.error(f"Error fetching collision events for simulator: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch collision events: {str(e)}")
