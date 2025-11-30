"""
Satellites API Endpoints
Full CRUD operations for satellite management
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from services.satellite_service import satellite_service
from core.utils.validators import SatelliteCreate, SatelliteUpdate
from core.utils.response import success_response, error_response

router = APIRouter()


@router.get("/")
async def list_satellites(
    limit: Optional[int] = Query(100, ge=1, le=100000, description="Maximum number of satellites to return"),
    status: Optional[str] = Query(None, pattern="^(active|inactive|deorbited)$"),
    all: bool = Query(False, description="If true, ignores limit and returns all satellites")
):
    """
    List all satellites
    
    Query Parameters:
        - limit: Maximum number of satellites to return
        - status: Filter by status (active, inactive, deorbited)
    """
    effective_limit = None if all else limit
    satellites = await satellite_service.get_all_satellites(limit=effective_limit)
    
    # Filter by status if provided
    if status:
        satellites = [s for s in satellites if s.get("status") == status]
    
    return success_response(
        data=satellites,
        message=f"Retrieved {len(satellites)} satellites",
        meta={
            "count": len(satellites),
            "all": all,
            "limit_used": effective_limit
        }
    )


@router.get("/{satellite_id}")
async def get_satellite(satellite_id: str):
    """
    Get satellite by ID
    
    Path Parameters:
        - satellite_id: Unique satellite identifier
    """
    satellite = await satellite_service.get_satellite_by_id(satellite_id)
    
    if not satellite:
        raise HTTPException(status_code=404, detail="Satellite not found")
    
    return success_response(
        data=satellite,
        message="Satellite retrieved successfully"
    )


@router.post("/")
async def create_satellite(satellite: SatelliteCreate):
    """
    Create new satellite
    
    Request Body:
        - name: Satellite name (required)
        - norad_id: NORAD catalog ID (optional)
        - altitude_km: Altitude in kilometers (required)
        - inclination_deg: Orbital inclination (required)
        - latitude: Current latitude (required)
        - longitude: Current longitude (required)
        - velocity_kmps: Velocity in km/s (required)
        - status: Operational status (optional, default: active)
    """
    result = await satellite_service.create_satellite(satellite.dict())
    
    return success_response(
        data=result,
        message="Satellite created successfully"
    )


@router.put("/{satellite_id}")
async def update_satellite(satellite_id: str, satellite: SatelliteUpdate):
    """
    Update satellite by ID
    
    Path Parameters:
        - satellite_id: Unique satellite identifier
        
    Request Body:
        - Any satellite fields to update (all optional)
    """
    # Check if satellite exists
    existing = await satellite_service.get_satellite_by_id(satellite_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Satellite not found")
    
    # Update only provided fields
    update_data = satellite.dict(exclude_unset=True)
    result = await satellite_service.update_satellite(satellite_id, update_data)
    
    return success_response(
        data=result,
        message="Satellite updated successfully"
    )


@router.delete("/{satellite_id}")
async def delete_satellite(satellite_id: str):
    """
    Delete satellite by ID
    
    Path Parameters:
        - satellite_id: Unique satellite identifier
    """
    # Check if satellite exists
    existing = await satellite_service.get_satellite_by_id(satellite_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Satellite not found")
    
    success = await satellite_service.delete_satellite(satellite_id)
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete satellite")
    
    return success_response(
        message="Satellite deleted successfully"
    )


@router.get("/{satellite_id}/position")
async def get_satellite_position(satellite_id: str):
    """
    Get current satellite position
    
    Returns real-time position data including:
    - Latitude, longitude, altitude
    - Velocity vector
    - Timestamp
    """
    satellite = await satellite_service.get_satellite_by_id(satellite_id)
    
    if not satellite:
        raise HTTPException(status_code=404, detail="Satellite not found")
    
    position_data = {
        "satellite_id": satellite_id,
        "name": satellite.get("name"),
        "latitude": satellite.get("latitude"),
        "longitude": satellite.get("longitude"),
        "altitude_km": satellite.get("altitude_km"),
        "velocity_kmps": satellite.get("velocity_kmps"),
        "inclination_deg": satellite.get("inclination_deg"),
        "timestamp": satellite.get("updated_at")
    }
    
    return success_response(
        data=position_data,
        message="Position data retrieved successfully"
    )
