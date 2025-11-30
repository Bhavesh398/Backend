"""
Debris API Endpoints
Full CRUD operations for space debris tracking
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from services.debris_service import debris_service
from core.utils.validators import DebrisCreate
from core.utils.response import success_response

router = APIRouter()


@router.get("/")
async def list_debris(
    limit: Optional[int] = Query(100, ge=1, le=100000, description="Maximum number of debris to return"),
    object_type: Optional[str] = Query(None, pattern="^(rocket_body|payload|debris|unknown)$"),
    all: bool = Query(False, description="If true, ignores limit and returns all debris")
):
    """
    List all debris objects
    
    Query Parameters:
        - limit: Maximum number of objects to return
        - object_type: Filter by type (rocket_body, payload, debris, unknown)
        - all: If true, returns all debris
    """
    effective_limit = None if all else limit
    debris_list = await debris_service.get_all_debris(limit=effective_limit)
    
    # Filter by type if provided
    if object_type:
        debris_list = [d for d in debris_list if d.get("object_type") == object_type]
    
    return success_response(
        data=debris_list,
        message=f"Retrieved {len(debris_list)} debris objects",
        meta={
            "count": len(debris_list),
            "all": all,
            "limit_used": effective_limit
        }
    )


@router.get("/{debris_id}")
async def get_debris(debris_id: str):
    """
    Get debris by ID
    
    Path Parameters:
        - debris_id: Unique debris identifier
    """
    debris = await debris_service.get_debris_by_id(debris_id)
    
    if not debris:
        raise HTTPException(status_code=404, detail="Debris object not found")
    
    return success_response(
        data=debris,
        message="Debris object retrieved successfully"
    )


@router.post("/")
async def create_debris(debris: DebrisCreate):
    """
    Create new debris entry
    
    Request Body:
        - name: Debris object name (required)
        - object_type: Type of debris (required)
        - altitude_km: Altitude in kilometers (required)
        - latitude: Current latitude (required)
        - longitude: Current longitude (required)
        - velocity_kmps: Velocity in km/s (required)
        - size_estimate_m: Size estimate in meters (optional)
    """
    result = await debris_service.create_debris(debris.dict())
    
    return success_response(
        data=result,
        message="Debris object created successfully"
    )


@router.put("/{debris_id}")
async def update_debris(debris_id: str, debris: DebrisCreate):
    """
    Update debris by ID
    
    Path Parameters:
        - debris_id: Unique debris identifier
    """
    existing = await debris_service.get_debris_by_id(debris_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Debris object not found")
    
    result = await debris_service.update_debris(debris_id, debris.dict())
    
    return success_response(
        data=result,
        message="Debris object updated successfully"
    )


@router.delete("/{debris_id}")
async def delete_debris(debris_id: str):
    """
    Delete debris by ID
    
    Path Parameters:
        - debris_id: Unique debris identifier
    """
    existing = await debris_service.get_debris_by_id(debris_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Debris object not found")
    
    success = await debris_service.delete_debris(debris_id)
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete debris object")
    
    return success_response(
        message="Debris object deleted successfully"
    )


@router.get("/type/{object_type}/count")
async def count_debris_by_type(object_type: str):
    """
    Count debris objects by type
    
    Path Parameters:
        - object_type: Type of debris object
    """
    all_debris = await debris_service.get_all_debris()
    count = sum(1 for d in all_debris if d.get("object_type") == object_type)
    
    return success_response(
        data={"object_type": object_type, "count": count},
        message=f"Found {count} debris objects of type {object_type}"
    )
