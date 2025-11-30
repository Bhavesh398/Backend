"""
Maneuvers API Endpoints
AI-powered maneuver planning using RL agent
"""
from fastapi import APIRouter, HTTPException, Body
from typing import Dict
from services.maneuver_service import maneuver_service
from core.ai.rl_maneuver_agent import simulate_maneuver_effect
from services.satellite_service import satellite_service
from core.utils.response import success_response

router = APIRouter()


@router.post("/plan")
async def plan_maneuver(
    satellite_id: str = Body(...),
    threat_data: Dict = Body(...)
):
    """
    Plan collision avoidance maneuver using RL agent
    
    Request Body:
        - satellite_id: Satellite requiring maneuver (required)
        - threat_data: Dict containing threat information:
            - distance_km: Distance to threat
            - satellite_position: {x, y, z}
            - satellite_velocity: {vx, vy, vz}
            - threat_direction: {x, y, z} unit vector
    
    Returns AI-generated maneuver plan with:
    - Delta-V requirements
    - Burn duration
    - Direction vector
    - Fuel cost estimate
    - Safety evaluation
    """
    maneuver_plan = await maneuver_service.plan_maneuver(satellite_id, threat_data)
    
    return success_response(
        data=maneuver_plan,
        message="Maneuver plan generated successfully"
    )


@router.post("/plan-multi-burn")
async def plan_multi_burn_maneuver(
    satellite_id: str = Body(...),
    threat_data: Dict = Body(...)
):
    """
    Plan multi-burn maneuver sequence
    
    More fuel-efficient alternative to single large burn.
    Recommended for medium-risk scenarios where time permits.
    
    Request Body:
        - satellite_id: Satellite ID (required)
        - threat_data: Threat information dict (required)
    
    Returns sequence of 3 maneuver plans
    """
    maneuver_sequence = await maneuver_service.plan_multi_burn_maneuver(
        satellite_id,
        threat_data
    )
    
    return success_response(
        data=maneuver_sequence,
        message=f"Generated {len(maneuver_sequence)}-burn maneuver sequence",
        meta={
            "total_burns": len(maneuver_sequence),
            "total_delta_v": sum(m["delta_v_mps"] for m in maneuver_sequence),
            "total_fuel_cost": sum(m["fuel_cost_kg"] for m in maneuver_sequence)
        }
    )


@router.get("/satellite/{satellite_id}")
async def get_satellite_maneuvers(satellite_id: str):
    """
    Get all maneuvers planned for a satellite
    
    Path Parameters:
        - satellite_id: Unique satellite identifier
    """
    maneuvers = await maneuver_service.get_maneuvers_for_satellite(satellite_id)
    
    return success_response(
        data=maneuvers,
        message=f"Retrieved {len(maneuvers)} maneuvers",
        meta={"count": len(maneuvers)}
    )


@router.patch("/{maneuver_id}/status")
async def update_maneuver_status(
    maneuver_id: str,
    status: str = Body(..., pattern="^(pending|approved|executing|completed|cancelled)$")
):
    """
    Update maneuver execution status
    
    Path Parameters:
        - maneuver_id: Unique maneuver identifier
        
    Request Body:
        - status: New status (pending, approved, executing, completed, cancelled)
    """
    result = await maneuver_service.update_maneuver_status(maneuver_id, status)
    
    if not result:
        raise HTTPException(status_code=404, detail="Maneuver not found")
    
    return success_response(
        data=result,
        message=f"Maneuver status updated to {status}"
    )


@router.post("/emergency-plan")
async def emergency_maneuver_plan(
    satellite_id: str = Body(...),
    threat_data: Dict = Body(...)
):
    """
    Generate emergency high-urgency maneuver
    
    For critical situations requiring immediate action.
    Optimizes for speed over fuel efficiency.
    
    Request Body:
        - satellite_id: Satellite ID (required)
        - threat_data: Threat information (required)
    """
    # Set high urgency in threat data
    threat_data["urgency"] = "high"
    threat_data["distance_km"] = min(threat_data.get("distance_km", 10), 5.0)
    
    maneuver_plan = await maneuver_service.plan_maneuver(satellite_id, threat_data)
    
    return success_response(
        data=maneuver_plan,
        message="Emergency maneuver plan generated",
        meta={"priority": "critical", "urgency": "high"}
    )


@router.post("/simulate")
async def simulate_maneuver(
    satellite_id: str = Body(...),
    threat_data: Dict = Body(...),
    maneuver: Dict | None = Body(None)
):
    """Simulate outcome of a proposed maneuver.

    If maneuver not provided, a fresh plan is generated first.
    Returns predicted miss distance and residual risk probability.
    """
    # Build satellite state in same shape used by planner
    sat_state = {
        "satellite_id": satellite_id,
        "position": threat_data.get("satellite_position", {"x": 0, "y": 0, "z": 400}),
        "velocity": threat_data.get("satellite_velocity", {"vx": 7.5, "vy": 0, "vz": 0}),
        "threat_distance_km": threat_data.get("distance_km", 10.0),
        "threat_direction": threat_data.get("threat_direction", {"x": 1, "y": 0, "z": 0})
    }
    if maneuver is None:
        planned = await maneuver_service.plan_maneuver(satellite_id, threat_data)
    else:
        planned = maneuver
    sim = simulate_maneuver_effect(sat_state, planned)
    return success_response({"maneuver": planned, "simulation": sim}, "Maneuver simulation completed")
