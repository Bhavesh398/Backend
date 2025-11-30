"""
Alerts API Endpoints
Alert management and notification system
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from services.alert_service import alert_service
from core.utils.validators import AlertBase
from core.utils.response import success_response

router = APIRouter()


@router.get("/")
async def list_alerts(
    severity: Optional[str] = Query(None, pattern="^(low|medium|high|critical)$"),
    acknowledged: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=200)
):
    """
    List all alerts with optional filtering
    
    Query Parameters:
        - severity: Filter by severity (low, medium, high, critical)
        - acknowledged: Filter by acknowledgment status (true/false)
        - limit: Maximum number of alerts to return
    """
    alerts = await alert_service.get_all_alerts(
        severity=severity,
        acknowledged=acknowledged,
        limit=limit
    )
    
    return success_response(
        data=alerts,
        message=f"Retrieved {len(alerts)} alerts",
        meta={
            "count": len(alerts),
            "unacknowledged": sum(1 for a in alerts if not a.get("acknowledged", False))
        }
    )


@router.get("/{alert_id}")
async def get_alert(alert_id: str):
    """
    Get specific alert by ID
    
    Path Parameters:
        - alert_id: Unique alert identifier
    """
    alert = await alert_service.get_alert_by_id(alert_id)
    
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    return success_response(
        data=alert,
        message="Alert retrieved successfully"
    )


@router.post("/")
async def create_alert(alert: AlertBase):
    """
    Create new alert
    
    Request Body:
        - alert_type: Type of alert (collision, debris, maneuver, system)
        - severity: Severity level (low, medium, high, critical)
        - title: Alert title (required)
        - message: Alert message (required)
        - satellite_id: Related satellite ID (optional)
    """
    result = await alert_service.create_alert(alert.dict())
    
    return success_response(
        data=result,
        message="Alert created successfully"
    )


@router.patch("/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str):
    """
    Mark alert as acknowledged
    
    Path Parameters:
        - alert_id: Unique alert identifier
    """
    result = await alert_service.acknowledge_alert(alert_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    return success_response(
        data=result,
        message="Alert acknowledged successfully"
    )


@router.get("/unacknowledged/count")
async def count_unacknowledged_alerts():
    """
    Get count of unacknowledged alerts
    
    Useful for dashboard notification badges
    """
    alerts = await alert_service.get_all_alerts(acknowledged=False)
    
    # Count by severity
    severity_counts = {
        "critical": sum(1 for a in alerts if a.get("severity") == "critical"),
        "high": sum(1 for a in alerts if a.get("severity") == "high"),
        "medium": sum(1 for a in alerts if a.get("severity") == "medium"),
        "low": sum(1 for a in alerts if a.get("severity") == "low")
    }
    
    return success_response(
        data={
            "total": len(alerts),
            "by_severity": severity_counts
        },
        message=f"{len(alerts)} unacknowledged alerts"
    )


@router.get("/satellite/{satellite_id}")
async def get_satellite_alerts(satellite_id: str):
    """
    Get all alerts for a specific satellite
    
    Path Parameters:
        - satellite_id: Unique satellite identifier
    """
    all_alerts = await alert_service.get_all_alerts()
    satellite_alerts = [a for a in all_alerts if a.get("satellite_id") == satellite_id]
    
    return success_response(
        data=satellite_alerts,
        message=f"Retrieved {len(satellite_alerts)} alerts for satellite",
        meta={
            "satellite_id": satellite_id,
            "count": len(satellite_alerts)
        }
    )


@router.get("/recent/high-priority")
async def get_recent_high_priority_alerts():
    """
    Get recent high-priority alerts (high and critical severity)
    
    Returns most recent 10 alerts with high or critical severity
    """
    all_alerts = await alert_service.get_all_alerts(limit=100)
    
    high_priority = [
        a for a in all_alerts
        if a.get("severity") in ["high", "critical"]
    ]
    
    # Sort by creation time (most recent first)
    high_priority.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    
    return success_response(
        data=high_priority[:10],
        message=f"Retrieved {len(high_priority[:10])} high-priority alerts",
        meta={"total_high_priority": len(high_priority)}
    )
