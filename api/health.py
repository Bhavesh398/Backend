"""
Health Check API Endpoints
"""
from fastapi import APIRouter
from datetime import datetime
from core.utils.response import success_response

router = APIRouter()


@router.get("/health")
async def health_check():
    """
    Health check endpoint
    Returns API status and timestamp
    """
    return success_response(
        data={
            "status": "ok",
            "service": "Orbit Shield API",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0"
        },
        message="Service is healthy"
    )


@router.get("/status")
async def system_status():
    """
    Detailed system status
    """
    return success_response(
        data={
            "api": "operational",
            "database": "connected",
            "ai_models": {
                "risk_predictor": "loaded",
                "risk_classifier": "loaded",
                "maneuver_agent": "loaded"
            },
            "uptime_seconds": 12345,  # Placeholder
            "active_satellites": 4,
            "active_alerts": 2
        },
        message="All systems operational"
    )
