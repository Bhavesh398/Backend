"""
Real-time Risk Stream WebSocket
Provides periodic broadcast of top collision risk events for live UI updates.
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import asyncio
import time
from services.collision_service import collision_service

router = APIRouter()


@router.websocket("/ws/risks")
async def risks_socket(websocket: WebSocket):
    """Stream top collision risks every few seconds.

    Message schema:
    {
        "timestamp": <epoch_seconds>,
        "events": [
            {
                "satellite_id": str,
                "satellite_name": str,
                "debris_id": str,
                "distance_km": float,
                "risk_score": float,
                "risk_level": int,
                "time_to_closest_approach_sec": float,
                "minimum_distance_km": float,
                "collision_probability": float
            }, ...
        ]
    }
    """
    await websocket.accept()
    try:
        while True:
            events = await collision_service.calculate_collision_risks()
            top_events = events[:10]
            payload = {
                "timestamp": time.time(),
                "events": [
                    {
                        "satellite_id": e.get("satellite_id"),
                        "satellite_name": e.get("satellite_name"),
                        "debris_id": e.get("debris_id"),
                        "distance_km": e.get("distance_km"),
                        "risk_score": e.get("risk_score"),
                        "risk_level": e.get("risk_level"),
                        "time_to_closest_approach_sec": e.get("time_to_closest_approach_sec"),
                        "minimum_distance_km": e.get("minimum_distance_km"),
                        "collision_probability": e.get("collision_probability")
                    } for e in top_events
                ]
            }
            await websocket.send_json(payload)
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        # Client disconnected normally
        pass
    except Exception:
        # On unexpected errors, close connection gracefully
        await websocket.close(code=1011)
