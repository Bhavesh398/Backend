"""
SpaceShield Backend - Main Application Entry Point
FastAPI-based space traffic management system
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import uvicorn

from api import satellites, debris, collision_events, maneuvers, alerts, health, satellite_analysis, risk_stream, report, cache
from config.settings import settings
from config.cache_manager import cache_manager
from config.supabase_client import supabase_client
import gemini_search
import asyncio


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    print("🚀 Orbit Shield Backend Starting...")
    print(f"📡 Environment: {settings.ENVIRONMENT}")
    print(f"🔧 Debug Mode: {settings.DEBUG}")
    
    # Sync cache from Supabase on startup
    print("💾 Syncing cache from Supabase...")
    try:
        results = await cache_manager.sync_all_tables(supabase_client)
        total = sum(results.values())
        print(f"✅ Cache sync complete: {total} total records")
    except Exception as e:
        print(f"⚠️ Cache sync failed: {e}. Will use existing cache.")
    
    yield
    print("🛑 Orbit Shield Backend Shutting Down...")


app = FastAPI(
    title="Orbit Shield API",
    description="Space Traffic Management System - Real-time satellite tracking, collision detection, and maneuver planning",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global Exception Handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": exc.detail,
            "status_code": exc.status_code
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": "Internal server error",
            "detail": str(exc) if settings.DEBUG else None
        }
    )


# Include Routers
app.include_router(health.router, prefix="/api", tags=["Health"])
app.include_router(cache.router, prefix="/api", tags=["Cache Management"])
app.include_router(satellites.router, prefix="/api/satellites", tags=["Satellites"])
app.include_router(debris.router, prefix="/api/debris", tags=["Debris"])
app.include_router(collision_events.router, prefix="/api/collision-events", tags=["Collision Events"])
app.include_router(maneuvers.router, prefix="/api/maneuvers", tags=["Maneuvers"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["Alerts"])
app.include_router(risk_stream.router, prefix="/api", tags=["Risk Stream"])
app.include_router(report.router, prefix="/api", tags=["Reports"])
app.include_router(gemini_search.router, tags=["Gemini AI"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Orbit Shield API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "operational"
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
app.include_router(satellite_analysis.router, prefix="/api/satellite-analysis", tags=["Satellite Analysis"])
