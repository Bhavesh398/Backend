"""
Data Validators
"""
from typing import Optional
from pydantic import BaseModel, Field, validator
from datetime import datetime


class SatelliteBase(BaseModel):
    """Base satellite model"""
    name: str = Field(..., min_length=1, max_length=100)
    norad_id: Optional[str] = Field(None, max_length=50)
    altitude_km: float = Field(..., gt=0, lt=50000)
    inclination_deg: float = Field(..., ge=0, le=180)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    velocity_kmps: float = Field(..., gt=0, lt=20)
    status: str = Field(default="active", pattern="^(active|inactive|deorbited)$")


class SatelliteCreate(SatelliteBase):
    """Satellite creation model"""
    pass


class SatelliteUpdate(BaseModel):
    """Satellite update model (all fields optional)"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    altitude_km: Optional[float] = Field(None, gt=0, lt=50000)
    inclination_deg: Optional[float] = Field(None, ge=0, le=180)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    velocity_kmps: Optional[float] = Field(None, gt=0, lt=20)
    status: Optional[str] = Field(None, pattern="^(active|inactive|deorbited)$")


class DebrisBase(BaseModel):
    """Base debris model"""
    name: str = Field(..., min_length=1, max_length=100)
    object_type: str = Field(..., pattern="^(rocket_body|payload|debris|unknown)$")
    altitude_km: float = Field(..., gt=0, lt=50000)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    velocity_kmps: float = Field(..., gt=0, lt=20)
    size_estimate_m: float = Field(default=1.0, gt=0, lt=100)


class DebrisCreate(DebrisBase):
    """Debris creation model"""
    pass


class CollisionEventBase(BaseModel):
    """Collision event model"""
    satellite_id: str
    debris_id: str
    risk_level: int = Field(..., ge=0, le=3)
    distance_km: float = Field(..., gt=0)
    relative_velocity_kmps: float = Field(..., gt=0)
    time_to_closest_approach_sec: Optional[float] = Field(None, ge=0)
    probability: float = Field(..., ge=0, le=1)


class ManeuverPlan(BaseModel):
    """Maneuver plan model"""
    satellite_id: str
    maneuver_type: str = Field(..., pattern="^(avoidance|station_keeping|deorbit)$")
    delta_v_mps: float = Field(..., ge=0, le=1000)
    burn_duration_sec: float = Field(..., ge=0, le=3600)
    direction_vector: list[float] = Field(..., min_items=3, max_items=3)
    fuel_cost_kg: float = Field(..., ge=0)
    
    @validator('direction_vector')
    def validate_unit_vector(cls, v):
        """Ensure direction vector is roughly normalized"""
        magnitude = sum(x**2 for x in v) ** 0.5
        if magnitude < 0.9 or magnitude > 1.1:
            raise ValueError("Direction vector must be approximately unit length")
        return v


class AlertBase(BaseModel):
    """Alert model"""
    alert_type: str = Field(..., pattern="^(collision|debris|maneuver|system)$")
    severity: str = Field(..., pattern="^(low|medium|high|critical)$")
    title: str = Field(..., min_length=1, max_length=200)
    message: str = Field(..., min_length=1, max_length=1000)
    satellite_id: Optional[str] = None
    acknowledged: bool = False
