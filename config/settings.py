"""
Configuration Settings
Environment variables and application configuration
"""
from pydantic_settings import BaseSettings
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """Application settings from environment variables"""
    
    # Application
    APP_NAME: str = "Orbit Shield"
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "True").lower() == "true"
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    
    # CORS
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8000",
        "https://frontend-lilac-seven-27.vercel.app",
        "https://*.vercel.app",
    ]
    
    # Supabase
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    
    # Google Gemini API
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    
    # AI Model Settings
    COLLISION_THRESHOLD_KM: float = 10.0
    HIGH_RISK_THRESHOLD_KM: float = 5.0
    MEDIUM_RISK_THRESHOLD_KM: float = 7.5
    
    # Orbital Mechanics
    EARTH_RADIUS_KM: float = 6371.0
    LEO_ALTITUDE_MIN_KM: float = 160.0
    LEO_ALTITUDE_MAX_KM: float = 2000.0
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
