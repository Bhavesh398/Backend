"""
Gemini API with Google Search Grounding
Provides live data fetching for satellite information
"""
import google.generativeai as genai
import os
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

router = APIRouter()
logger = logging.getLogger(__name__)

class ChatRequest(BaseModel):
    satellite_data: dict
    user_message: str
    api_key: str

@router.post("/api/gemini-chat")
async def gemini_chat(request: ChatRequest):
    """Chat endpoint for Gemini satellite assistant.

    Improvements:
    - Validates API key (returns 400 instead of 500 when missing/invalid)
    - Falls back to environment GEMINI_API_KEY if no key provided in request
    - Distinguishes rate-limit / auth / blocked prompt errors
    - Returns structured error details for frontend to decide fallback
    """
    # Resolve API key precedence: request.api_key > env variable
    raw_key = (request.api_key or os.getenv("GEMINI_API_KEY") or "").strip()
    if not raw_key:
        raise HTTPException(status_code=400, detail="Missing Gemini API key. Provide api_key in body or set GEMINI_API_KEY env variable.")
    if not raw_key.startswith("AIza") or len(raw_key) < 30:  # Basic heuristic
        raise HTTPException(status_code=400, detail="Invalid Gemini API key format. Keys typically start with 'AIza'.")

    try:
        logger.info(
            "Gemini chat request received satellite=%s key_len=%d user_msg_len=%d",
            request.satellite_data.get('name', 'Unknown'), len(raw_key), len(request.user_message or '')
        )

        # Configure client
        genai.configure(api_key=raw_key)

        # Build context
        satellite_context = build_satellite_context(request.satellite_data)
        system_prompt = (
            "You are an expert satellite analyst assistant. You have detailed information about the satellite: "
            f"{request.satellite_data.get('name') or request.satellite_data.get('sat_name') or request.satellite_data.get('norad_id')}\n\n"
            f"{satellite_context}\n\n"
            "Provide detailed, accurate, and helpful information about this satellite based on the available data. "
            "Use concise bullet points and sections. If user asks for recent status or live events, explain that live web search is not active yet and rely only on provided data."
        )

        model = genai.GenerativeModel('gemini-2.5-flash')
        full_prompt = system_prompt + "\n\nUser question: " + (request.user_message or "")
        logger.info("Prompt prepared chars=%d", len(full_prompt))

        try:
            response = model.generate_content(full_prompt)
        except Exception as gen_err:  # Narrow classification
            msg = str(gen_err)
            if "quota" in msg.lower() or "rate" in msg.lower():
                raise HTTPException(status_code=429, detail="Gemini rate limit or quota exceeded. Please retry later.")
            if "permission" in msg.lower() or "unauthorized" in msg.lower():
                raise HTTPException(status_code=401, detail="Gemini authentication failed. Verify API key.")
            if "blocked" in msg.lower():
                raise HTTPException(status_code=400, detail="Prompt was blocked by safety filters. Try rephrasing.")
            logger.error("Unhandled Gemini inner error: %s", msg, exc_info=True)
            raise

        text = getattr(response, 'text', None) or "No response text returned."
        logger.info("Gemini response length=%d", len(text))

        return {
            "response": text,
            "has_live_data": False,  # Search grounding not enabled
            "sources": None
        }

    except HTTPException:
        # Re-raise structured errors untouched
        raise
    except Exception as e:
        # Graceful fallback: do NOT raise 500; return simulated response so frontend keeps UX
        logger.error("Gemini API fatal error (fallback engaged): %s", e, exc_info=True)
        fallback = build_satellite_context(request.satellite_data)
        synthetic = (
            "⚠️ AI service unavailable. Fallback mode.\n\n" +
            (fallback or "Limited satellite data.") +
            "\n\nUser Question: " + (request.user_message or "(none)") +
            "\n\nSuggested Next Steps:\n- Verify GEMINI_API_KEY validity.\n- Check internet connectivity from backend.\n- Ensure 'google-generativeai' package version matches requirements."
        )
        return {
            "response": synthetic,
            "has_live_data": False,
            "sources": None,
            "error": str(e)
        }


def build_satellite_context(sat_data: dict) -> str:
    """Build satellite context from available data, excluding unknowns"""
    context_lines = []
    
    # Helper to check if value is valid
    def is_valid(val):
        if val is None:
            return False
        if isinstance(val, str) and val.lower() in ['unknown', 'n/a', 'data not available', '']:
            return False
        return True
    
    # Add fields if they exist and are valid
    if is_valid(sat_data.get('name') or sat_data.get('sat_name')):
        context_lines.append(f"- Name: {sat_data.get('name') or sat_data.get('sat_name')}")
    
    if is_valid(sat_data.get('sat_id')):
        context_lines.append(f"- NORAD ID: {sat_data.get('sat_id')}")
    elif is_valid(sat_data.get('norad_id')):
        context_lines.append(f"- NORAD ID: {sat_data.get('norad_id')}")
    
    if is_valid(sat_data.get('altitude_km') or sat_data.get('altitude')):
        alt = sat_data.get('altitude_km') or sat_data.get('altitude')
        context_lines.append(f"- Altitude: {alt} km")
    
    if is_valid(sat_data.get('latitude')) and is_valid(sat_data.get('longitude')):
        context_lines.append(f"- Position: {sat_data.get('latitude')}°N, {sat_data.get('longitude')}°E")
    
    if is_valid(sat_data.get('inclination_deg') or sat_data.get('inclination')):
        inc = sat_data.get('inclination_deg') or sat_data.get('inclination')
        context_lines.append(f"- Inclination: {inc}°")
    
    if is_valid(sat_data.get('velocity_kmps') or sat_data.get('velocity')):
        vel = sat_data.get('velocity_kmps') or sat_data.get('velocity')
        context_lines.append(f"- Velocity: {vel} km/s")
    
    if is_valid(sat_data.get('status')):
        context_lines.append(f"- Status: {sat_data.get('status')}")
    
    if is_valid(sat_data.get('launch_date')):
        context_lines.append(f"- Launch Date: {sat_data.get('launch_date')}")
    
    if is_valid(sat_data.get('country')):
        context_lines.append(f"- Country: {sat_data.get('country')}")
    
    if is_valid(sat_data.get('purpose')):
        context_lines.append(f"- Purpose: {sat_data.get('purpose')}")
    
    if is_valid(sat_data.get('mass')):
        context_lines.append(f"- Mass: {sat_data.get('mass')} kg")
    
    if context_lines:
        return "Satellite Data:\n" + "\n".join(context_lines)
    else:
        return "Limited satellite data available."
