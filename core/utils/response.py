"""
Standard API Response Utilities
"""
from typing import Any, Optional, Dict
from datetime import datetime


def success_response(
    data: Any = None,
    message: str = "Success",
    meta: Optional[Dict] = None
) -> Dict:
    """
    Standard success response format
    
    Args:
        data: Response data
        message: Success message
        meta: Additional metadata
        
    Returns:
        Formatted response dict
    """
    response = {
        "success": True,
        "message": message,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    if data is not None:
        response["data"] = data
    
    if meta:
        response["meta"] = meta
        
    return response


def error_response(
    error: str,
    detail: Optional[str] = None,
    code: Optional[str] = None
) -> Dict:
    """
    Standard error response format
    
    Args:
        error: Error message
        detail: Detailed error description
        code: Error code
        
    Returns:
        Formatted error dict
    """
    response = {
        "success": False,
        "error": error,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    if detail:
        response["detail"] = detail
    
    if code:
        response["code"] = code
        
    return response


def paginated_response(
    data: list,
    page: int = 1,
    page_size: int = 10,
    total: Optional[int] = None
) -> Dict:
    """
    Paginated response format
    
    Args:
        data: List of items
        page: Current page number
        page_size: Items per page
        total: Total number of items
        
    Returns:
        Paginated response dict
    """
    total_items = total if total is not None else len(data)
    total_pages = (total_items + page_size - 1) // page_size
    
    return success_response(
        data=data,
        meta={
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total_items,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
        }
    )
