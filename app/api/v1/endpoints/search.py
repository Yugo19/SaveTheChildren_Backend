from fastapi import APIRouter, Depends, Query
from typing import Optional
from app.db.client import get_database
from app.core.security import get_current_user, TokenData
from app.core.logging import logger
from bson import ObjectId

router = APIRouter(prefix="/search", tags=["Search"])


@router.get("/cases")
async def search_cases(
    q: str = Query(..., min_length=1),
    county: Optional[str] = None,
    abuse_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Search cases by various criteria"""
    try:
        filters = {
            "$or": [
                {"case_id": {"$regex": q, "$options": "i"}},
                {"description": {"$regex": q, "$options": "i"}},
                {"county": {"$regex": q, "$options": "i"}},
                {"subcounty": {"$regex": q, "$options": "i"}}
            ]
        }

        # Add additional filters
        if county:
            filters["county"] = county
        if abuse_type:
            filters["abuse_type"] = abuse_type
        if status:
            filters["status"] = status

        cases = await db.cases.find(filters)\
            .limit(limit)\
            .sort("created_at", -1)\
            .to_list(limit)

        logger.info(f"Case search performed by {current_user.user_id}: {q}")

        return {
            "query": q,
            "total": len(cases),
            "results": [
                {
                    "id": str(c["_id"]),
                    "case_id": c.get("case_id"),
                    "county": c.get("county"),
                    "abuse_type": c.get("abuse_type"),
                    "severity": c.get("severity"),
                    "status": c.get("status"),
                    "created_at": c.get("created_at")
                }
                for c in cases
            ]
        }
    except Exception as e:
        logger.error(f"Error searching cases: {e}")
        raise


@router.get("/users")
async def search_users(
    q: str = Query(..., min_length=1),
    role: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Search users (admin only or for self)"""
    try:
        filters = {
            "$or": [
                {"username": {"$regex": q, "$options": "i"}},
                {"email": {"$regex": q, "$options": "i"}},
                {"full_name": {"$regex": q, "$options": "i"}}
            ]
        }

        if role:
            filters["role"] = role

        # Non-admin users can only see their own info
        if current_user.role != "admin":
            filters["_id"] = ObjectId(current_user.user_id)

        users = await db.users.find(filters)\
            .limit(limit)\
            .to_list(limit)

        logger.info(f"User search performed by {current_user.user_id}: {q}")

        return {
            "query": q,
            "total": len(users),
            "results": [
                {
                    "id": str(u["_id"]),
                    "username": u.get("username"),
                    "email": u.get("email"),
                    "full_name": u.get("full_name"),
                    "role": u.get("role")
                }
                for u in users
            ]
        }
    except Exception as e:
        logger.error(f"Error searching users: {e}")
        raise
