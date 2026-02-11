from fastapi import APIRouter, HTTPException, status, Depends, Query
from bson import ObjectId
from typing import Optional
from pydantic import BaseModel
from app.db.client import get_database
from app.db.models import UserResponse
from app.services.user_service import UserService
from app.core.security import get_current_user, require_role, TokenData
from app.core.logging import logger

router = APIRouter(prefix="/users", tags=["Users"])


class UpdateUserRequest(BaseModel):
    full_name: Optional[str] = None
    preferences: Optional[dict] = None


class UpdateRoleRequest(BaseModel):
    role: str


@router.get("", response_model=dict)
async def list_users(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    role: Optional[str] = None,
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """List all users (admin only)"""
    user_service = UserService(db)
    result = await user_service.get_all_users(page=page, limit=limit, role=role)

    return {
        "total": result["total"],
        "page": result["page"],
        "limit": result["limit"],
        "users": [
            UserResponse(**{**u, "_id": str(u["_id"])})
            for u in result["users"]
        ]
    }


@router.get("/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: str,
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Get user by ID (admin only)"""
    try:
        user_service = UserService(db)
        user = await user_service.get_user_by_id(user_id)
        return UserResponse(**{**user, "_id": str(user["_id"])})
    except Exception as e:
        logger.error(f"Error getting user: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )


@router.put("/me", response_model=UserResponse)
async def update_own_profile(
    update_data: UpdateUserRequest,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Update own user profile"""
    user_service = UserService(db)

    update_dict = update_data.dict(exclude_unset=True)

    user = await user_service.update_user_profile(current_user.user_id, update_dict)
    logger.info(f"User updated own profile: {current_user.user_id}")

    return UserResponse(**{**user, "_id": str(user["_id"])})


@router.put("/{user_id}/role", response_model=UserResponse)
async def update_user_role(
    user_id: str,
    role_data: UpdateRoleRequest,
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Update user role (admin only)"""
    user_service = UserService(db)
    user = await user_service.update_user_role(user_id, role_data.role)

    logger.info(f"User role updated by {current_user.user_id}")
    return UserResponse(**{**user, "_id": str(user["_id"])})


@router.delete("/{user_id}")
async def deactivate_user(
    user_id: str,
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Deactivate user account (admin only)"""
    user_service = UserService(db)
    await user_service.deactivate_user(user_id)

    logger.info(f"User deactivated by {current_user.user_id}")
    return {"message": "User deactivated successfully"}


@router.post("/{user_id}/reactivate")
async def reactivate_user(
    user_id: str,
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Reactivate user account (admin only)"""
    user_service = UserService(db)
    await user_service.reactivate_user(user_id)

    logger.info(f"User reactivated by {current_user.user_id}")
    return {"message": "User reactivated successfully"}
