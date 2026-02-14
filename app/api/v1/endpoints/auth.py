from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from datetime import datetime
from bson import ObjectId
from app.db.client import get_database
from app.db.models import UserCreate, UserResponse
from app.services.auth_service import AuthService
from app.core.security import get_current_user, verify_refresh_token, create_access_token, create_refresh_token, TokenData
from app.core.logging import logger

router = APIRouter(prefix="/auth", tags=["Authentication"])


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshTokenRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserResponse


class AccessTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


@router.post("/register", response_model=UserResponse, status_code=201)
async def register(user_data: UserCreate, db=Depends(get_database)):
    """Register a new user"""
    auth_service = AuthService(db)
    return await auth_service.register_user(user_data)


@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest, db=Depends(get_database)):
    """
    Authenticate user and return access + refresh tokens
    
    Returns:
    - access_token: Short-lived token for API requests (default: 30 minutes)
    - refresh_token: Long-lived token for getting new access tokens (default: 7 days)
    - user: User profile information
    """
    auth_service = AuthService(db)
    access_token, refresh_token = await auth_service.authenticate_user(
        request.email,
        request.password
    )

    user = await db.users.find_one({"email": request.email})
    user_response = UserResponse(**{**user, "_id": str(user["_id"])})

    logger.info(f"User logged in: {request.email}")
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        user=user_response
    )


@router.post("/refresh", response_model=AccessTokenResponse)
async def refresh_access_token(
    request: RefreshTokenRequest,
    db=Depends(get_database)
):
    """
    Get a new access token using a refresh token
    
    Use this endpoint when the access token expires.
    The refresh token remains valid for longer (default: 7 days).
    
    Request body:
    - refresh_token: The refresh token received during login
    
    Returns:
    - access_token: New access token
    - token_type: "bearer"
    """
    # Verify refresh token
    token_data = verify_refresh_token(request.refresh_token)
    
    # Verify user still exists and is active
    user = await db.users.find_one({"_id": ObjectId(token_data.user_id)})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or deactivated"
        )
    
    # Create new access token with same user data
    new_access_token = create_access_token(
        data={
            "sub": str(user["_id"]),
            "email": user["email"],
            "role": user["role"]
        }
    )
    
    logger.info(f"Access token refreshed for user: {user['email']}")
    
    return AccessTokenResponse(
        access_token=new_access_token
    )


@router.get("/me", response_model=UserResponse)
async def get_current_user_profile(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get current user profile"""
    user = await db.users.find_one({"_id": ObjectId(current_user.user_id)})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return UserResponse(**{**user, "_id": str(user["_id"])})


@router.post("/logout")
async def logout(current_user: TokenData = Depends(get_current_user)):
    """Logout user"""
    logger.info(f"User logged out: {current_user.email}")
    return {"message": "Successfully logged out"}


@router.post("/change-password")
async def change_password(
    old_password: str,
    new_password: str,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Change user password"""
    auth_service = AuthService(db)
    await auth_service.change_password(current_user.user_id, old_password, new_password)
    return {"message": "Password changed successfully"}
