from typing import Dict, Tuple, Optional
from datetime import datetime, timedelta, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from app.core.security import (
    hash_password, verify_password, create_access_token,
    create_refresh_token, verify_token
)
from app.db.models import UserCreate, UserResponse
from app.core.logging import logger


class AuthService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.users_collection = db.users

    async def register_user(self, user_data: UserCreate) -> UserResponse:
        """Register a new user"""
        existing_user = await self.users_collection.find_one({
            "$or": [
                {"email": user_data.email},
                {"username": user_data.username}
            ]
        })

        if existing_user:
            logger.warning(f"Registration attempt with existing email: {user_data.email}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User already exists"
            )

        hashed_password = hash_password(user_data.password)

        user_doc = {
            "username": user_data.username,
            "email": user_data.email,
            "full_name": user_data.full_name,
            "password_hash": hashed_password,
            "role": user_data.role.value,
            "is_active": True,
            "preferences": {
                "theme": "dark",
                "notifications": True
            },
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "last_login": None
        }

        result = await self.users_collection.insert_one(user_doc)
        logger.info(f"New user registered: {user_data.email}")

        user_doc["_id"] = str(result.inserted_id)
        return UserResponse(**user_doc)

    async def authenticate_user(self, email: str, password: str) -> Tuple[str, str]:
        """Authenticate user and return tokens"""
        user = await self.users_collection.find_one({"email": email})

        if not user or not verify_password(password, user["password_hash"]):
            logger.warning(f"Failed login attempt for: {email}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password"
            )

        if not user["is_active"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is inactive"
            )

        await self.users_collection.update_one(
            {"_id": user["_id"]},
            {"$set": {"last_login": datetime.now(timezone.utc)}}
        )

        access_token = create_access_token({
            "sub": str(user["_id"]),
            "role": user["role"],
            "email": user["email"]
        })

        refresh_token = create_refresh_token({
            "sub": str(user["_id"]),
            "role": user["role"]
        })

        logger.info(f"User authenticated: {email}")
        return access_token, refresh_token

    async def change_password(self, user_id: str, old_password: str, new_password: str):
        """Change user password"""
        user = await self.users_collection.find_one({"_id": ObjectId(user_id)})

        if not user or not verify_password(old_password, user["password_hash"]):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Old password is incorrect"
            )

        hashed_new_password = hash_password(new_password)
        await self.users_collection.update_one(
            {"_id": user["_id"]},
            {"$set": {"password_hash": hashed_new_password}}
        )

        logger.info(f"Password changed for user: {user_id}")
