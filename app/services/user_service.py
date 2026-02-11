from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from datetime import datetime, timezone
from app.db.models import UserRole
from app.core.logging import logger


class UserService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.users_collection = db.users

    async def get_user_by_id(self, user_id: str):
        """Get user by ID"""
        try:
            user = await self.users_collection.find_one({"_id": ObjectId(user_id)})
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )
            return user
        except Exception as e:
            logger.error(f"Error getting user: {e}")
            raise

    async def get_user_by_email(self, email: str):
        """Get user by email"""
        return await self.users_collection.find_one({"email": email})

    async def get_all_users(self, page: int = 1, limit: int = 20, role: Optional[str] = None):
        """Get all users with pagination"""
        filters = {}
        if role:
            filters["role"] = role

        total = await self.users_collection.count_documents(filters)

        users = await self.users_collection.find(filters)\
            .skip((page - 1) * limit)\
            .limit(limit)\
            .sort("created_at", -1)\
            .to_list(limit)

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "users": users
        }

    async def update_user_profile(self, user_id: str, update_data: dict):
        """Update user profile"""
        try:
            update_data["updated_at"] = datetime.now(timezone.utc)

            user = await self.users_collection.find_one_and_update(
                {"_id": ObjectId(user_id)},
                {"$set": update_data},
                return_document=True
            )

            if not user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )

            logger.info(f"User profile updated: {user_id}")
            return user
        except Exception as e:
            logger.error(f"Error updating user profile: {e}")
            raise

    async def update_user_role(self, user_id: str, new_role: str):
        """Update user role (admin only)"""
        try:
            if new_role not in [r.value for r in UserRole]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid role"
                )

            user = await self.users_collection.find_one_and_update(
                {"_id": ObjectId(user_id)},
                {
                    "$set": {
                        "role": new_role,
                        "updated_at": datetime.now(timezone.utc)
                    }
                },
                return_document=True
            )

            if not user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )

            logger.info(f"User role updated to {new_role}: {user_id}")
            return user
        except Exception as e:
            logger.error(f"Error updating user role: {e}")
            raise

    async def deactivate_user(self, user_id: str):
        """Deactivate user account"""
        try:
            result = await self.users_collection.find_one_and_update(
                {"_id": ObjectId(user_id)},
                {
                    "$set": {
                        "is_active": False,
                        "updated_at": datetime.now(timezone.utc)
                    }
                },
                return_document=True
            )

            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )

            logger.info(f"User deactivated: {user_id}")
            return result
        except Exception as e:
            logger.error(f"Error deactivating user: {e}")
            raise

    async def reactivate_user(self, user_id: str):
        """Reactivate user account"""
        try:
            result = await self.users_collection.find_one_and_update(
                {"_id": ObjectId(user_id)},
                {
                    "$set": {
                        "is_active": True,
                        "updated_at": datetime.now(timezone.utc)
                    }
                },
                return_document=True
            )

            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )

            logger.info(f"User reactivated: {user_id}")
            return result
        except Exception as e:
            logger.error(f"Error reactivating user: {e}")
            raise
