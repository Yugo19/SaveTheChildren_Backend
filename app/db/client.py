from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING
from app.core.logging import logger
from app.config import settings


class MongoDBClient:
    def __init__(self):
        self.client: AsyncIOMotorClient = None
        self.db: AsyncIOMotorDatabase = None

    async def connect(self):
        try:
            self.client = AsyncIOMotorClient(settings.DB_URI)
            self.db = self.client[settings.DB_NAME]
            await self.client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {settings.DB_NAME}")
            await self._create_indexes()
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def disconnect(self):
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")

    async def _create_indexes(self):
        """Create database indexes for performance"""
        try:
            await self.db.users.create_index("email", unique=True)
            await self.db.users.create_index("username", unique=True)

            await self.db.cases.create_index([("county", ASCENDING)])
            await self.db.cases.create_index([("abuse_type", ASCENDING)])
            await self.db.cases.create_index([("status", ASCENDING)])
            await self.db.cases.create_index([("date_reported", DESCENDING)])
            await self.db.cases.create_index([("created_at", DESCENDING)])

            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")


mongodb_client = MongoDBClient()


async def get_database():
    return mongodb_client.db
