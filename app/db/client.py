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
            # Configure connection with pooling and performance settings
            self.client = AsyncIOMotorClient(
                settings.DB_URI,
                maxPoolSize=50,  # Max connections in pool
                minPoolSize=10,  # Min connections to maintain
                maxIdleTimeMS=45000,  # Close idle connections after 45s
                waitQueueTimeoutMS=10000,  # Wait up to 10s for connection from pool
                serverSelectionTimeoutMS=5000,  # Server selection timeout
                connectTimeoutMS=10000,  # Connection timeout
                socketTimeoutMS=20000,  # Socket timeout
                retryWrites=True,  # Retry write operations
                retryReads=True,  # Retry read operations
                maxConnecting=5,  # Max simultaneous connection attempts
            )
            self.db = self.client[settings.DB_NAME]
            await self.client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {settings.DB_NAME} (pool: 10-50)")
            await self._create_indexes()
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def disconnect(self):
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")

    async def _create_indexes(self):
        """Create database indexes for performance - optimized set"""
        try:
            # User indexes
            await self.db.users.create_index("email", unique=True)
            await self.db.users.create_index("username", unique=True)

            # Essential single field indexes for cases
            await self.db.cases.create_index([("county", ASCENDING)], background=True)
            await self.db.cases.create_index([("abuse_type", ASCENDING)], background=True)
            await self.db.cases.create_index([("status", ASCENDING)], background=True)
            await self.db.cases.create_index([("case_date", DESCENDING)], background=True)
            await self.db.cases.create_index([("source", ASCENDING)], background=True)
            await self.db.cases.create_index([("sex", ASCENDING)], background=True)
            await self.db.cases.create_index([("age_range", ASCENDING)], background=True)
            
            # Compound indexes for common filter combinations
            await self.db.cases.create_index([("county", ASCENDING), ("case_date", DESCENDING)], background=True)
            await self.db.cases.create_index([("status", ASCENDING), ("case_date", DESCENDING)], background=True)
            await self.db.cases.create_index([("abuse_type", ASCENDING), ("case_date", DESCENDING)], background=True)
            await self.db.cases.create_index([("county", ASCENDING), ("abuse_type", ASCENDING), ("case_date", DESCENDING)], background=True)
            
            # Demographics compound index
            await self.db.cases.create_index([("county", ASCENDING), ("sex", ASCENDING), ("age_range", ASCENDING)], background=True)
            
            # Sparse index for case_id (not all docs have it)
            await self.db.cases.create_index([("case_id", ASCENDING)], sparse=True, background=True)

            logger.info("Database indexes ensured (background mode)")
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")


mongodb_client = MongoDBClient()


async def get_database():
    return mongodb_client.db
