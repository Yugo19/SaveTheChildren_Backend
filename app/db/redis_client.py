import redis.asyncio as redis
from app.config import settings
from app.core.logging import logger


class RedisClient:
    def __init__(self):
        self.client = None

    async def connect(self):
        try:
            self.client = await redis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
            await self.client.ping()
            logger.info(f"Connected to Redis: {settings.REDIS_URL}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self):
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Redis")


redis_client = RedisClient()


def get_redis():
    """Get Redis client instance"""
    return redis_client.client
