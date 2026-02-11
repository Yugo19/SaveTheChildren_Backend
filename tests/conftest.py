import pytest
from motor.motor_asyncio import AsyncMotorClient
from app.config import settings
import asyncio


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture
async def test_db():
    """Create test database"""
    client = AsyncMotorClient(settings.DB_URI)
    db = client["stc-db-test"]
    yield db
    await client.drop_database("stc-db-test")


@pytest.fixture
def test_user():
    return {
        "username": "testuser",
        "email": "test@example.com",
        "password": "testpass123",
        "full_name": "Test User"
    }
