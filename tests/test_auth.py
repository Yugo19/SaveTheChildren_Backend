import pytest
from app.services.auth_service import AuthService
from app.db.models import UserCreate, UserRole


@pytest.mark.asyncio
async def test_register_user(test_db, test_user):
    """Test user registration"""
    auth_service = AuthService(test_db)

    user_data = UserCreate(
        username=test_user["username"],
        email=test_user["email"],
        password=test_user["password"],
        full_name=test_user["full_name"],
        role=UserRole.VIEWER
    )

    user = await auth_service.register_user(user_data)

    assert user.username == test_user["username"]
    assert user.email == test_user["email"]
    assert user.is_active is True


@pytest.mark.asyncio
async def test_authenticate_user(test_db, test_user):
    """Test user authentication"""
    auth_service = AuthService(test_db)

    user_data = UserCreate(
        username=test_user["username"],
        email=test_user["email"],
        password=test_user["password"],
        full_name=test_user["full_name"],
        role=UserRole.VIEWER
    )

    await auth_service.register_user(user_data)

    access_token, refresh_token = await auth_service.authenticate_user(
        test_user["email"],
        test_user["password"]
    )

    assert access_token is not None
    assert refresh_token is not None
