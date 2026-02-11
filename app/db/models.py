from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class UserRole(str, Enum):
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


class CaseStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    PENDING = "pending"


class SeverityLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# User Schemas
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8)
    full_name: str
    role: UserRole = UserRole.VIEWER


class UserResponse(BaseModel):
    id: str = Field(alias="_id")
    username: str
    email: str
    full_name: str
    role: UserRole
    is_active: bool
    created_at: datetime

    class Config:
        populate_by_name = True


# Case Schemas
class CaseCreate(BaseModel):
    case_id: str
    date_reported: datetime
    county: str
    subcounty: str
    child_age: int = Field(..., ge=0, le=18)
    child_sex: str
    abuse_type: str
    description: str
    severity: SeverityLevel
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class CaseResponse(BaseModel):
    id: str = Field(alias="_id")
    case_id: str
    date_reported: datetime
    county: str
    abuse_type: str
    status: CaseStatus
    severity: SeverityLevel
    created_at: datetime
    updated_at: datetime

    class Config:
        populate_by_name = True


class CaseUpdate(BaseModel):
    status: Optional[CaseStatus] = None
    severity: Optional[SeverityLevel] = None
    description: Optional[str] = None
