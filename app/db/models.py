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
    UNKNOWN = "unknown"


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
    case_date: datetime
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
    case_date: datetime
    county: str
    subcounty: Optional[str] = None
    child_age: Optional[int] = None
    age_range: Optional[str] = None
    child_sex: Optional[str] = None
    abuse_type: str
    description: Optional[str] = None
    intervention: Optional[str] = None
    status: CaseStatus
    severity: SeverityLevel
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        populate_by_name = True


class CaseUpdate(BaseModel):
    case_date: Optional[datetime] = None
    status: Optional[CaseStatus] = None
    severity: Optional[SeverityLevel] = None
    description: Optional[str] = None
