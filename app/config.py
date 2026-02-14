from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    # Application
    DEBUG: bool = False
    ENVIRONMENT: str = "production"
    API_TITLE: str = "Save The Children API"
    API_VERSION: str = "1.0.0"

    # Database
    DB_URI: str = "mongodb://localhost:27017"
    DB_NAME: str = "stc-db"
    
    # Performance settings
    ENABLE_QUERY_CACHE: bool = True
    QUERY_CACHE_TTL: int = 300  # 5 minutes
    MAX_PAGE_SIZE: int = 500
    DEFAULT_PAGE_SIZE: int = 50

    # JWT
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # Azure (Optional - replaced by Pinecone)
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = None
    AZURE_CONTAINER_NAME: str = "stc-container"

    # LLM APIs
    GROQ_API_KEY: str
    GOOGLE_API_KEY: Optional[str] = None  # Optional if using local embeddings
    OPENAI_API_KEY: Optional[str] = None
    
    # PostgreSQL Vector Database (Local)
    POSTGRES_URI: str
    
    # Pinecone Vector Database (Optional - for cloud deployment)
    PINECONE_API_KEY: Optional[str] = None
    PINECONE_INDEX: Optional[str] = None
    
    # Embedding Provider (google, local, huggingface, auto)
    EMBEDDING_PROVIDER: str = "auto"  # Auto-select with fallback
    
    # Redis & Celery
    REDIS_URL: Optional[str] = "redis://localhost:6379/0"
    CELERY_BROKER_URL: Optional[str] = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: Optional[str] = "redis://localhost:6379/1"

    # Security
    ALLOWED_ORIGINS: list = [
        "https://app.childprotection.com",
        "http://localhost:3000"
    ]
    BCRYPT_LOG_ROUNDS: int = 12

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/app.log"
    
    # Performance & Caching
    ENABLE_QUERY_CACHE: bool = True
    CACHE_TTL: int = 300  # 5 minutes
    MAX_PAGE_SIZE: int = 500
    DEFAULT_PAGE_SIZE: int = 50

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"


settings = Settings()
