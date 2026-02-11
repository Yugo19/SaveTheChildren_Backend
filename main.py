from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from app.api.v1.router import router as api_v1_router
from app.core.exceptions import setup_exception_handlers
from app.core.logging import logger
from app.db.client import mongodb_client
from app.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FastAPI application...")
    await mongodb_client.connect()
    logger.info("MongoDB connected successfully")
    
    # Start background tasks
    from app.tasks.scheduler import start_background_tasks
    start_background_tasks()
    logger.info("Background tasks started")
    
    yield
    
    logger.info("Shutting down FastAPI application...")
    await mongodb_client.disconnect()
    logger.info("MongoDB disconnected")


app = FastAPI(
    title=settings.API_TITLE,
    description="Child Protection Dashboard API",
    version=settings.API_VERSION,
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

setup_exception_handlers(app)

app.include_router(api_v1_router)


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "version": settings.API_VERSION
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="info"
    )
