from fastapi import APIRouter, Depends, Query, HTTPException, status
from pydantic import BaseModel, HttpUrl
from typing import Optional, Dict
from app.db.client import get_database
from app.services.scraping_service import ScrapingService
from app.core.security import get_current_user, TokenData
from app.core.logging import logger

router = APIRouter(prefix="/scraping", tags=["Scraping"])


class CreateScrapingJobRequest(BaseModel):
    url: str
    selectors: Dict[str, str]
    job_name: Optional[str] = None
    schedule: Optional[str] = None  # manual, hourly, daily, weekly, monthly
    target_type: Optional[str] = "general"  # general, news, child_violence_indicators


@router.post("/jobs")
async def create_scraping_job(
    request: CreateScrapingJobRequest,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Create a new web scraping job
    
    Request body:
    {
        "url": "https://example.com",
        "selectors": {
            "title": "h1",
            "paragraphs": "p"
        },
        "job_name": "Example Scraper",
        "schedule": "manual"
    }
    """
    scraping_service = ScrapingService(db)
    result = await scraping_service.create_scraping_job(
        request.url,
        request.selectors,
        current_user.user_id,
        request.job_name,
        request.schedule,
        request.target_type or "general"
    )
    logger.info(f"Scraping job created by {current_user.user_id}: {request.url}")
    return result


@router.get("/jobs")
async def list_scraping_jobs(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """List user's web scraping jobs"""
    scraping_service = ScrapingService(db)
    result = await scraping_service.list_scraping_jobs(current_user.user_id, page, limit)
    logger.info(f"Scraping jobs listed for {current_user.user_id}")
    return result


@router.post("/jobs/{job_id}/run")
async def run_scraping_job(
    job_id: str,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Execute a scraping job now
    
    Returns the scraped data immediately
    """
    scraping_service = ScrapingService(db)
    result = await scraping_service.run_scraping_job(job_id, current_user.user_id)
    logger.info(f"Scraping job executed by {current_user.user_id}: {job_id}")
    return result


@router.get("/jobs/{job_id}/results")
async def get_scraping_results(
    job_id: str,
    limit: int = Query(50, ge=1, le=500),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get historical results for a scraping job
    
    Returns last N scraping results for this job
    """
    scraping_service = ScrapingService(db)
    result = await scraping_service.get_scraping_results(job_id, current_user.user_id, limit)
    logger.info(f"Scraping results retrieved for {current_user.user_id}")
    return result


@router.delete("/jobs/{job_id}")
async def delete_scraping_job(
    job_id: str,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Delete scraping job and all its results"""
    scraping_service = ScrapingService(db)
    await scraping_service.delete_scraping_job(job_id, current_user.user_id)
    logger.info(f"Scraping job deleted by {current_user.user_id}: {job_id}")
    return {"message": "Scraping job and its results deleted successfully"}


@router.put("/jobs/{job_id}/toggle")
async def toggle_scraping_job(
    job_id: str,
    enabled: bool = Query(..., description="Enable or disable the job"),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Enable or disable a scraping job
    
    When disabled, scheduled jobs will not run automatically
    """
    scraping_service = ScrapingService(db)
    result = await scraping_service.toggle_job(job_id, current_user.user_id, enabled)
    logger.info(f"Scraping job {job_id} toggled by {current_user.user_id}: enabled={enabled}")
    return result


@router.get("/stats")
async def get_scraping_statistics(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get comprehensive scraping statistics
    
    Returns:
    - Total jobs created
    - Active jobs
    - Total runs, successes, errors
    - Success rate
    """
    scraping_service = ScrapingService(db)
    result = await scraping_service.get_scraping_stats(current_user.user_id)
    logger.info(f"Scraping statistics retrieved by {current_user.user_id}")
    return result
