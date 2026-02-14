from fastapi import APIRouter, Depends, Query
from typing import Optional
from app.db.client import get_database
from app.services.analytics_service import AnalyticsService
from app.core.security import any_authenticated, TokenData
from app.core.logging import logger

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/dashboard")
async def get_dashboard_summary(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get dashboard summary with key metrics"""
    analytics_service = AnalyticsService(db)
    summary = await analytics_service.get_dashboard_summary(date_from, date_to)
    logger.info(f"Dashboard summary retrieved for user: {current_user.user_id}")
    return summary


@router.get("/county/{county}")
async def get_county_analysis(
    county: str,
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get detailed analysis for a specific county"""
    analytics_service = AnalyticsService(db)
    analysis = await analytics_service.get_county_analysis(county)
    logger.info(f"County analysis retrieved: {county}")
    return analysis


@router.get("/abuse-type/{abuse_type}")
async def get_abuse_type_analysis(
    abuse_type: str,
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get detailed analysis for a specific abuse type"""
    analytics_service = AnalyticsService(db)
    analysis = await analytics_service.get_abuse_type_analysis(abuse_type)
    logger.info(f"Abuse type analysis retrieved: {abuse_type}")
    return analysis


@router.get("/timeseries")
async def get_time_series(
    granularity: str = Query("monthly", enum=["daily", "weekly", "monthly"]),
    year: Optional[int] = None,
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get time series data for trend analysis"""
    analytics_service = AnalyticsService(db)
    data = await analytics_service.get_time_series_data(granularity, year)
    logger.info(f"Time series data retrieved: {granularity}")
    return data


@router.get("/severity-distribution")
async def get_severity_distribution(
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get severity distribution across all cases"""
    analytics_service = AnalyticsService(db)
    distribution = await analytics_service.get_severity_distribution()
    logger.info("Severity distribution retrieved")
    return {
        "distribution": distribution
    }
