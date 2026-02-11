from fastapi import APIRouter, Depends, Query, HTTPException, status
from pydantic import BaseModel
from typing import Optional
from app.db.client import get_database
from app.services.kenya_api_service import KenyaAPIService
from app.core.security import get_current_user, TokenData
from app.core.logging import logger

router = APIRouter(prefix="/kenya-api", tags=["Kenya API"])


class FetchKenyaDataRequest(BaseModel):
    county: Optional[str] = None
    sub_county: Optional[str] = None
    case_category: Optional[str] = None
    force_refresh: bool = False


@router.post("/fetch")
async def fetch_kenya_data(
    request: FetchKenyaDataRequest,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Fetch and import data from Kenya Child Protection API
    
    This endpoint:
    - Fetches data from https://data.childprotection.go.ke/api/v2/cld/
    - Caches the data for 24 hours
    - Automatically integrates with the case management system
    - Supports filtering by county, sub-county, and case category
    
    Data fields captured:
    - Case date, county, sub-county
    - Victim demographics (age, sex, age_range)
    - Abuse type (case_category)
    - Intervention status
    - Geographic coordinates (for mapping)
    """
    try:
        kenya_service = KenyaAPIService(db)
        
        filters = {}
        if request.county:
            filters["county"] = request.county
        if request.sub_county:
            filters["sub_county"] = request.sub_county
        if request.case_category:
            filters["case_category"] = request.case_category
        
        result = await kenya_service.fetch_and_store_data(
            filters=filters if filters else None,
            force_refresh=request.force_refresh
        )
        
        logger.info(f"Kenya API data fetched by user {current_user.user_id}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in fetch_kenya_data endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching Kenya API data"
        )


@router.get("/stats")
async def get_kenya_data_stats(
    group_by: str = Query("county", enum=["county", "sub_county", "case_category", "victim_sex", "age_range"]),
    county: Optional[str] = None,
    case_category: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Get aggregated statistics from Kenya API data
    
    Returns data grouped by the specified field (county, case_category, etc.)
    Optimized for frontend visualization and charts
    """
    try:
        kenya_service = KenyaAPIService(db)
        
        filters = {}
        if county:
            filters["county"] = county
        if case_category:
            filters["case_category"] = case_category
        
        result = await kenya_service.get_aggregated_data(
            group_by=group_by,
            filters=filters if filters else None
        )
        
        logger.info(f"Kenya API stats retrieved by user {current_user.user_id}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting Kenya API stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving statistics"
        )


@router.get("/status")
async def get_import_status(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Get status of the most recent Kenya API import
    
    Returns information about:
    - Last import timestamp
    - Number of records imported
    - Filters used in last import
    """
    try:
        kenya_service = KenyaAPIService(db)
        result = await kenya_service.get_latest_import_status()
        
        logger.info(f"Kenya API import status checked by user {current_user.user_id}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting import status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving import status"
        )


@router.get("/counties")
async def get_available_counties(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get list of counties available in Kenya API data"""
    try:
        kenya_service = KenyaAPIService(db)
        
        # Get distinct counties from imported data
        counties = await kenya_service.cases_collection.distinct(
            "county",
            {"source": "kenya_api"}
        )
        
        return {
            "counties": sorted([c for c in counties if c]),
            "total": len(counties)
        }
        
    except Exception as e:
        logger.error(f"Error getting counties: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving counties"
        )


@router.get("/case-categories")
async def get_available_categories(
    county: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get list of case categories available in Kenya API data"""
    try:
        kenya_service = KenyaAPIService(db)
        
        filters = {"source": "kenya_api"}
        if county:
            filters["county"] = county
        
        categories = await kenya_service.cases_collection.distinct(
            "case_category",
            filters
        )
        
        return {
            "case_categories": sorted([c for c in categories if c]),
            "total": len(categories),
            "filtered_by_county": county
        }
        
    except Exception as e:
        logger.error(f"Error getting case categories: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving case categories"
        )
