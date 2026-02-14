from fastapi import APIRouter, Depends, Query
from typing import Optional
from app.db.client import get_database
from app.services.geospatial_service import GeospatialService
from app.core.security import any_authenticated, TokenData
from app.core.logging import logger

router = APIRouter(prefix="/geospatial", tags=["Geospatial"])


@router.get("/nearby")
async def get_nearby_cases(
    latitude: float = Query(...),
    longitude: float = Query(...),
    radius_km: float = Query(10, ge=1, le=100),
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get cases near a specific location"""
    geospatial_service = GeospatialService(db)
    result = await geospatial_service.get_nearby_cases(latitude, longitude, radius_km)
    logger.info(f"Nearby cases retrieved for {current_user.user_id}")
    return result


@router.get("/hotspots")
async def get_hotspots(
    radius_km: float = Query(5, ge=1, le=50),
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get case hotspots/clusters"""
    geospatial_service = GeospatialService(db)
    result = await geospatial_service.get_hotspots(radius_km)
    logger.info(f"Hotspots retrieved for {current_user.user_id}")
    return result


@router.get("/counties")
async def get_county_boundaries(
    source: Optional[str] = Query(None, description="Filter by data source (e.g., 'kenya_api')"),
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get county statistics with geographic info. Supports Kenya API data via source='kenya_api'"""
    geospatial_service = GeospatialService(db)
    result = await geospatial_service.get_county_boundaries(source)
    logger.info(f"County boundaries retrieved for {current_user.user_id}")
    return result


@router.get("/heatmap")
async def get_heatmap_data(
    county: Optional[str] = None,
    abuse_type: Optional[str] = None,
    source: Optional[str] = Query(None, description="Filter by data source (e.g., 'kenya_api')"),
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get heatmap data for visualization. Supports Kenya API data via source='kenya_api'"""
    geospatial_service = GeospatialService(db)
    result = await geospatial_service.get_heatmap_data(county, abuse_type, source)
    logger.info(f"Heatmap data retrieved for {current_user.user_id}")
    return result


@router.get("/density")
async def get_case_density(
    zoom_level: int = Query(10, ge=1, le=20),
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get case density grid"""
    geospatial_service = GeospatialService(db)
    result = await geospatial_service.get_case_density(zoom_level)
    logger.info(f"Case density retrieved for {current_user.user_id}")
    return result


@router.get("/map-data")
async def get_map_data(
    county: Optional[str] = None,
    abuse_type: Optional[str] = None,
    year: Optional[int] = None,
    source: Optional[str] = Query(None, description="Filter by data source (e.g., 'kenya_api')"),
    format: str = Query("geojson", description="Output format: 'geojson' or 'simple'"),
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """
    Get map data in GeoJSON format for visualization.
    
    Supports filtering by:
    - county: Filter by specific county
    - abuse_type: Filter by abuse category
    - year: Filter by year
    - source: Filter by data source (e.g., 'kenya_api' for Kenya data)
    
    Returns GeoJSON FeatureCollection ready for Leaflet, Mapbox, Google Maps.
    """
    geospatial_service = GeospatialService(db)
    result = await geospatial_service.get_map_data(county, abuse_type, year, source, format)
    logger.info(f"Map data retrieved for {current_user.user_id}")
    return result
