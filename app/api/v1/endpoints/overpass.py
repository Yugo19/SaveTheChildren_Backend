from fastapi import APIRouter, Depends, Query
from typing import Optional
from app.db.client import get_database
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.services.overpass_service import OverpassService
from app.core.logging import logger

router = APIRouter(prefix="/overpass", tags=["Overpass API"])


@router.get("/counties")
async def get_counties(
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get list of all Kenya counties from database
    """
    service = OverpassService(db)
    counties = await service.get_counties_from_db()
    return {
        "total": len(counties),
        "counties": [c["name"] for c in counties]
    }


@router.get("/amenities")
async def get_amenities(
    county: Optional[str] = Query(None, description="County name to filter by"),
    type: Optional[str] = Query(None, description="Filter by type: 'police', 'ngo', or leave empty for both"),
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get child protection amenities (police stations and NGOs) from OpenStreetMap.
    
    - **county**: Optional county name to filter results
    - **type**: Optional filter - 'police' for police stations only, 'ngo' for NGOs only, or omit for both
    - Returns police stations and/or child protection facilities with locations
    """
    service = OverpassService(db)
    
    if type == "police":
        result = await service.get_police_stations(county)
        return {
            "county": county or "All Kenya",
            "police_stations": result,
            "child_protection_ngos": {"type": "child_protection_ngos", "count": 0, "amenities": []},
            "total_amenities": result["count"]
        }
    elif type == "ngo":
        result = await service.get_child_protection_ngos(county)
        return {
            "county": county or "All Kenya",
            "police_stations": {"type": "police_stations", "count": 0, "amenities": []},
            "child_protection_ngos": result,
            "total_amenities": result["count"]
        }
    else:
        return await service.get_all_amenities(county)


@router.get("/amenities-by-county")
async def get_amenities_by_all_counties(
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get child protection amenities grouped by all Kenya counties.
    
    This endpoint queries all counties and returns amenities grouped by county.
    Note: This may take longer as it queries multiple counties.
    """
    service = OverpassService(db)
    return await service.get_amenities_by_all_counties()


@router.get("/server-status")
async def check_server_status(
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Check status of all Overpass API mirror servers.
    
    Returns availability status for each server.
    """
    import aiohttp
    import asyncio
    
    service = OverpassService(db)
    results = []
    
    # Simple test query for a single node
    test_query = "[out:json][timeout:10];node(1);out;"
    
    async def check_server(server_url: str):
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            start_time = asyncio.get_event_loop().time()
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    server_url,
                    data={"data": test_query},
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                ) as response:
                    response_time = asyncio.get_event_loop().time() - start_time
                    
                    if response.status == 200:
                        return {
                            "server": server_url,
                            "status": "available",
                            "response_time_ms": round(response_time * 1000, 2),
                            "http_status": response.status
                        }
                    else:
                        return {
                            "server": server_url,
                            "status": "error",
                            "response_time_ms": round(response_time * 1000, 2),
                            "http_status": response.status
                        }
        except asyncio.TimeoutError:
            return {
                "server": server_url,
                "status": "timeout",
                "response_time_ms": None,
                "http_status": None
            }
        except Exception as e:
            return {
                "server": server_url,
                "status": "unavailable",
                "error": str(e),
                "response_time_ms": None,
                "http_status": None
            }
    
    # Check all servers in parallel
    tasks = [check_server(url) for url in service.OVERPASS_SERVERS]
    results = await asyncio.gather(*tasks)
    
    available_count = sum(1 for r in results if r["status"] == "available")
    
    return {
        "total_servers": len(service.OVERPASS_SERVERS),
        "available_servers": available_count,
        "servers": results
    }
