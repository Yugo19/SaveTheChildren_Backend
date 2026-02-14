from typing import List, Dict, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
import aiohttp
import asyncio
from app.core.logging import logger
from fastapi import HTTPException, status


class OverpassService:
    """Service to query OpenStreetMap data via Overpass API for child protection amenities"""
    
    # Multiple Overpass API mirror servers for redundancy
    OVERPASS_SERVERS = [
        "https://overpass-api.de/api/interpreter",           # Main server (Germany)
        "https://overpass.kumi.systems/api/interpreter",     # Switzerland
        "https://overpass.openstreetmap.ru/api/interpreter", # Russia
        "https://overpass.openstreetmap.fr/api/interpreter", # France
    ]
    
    # Kenya bounding box coordinates [south, west, north, east]
    KENYA_BBOX = [-4.67, 33.83, 5.51, 41.86]
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.cases_collection = db.cases
        self.current_server_index = 0
    
    async def get_counties_from_db(self) -> List[Dict]:
        """Fetch unique counties from database with their geographic centers"""
        try:
            pipeline = [
                {
                    "$match": {
                        "county": {"$exists": True, "$ne": None},
                        "latitude": {"$exists": True, "$ne": None},
                        "longitude": {"$exists": True, "$ne": None}
                    }
                },
                {
                    "$group": {
                        "_id": "$county",
                        "center_lat": {"$avg": "$latitude"},
                        "center_lon": {"$avg": "$longitude"},
                        "min_lat": {"$min": "$latitude"},
                        "max_lat": {"$max": "$latitude"},
                        "min_lon": {"$min": "$longitude"},
                        "max_lon": {"$max": "$longitude"}
                    }
                }
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            
            counties = []
            for r in results:
                county_name = r["_id"]
                # Create bounding box with some padding
                padding = 0.2  # ~22km
                counties.append({
                    "name": county_name,
                    "bbox": [
                        r["min_lat"] - padding,
                        r["min_lon"] - padding,
                        r["max_lat"] + padding,
                        r["max_lon"] + padding
                    ],
                    "center": [r["center_lat"], r["center_lon"]]
                })
            
            logger.info(f"Fetched {len(counties)} counties from database")
            return counties
            
        except Exception as e:
            logger.error(f"Error fetching counties from DB: {str(e)}")
            raise
    
    async def query_overpass(self, query: str) -> Dict:
        """Execute an Overpass API query with automatic failover to mirror servers"""
        last_error = None
        
        # Try each server in sequence
        for attempt, server_url in enumerate(self.OVERPASS_SERVERS):
            try:
                logger.info(f"Querying Overpass API server: {server_url} (attempt {attempt + 1}/{len(self.OVERPASS_SERVERS)})")
                
                timeout = aiohttp.ClientTimeout(total=60)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(
                        server_url,
                        data={"data": query},
                        headers={"Content-Type": "application/x-www-form-urlencoded"}
                    ) as response:
                        if response.status == 200:
                            logger.info(f"Successfully fetched data from {server_url}")
                            return await response.json()
                        else:
                            error_text = await response.text()
                            logger.warning(f"Server {server_url} returned {response.status}: {error_text[:200]}")
                            last_error = f"Server error: {response.status}"
                            # Try next server
                            continue
                            
            except asyncio.TimeoutError:
                logger.warning(f"Timeout querying {server_url}, trying next server...")
                last_error = "Request timed out"
                continue
                
            except aiohttp.ClientError as e:
                logger.warning(f"Connection error to {server_url}: {str(e)}, trying next server...")
                last_error = f"Connection error: {str(e)}"
                continue
                
            except Exception as e:
                logger.warning(f"Error with {server_url}: {str(e)}, trying next server...")
                last_error = str(e)
                continue
        
        # All servers failed
        logger.error(f"All Overpass API servers failed. Last error: {last_error}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"All Overpass API servers are unavailable. Last error: {last_error}"
        )
    
    def build_police_query(self, bbox: List[float]) -> str:
        """Build Overpass QL query for police stations"""
        south, west, north, east = bbox
        query = f"""[out:json][timeout:60];
(
  node["amenity"="police"]({south},{west},{north},{east});
  way["amenity"="police"]({south},{west},{north},{east});
);
out center;
"""
        return query
    
    def build_ngo_query(self, bbox: List[float]) -> str:
        """Build Overpass QL query for NGOs and child protection facilities"""
        south, west, north, east = bbox
        query = f"""[out:json][timeout:60];
(
  node["amenity"="social_facility"]({south},{west},{north},{east});
  way["amenity"="social_facility"]({south},{west},{north},{east});
  node["office"="ngo"]({south},{west},{north},{east});
  way["office"="ngo"]({south},{west},{north},{east});
  node["amenity"="community_centre"]({south},{west},{north},{east});
  way["amenity"="community_centre"]({south},{west},{north},{east});
  node["social_facility"="outreach"]({south},{west},{north},{east});
  way["social_facility"="outreach"]({south},{west},{north},{east});
);
out center;
"""
        return query
    
    async def get_police_stations(self, county: Optional[str] = None) -> Dict:
        """Get all police stations in Kenya or a specific county"""
        counties = await self.get_counties_from_db()
        
        if county:
            county_data = next((c for c in counties if c["name"].lower() == county.lower()), None)
            if not county_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"County '{county}' not found in database"
                )
            bbox = county_data["bbox"]
        else:
            bbox = self.KENYA_BBOX
        
        query = self.build_police_query(bbox)
        result = await self.query_overpass(query)
        return self._format_response(result, "police_stations", county)
    
    async def get_child_protection_ngos(self, county: Optional[str] = None) -> Dict:
        """Get NGOs working for child protection in Kenya or a specific county"""
        counties = await self.get_counties_from_db()
        
        if county:
            county_data = next((c for c in counties if c["name"].lower() == county.lower()), None)
            if not county_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"County '{county}' not found in database"
                )
            bbox = county_data["bbox"]
        else:
            bbox = self.KENYA_BBOX
        
        query = self.build_ngo_query(bbox)
        result = await self.query_overpass(query)
        return self._format_response(result, "child_protection_ngos", county)
    
    async def get_all_amenities(self, county: Optional[str] = None) -> Dict:
        """Get both police stations and NGOs in Kenya or a specific county"""
        police_task = self.get_police_stations(county)
        ngos_task = self.get_child_protection_ngos(county)
        
        police_result, ngos_result = await asyncio.gather(police_task, ngos_task)
        
        return {
            "county": county or "All Kenya",
            "police_stations": police_result,
            "child_protection_ngos": ngos_result,
            "total_amenities": police_result["count"] + ngos_result["count"]
        }
    
    async def get_amenities_by_all_counties(self) -> Dict:
        """Get child protection amenities grouped by all Kenya counties"""
        counties = await self.get_counties_from_db()
        
        results = {}
        tasks = []
        
        for county_data in counties:
            tasks.append(self._get_county_amenities(county_data["name"]))
        
        county_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for county_data, result in zip(counties, county_results):
            county_name = county_data["name"]
            if isinstance(result, Exception):
                logger.error(f"Error fetching data for {county_name}: {str(result)}")
                results[county_name] = {"error": str(result)}
            else:
                results[county_name] = result
        
        return {
            "country": "Kenya",
            "total_counties": len(counties),
            "counties": results
        }
    
    async def _get_county_amenities(self, county_name: str) -> Dict:
        """Helper to get amenities for a single county"""
        try:
            return await self.get_all_amenities(county_name)
        except Exception as e:
            logger.error(f"Error fetching amenities for {county_name}: {str(e)}")
            return {
                "error": str(e),
                "county": county_name,
                "police_stations": {"count": 0, "amenities": []},
                "child_protection_ngos": {"count": 0, "amenities": []},
                "total_amenities": 0
            }
    
    def _format_response(self, overpass_result: Dict, amenity_type: str, county: Optional[str]) -> Dict:
        """Format Overpass API response into structured format"""
        elements = overpass_result.get("elements", [])
        
        formatted_amenities = []
        for element in elements:
            tags = element.get("tags", {})
            
            # Get coordinates
            if element["type"] == "node":
                lat = element.get("lat")
                lon = element.get("lon")
            elif element["type"] == "way" and "center" in element:
                lat = element["center"].get("lat")
                lon = element["center"].get("lon")
            else:
                lat, lon = None, None
            
            amenity = {
                "id": element.get("id"),
                "type": element.get("type"),
                "name": tags.get("name", "Unnamed"),
                "amenity": tags.get("amenity") or tags.get("office") or tags.get("social_facility"),
                "latitude": lat,
                "longitude": lon,
                "address": {
                    "street": tags.get("addr:street"),
                    "city": tags.get("addr:city"),
                    "county": tags.get("addr:county") or county,
                    "postcode": tags.get("addr:postcode")
                },
                "contact": {
                    "phone": tags.get("phone"),
                    "email": tags.get("email"),
                    "website": tags.get("website")
                },
                "additional_info": {
                    "operator": tags.get("operator"),
                    "description": tags.get("description"),
                    "opening_hours": tags.get("opening_hours"),
                    "social_facility": tags.get("social_facility")
                }
            }
            formatted_amenities.append(amenity)
        
        return {
            "type": amenity_type,
            "county": county or "All Kenya",
            "count": len(formatted_amenities),
            "amenities": formatted_amenities
        }
