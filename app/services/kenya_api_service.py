from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from datetime import datetime, timezone
from app.core.logging import logger
from typing import Optional, Dict, List
import aiohttp
import asyncio
from app.services.geocoding_service import GeocodingService


class KenyaAPIService:
    """Service to fetch and process data from Kenya Child Protection API"""
    
    BASE_URL = "https://data.childprotection.go.ke:8040/api/v2/cld/$"
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.kenya_data_collection = db.kenya_api_data
        self.cases_collection = db.cases
        self.geocoding_service = GeocodingService()
        
    async def fetch_and_store_data(
        self,
        filters: Optional[Dict] = None,
        force_refresh: bool = False
    ) -> dict:
        """Fetch data from Kenya API and store in database"""
        try:
            # Check if we have recent data (less than 24 hours old)
            if not force_refresh:
                recent_data = await self.kenya_data_collection.find_one(
                    {"fetched_at": {"$gte": datetime.now(timezone.utc).replace(hour=0, minute=0, second=0)}}
                )
                if recent_data:
                    logger.info("Using cached Kenya API data from today")
                    return {
                        "status": "cached",
                        "records": recent_data.get("record_count", 0),
                        "fetched_at": recent_data["fetched_at"]
                    }
            
            # Fetch fresh data from API
            logger.info("Fetching fresh data from Kenya Child Protection API")
            data = await self._fetch_api_data(filters)
            
            if not data:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Kenya Child Protection API is currently unavailable or experiencing issues. The API may be down for maintenance or encountering server errors. Please try again later."
                )
            
            # Store raw data
            stored_doc = {
                "raw_data": data,
                "fetched_at": datetime.now(timezone.utc),
                "record_count": len(data) if isinstance(data, list) else 1,
                "filters": filters or {}
            }
            
            await self.kenya_data_collection.insert_one(stored_doc)
            
            # Process and integrate with cases if records exist
            if isinstance(data, list) and len(data) > 0:
                await self._integrate_with_cases(data)
            
            logger.info(f"Successfully fetched and stored {stored_doc['record_count']} records from Kenya API")
            
            return {
                "status": "success",
                "records": stored_doc["record_count"],
                "fetched_at": stored_doc["fetched_at"]
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error fetching Kenya API data: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error fetching data: {str(e)}"
            )
    
    async def _fetch_api_data(self, filters: Optional[Dict] = None) -> List[Dict]:
        """Fetch data from the Kenya API (no authentication required)"""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Build request headers to mimic browser (avoid bot detection)
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": "https://data.childprotection.go.ke/"
                }
                
                timeout = aiohttp.ClientTimeout(
                    total=300,  # 5 minutes total timeout
                    connect=60,  # 1 minute for establishing connection
                    sock_read=180  # 3 minutes for reading response data
                )
                
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    params = {
                        "area_id": "",
                        "cat_id": "",
                        "year": filters.get("year", "") if filters else "",
                        "mon": "ALL",
                        "_": str(int(datetime.now(timezone.utc).timestamp() * 1000))  # Cache busting
                    }
                    
                    if filters:
                        if filters.get("county"):
                            params["county"] = filters["county"]
                        if filters.get("sub_county"):
                            params["sub_county"] = filters["sub_county"]
                        if filters.get("case_category"):
                            params["case_category"] = filters["case_category"]
                    
                    logger.info(f"Fetching Kenya API data (attempt {attempt + 1}/{max_retries})")
                    
                    async with session.get(
                        self.BASE_URL,
                        params=params,
                        headers=headers,
                        ssl=False  # Kenya API may have SSL issues
                    ) as response:
                        response_text = await response.text()
                        
                        if response.status == 200:
                            try:
                                json_data = await response.json()
                                
                                # Handle different response formats
                                if isinstance(json_data, list):
                                    # Format 1: Direct array of records
                                    logger.info(f"Successfully fetched {len(json_data)} records from Kenya API")
                                    return json_data
                                elif isinstance(json_data, dict):
                                    # Format 2: Object with data array
                                    if "data" in json_data:
                                        logger.info(f"Successfully fetched {len(json_data['data'])} records from Kenya API")
                                        return json_data["data"]
                                    elif "results" in json_data:
                                        logger.info(f"Successfully fetched {len(json_data['results'])} records from Kenya API")
                                        return json_data["results"]
                                    elif "records" in json_data:
                                        logger.info(f"Successfully fetched {len(json_data['records'])} records from Kenya API")
                                        return json_data["records"]
                                    else:
                                        # Treat the whole object as a single record
                                        logger.info("Successfully fetched 1 record from Kenya API")
                                        return [json_data]
                                else:
                                    logger.warning(f"Unexpected Kenya API response type: {type(json_data)}")
                                    return []
                            except Exception as parse_error:
                                logger.error(f"Failed to parse Kenya API response as JSON: {parse_error}")
                                logger.debug(f"Response text: {response_text[:500]}")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(retry_delay)
                                    continue
                                return []
                        elif response.status == 500:
                            # Kenya API server error - check if it's a known issue
                            if "KeyError" in response_text or "Exception" in response_text:
                                logger.error(f"Kenya API server error (KeyError/Exception) - API may be misconfigured or parameters invalid")
                                logger.debug(f"Error response preview: {response_text[:200]}")
                            else:
                                logger.error(f"Kenya API returned status 500 - Internal Server Error")
                            
                            if attempt < max_retries - 1:
                                await asyncio.sleep(retry_delay)
                                continue
                            return []
                        else:
                            logger.error(f"Kenya API returned status {response.status}")
                            logger.debug(f"Response preview: {response_text[:200]}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(retry_delay)
                                continue
                            return []
                            
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching Kenya API data (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
                logger.error("All retry attempts failed due to timeout")
                raise HTTPException(
                    status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                    detail="Kenya API is not responding. Please try again later."
                )
            except aiohttp.ClientError as e:
                logger.warning(f"Network error fetching Kenya API data (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
                logger.error(f"All retry attempts failed: {e}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Unable to connect to Kenya API. Please try again later."
                )
            except Exception as e:
                logger.error(f"Unexpected error in API request: {e}")
                raise
        
        return []
    
    async def _integrate_with_cases(self, data: List[Dict]):
        """Process Kenya API data, geocode, and integrate with case system"""
        try:
            integrated_count = 0
            
            # Batch geocode all unique county/sub-county combinations
            unique_locations = []
            location_map = {}
            
            for record in data:
                county = record.get("county")
                sub_county = record.get("sub_county")
                key = f"{county}|{sub_county}"
                
                if key not in location_map and county:
                    unique_locations.append({
                        "county": county,
                        "sub_county": sub_county
                    })
                    location_map[key] = None
            
            # Geocode all locations
            if unique_locations:
                geocoded = await self.geocoding_service.batch_geocode(unique_locations)
                for loc in geocoded:
                    key = f"{loc['county']}|{loc.get('sub_county')}"
                    location_map[key] = {
                        "latitude": loc["latitude"],
                        "longitude": loc["longitude"],
                        "coordinates": loc["coordinates"]
                    }
            
            # Process each record
            for record in data:
                # Transform Kenya API data to our case format
                case_data = self._transform_kenya_data(record)
                
                if case_data:
                    # Add geocoded coordinates
                    key = f"{record.get('county')}|{record.get('sub_county')}"
                    coords = location_map.get(key)
                    if coords:
                        case_data.update(coords)
                    
                    # Check if case already exists (avoid duplicates)
                    existing = await self.cases_collection.find_one({
                        "external_id": case_data.get("external_id"),
                        "source": "kenya_api"
                    })
                    
                    if not existing:
                        await self.cases_collection.insert_one(case_data)
                        integrated_count += 1
            
            logger.info(f"Integrated {integrated_count} new cases from Kenya API with geocoding")
            
        except Exception as e:
            logger.error(f"Error integrating Kenya data with cases: {e}")
    
    def _transform_kenya_data(self, record: Dict) -> Optional[Dict]:
        """
        Transform Kenya API record to our case format.
        
        API fields: id, sex, age_range, case_category, case_date, 
                   county, sub_county, intervention
        
        Note: Coordinates are NOT in API - added via geocoding service
        """
        try:
            # Handle intervention field: "None" → "Not Resolved"
            intervention = record.get("intervention", "Not Resolved")
            if intervention == "None" or not intervention:
                intervention = "Not Resolved"
            
            # Parse date
            case_date = self._parse_date(record.get("case_date"))
            
            # Extract and normalize fields according to actual API structure
            case_data = {
                "external_id": record.get("id") or f"kenya_{record.get('case_date', '')}_{record.get('county', '')}_{record.get('sub_county', '')}",
                "source": "kenya_api",
                "case_date": case_date,
                "date_reported": case_date,
                "county": record.get("county", "Unknown"),
                "sub_county": record.get("sub_county"),
                
                # Map case_category to abuse_type
                "abuse_type": record.get("case_category", "Unspecified"),
                "case_category": record.get("case_category", "Unspecified"),
                
                # Victim demographics
                "victim_sex": record.get("sex"),  # "Male" or "Female"
                "victim_age_range": record.get("age_range"),  # e.g., "10-14"
                
                # Status and intervention
                "intervention": intervention,
                "status": self._map_status(intervention),
                
                # Processed fields (matching other repo format)
                "case_count": 1,
                "year": case_date.year if case_date else None,
                "month": case_date.month if case_date else None,
                "month_name": case_date.strftime("%B") if case_date else None,
                "country": "Kenya",
                
                # Coordinates added later by geocoding service
                # Fields: latitude, longitude, coordinates
                
                # Metadata
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "imported_from_kenya_api": True,
                "raw_api_data": record  # Store original for reference
            }
            
            return case_data
            
        except Exception as e:
            logger.warning(f"Error transforming Kenya data record: {e}")
            return None
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string to datetime"""
        if not date_str:
            return None
        
        try:
            # Try common date formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]:
                try:
                    return datetime.strptime(str(date_str), fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            return None
    
    def _map_status(self, intervention: Optional[str]) -> str:
        """Map Kenya API intervention to our status"""
        if not intervention or intervention == "Not Resolved" or intervention == "None":
            return "open"
        
        status_lower = str(intervention).lower()
        
        if "completed" in status_lower or "resolved" in status_lower or "closed" in status_lower:
            return "closed"
        elif "progress" in status_lower or "ongoing" in status_lower or "active" in status_lower:
            return "in_progress"
        else:
            return "open"
    
    async def get_aggregated_data(
        self,
        group_by: str = "county",
        filters: Optional[Dict] = None
    ) -> Dict:
        """Get aggregated statistics from Kenya API data"""
        try:
            match_filters = {"source": "kenya_api"}
            
            if filters:
                if filters.get("county"):
                    match_filters["county"] = filters["county"]
                if filters.get("abuse_type"):
                    match_filters["abuse_type"] = filters["abuse_type"]
            
            pipeline = [
                {"$match": match_filters},
                {
                    "$group": {
                        "_id": f"${group_by}",
                        "count": {"$sum": 1},
                        "avg_age": {"$avg": "$victim_age"}
                    }
                },
                {"$sort": {"count": -1}},
                {"$limit": 50}
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(50)
            
            return {
                "group_by": group_by,
                "total_records": len(results),
                "data": [
                    {
                        group_by: r["_id"],
                        "count": r["count"],
                        "avg_age": round(r["avg_age"], 1) if r.get("avg_age") else None
                    }
                    for r in results
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting aggregated data: {e}")
            raise
    
    async def get_latest_import_status(self) -> Dict:
        """Get status of the most recent data import"""
        try:
            latest = await self.kenya_data_collection.find_one(
                {},
                sort=[("fetched_at", -1)]
            )
            
            if not latest:
                return {
                    "status": "no_data",
                    "message": "No data has been imported yet"
                }
            
            return {
                "status": "success",
                "last_import": latest["fetched_at"],
                "record_count": latest.get("record_count", 0),
                "filters_used": latest.get("filters", {})
            }
            
        except Exception as e:
            logger.error(f"Error getting import status: {e}")
            raise
