from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from datetime import datetime, timezone
from app.db.models import CaseStatus, SeverityLevel
from app.core.logging import logger
from app.core.cache import cache
from app.config import settings
from app.utils.severity_mapping import get_severity_aggregation_stage
from app.utils.date_filters import build_date_filter
from app.services.geocoding_service import GeocodingService
import hashlib
import json


class CaseService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.cases_collection = db.cases
        self.kenya_data_collection = db.kenya_api_data
        self.geocoding_service = GeocodingService()
        self._kenya_metadata_cache = None
        self._kenya_metadata_cache_time = None
        self._cache_ttl = 300  # 5 minutes cache

    async def create_case(self, case_data: dict, user_id: str):
        """Create a new case with automatic geocoding"""
        case_data["status"] = CaseStatus.OPEN.value
        case_data["created_by"] = ObjectId(user_id)
        case_data["created_at"] = datetime.now(timezone.utc)
        case_data["updated_at"] = datetime.now(timezone.utc)

        # Auto-geocode if county is provided and coordinates are missing
        if case_data.get("county") and not case_data.get("latitude"):
            try:
                coords = await self.geocoding_service.geocode_location(
                    case_data["county"],
                    case_data.get("sub_county")
                )
                case_data["latitude"] = coords["lat"]
                case_data["longitude"] = coords["lon"]
                case_data["location"] = {
                    "type": "Point",
                    "coordinates": [coords["lon"], coords["lat"]]
                }
                logger.info(f"Auto-geocoded case for {case_data['county']}")
            except Exception as e:
                logger.warning(f"Failed to geocode case: {e}")

        result = await self.cases_collection.insert_one(case_data)
        case_data["_id"] = result.inserted_id

        logger.info(f"Case created: {case_data.get('case_id')}")
        return case_data

    async def get_case_by_id(self, case_id: str):
        """Get case by ID"""
        try:
            case = await self.cases_collection.find_one({"_id": ObjectId(case_id)})
            if not case:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Case not found"
                )
            return case
        except Exception as e:
            logger.error(f"Error getting case: {e}")
            raise

    async def list_cases(
        self,
        page: int = 1,
        limit: int = 50,
        county: Optional[str] = None,
        abuse_type: Optional[str] = None,
        status_filter: Optional[str] = None,
        severity: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        include_kenya_data: bool = False,
        auto_sync_kenya: bool = False
    ):
        """List cases with filtering and pagination, optionally including Kenya API data"""
        
        # Generate cache key for this query
        cache_key = None
        if settings.ENABLE_QUERY_CACHE and not include_kenya_data and not auto_sync_kenya:
            cache_params = {
                'page': page,
                'limit': limit,
                'county': county,
                'abuse_type': abuse_type,
                'status': status_filter,
                'severity': severity,
                'date_from': date_from,
                'date_to': date_to
            }
            cache_key_str = f"cases_list:{json.dumps(cache_params, sort_keys=True)}"
            cache_key = hashlib.md5(cache_key_str.encode()).hexdigest()
            
            # Try to get from cache
            cached_result = cache.get(cache_key)
            if cached_result:
                logger.debug(f"Cache hit for cases list query")
                return cached_result
        
        # Auto-sync Kenya API data if requested and data is stale
        if auto_sync_kenya:
            await self._auto_sync_kenya_data()
        
        filters = {}
        if county:
            filters["county"] = county
        if abuse_type:
            filters["abuse_type"] = abuse_type
        if status_filter:
            filters["status"] = status_filter
        if severity:
            filters["severity"] = severity
        
        # Use centralized date filter utility
        date_filters = build_date_filter(date_from, date_to)
        filters.update(date_filters)

        # Optimize: For large limits, skip count query to improve performance
        skip_count = limit > 500
        
        if skip_count:
            # Fast path: Just get data without counting total
            pipeline = [
                {"$match": filters},
                {"$sort": {"created_at": -1}},
                {"$skip": (page - 1) * limit},
                {"$limit": limit},
                {
                    "$project": {
                        "_id": {"$toString": "$_id"},
                        "case_id": 1,
                        "case_date": 1,
                        "county": 1,
                        "subcounty": 1,
                        "abuse_type": 1,
                        "status": 1,
                        "severity": 1,
                        "created_at": 1,
                        "updated_at": 1,
                        "child_age": 1,
                        "child_sex": 1,
                        "source": 1
                    }
                }
            ]
            cases = await self.cases_collection.aggregate(pipeline).to_list(limit)
            total = -1  # Indicate count was skipped for performance
        else:
            # Normal path: Get count and data
            pipeline = [
                {"$match": filters},
                {
                    "$facet": {
                        "metadata": [{"$count": "total"}],
                        "data": [
                            {"$sort": {"created_at": -1}},
                            {"$skip": (page - 1) * limit},
                            {"$limit": limit},
                            {
                                "$project": {
                                    "_id": {"$toString": "$_id"},
                                    "case_id": 1,
                                    "case_date": 1,
                                    "county": 1,
                                    "subcounty": 1,
                                    "abuse_type": 1,
                                    "status": 1,
                                    "severity": 1,
                                    "created_at": 1,
                                    "updated_at": 1,
                                    "child_age": 1,
                                    "child_sex": 1,
                                    "source": 1
                                }
                            }
                        ]
                    }
                }
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(1)
            
            if results and results[0]["metadata"]:
                total = results[0]["metadata"][0]["total"]
                cases = results[0]["data"]
            else:
                total = 0
                cases = []
        # Add Kenya API metadata to response only if requested
        kenya_metadata = None
        if include_kenya_data:
            kenya_metadata = await self._get_kenya_data_metadata()

        result = {
            "total": total,
            "page": page,
            "limit": limit,
            "cases": cases,
            "kenya_api_metadata": kenya_metadata
        }
        
        # Cache the result
        if cache_key:
            cache.set(cache_key, result, ttl=settings.CACHE_TTL)
        
        return result

    async def update_case(self, case_id: str, update_data: dict):
        """Update case"""
        try:
            update_data["updated_at"] = datetime.now(timezone.utc)

            result = await self.cases_collection.find_one_and_update(
                {"_id": ObjectId(case_id)},
                {"$set": update_data},
                return_document=True
            )

            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Case not found"
                )

            logger.info(f"Case updated: {case_id}")
            return result
        except Exception as e:
            logger.error(f"Error updating case: {e}")
            raise

    async def delete_case(self, case_id: str):
        """Delete case"""
        try:
            result = await self.cases_collection.delete_one({"_id": ObjectId(case_id)})

            if result.deleted_count == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Case not found"
                )

            logger.info(f"Case deleted: {case_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting case: {e}")
            raise

    async def get_case_statistics(
        self,
        county: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ):
        """Get case statistics with aggregation and caching"""
        
        # Try cache first
        cache_key = None
        if settings.ENABLE_QUERY_CACHE:
            cache_params = {
                'county': county,
                'date_from': date_from,
                'date_to': date_to
            }
            cache_key_str = f"case_stats:{json.dumps(cache_params, sort_keys=True)}"
            cache_key = hashlib.md5(cache_key_str.encode()).hexdigest()
            
            cached_result = cache.get(cache_key)
            if cached_result:
                logger.debug("Cache hit for case statistics")
                return cached_result
        
        filters = {}
        if county:
            filters["county"] = county
        
        # Use centralized date filter utility
        date_filters = build_date_filter(date_from, date_to)
        filters.update(date_filters)
        
        severity_expr = get_severity_aggregation_stage()

        pipeline = [
            {"$match": filters},
            {"$addFields": {"derived_severity": severity_expr}},
            {
                "$facet": {
                    "total_cases": [{"$count": "count"}],
                    "by_abuse_type": [
                        {"$group": {"_id": "$abuse_type", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 20}
                    ],
                    "by_status": [
                        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                    ],
                    "by_severity": [
                        {"$group": {"_id": "$derived_severity", "count": {"$sum": 1}}}
                    ],
                    "by_county": [
                        {"$group": {"_id": "$county", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 20}
                    ]
                }
            }
        ]

        results = await self.cases_collection.aggregate(pipeline).to_list(None)
        result = results[0] if results else {}
        
        # Cache the result
        if cache_key:
            cache.set(cache_key, result, ttl=settings.CACHE_TTL)
        
        return result

    async def get_high_severity_cases(self, limit: int = 10):
        """Get high severity cases"""
        severity_expr = get_severity_aggregation_stage()
        pipeline = [
            {"$addFields": {"derived_severity": severity_expr}},
            {"$match": {"derived_severity": "high"}},
            {"$sort": {"created_at": -1}},
            {"$limit": limit}
        ]
        cases = await self.cases_collection.aggregate(pipeline).to_list(limit)
        return cases

    async def search_cases(self, query: str, limit: int = 20):
        """Search cases by description or case ID using text index"""
        # Use text search if available, fallback to regex
        try:
            # Try text search first (faster with text index)
            filters = {"$text": {"$search": query}}
            cases = await self.cases_collection.find(
                filters,
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})]).limit(limit).to_list(limit)
            
            if cases:
                return cases
        except Exception as e:
            logger.debug(f"Text search not available, falling back to regex: {e}")
        
        # Fallback to regex search
        filters = {
            "$or": [
                {"case_id": {"$regex": query, "$options": "i"}},
                {"county": {"$regex": query, "$options": "i"}}
            ]
        }

        cases = await self.cases_collection.find(filters).limit(limit).to_list(limit)
        return cases
    
    async def _auto_sync_kenya_data(self):
        """Automatically sync Kenya API data if stale (older than 24 hours)"""
        try:
            latest = await self.kenya_data_collection.find_one(
                {},
                sort=[("fetched_at", -1)]
            )
            
            # Check if data is stale (older than 24 hours)
            if not latest or (datetime.now(timezone.utc) - latest["fetched_at"]).total_seconds() > 86400:
                logger.info("Kenya API data is stale, initiating sync...")
                from app.services.kenya_api_service import KenyaAPIService
                kenya_service = KenyaAPIService(self.db)
                await kenya_service.fetch_and_store_data(force_refresh=True)
        except Exception as e:
            logger.error(f"Error auto-syncing Kenya data: {e}")
    
    async def _get_kenya_data_metadata(self):
        """Get metadata about Kenya API data with caching"""
        try:
            # Check cache
            if self._kenya_metadata_cache and self._kenya_metadata_cache_time:
                cache_age = (datetime.now(timezone.utc) - self._kenya_metadata_cache_time).total_seconds()
                if cache_age < self._cache_ttl:
                    return self._kenya_metadata_cache
            
            latest = await self.kenya_data_collection.find_one(
                {},
                sort=[("fetched_at", -1)]
            )
            
            if not latest:
                return None
            
            # Count cases from Kenya API
            kenya_case_count = await self.cases_collection.count_documents(
                {"source": "kenya_api"}
            )
            
            metadata = {
                "last_sync": latest["fetched_at"],
                "total_kenya_records": latest.get("record_count", 0),
                "integrated_cases": kenya_case_count,
                "data_age_hours": (datetime.now(timezone.utc) - latest["fetched_at"]).total_seconds() / 3600
            }
            
            # Update cache
            self._kenya_metadata_cache = metadata
            self._kenya_metadata_cache_time = datetime.now(timezone.utc)
            
            return metadata
        except Exception as e:
            logger.error(f"Error getting Kenya data metadata: {e}")
            return None
    
    async def sync_kenya_api_data(self, filters: Optional[dict] = None):
        """Manually trigger Kenya API data sync - exposed as API endpoint"""
        try:
            from app.services.kenya_api_service import KenyaAPIService
            kenya_service = KenyaAPIService(self.db)
            result = await kenya_service.fetch_and_store_data(
                filters=filters,
                force_refresh=True
            )
            logger.info(f"Kenya API sync completed: {result}")
            return result
        except Exception as e:
            logger.error(f"Error syncing Kenya API: {e}")
            raise
    
    async def get_case_statistics(self, include_kenya: bool = True):
        """Get comprehensive case statistics including Kenya API data"""
        try:
            pipeline = [
                {
                    "$facet": {
                        "total": [{"$count": "count"}],
                        "by_county": [
                            {"$group": {"_id": "$county", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}},
                            {"$limit": 10}
                        ],
                        "by_abuse_type": [
                            {"$group": {"_id": "$abuse_type", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}}
                        ],
                        "by_source": [
                            {"$group": {"_id": "$source", "count": {"$sum": 1}}}
                        ],
                        "by_status": [
                            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                        ]
                    }
                }
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(1)
            
            if not results:
                return {
                    "total_cases": 0,
                    "by_county": [],
                    "by_abuse_type": [],
                    "by_source": [],
                    "by_status": []
                }
            
            data = results[0]
            stats = {
                "total_cases": data["total"][0]["count"] if data["total"] else 0,
                "by_county": data["by_county"],
                "by_abuse_type": data["by_abuse_type"],
                "by_source": data["by_source"],
                "by_status": data["by_status"]
            }
            
            # Add Kenya API metadata if requested
            if include_kenya:
                stats["kenya_api"] = await self._get_kenya_data_metadata()
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting case statistics: {e}")
            raise
