from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime, timedelta, timezone
from app.core.logging import logger
from app.utils.severity_mapping import get_severity_aggregation_stage
from app.utils.date_filters import build_date_filter
from app.db.redis_client import get_redis
from typing import Optional
import hashlib
import json


class AnalyticsService:
    CACHE_TTL_SECONDS = 60 * 60 * 4  # 4 hours
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.cases_collection = db.cases
        self.redis = get_redis()
        self._date_field_cache = None
    
    def _get_cache_key(self, method: str, **kwargs) -> str:
        """Generate cache key based on method and parameters"""
        params_str = json.dumps(kwargs, sort_keys=True)
        params_hash = hashlib.md5(params_str.encode()).hexdigest()
        return f"analytics:{method}:{params_hash}"
    
    async def _get_from_cache(self, cache_key: str) -> Optional[dict]:
        """Get cached result from Redis"""
        try:
            cached_json = await self.redis.get(cache_key)
            if cached_json:
                logger.info(f"Returning cached result for: {cache_key}")
                return json.loads(cached_json)
        except Exception as e:
            logger.warning(f"Cache read error: {str(e)}")
        return None
    
    async def _save_to_cache(self, cache_key: str, data: dict):
        """Save result to Redis cache"""
        try:
            await self.redis.setex(
                cache_key,
                self.CACHE_TTL_SECONDS,
                json.dumps(data, default=str)
            )
            logger.info(f"Cached result for: {cache_key} (TTL: 4 hours)")
        except Exception as e:
            logger.warning(f"Cache write error: {str(e)}")
    
    async def _get_date_field(self) -> str:
        """Detect which date field name is used in the database"""
        if self._date_field_cache:
            return self._date_field_cache
        
        # Check sample document for date field
        sample = await self.cases_collection.find_one()
        if sample:
            # Try common date field names in order of preference
            for field in ["case_date", "Case Date", "Date", "created_at"]:
                if field in sample:
                    self._date_field_cache = field
                    logger.info(f"Using date field: {field}")
                    return field
        
        # Default fallback
        logger.warning("No date field found, defaulting to 'case_date'")
        return "case_date"

    async def get_dashboard_summary(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ):
        """Get dashboard summary statistics"""
        # Check cache first
        cache_key = self._get_cache_key("dashboard_summary", date_from=date_from, date_to=date_to)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            # Use centralized date filter utility
            filters = build_date_filter(date_from, date_to)
            
            logger.info(f"Dashboard filters: {filters}, date_from={date_from}, date_to={date_to}")

            total_cases = await self.cases_collection.count_documents(filters)
            logger.info(f"Total cases with filters: {total_cases}")
            new_cases = await self.cases_collection.count_documents({
                **filters,
                "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(days=30)}
            })
            closed_cases = await self.cases_collection.count_documents({
                **filters,
                "status": "closed"
            })
            pending_cases = await self.cases_collection.count_documents({
                **filters,
                "status": "pending"
            })
            
            # Get high severity count using derived severity
            severity_expr = get_severity_aggregation_stage()
            logger.info(f"Severity expression: {severity_expr}")
            high_severity_pipeline = [
                {"$match": filters},
                {"$addFields": {"derived_severity": severity_expr}},
                {"$match": {"derived_severity": "high"}},
                {"$count": "total"}
            ]
            logger.info(f"High severity pipeline: {high_severity_pipeline}")
            high_severity_result = await self.cases_collection.aggregate(high_severity_pipeline).to_list(1)
            logger.info(f"High severity result: {high_severity_result}")
            high_severity = high_severity_result[0]["total"] if high_severity_result else 0
            logger.info(f"High severity count: {high_severity}")

            pipeline = [
                {"$match": filters},
                {
                    "$facet": {
                        "top_counties": [
                            {"$group": {"_id": "$county", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}},
                            {"$limit": 5}
                        ],
                        "top_abuse_types": [
                            {"$group": {"_id": "$abuse_type", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}},
                            {"$limit": 5}
                        ]
                    }
                }
            ]

            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            facets = results[0] if results else {}

            result = {
                "period": {
                    "from": date_from or "all-time",
                    "to": date_to or "today"
                },
                "summary": {
                    "total_cases": total_cases,
                    "new_cases": new_cases,
                    "closed_cases": closed_cases,
                    "pending_cases": pending_cases,
                    "high_severity_cases": high_severity
                },
                "top_counties": facets.get("top_counties", []),
                "top_abuse_types": facets.get("top_abuse_types", []),
                "trend": "up" if new_cases > 0 else "stable"
            }
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Error getting dashboard summary: {e}")
            raise

    async def get_county_analysis(self, county: str):
        """Get analysis for specific county"""
        # Check cache first
        cache_key = self._get_cache_key("county_analysis", county=county)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            severity_expr = get_severity_aggregation_stage()
            
            pipeline = [
                {"$match": {"county": county}},
                {"$addFields": {"derived_severity": severity_expr}},
                {
                    "$facet": {
                        "total": [{"$count": "count"}],
                        "by_abuse_type": [
                            {"$group": {"_id": "$abuse_type", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}}
                        ],
                        "by_severity": [
                            {"$group": {"_id": "$derived_severity", "count": {"$sum": 1}}}
                        ],
                        "by_status": [
                            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                        ],
                        "age_distribution": [
                            {
                                "$group": {
                                    "_id": {
                                        "$cond": [
                                            {"$lt": ["$child_age", 5]},
                                            "0-5",
                                            {"$cond": [
                                                {"$lt": ["$child_age", 12]},
                                                "6-11",
                                                "12-18"
                                            ]}
                                        ]
                                    },
                                    "count": {"$sum": 1}
                                }
                            }
                        ]
                    }
                }
            ]

            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            result = results[0] if results else {}
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Error analyzing county: {e}")
            raise

    async def get_abuse_type_analysis(self, abuse_type: str):
        """Get analysis for specific abuse type"""
        # Check cache first
        cache_key = self._get_cache_key("abuse_type_analysis", abuse_type=abuse_type)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            severity_expr = get_severity_aggregation_stage()
            
            pipeline = [
                {"$match": {"abuse_type": abuse_type}},
                {"$addFields": {"derived_severity": severity_expr}},
                {
                    "$facet": {
                        "total": [{"$count": "count"}],
                        "by_county": [
                            {"$group": {"_id": "$county", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}}
                        ],
                        "by_severity": [
                            {"$group": {"_id": "$derived_severity", "count": {"$sum": 1}}}
                        ],
                        "recent_cases": [
                            {"$sort": {"created_at": -1}},
                            {"$limit": 10},
                            {"$project": {
                                "case_id": 1,
                                "county": 1,
                                "derived_severity": 1,
                                "created_at": 1
                            }}
                        ]
                    }
                }
            ]

            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            result = results[0] if results else {}
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Error analyzing abuse type: {e}")
            raise

    async def get_time_series_data(
        self,
        granularity: str = "monthly",
        year: Optional[int] = None
    ):
        """Get time series data for trends"""
        # Check cache first
        cache_key = self._get_cache_key("time_series", granularity=granularity, year=year)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            if not year:
                year = datetime.now(timezone.utc).year

            # Try multiple date field names for compatibility
            date_field = await self._get_date_field()
            
            if granularity == "monthly":
                pipeline = [
                    {
                        "$addFields": {
                            "date_parsed": {
                                "$cond": {
                                    "if": {"$eq": [{"$type": f"${date_field}"}, "string"]},
                                    "then": {"$dateFromString": {"dateString": f"${date_field}"}},
                                    "else": f"${date_field}"
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "date_parsed": {
                                "$gte": datetime(year, 1, 1),
                                "$lt": datetime(year + 1, 1, 1)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"$dateToString": {"format": "%Y-%m", "date": "$date_parsed"}},
                            "cases": {"$sum": 1}
                        }
                    },
                    {"$sort": {"_id": 1}}
                ]
            elif granularity == "weekly":
                pipeline = [
                    {
                        "$addFields": {
                            "date_parsed": {
                                "$cond": {
                                    "if": {"$eq": [{"$type": f"${date_field}"}, "string"]},
                                    "then": {"$dateFromString": {"dateString": f"${date_field}"}},
                                    "else": f"${date_field}"
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "date_parsed": {
                                "$gte": datetime(year, 1, 1),
                                "$lt": datetime(year + 1, 1, 1)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"$isoWeek": "$date_parsed"},
                            "cases": {"$sum": 1}
                        }
                    },
                    {"$sort": {"_id": 1}}
                ]
            else:  # daily
                pipeline = [
                    {
                        "$addFields": {
                            "date_parsed": {
                                "$cond": {
                                    "if": {"$eq": [{"$type": f"${date_field}"}, "string"]},
                                    "then": {"$dateFromString": {"dateString": f"${date_field}"}},
                                    "else": f"${date_field}"
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "date_parsed": {
                                "$gte": datetime(year, 1, 1),
                                "$lt": datetime(year + 1, 1, 1)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_parsed"}},
                            "cases": {"$sum": 1}
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {"$limit": 365}
                ]

            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            result = {
                "year": year,
                "granularity": granularity,
                "data": results,
                "date_field_used": date_field
            }
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Error getting time series data: {e}")
            raise

    async def get_severity_distribution(self):
        """Get severity distribution across all cases"""
        # Check cache first
        cache_key = self._get_cache_key("severity_distribution")
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            severity_expr = get_severity_aggregation_stage()
            
            pipeline = [
                {"$addFields": {"derived_severity": severity_expr}},
                {
                    "$group": {
                        "_id": "$derived_severity",
                        "count": {"$sum": 1}
                    }
                },
                {"$sort": {"count": -1}}
            ]

            results = await self.cases_collection.aggregate(pipeline).to_list(None)

            # Calculate percentages
            total = sum(r["count"] for r in results)
            for r in results:
                r["percentage"] = round((r["count"] / total * 100), 2) if total > 0 else 0

            # Cache the result
            await self._save_to_cache(cache_key, results)
            
            return results
        except Exception as e:
            logger.error(f"Error getting severity distribution: {e}")
            raise
