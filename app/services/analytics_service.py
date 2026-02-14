from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime, timedelta, timezone
from app.core.logging import logger
from app.utils.severity_mapping import get_severity_aggregation_stage
from app.utils.date_filters import build_date_filter
from typing import Optional


class AnalyticsService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.cases_collection = db.cases
        self._date_field_cache = None
    
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

            return {
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
        except Exception as e:
            logger.error(f"Error getting dashboard summary: {e}")
            raise

    async def get_county_analysis(self, county: str):
        """Get analysis for specific county"""
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
            return results[0] if results else {}
        except Exception as e:
            logger.error(f"Error analyzing county: {e}")
            raise

    async def get_abuse_type_analysis(self, abuse_type: str):
        """Get analysis for specific abuse type"""
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
            return results[0] if results else {}
        except Exception as e:
            logger.error(f"Error analyzing abuse type: {e}")
            raise

    async def get_time_series_data(
        self,
        granularity: str = "monthly",
        year: Optional[int] = None
    ):
        """Get time series data for trends"""
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
            return {
                "year": year,
                "granularity": granularity,
                "data": results,
                "date_field_used": date_field
            }
        except Exception as e:
            logger.error(f"Error getting time series data: {e}")
            raise

    async def get_severity_distribution(self):
        """Get severity distribution across all cases"""
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

            return results
        except Exception as e:
            logger.error(f"Error getting severity distribution: {e}")
            raise
