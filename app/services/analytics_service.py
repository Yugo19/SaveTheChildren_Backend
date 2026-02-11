from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime, timedelta, timezone
from app.core.logging import logger
from typing import Optional


class AnalyticsService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.cases_collection = db.cases

    async def get_dashboard_summary(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ):
        """Get dashboard summary statistics"""
        try:
            filters = {}
            if date_from or date_to:
                date_filter = {}
                if date_from:
                    date_filter["$gte"] = datetime.fromisoformat(date_from)
                if date_to:
                    date_filter["$lte"] = datetime.fromisoformat(date_to)
                filters["date_reported"] = date_filter

            total_cases = await self.cases_collection.count_documents(filters)
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
            high_severity = await self.cases_collection.count_documents({
                **filters,
                "severity": "high"
            })

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
            pipeline = [
                {"$match": {"county": county}},
                {
                    "$facet": {
                        "total": [{"$count": "count"}],
                        "by_abuse_type": [
                            {"$group": {"_id": "$abuse_type", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}}
                        ],
                        "by_severity": [
                            {"$group": {"_id": "$severity", "count": {"$sum": 1}}}
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
            pipeline = [
                {"$match": {"abuse_type": abuse_type}},
                {
                    "$facet": {
                        "total": [{"$count": "count"}],
                        "by_county": [
                            {"$group": {"_id": "$county", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}}
                        ],
                        "by_severity": [
                            {"$group": {"_id": "$severity", "count": {"$sum": 1}}}
                        ],
                        "recent_cases": [
                            {"$sort": {"created_at": -1}},
                            {"$limit": 10},
                            {"$project": {
                                "case_id": 1,
                                "county": 1,
                                "severity": 1,
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

            if granularity == "monthly":
                pipeline = [
                    {
                        "$match": {
                            "date_reported": {
                                "$gte": datetime(year, 1, 1),
                                "$lt": datetime(year + 1, 1, 1)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"$dateToString": {"format": "%Y-%m", "date": "$date_reported"}},
                            "cases": {"$sum": 1}
                        }
                    },
                    {"$sort": {"_id": 1}}
                ]
            elif granularity == "weekly":
                pipeline = [
                    {
                        "$match": {
                            "date_reported": {
                                "$gte": datetime(year, 1, 1),
                                "$lt": datetime(year + 1, 1, 1)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"$isoWeek": "$date_reported"},
                            "cases": {"$sum": 1}
                        }
                    },
                    {"$sort": {"_id": 1}}
                ]
            else:  # daily
                pipeline = [
                    {
                        "$match": {
                            "date_reported": {
                                "$gte": datetime(year, 1, 1),
                                "$lt": datetime(year + 1, 1, 1)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_reported"}},
                            "cases": {"$sum": 1}
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {"$limit": 30}
                ]

            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            return {
                "year": year,
                "granularity": granularity,
                "data": results
            }
        except Exception as e:
            logger.error(f"Error getting time series data: {e}")
            raise

    async def get_severity_distribution(self):
        """Get severity distribution across all cases"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$severity",
                        "count": {"$sum": 1},
                        "percentage": {
                            "$avg": {"$cond": [True, 1, 0]}
                        }
                    }
                },
                {"$sort": {"count": -1}}
            ]

            results = await self.cases_collection.aggregate(pipeline).to_list(None)

            # Calculate percentages
            total = sum(r["count"] for r in results)
            for r in results:
                r["percentage"] = (r["count"] / total * 100) if total > 0 else 0

            return results
        except Exception as e:
            logger.error(f"Error getting severity distribution: {e}")
            raise
