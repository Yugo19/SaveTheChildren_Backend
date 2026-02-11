from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from datetime import datetime, timedelta, timezone
from app.core.logging import logger
import uuid
import json


class ReportService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.reports_collection = db.reports
        self.cases_collection = db.cases

    async def create_report(
        self,
        report_type: str,
        title: str,
        filters: dict,
        user_id: str,
        include_kenya_data: bool = True
    ):
        """Create a new report with optional Kenya API data"""
        try:
            report_id = str(uuid.uuid4())

            # Get the data for the report
            if report_type == "summary":
                data = await self._generate_summary_report(filters, include_kenya_data)
            elif report_type == "detailed":
                data = await self._generate_detailed_report(filters, include_kenya_data)
            elif report_type == "county":
                data = await self._generate_county_report(filters, include_kenya_data)
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid report type"
                )

            report_doc = {
                "report_id": report_id,
                "title": title,
                "report_type": report_type,
                "status": "completed",
                "filters": filters,
                "data": data,
                "includes_kenya_data": include_kenya_data,
                "generated_by": ObjectId(user_id),
                "generated_at": datetime.now(timezone.utc),
                "expires_at": datetime.now(timezone.utc) + timedelta(days=30)
            }

            result = await self.reports_collection.insert_one(report_doc)
            logger.info(f"Report created: {report_id} (Kenya data: {include_kenya_data})")

            return {
                "report_id": report_id,
                "title": title,
                "status": "completed",
                "generated_at": report_doc["generated_at"]
            }
        except Exception as e:
            logger.error(f"Error creating report: {e}")
            raise

    async def get_report(self, report_id: str):
        """Get report by ID"""
        try:
            report = await self.reports_collection.find_one({"report_id": report_id})
            if not report:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Report not found"
                )
            return report
        except Exception as e:
            logger.error(f"Error getting report: {e}")
            raise

    async def list_reports(self, page: int = 1, limit: int = 20, user_id: str = None):
        """List reports with pagination"""
        try:
            filters = {}
            if user_id:
                filters["generated_by"] = ObjectId(user_id)

            total = await self.reports_collection.count_documents(filters)

            reports = await self.reports_collection.find(filters)\
                .skip((page - 1) * limit)\
                .limit(limit)\
                .sort("generated_at", -1)\
                .to_list(limit)

            return {
                "total": total,
                "page": page,
                "limit": limit,
                "reports": reports
            }
        except Exception as e:
            logger.error(f"Error listing reports: {e}")
            raise

    async def delete_report(self, report_id: str):
        """Delete report"""
        try:
            result = await self.reports_collection.delete_one({"report_id": report_id})

            if result.deleted_count == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Report not found"
                )

            logger.info(f"Report deleted: {report_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting report: {e}")
            raise

    async def _generate_summary_report(self, filters: dict, include_kenya: bool = True):
        """Generate summary report data with optional Kenya API data"""
        match_filters = self._build_filters(filters)

        pipeline = [
            {"$match": match_filters},
            {
                "$facet": {
                    "total_cases": [{"$count": "count"}],
                    "by_abuse_type": [
                        {"$group": {"_id": "$abuse_type", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ],
                    "by_status": [
                        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                    ],
                    "by_severity": [
                        {"$group": {"_id": "$severity", "count": {"$sum": 1}}}
                    ],
                    "by_source": [
                        {"$group": {"_id": "$source", "count": {"$sum": 1}}}
                    ]
                }
            }
        ]

        results = await self.cases_collection.aggregate(pipeline).to_list(None)
        data = results[0] if results else {}
        
        # Add Kenya API metadata if requested
        if include_kenya:
            kenya_metadata = await self._get_kenya_metadata()
            data["kenya_api_metadata"] = kenya_metadata
        
        return data

    async def _generate_detailed_report(self, filters: dict, include_kenya: bool = True):
        """Generate detailed report with case data"""
        match_filters = self._build_filters(filters)

        cases = await self.cases_collection.find(match_filters)\
            .sort("created_at", -1)\
            .limit(100)\
            .to_list(100)

        # Convert ObjectId to strings
        for case in cases:
            case["_id"] = str(case["_id"])
            if "created_by" in case:
                case["created_by"] = str(case["created_by"])
        
        data = {
            "total_cases": len(cases),
            "cases": cases
        }
        
        # Add Kenya API metadata if requested
        if include_kenya:
            kenya_metadata = await self._get_kenya_metadata()
            data["kenya_api_metadata"] = kenya_metadata

        return data

    async def _generate_county_report(self, filters: dict, include_kenya: bool = True):
        """Generate county-wise report"""
        match_filters = self._build_filters(filters)

        pipeline = [
            {"$match": match_filters},
            {
                "$group": {
                    "_id": "$county",
                    "total": {"$sum": 1},
                    "high_severity": {
                        "$sum": {"$cond": [{"$eq": ["$severity", "high"]}, 1, 0]}
                    },
                    "open_cases": {
                        "$sum": {"$cond": [{"$eq": ["$status", "open"]}, 1, 0]}
                    },
                    "kenya_api_cases": {
                        "$sum": {"$cond": [{"$eq": ["$source", "kenya_api"]}, 1, 0]}
                    }
                }
            },
            {"$sort": {"total": -1}}
        ]

        results = await self.cases_collection.aggregate(pipeline).to_list(None)
        data = {"counties": results}
        
        # Add Kenya API metadata if requested
        if include_kenya:
            kenya_metadata = await self._get_kenya_metadata()
            data["kenya_api_metadata"] = kenya_metadata
        
        return data
    
    async def _get_kenya_metadata(self):
        """Get Kenya API metadata for reports"""
        try:
            latest = await self.db.kenya_api_data.find_one(
                {},
                sort=[("fetched_at", -1)]
            )
            
            if not latest:
                return None
            
            kenya_case_count = await self.cases_collection.count_documents(
                {"source": "kenya_api"}
            )
            
            return {
                "last_sync": latest["fetched_at"],
                "total_records_imported": latest.get("record_count", 0),
                "integrated_cases": kenya_case_count,
                "data_age_hours": (datetime.now(timezone.utc) - latest["fetched_at"]).total_seconds() / 3600
            }
        except Exception as e:
            logger.error(f"Error getting Kenya metadata: {e}")
            return None

    def _build_filters(self, filters: dict):
        """Build MongoDB match filters"""
        match_filters = {}

        if filters.get("county"):
            match_filters["county"] = filters["county"]
        if filters.get("abuse_type"):
            match_filters["abuse_type"] = filters["abuse_type"]
        if filters.get("status"):
            match_filters["status"] = filters["status"]

        # Date range
        if filters.get("date_from") or filters.get("date_to"):
            date_filter = {}
            if filters.get("date_from"):
                date_filter["$gte"] = datetime.fromisoformat(filters["date_from"])
            if filters.get("date_to"):
                date_filter["$lte"] = datetime.fromisoformat(filters["date_to"])
            match_filters["date_reported"] = date_filter

        return match_filters
