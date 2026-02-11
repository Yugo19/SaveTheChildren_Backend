from fastapi import APIRouter, Depends, Query
from typing import Optional
from datetime import datetime
from app.db.client import get_database
from app.core.security import get_current_user, TokenData
from app.core.logging import logger
import csv
import json
import io
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/data", tags=["Data"])


@router.get("/aggregate")
async def get_aggregated_data(
    county: Optional[str] = None,
    abuse_type: Optional[str] = None,
    year: Optional[int] = None,
    group_by: str = Query("abuse_type", enum=["abuse_type", "county", "severity", "status"]),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get aggregated data grouped by specified dimension"""
    filters = {}

    if county:
        filters["county"] = county
    if abuse_type:
        filters["abuse_type"] = abuse_type
    if year:
        filters["date_reported"] = {
            "$gte": datetime(year, 1, 1),
            "$lt": datetime(year + 1, 1, 1)
        }

    pipeline = [
        {"$match": filters},
        {
            "$group": {
                "_id": f"${group_by}",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}}
    ]

    results = await db.cases.aggregate(pipeline).to_list(None)

    total = sum(r["count"] for r in results)

    aggregations = []
    for result in results:
        aggregations.append({
            group_by: result["_id"],
            "count": result["count"],
            "percentage": (result["count"] / total * 100) if total > 0 else 0
        })

    logger.info(f"Aggregated data retrieved by {current_user.user_id}")

    return {
        "filters": {
            "county": county,
            "abuse_type": abuse_type,
            "year": year
        },
        "group_by": group_by,
        "total": total,
        "aggregations": aggregations
    }


@router.get("/export/csv")
async def export_to_csv(
    county: Optional[str] = None,
    abuse_type: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Export case data to CSV"""
    try:
        filters = {}
        if county:
            filters["county"] = county
        if abuse_type:
            filters["abuse_type"] = abuse_type
        if status:
            filters["status"] = status
        if date_from or date_to:
            date_filter = {}
            if date_from:
                date_filter["$gte"] = datetime.fromisoformat(date_from)
            if date_to:
                date_filter["$lte"] = datetime.fromisoformat(date_to)
            filters["date_reported"] = date_filter

        cases = await db.cases.find(filters).to_list(None)

        # Create CSV
        output = io.StringIO()
        if cases:
            fieldnames = cases[0].keys()
            writer = csv.DictWriter(output, fieldnames=[f for f in fieldnames if f != '_id'])
            writer.writeheader()

            for case in cases:
                row = {k: v for k, v in case.items() if k != '_id'}
                # Convert ObjectId and datetime to strings
                for key, value in row.items():
                    if hasattr(value, '__dict__'):
                        row[key] = str(value)
                writer.writerow(row)

        logger.info(f"Data exported to CSV by {current_user.user_id}")

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=cases_export.csv"}
        )
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        raise


@router.get("/export/json")
async def export_to_json(
    county: Optional[str] = None,
    abuse_type: Optional[str] = None,
    status: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Export case data to JSON"""
    try:
        filters = {}
        if county:
            filters["county"] = county
        if abuse_type:
            filters["abuse_type"] = abuse_type
        if status:
            filters["status"] = status

        cases = await db.cases.find(filters).to_list(None)

        # Convert ObjectId to strings
        for case in cases:
            case["_id"] = str(case["_id"])
            if "created_by" in case:
                case["created_by"] = str(case["created_by"])

        logger.info(f"Data exported to JSON by {current_user.user_id}")

        return StreamingResponse(
            iter([json.dumps({"cases": cases}, default=str)]),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=cases_export.json"}
        )
    except Exception as e:
        logger.error(f"Error exporting to JSON: {e}")
        raise


@router.get("/filters")
async def get_available_filters(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get available filter values for data filtering"""
    try:
        # Get distinct values for each field
        counties = await db.cases.distinct("county")
        abuse_types = await db.cases.distinct("abuse_type")
        statuses = await db.cases.distinct("status")
        severities = await db.cases.distinct("severity")

        # Get available years
        pipeline = [
            {
                "$group": {
                    "_id": {"$year": "$date_reported"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        years_result = await db.cases.aggregate(pipeline).to_list(None)
        years = [r["_id"] for r in years_result if r["_id"]]

        logger.info(f"Available filters retrieved by {current_user.user_id}")

        return {
            "counties": sorted(counties),
            "abuse_types": sorted(abuse_types),
            "statuses": sorted(statuses),
            "severities": sorted(severities),
            "years": sorted(years)
        }
    except Exception as e:
        logger.error(f"Error getting available filters: {e}")
        raise
