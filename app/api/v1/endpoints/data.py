from fastapi import APIRouter, Depends, Query
from typing import Optional
from datetime import datetime
from app.db.client import get_database
from app.core.security import get_current_user, TokenData
from app.core.logging import logger
from app.utils.date_filters import build_date_filter
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
    """Get aggregated data grouped by specified dimension with caching"""
    from app.core.cache import cache
    from app.config import settings
    import hashlib
    import json
    
    # Try cache first
    cache_key = None
    if settings.ENABLE_QUERY_CACHE:
        cache_params = {
            'county': county,
            'abuse_type': abuse_type,
            'year': year,
            'group_by': group_by
        }
        cache_key_str = f"aggregate:{json.dumps(cache_params, sort_keys=True)}"
        cache_key = hashlib.md5(cache_key_str.encode()).hexdigest()
        
        cached_result = cache.get(cache_key)
        if cached_result:
            logger.debug("Cache hit for aggregation query")
            return cached_result
    
    filters = {}

    if county:
        filters["county"] = county
    if abuse_type:
        filters["abuse_type"] = abuse_type
    if year:
        filters["case_date"] = {
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
        {"$sort": {"count": -1}},
        {"$limit": 100}  # Limit results for performance
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

    response = {
        "filters": {
            "county": county,
            "abuse_type": abuse_type,
            "year": year
        },
        "group_by": group_by,
        "total": total,
        "aggregations": aggregations
    }
    
    # Cache the result
    if cache_key:
        cache.set(cache_key, response, ttl=settings.CACHE_TTL)
    
    return response


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
        
        # Use centralized date filter utility
        date_filters = build_date_filter(date_from, date_to)
        filters.update(date_filters)

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
    """Get available filter values for data filtering with caching"""
    from app.core.cache import cache
    from app.config import settings
    
    cache_key = "available_filters"
    
    # Try cache first
    if settings.ENABLE_QUERY_CACHE:
        cached_result = cache.get(cache_key)
        if cached_result:
            logger.debug("Cache hit for filters")
            return cached_result
    
    try:
        # Use aggregation to get distinct values efficiently in one query
        pipeline = [
            {
                "$facet": {
                    "counties": [
                        {"$group": {"_id": "$county"}},
                        {"$sort": {"_id": 1}},
                        {"$project": {"_id": 0, "value": "$_id"}}
                    ],
                    "abuse_types": [
                        {"$group": {"_id": "$abuse_type"}},
                        {"$sort": {"_id": 1}},
                        {"$project": {"_id": 0, "value": "$_id"}}
                    ],
                    "statuses": [
                        {"$group": {"_id": "$status"}},
                        {"$sort": {"_id": 1}},
                        {"$project": {"_id": 0, "value": "$_id"}}
                    ],
                    "severities": [
                        {"$group": {"_id": "$severity"}},
                        {"$sort": {"_id": 1}},
                        {"$project": {"_id": 0, "value": "$_id"}}
                    ],
                    "years": [
                        {"$match": {"case_date": {"$exists": True}}},
                        {"$limit": 100000},
                        {"$project": {
                            "year_str": {"$substr": ["$case_date", 0, 4]}
                        }},
                        {"$group": {"_id": "$year_str"}},
                        {"$sort": {"_id": -1}},
                        {"$project": {"_id": 0, "value": "$_id"}}
                    ]
                }
            }
        ]
        
        results = await db.cases.aggregate(pipeline).to_list(1)
        
        if results:
            result = results[0]
            
            # Extract and validate years
            years = []
            for item in result["years"]:
                try:
                    year_val = int(item["value"])
                    if 2000 <= year_val <= 2030:
                        years.append(year_val)
                except (ValueError, TypeError):
                    pass
            
            response = {
                "counties": sorted([item["value"] for item in result["counties"] if item["value"]]),
                "abuse_types": sorted([item["value"] for item in result["abuse_types"] if item["value"]]),
                "statuses": sorted([item["value"] for item in result["statuses"] if item["value"]]),
                "severities": sorted([item["value"] for item in result["severities"] if item["value"]]),
                "years": sorted(years, reverse=True)
            }
        else:
            response = {
                "counties": [],
                "abuse_types": [],
                "statuses": [],
                "severities": [],
                "years": []
            }
        
        # Cache for longer since filters don't change often
        if settings.ENABLE_QUERY_CACHE:
            cache.set(cache_key, response, ttl=600)  # 10 minutes
        
        logger.info(f"Available filters retrieved by {current_user.user_id}")
        return response
        
    except Exception as e:
        logger.error(f"Error getting available filters: {e}")
        raise
