from fastapi import APIRouter, HTTPException, status, Depends, Query
from datetime import datetime, timezone
from bson import ObjectId
from typing import Optional
from app.db.client import get_database
from app.db.models import CaseCreate, CaseResponse, CaseStatus, CaseUpdate
from app.core.security import admin_or_member, admin_required, any_authenticated, get_current_user, TokenData
from app.core.logging import logger
from app.utils.severity_mapping import get_severity_aggregation_stage
from app.utils.date_filters import build_date_filter

router = APIRouter(prefix="/cases", tags=["Cases"])


def _prepare_case_response(case: dict) -> dict:
    """Prepare case document for CaseResponse model validation"""
    # Convert _id to string
    case["_id"] = str(case["_id"])
    
    # Ensure case_id is string
    if "case_id" in case and not isinstance(case["case_id"], str):
        case["case_id"] = str(case["case_id"])
    
    # Provide default severity if missing
    if "severity" not in case or case["severity"] is None:
        case["severity"] = "unknown"
    
    return case


@router.post("", response_model=CaseResponse, status_code=201)
async def create_case(
    case_data: CaseCreate,
    current_user: TokenData = Depends(admin_or_member),
    db=Depends(get_database)
):
    """Create a new case (Admin & Member only)"""
    case_doc = case_data.dict()
    case_doc["status"] = CaseStatus.OPEN.value
    case_doc["created_by"] = ObjectId(current_user.user_id)
    case_doc["created_at"] = datetime.now(timezone.utc)
    case_doc["updated_at"] = datetime.now(timezone.utc)

    if case_data.latitude and case_data.longitude:
        case_doc["location"] = {
            "type": "Point",
            "coordinates": [case_data.longitude, case_data.latitude]
        }

    result = await db.cases.insert_one(case_doc)
    case_doc["_id"] = result.inserted_id

    logger.info(f"Case created: {case_doc.get('case_id')}")
    return CaseResponse(**_prepare_case_response(case_doc))


@router.get("", response_model=dict)
async def list_cases(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000, description="Max 1000 per page for performance"),
    county: Optional[str] = None,
    abuse_type: Optional[str] = None,
    status_filter: Optional[CaseStatus] = Query(None, alias="status"),
    auto_sync_kenya: bool = Query(False, description="Auto-sync Kenya API data if stale"),
    include_kenya_metadata: bool = Query(False, description="Include Kenya API metadata"),
    include_demographics: bool = Query(False, description="Include demographics analysis"),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """
    List cases with filtering (All authenticated users)
    
    Performance optimizations:
    - Kenya API metadata is now opt-in (default=False)
    - Demographics are opt-in (default=False)
    - Single aggregation query for count + data
    - ObjectId conversion done in projection
    """
    from app.services.case_service import CaseService
    
    case_service = CaseService(db)
    result = await case_service.list_cases(
        page=page,
        limit=limit,
        county=county,
        abuse_type=abuse_type,
        status_filter=status_filter.value if status_filter else None,
        date_from=date_from,
        date_to=date_to,
        include_kenya_data=include_kenya_metadata,
        auto_sync_kenya=auto_sync_kenya
    )
    
    # Add demographics if requested
    if include_demographics:
        demographics = await _calculate_demographics(
            db=db,
            date_from=date_from,
            date_to=date_to,
            status=status_filter.value if status_filter else None,
            county=county
        )
        result["demographics"] = demographics
    
    logger.info(f"Cases listed: {result['total']} total, page {page}")
    return result


@router.get("/stats/summary", response_model=dict)
async def case_statistics(
    county: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get case statistics (All authenticated users)"""
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
                    {"$sort": {"count": -1}}
                ],
                "by_status": [
                    {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                ],
                "by_severity": [
                    {"$group": {"_id": "$derived_severity", "count": {"$sum": 1}}}
                ]
            }
        }
    ]

    results = await db.cases.aggregate(pipeline).to_list(None)
    return results[0] if results else {}


@router.get("/statistics")
async def get_case_statistics(
    include_kenya: bool = Query(True, description="Include Kenya API metadata"),
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get comprehensive case statistics (All authenticated users)"""
    from app.services.case_service import CaseService
    
    case_service = CaseService(db)
    stats = await case_service.get_case_statistics(include_kenya=include_kenya)
    
    logger.info(f"Case statistics retrieved by user {current_user.user_id}")
    
    return stats


@router.get("/{case_id}", response_model=CaseResponse)
async def get_case(
    case_id: str,
    current_user: TokenData = Depends(any_authenticated),
    db=Depends(get_database)
):
    """Get single case by ID (All authenticated users)"""
    logger.info(f"Attempting to retrieve case: {case_id}")
    
    # Try to find by MongoDB _id first
    try:
        case = await db.cases.find_one({"_id": ObjectId(case_id)})
        if case:
            logger.info(f"Case found by ObjectId: {case_id}")
            case.pop("child_age", None)
            return _prepare_case_response(case)
    except Exception as e:
        logger.debug(f"Failed to find by ObjectId: {e}")
    
    # If not found or invalid ObjectId, try to find by case_id field as string
    logger.info(f"Trying to find case by case_id field (string): {case_id}")
    case = await db.cases.find_one({"case_id": case_id})
    
    if not case:
        # Try as integer if the case_id is numeric
        if case_id.isdigit():
            logger.info(f"Trying to find case by case_id field (integer): {case_id}")
            case = await db.cases.find_one({"case_id": int(case_id)})
    
    if not case:
        logger.warning(f"Case not found with ObjectId, string case_id, or int case_id: {case_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Case not found"
        )

    logger.info(f"Case found by case_id field: {case_id}")
    case.pop("child_age", None)
    return _prepare_case_response(case)


@router.put("/{case_id}", response_model=CaseResponse)
async def update_case(
    case_id: str,
    case_update: CaseUpdate,
    current_user: TokenData = Depends(admin_or_member),
    db=Depends(get_database)
):
    """Update case (Admin & Member only)"""
    # Try to find by MongoDB _id or case_id field
    case_query = None
    existing_case = None
    
    try:
        case_query = {"_id": ObjectId(case_id)}
        existing_case = await db.cases.find_one(case_query)
    except:
        pass
    
    if not existing_case:
        # Try by case_id as string
        case_query = {"case_id": case_id}
        existing_case = await db.cases.find_one(case_query)
    
    if not existing_case and case_id.isdigit():
        # Try by case_id as integer
        case_query = {"case_id": int(case_id)}
        existing_case = await db.cases.find_one(case_query)
    
    if not existing_case:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Case not found"
        )

    update_data = case_update.dict(exclude_unset=True)
    update_data["updated_at"] = datetime.now(timezone.utc)

    if update_data:
        await db.cases.update_one(
            case_query,
            {"$set": update_data}
        )
    
    result = await db.cases.find_one(case_query)

    logger.info(f"Case updated: {case_id}")
    return CaseResponse(**_prepare_case_response(result))


@router.delete("/{case_id}")
async def delete_case(
    case_id: str,
    current_user: TokenData = Depends(admin_required),
    db=Depends(get_database)
):
    """Delete case (Admin only)"""
    # Try to find by MongoDB _id or case_id field
    case_query = None
    try:
        case_query = {"_id": ObjectId(case_id)}
        result = await db.cases.delete_one(case_query)
        if result.deleted_count > 0:
            logger.info(f"Case deleted: {case_id}")
            return {"message": "Case deleted successfully"}
    except:
        pass
    
    # Try by case_id as string
    case_query = {"case_id": case_id}
    result = await db.cases.delete_one(case_query)
    
    if result.deleted_count == 0 and case_id.isdigit():
        # Try by case_id as integer
        case_query = {"case_id": int(case_id)}
        result = await db.cases.delete_one(case_query)

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Case not found"
        )

    logger.info(f"Case deleted: {case_id}")
    return {"message": "Case deleted successfully"}


@router.post("/sync-kenya-data")
async def sync_kenya_api_data(
    county: Optional[str] = None,
    sub_county: Optional[str] = None,
    case_category: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Manually sync Kenya Child Protection API data
    
    This endpoint:
    - Fetches latest data from https://data.childprotection.go.ke/api/v2/cld/
    - Automatically integrates with existing cases
    - Supports filtering by county, sub-county, and case category
    - Useful for refreshing data on-demand
    """
    from app.services.case_service import CaseService
    
    case_service = CaseService(db)
    
    filters = {}
    if county:
        filters["county"] = county
    if sub_county:
        filters["sub_county"] = sub_county
    if case_category:
        filters["case_category"] = case_category
    
    result = await case_service.sync_kenya_api_data(filters if filters else None)
    
    logger.info(f"Kenya API sync triggered by user {current_user.user_id}")
    
    return {
        "message": "Kenya API data sync completed",
        "details": result
    }


async def _calculate_demographics(db, date_from: Optional[str], date_to: Optional[str], status: Optional[str], county: Optional[str]):
    """Calculate demographics analysis with optimized single aggregation pipeline"""
    filters = {}
    
    # Use centralized date filter utility
    date_filters = build_date_filter(date_from, date_to)
    filters.update(date_filters)
    
    if status:
        filters["status"] = status
    
    if county:
        filters["county"] = county
    
    # Single aggregation pipeline for all demographics
    pipeline = [
        {"$match": filters},
        {
            "$facet": {
                "total": [{"$count": "count"}],
                "active": [
                    {"$match": {"status": {"$in": ["open", "in_progress", "active"]}}},
                    {"$count": "count"}
                ],
                "by_gender": [
                    {
                        "$group": {
                            "_id": {"$ifNull": ["$sex", {"$ifNull": ["$victim_sex", "$Sex"]}]},
                            "count": {"$sum": 1}
                        }
                    }
                ],
                "by_age": [
                    {
                        "$group": {
                            "_id": {"$ifNull": ["$age_range", {"$ifNull": ["$victim_age_range", "$Age Range"]}]},
                            "count": {"$sum": 1}
                        }
                    }
                ],
                "distribution": [
                    {
                        "$group": {
                            "_id": {
                                "age_band": {"$ifNull": ["$age_range", {"$ifNull": ["$victim_age_range", "$Age Range"]}]},
                                "sex": {"$ifNull": ["$sex", {"$ifNull": ["$victim_sex", "$Sex"]}]}
                            },
                            "count": {"$sum": 1}
                        }
                    }
                ]
            }
        }
    ]
    
    results = await db.cases.aggregate(pipeline).to_list(1)
    
    if not results:
        return _empty_demographics()
    
    data = results[0]
    
    # Process results
    total_cases = data["total"][0]["count"] if data["total"] else 0
    active_cases = data["active"][0]["count"] if data["active"] else 0
    
    # Gender breakdown
    female_count = 0
    male_count = 0
    for result in data.get("by_gender", []):
        sex = result["_id"]
        count = result["count"]
        if sex and sex.lower() in ["female", "f"]:
            female_count = count
        elif sex and sex.lower() in ["male", "m"]:
            male_count = count
    
    female_share_pct = (female_count / total_cases * 100) if total_cases > 0 else 0
    male_share_pct = (male_count / total_cases * 100) if total_cases > 0 else 0
    
    # High risk age count
    high_risk_count = 0
    for result in data.get("by_age", []):
        age_range = result["_id"]
        count = result["count"]
        if age_range and _is_high_risk_age(age_range):
            high_risk_count += count
    
    # Distribution by sex and age
    age_bands_dict = {}
    for result in data.get("distribution", []):
        age_band = result["_id"].get("age_band") or "Unknown"
        sex = result["_id"].get("sex") or "Unknown"
        count = result["count"]
        
        normalized_band = _normalize_age_band(age_band)
        
        if normalized_band not in age_bands_dict:
            age_bands_dict[normalized_band] = {
                "ageBand": normalized_band,
                "male": 0,
                "female": 0,
                "unknown": 0
            }
        
        if sex and sex.lower() in ["male", "m"]:
            age_bands_dict[normalized_band]["male"] += count
        elif sex and sex.lower() in ["female", "f"]:
            age_bands_dict[normalized_band]["female"] += count
        else:
            age_bands_dict[normalized_band]["unknown"] += count
    
    distribution_by_sex_age = sorted(
        age_bands_dict.values(),
        key=lambda x: _age_band_sort_key(x["ageBand"])
    )
    
    return {
        "summary": {
            "totalCases": total_cases,
            "totalDeltaPct": 0,
            "activeCases": active_cases,
            "femaleMinors": female_count,
            "femaleSharePct": round(female_share_pct, 2),
            "maleMinors": male_count,
            "maleSharePct": round(male_share_pct, 2),
            "highRiskAge0to5": high_risk_count,
            "highRiskDeltaPct": 0
        },
        "distributionBySexAge": distribution_by_sex_age
    }


def _empty_demographics():
    """Return empty demographics structure"""
    return {
        "summary": {
            "totalCases": 0,
            "totalDeltaPct": 0,
            "activeCases": 0,
            "femaleMinors": 0,
            "femaleSharePct": 0,
            "maleMinors": 0,
            "maleSharePct": 0,
            "highRiskAge0to5": 0,
            "highRiskDeltaPct": 0
        },
        "distributionBySexAge": []
    }


def _is_high_risk_age(age_range: str) -> bool:
    """Check if age range falls into high-risk category (0-5 years)"""
    if not age_range:
        return False
    
    age_range_lower = age_range.lower().strip()
    high_risk_patterns = ["0-5", "0-4", "1-5", "<5", "under 5", "0 to 5", "0-3", "3-5"]
    
    for pattern in high_risk_patterns:
        if pattern in age_range_lower:
            return True
    
    return False


def _normalize_age_band(age_range: str) -> str:
    """Normalize age range to standard bands"""
    if not age_range or age_range == "Unknown":
        return "Unknown"
    
    age_range_lower = age_range.lower().strip()
    
    if any(x in age_range_lower for x in ["0-5", "0-4", "1-5", "0 to 5", "0-3", "3-5", "<5", "under 5"]):
        return "0-5"
    elif any(x in age_range_lower for x in ["6-9", "5-9", "6-10"]):
        return "6-9"
    elif any(x in age_range_lower for x in ["10-14", "10-13"]):
        return "10-14"
    elif any(x in age_range_lower for x in ["15-17", "15-18", "15-19"]):
        return "15-17"
    elif any(x in age_range_lower for x in ["18+", "18-", "18 and above", ">18"]):
        return "18+"
    
    return age_range


def _age_band_sort_key(age_band: str) -> int:
    """Get sort key for age band"""
    order = {
        "0-5": 0,
        "6-9": 1,
        "10-14": 2,
        "15-17": 3,
        "18+": 4,
        "Unknown": 999
    }
    
    return order.get(age_band, 500)
