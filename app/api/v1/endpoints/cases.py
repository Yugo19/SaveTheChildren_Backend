from fastapi import APIRouter, HTTPException, status, Depends, Query
from datetime import datetime, timezone
from bson import ObjectId
from typing import Optional
from app.db.client import get_database
from app.db.models import CaseCreate, CaseResponse, CaseStatus, CaseUpdate
from app.core.security import get_current_user, require_role, TokenData
from app.core.logging import logger

router = APIRouter(prefix="/cases", tags=["Cases"])


@router.post("", response_model=CaseResponse, status_code=201)
async def create_case(
    case_data: CaseCreate,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Create a new case"""
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
    return CaseResponse(**{**case_doc, "_id": str(case_doc["_id"])})


@router.get("", response_model=dict)
async def list_cases(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
    county: Optional[str] = None,
    abuse_type: Optional[str] = None,
    status_filter: Optional[CaseStatus] = Query(None, alias="status"),
    auto_sync_kenya: bool = Query(False, description="Auto-sync Kenya API data if stale"),
    include_kenya_metadata: bool = Query(True, description="Include Kenya API metadata"),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    List cases with filtering
    
    Now includes Kenya Child Protection API data integration:
    - Cases from Kenya API are automatically included in results
    - Use auto_sync_kenya=true to refresh Kenya data if older than 24 hours
    - Kenya API metadata shows last sync time and data freshness
    """
    from app.services.case_service import CaseService
    
    case_service = CaseService(db)
    result = await case_service.list_cases(
        page=page,
        limit=limit,
        county=county,
        abuse_type=abuse_type,
        status_filter=status_filter.value if status_filter else None,
        include_kenya_data=include_kenya_metadata,
        auto_sync_kenya=auto_sync_kenya
    )
    
    # Convert ObjectIds to strings for JSON serialization
    for case in result["cases"]:
        case["_id"] = str(case["_id"])
        if "created_by" in case:
            case["created_by"] = str(case["created_by"])
    
    logger.info(f"Cases listed: {result['total']} total, page {page}")
    return result


@router.get("/{case_id}", response_model=CaseResponse)
async def get_case(
    case_id: str,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get single case by ID"""
    try:
        case = await db.cases.find_one({"_id": ObjectId(case_id)})
    except:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid case ID"
        )

    if not case:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Case not found"
        )

    return CaseResponse(**{**case, "_id": str(case["_id"])})


@router.put("/{case_id}", response_model=CaseResponse)
async def update_case(
    case_id: str,
    case_update: CaseUpdate,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Update case"""
    try:
        case_id_obj = ObjectId(case_id)
    except:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid case ID"
        )

    update_data = case_update.dict(exclude_unset=True)
    update_data["updated_at"] = datetime.now(timezone.utc)

    if update_data:
        result = await db.cases.find_one_and_update(
            {"_id": case_id_obj},
            {"$set": update_data},
            return_document=True
        )
    else:
        result = await db.cases.find_one({"_id": case_id_obj})

    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Case not found"
        )

    logger.info(f"Case updated: {case_id}")
    return CaseResponse(**{**result, "_id": str(result["_id"])})


@router.delete("/{case_id}")
async def delete_case(
    case_id: str,
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Delete case (admin only)"""
    try:
        case_id_obj = ObjectId(case_id)
    except:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid case ID"
        )

    result = await db.cases.delete_one({"_id": case_id_obj})

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Case not found"
        )

    logger.info(f"Case deleted: {case_id}")
    return {"message": "Case deleted successfully"}


@router.get("/stats/summary", response_model=dict)
async def case_statistics(
    county: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get case statistics"""
    filters = {}
    if county:
        filters["county"] = county
    if date_from or date_to:
        date_filter = {}
        if date_from:
            date_filter["$gte"] = datetime.fromisoformat(date_from)
        if date_to:
            date_filter["$lte"] = datetime.fromisoformat(date_to)
        filters["date_reported"] = date_filter

    pipeline = [
        {"$match": filters},
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
                ]
            }
        }
    ]

    results = await db.cases.aggregate(pipeline).to_list(None)
    return results[0] if results else {}


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


@router.get("/statistics")
async def get_case_statistics(
    include_kenya: bool = Query(True, description="Include Kenya API metadata"),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Get comprehensive case statistics
    
    Returns:
    - Total cases count
    - Cases by county (top 10)
    - Cases by abuse type
    - Cases by source (local vs Kenya API)
    - Cases by status
    - Kenya API metadata (last sync, data freshness)
    """
    from app.services.case_service import CaseService
    
    case_service = CaseService(db)
    stats = await case_service.get_case_statistics(include_kenya=include_kenya)
    
    logger.info(f"Case statistics retrieved by user {current_user.user_id}")
    
    return stats
