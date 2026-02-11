from fastapi import APIRouter, Depends
from app.db.client import get_database
from app.services.chatbot_service import ChatbotService
from app.services.scraping_service import ScrapingService
from app.services.kenya_api_service import KenyaAPIService
from app.core.security import get_current_user, TokenData, admin_required
from app.core.logging import logger
from datetime import datetime, timezone, timedelta
from typing import Dict

router = APIRouter(prefix="/monitoring", tags=["Monitoring"])


@router.get("/dashboard")
async def get_monitoring_dashboard(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Comprehensive monitoring dashboard showing:
    - Chatbot health and token usage
    - Scraping system status
    - Kenya API data status
    - Database statistics
    - System health indicators
    """
    try:
        dashboard = {}
        
        # Chatbot Status
        chatbot_service = ChatbotService(db)
        dashboard["chatbot"] = await chatbot_service.get_chatbot_health()
        
        # Token Usage (last 7 days)
        token_stats = await chatbot_service.get_token_usage_stats(current_user.user_id)
        dashboard["token_usage"] = {
            "total_tokens": token_stats["totals"].get("total_tokens", 0),
            "total_requests": token_stats["totals"].get("total_requests", 0),
            "last_7_days": token_stats["daily_usage"][:7] if token_stats["daily_usage"] else []
        }
        
        # Scraping Status
        scraping_service = ScrapingService(db)
        scraping_stats = await scraping_service.get_scraping_stats(current_user.user_id)
        dashboard["scraping"] = scraping_stats
        
        # Kenya API Status
        kenya_service = KenyaAPIService(db)
        kenya_status = await kenya_service.get_latest_import_status()
        dashboard["kenya_api"] = kenya_status
        
        # Database Statistics
        dashboard["database"] = {
            "total_cases": await db.cases.count_documents({}),
            "total_users": await db.users.count_documents({}),
            "total_reports": await db.reports.count_documents({}),
            "total_conversations": await db.conversations.count_documents({})
        }
        
        # System timestamp
        dashboard["timestamp"] = datetime.now(timezone.utc)
        dashboard["status"] = "healthy"
        
        logger.info(f"Monitoring dashboard accessed by {current_user.user_id}")
        
        return dashboard
        
    except Exception as e:
        logger.error(f"Error generating monitoring dashboard: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        }


@router.get("/system-health")
async def get_system_health(
    current_user: TokenData = Depends(admin_required),
    db=Depends(get_database)
):
    """
    Detailed system health check (Admin only)
    
    Returns:
    - Database connection status
    - Collection sizes
    - Recent activity metrics
    - Error rates
    """
    try:
        health = {
            "timestamp": datetime.now(timezone.utc),
            "status": "healthy"
        }
        
        # Database collections health
        collections = {
            "users": await db.users.count_documents({}),
            "cases": await db.cases.count_documents({}),
            "reports": await db.reports.count_documents({}),
            "conversations": await db.conversations.count_documents({}),
            "messages": await db.messages.count_documents({}),
            "scraping_jobs": await db.scraping_jobs.count_documents({}),
            "scraping_results": await db.scraping_results.count_documents({}),
            "kenya_api_data": await db.kenya_api_data.count_documents({}),
            "token_usage": await db.token_usage.count_documents({})
        }
        health["collections"] = collections
        
        # Recent activity (last 24 hours)
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        
        recent_activity = {
            "new_cases": await db.cases.count_documents({"created_at": {"$gte": yesterday}}),
            "new_conversations": await db.conversations.count_documents({"created_at": {"$gte": yesterday}}),
            "scraping_runs": await db.scraping_results.count_documents({"timestamp": {"$gte": yesterday}}),
            "messages_sent": await db.messages.count_documents({"timestamp": {"$gte": yesterday}})
        }
        health["recent_activity"] = recent_activity
        
        # Error rates
        total_scraping = await db.scraping_results.count_documents({})
        failed_scraping = await db.scraping_results.count_documents({"status": "failed"})
        
        health["error_rates"] = {
            "scraping_error_rate": (failed_scraping / total_scraping * 100) if total_scraping > 0 else 0
        }
        
        logger.info(f"System health check by admin {current_user.user_id}")
        
        return health
        
    except Exception as e:
        logger.error(f"Error in system health check: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        }


@router.get("/token-limits")
async def get_token_limits(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Get token usage limits and subscription status
    
    Returns:
    - Current token usage
    - Monthly limits
    - Usage percentage
    - Days until reset
    """
    try:
        # Get current month usage
        now = datetime.now(timezone.utc)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        pipeline = [
            {
                "$match": {
                    "user_id": current_user.user_id if hasattr(current_user, 'user_id') else None,
                    "timestamp": {"$gte": month_start}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_tokens": {"$sum": "$tokens"}
                }
            }
        ]
        
        result = await db.token_usage.aggregate(pipeline).to_list(1)
        current_usage = result[0]["total_tokens"] if result else 0
        
        # Define limits (these could be stored in user profile)
        monthly_limit = 1000000  # 1M tokens per month
        
        # Calculate days until reset
        if now.month == 12:
            next_month = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0)
        else:
            next_month = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0)
        
        days_until_reset = (next_month - now).days
        
        return {
            "current_usage": current_usage,
            "monthly_limit": monthly_limit,
            "usage_percentage": (current_usage / monthly_limit * 100) if monthly_limit > 0 else 0,
            "remaining_tokens": max(0, monthly_limit - current_usage),
            "days_until_reset": days_until_reset,
            "status": "healthy" if current_usage < monthly_limit * 0.9 else "warning"
        }
        
    except Exception as e:
        logger.error(f"Error getting token limits: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


@router.get("/data-freshness")
async def get_data_freshness(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Check freshness of data from various sources
    
    Returns timestamps of last updates for:
    - Kenya API data
    - Scraped data
    - Case reports
    """
    try:
        freshness = {}
        
        # Kenya API data
        latest_kenya = await db.kenya_api_data.find_one(
            {},
            sort=[("fetched_at", -1)]
        )
        if latest_kenya:
            freshness["kenya_api"] = {
                "last_update": latest_kenya["fetched_at"],
                "age_hours": (datetime.now(timezone.utc) - latest_kenya["fetched_at"]).total_seconds() / 3600,
                "record_count": latest_kenya.get("record_count", 0)
            }
        
        # Scraped data
        latest_scrape = await db.scraping_results.find_one(
            {"status": "success"},
            sort=[("timestamp", -1)]
        )
        if latest_scrape:
            freshness["web_scraping"] = {
                "last_update": latest_scrape["timestamp"],
                "age_hours": (datetime.now(timezone.utc) - latest_scrape["timestamp"]).total_seconds() / 3600
            }
        
        # Case reports
        latest_case = await db.cases.find_one(
            {},
            sort=[("created_at", -1)]
        )
        if latest_case:
            freshness["cases"] = {
                "last_update": latest_case["created_at"],
                "age_hours": (datetime.now(timezone.utc) - latest_case["created_at"]).total_seconds() / 3600
            }
        
        freshness["timestamp"] = datetime.now(timezone.utc)
        
        return freshness
        
    except Exception as e:
        logger.error(f"Error checking data freshness: {e}")
        return {
            "status": "error",
            "error": str(e)
        }
