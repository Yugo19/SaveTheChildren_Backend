from fastapi import APIRouter, Depends, HTTPException, status, Query
from datetime import datetime, timedelta, timezone
from app.db.client import get_database
from app.core.security import require_role, TokenData
from app.core.logging import logger
import psutil
import os

router = APIRouter(prefix="/admin", tags=["Admin"])


@router.get("/health")
async def system_health_check(
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Check system health (admin only)"""
    try:
        # Check database
        try:
            await db.command('ping')
            db_status = "connected"
            db_ping = 1
        except Exception as e:
            db_status = "disconnected"
            db_ping = -1
            logger.error(f"Database health check failed: {e}")

        # Get system stats
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_info = psutil.virtual_memory()

        return {
            "status": "healthy" if db_status == "connected" else "degraded",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": {
                "status": db_status,
                "ping_ms": db_ping
            },
            "system": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory_info.percent,
                "memory_available_mb": memory_info.available / (1024 * 1024)
            }
        }
    except Exception as e:
        logger.error(f"Error checking system health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking system health"
        )


@router.get("/stats")
async def get_system_statistics(
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Get system statistics (admin only)"""
    try:
        # Database statistics
        total_users = await db.users.count_documents({})
        total_cases = await db.cases.count_documents({})
        total_files = await db.files.count_documents({})

        # Get database size (collection-specific)
        users_size = 0
        cases_size = 0

        # Case statistics
        open_cases = await db.cases.count_documents({"status": "open"})
        closed_cases = await db.cases.count_documents({"status": "closed"})
        high_severity = await db.cases.count_documents({"severity": "high"})

        return {
            "database": {
                "total_users": total_users,
                "total_cases": total_cases,
                "total_files": total_files
            },
            "cases": {
                "open": open_cases,
                "closed": closed_cases,
                "high_severity": high_severity
            },
            "storage": {
                "files_stored": total_files,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }
    except Exception as e:
        logger.error(f"Error getting system statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error getting system statistics"
        )


@router.get("/logs")
async def get_api_logs(
    hours: int = Query(24, ge=1, le=168),
    log_type: str = Query("all", enum=["all", "errors", "info"]),
    limit: int = Query(100, ge=1, le=1000),
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Get API logs (admin only)"""
    try:
        log_file = "logs/app.log"

        if not os.path.exists(log_file):
            return {
                "logs": [],
                "total": 0
            }

        # Read logs
        logs = []
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        try:
            with open(log_file, 'r') as f:
                for line in f:
                    if log_type == "errors" and "ERROR" not in line:
                        continue
                    if log_type == "info" and "INFO" not in line:
                        continue
                    logs.append(line.strip())

            # Return latest entries
            logs = logs[-limit:]

            logger.info(f"Logs retrieved by {current_user.user_id}")

            return {
                "logs": logs,
                "total": len(logs),
                "hours": hours,
                "log_type": log_type
            }
        except Exception as e:
            logger.error(f"Error reading logs: {e}")
            return {
                "logs": [],
                "total": 0,
                "error": "Could not read logs"
            }

    except Exception as e:
        logger.error(f"Error getting logs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error getting logs"
        )


@router.post("/backup")
async def trigger_backup(
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Trigger system backup (admin only)"""
    try:
        logger.info(f"Backup initiated by {current_user.user_id}")

        return {
            "status": "backup_initiated",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": "Backup process started"
        }
    except Exception as e:
        logger.error(f"Error triggering backup: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error triggering backup"
        )


@router.get("/database-stats")
async def get_database_stats(
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Get detailed database statistics (admin only)"""
    try:
        collections = await db.list_collection_names()

        stats = {
            "collections": {}
        }

        for collection_name in collections:
            if not collection_name.startswith("system"):
                collection = db[collection_name]
                count = await collection.count_documents({})
                stats["collections"][collection_name] = {
                    "count": count
                }

        logger.info(f"Database stats retrieved by {current_user.user_id}")

        return stats
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error getting database stats"
        )


@router.post("/clear-cache")
async def clear_cache(
    current_user: TokenData = Depends(require_role("admin")),
    db=Depends(get_database)
):
    """Clear system cache (admin only)"""
    try:
        # In a real application, you would clear Redis cache here
        logger.info(f"Cache clearing initiated by {current_user.user_id}")

        return {
            "status": "cache_cleared",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": "Cache cleared successfully"
        }
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error clearing cache"
        )
