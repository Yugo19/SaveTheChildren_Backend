"""
Background tasks for automated data fetching and processing
"""
from app.core.logging import logger
from app.services.scraping_service import ScrapingService
from app.services.kenya_api_service import KenyaAPIService
from app.db.client import mongodb_client
import asyncio
from datetime import datetime, timezone


async def run_scheduled_scrapers():
    """Run all scraping jobs that are due"""
    try:
        db = mongodb_client.db
        scraping_service = ScrapingService(db)
        
        # Get jobs due for execution
        jobs = await scraping_service.get_jobs_due_for_run()
        
        logger.info(f"Found {len(jobs)} scraping jobs due for execution")
        
        for job in jobs:
            try:
                logger.info(f"Running scheduled scraping job: {job['job_id']}")
                await scraping_service.run_scraping_job(
                    job['job_id'],
                    str(job['user_id'])
                )
            except Exception as e:
                logger.error(f"Error running scheduled job {job['job_id']}: {e}")
                
    except Exception as e:
        logger.error(f"Error in run_scheduled_scrapers: {e}")


async def refresh_kenya_api_data():
    """Refresh Kenya API data daily"""
    try:
        db = mongodb_client.db
        kenya_service = KenyaAPIService(db)
        
        logger.info("Refreshing Kenya API data")
        
        result = await kenya_service.fetch_and_store_data(force_refresh=True)
        
        logger.info(f"Kenya API refresh completed: {result}")
        
    except Exception as e:
        logger.error(f"Error refreshing Kenya API data: {e}")


async def cleanup_old_data():
    """Clean up old data to save space"""
    try:
        db = mongodb_client.db
        
        # Delete scraping results older than 90 days
        from datetime import timedelta
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
        
        result = await db.scraping_results.delete_many({
            "timestamp": {"$lt": cutoff_date}
        })
        
        logger.info(f"Cleaned up {result.deleted_count} old scraping results")
        
        # Delete old token usage records (keep last 6 months)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=180)
        result = await db.token_usage.delete_many({
            "timestamp": {"$lt": cutoff_date}
        })
        
        logger.info(f"Cleaned up {result.deleted_count} old token usage records")
        
    except Exception as e:
        logger.error(f"Error in cleanup_old_data: {e}")


async def scheduler_loop():
    """Main scheduler loop - runs periodic tasks"""
    logger.info("Background scheduler started")
    
    last_hourly = datetime.now(timezone.utc)
    last_daily = datetime.now(timezone.utc)
    
    while True:
        try:
            now = datetime.now(timezone.utc)
            
            # Run hourly tasks
            if (now - last_hourly).total_seconds() >= 3600:
                logger.info("Running hourly tasks")
                await run_scheduled_scrapers()
                last_hourly = now
            
            # Run daily tasks (at 2 AM)
            if now.hour == 2 and (now - last_daily).total_seconds() >= 86400:
                logger.info("Running daily tasks")
                await refresh_kenya_api_data()
                await cleanup_old_data()
                last_daily = now
            
            # Sleep for 5 minutes before next check
            await asyncio.sleep(300)
            
        except Exception as e:
            logger.error(f"Error in scheduler loop: {e}")
            await asyncio.sleep(60)


def start_background_tasks():
    """Start background task scheduler"""
    try:
        asyncio.create_task(scheduler_loop())
        logger.info("Background tasks initialized")
    except Exception as e:
        logger.error(f"Failed to start background tasks: {e}")
