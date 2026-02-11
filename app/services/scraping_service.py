from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from datetime import datetime, timezone, timedelta
from app.core.logging import logger
from typing import Optional, Dict, List
import uuid
import asyncio
import re

try:
    import requests
    from bs4 import BeautifulSoup
    import aiohttp
except ImportError:
    logger.warning("Scraping dependencies not installed.")


class ScrapingService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.scraping_jobs_collection = db.scraping_jobs
        self.scraping_results_collection = db.scraping_results

    async def create_scraping_job(
        self,
        url: str,
        selectors: dict,
        user_id: str,
        job_name: str = None,
        schedule: str = None,
        target_type: str = "general"
    ) -> dict:
        """Create a new web scraping job with enhanced scheduling"""
        try:
            job_id = str(uuid.uuid4())
            
            # Validate URL
            if not url.startswith(('http://', 'https://')):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="URL must start with http:// or https://"
                )
            
            # Calculate next run time based on schedule
            next_run = self._calculate_next_run(schedule or "manual")
            
            job_doc = {
                "job_id": job_id,
                "url": url,
                "selectors": selectors,
                "user_id": ObjectId(user_id),
                "job_name": job_name or f"Scrape: {url}",
                "status": "pending",
                "schedule": schedule or "manual",
                "target_type": target_type,  # general, news, child_violence_indicators
                "created_at": datetime.now(timezone.utc),
                "last_run": None,
                "next_run": next_run,
                "run_count": 0,
                "success_count": 0,
                "error_count": 0,
                "last_error": None,
                "enabled": True
            }
            
            await self.scraping_jobs_collection.insert_one(job_doc)
            logger.info(f"Scraping job created: {job_id} for URL: {url}, Schedule: {schedule}")
            
            return {
                "job_id": job_id,
                "url": url,
                "job_name": job_doc["job_name"],
                "status": "pending",
                "schedule": schedule or "manual",
                "next_run": next_run,
                "created_at": job_doc["created_at"]
            }
        except Exception as e:
            logger.error(f"Error creating scraping job: {e}")
            raise

    async def run_scraping_job(self, job_id: str, user_id: str):
        """Execute a web scraping job"""
        try:
            job = await self.scraping_jobs_collection.find_one({
                "job_id": job_id,
                "user_id": ObjectId(user_id)
            })
            
            if not job:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Scraping job not found"
                )
            
            await self.scraping_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {"status": "running"}}
            )
            
            try:
                # Scrape the website
                scraped_data = await self._scrape_website(job["url"], job["selectors"])
                
                result_doc = {
                    "result_id": str(uuid.uuid4()),
                    "job_id": job_id,
                    "url": job["url"],
                    "data": scraped_data,
                    "timestamp": datetime.now(timezone.utc),
                    "status": "success",
                    "item_count": len(self._flatten_data(scraped_data))
                }
            except Exception as e:
                logger.error(f"Error scraping website {job['url']}: {e}")
                result_doc = {
                    "result_id": str(uuid.uuid4()),
                    "job_id": job_id,
                    "url": job["url"],
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc),
                    "status": "failed",
                    "item_count": 0
                }
            
            # Store the result
            await self.scraping_results_collection.insert_one(result_doc)
            
            # Calculate next run time
            next_run = self._calculate_next_run(job.get("schedule", "manual"))
            
            # Update job status
            update_data = {
                "$set": {
                    "status": result_doc["status"],
                    "last_run": datetime.now(timezone.utc),
                    "next_run": next_run
                },
                "$inc": {
                    "run_count": 1,
                    f"{'success_count' if result_doc['status'] == 'success' else 'error_count'}": 1
                }
            }
            
            # Store last error if failed
            if result_doc["status"] == "failed":
                update_data["$set"]["last_error"] = result_doc.get("error")
            
            await self.scraping_jobs_collection.update_one(
                {"job_id": job_id},
                update_data
            )
            
            logger.info(f"Scraping job completed: {job_id} - {result_doc['status']}")
            
            return {
                "result_id": result_doc["result_id"],
                "status": result_doc["status"],
                "item_count": result_doc["item_count"],
                "url": job["url"],
                "timestamp": result_doc["timestamp"],
                "next_run": next_run,
                "data": result_doc.get("data"),
                "error": result_doc.get("error")
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error running scraping job: {e}")
            raise

    async def list_scraping_jobs(self, user_id: str, page: int = 1, limit: int = 20):
        """List user's scraping jobs"""
        try:
            filters = {"user_id": ObjectId(user_id)}
            
            total = await self.scraping_jobs_collection.count_documents(filters)
            
            jobs = await self.scraping_jobs_collection.find(filters)\
                .skip((page - 1) * limit)\
                .limit(limit)\
                .sort("created_at", -1)\
                .to_list(limit)
            
            return {
                "total": total,
                "page": page,
                "limit": limit,
                "jobs": [
                    {
                        "job_id": j["job_id"],
                        "job_name": j["job_name"],
                        "url": j["url"],
                        "status": j["status"],
                        "run_count": j["run_count"],
                        "success_count": j.get("success_count", 0),
                        "error_count": j.get("error_count", 0),
                        "last_run": j["last_run"],
                        "created_at": j["created_at"],
                        "schedule": j.get("schedule", "manual")
                    }
                    for j in jobs
                ],
                "stats": {
                    "total_runs": sum(j.get("run_count", 0) for j in jobs),
                    "total_successes": sum(j.get("success_count", 0) for j in jobs),
                    "total_errors": sum(j.get("error_count", 0) for j in jobs),
                    "active_jobs": len([j for j in jobs if j.get("enabled", True)])
                }
            }
        except Exception as e:
            logger.error(f"Error listing scraping jobs: {e}")
            raise

    async def get_scraping_results(self, job_id: str, user_id: str, limit: int = 50):
        """Get results for a scraping job"""
        try:
            job = await self.scraping_jobs_collection.find_one({
                "job_id": job_id,
                "user_id": ObjectId(user_id)
            })
            
            if not job:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Scraping job not found"
                )
            
            results = await self.scraping_results_collection.find(
                {"job_id": job_id}
            ).sort("timestamp", -1).limit(limit).to_list(limit)
            
            return {
                "job_id": job_id,
                "url": job["url"],
                "job_name": job["job_name"],
                "total_results": len(results),
                "results": [
                    {
                        "result_id": r["result_id"],
                        "status": r["status"],
                        "item_count": r.get("item_count", 0),
                        "timestamp": r["timestamp"],
                        "data": r.get("data"),
                        "error": r.get("error")
                    }
                    for r in results
                ]
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting scraping results: {e}")
            raise

    async def delete_scraping_job(self, job_id: str, user_id: str):
        """Delete scraping job and all its results"""
        try:
            # Delete all results for this job
            await self.scraping_results_collection.delete_many(
                {"job_id": job_id}
            )
            
            # Delete the job
            result = await self.scraping_jobs_collection.delete_one({
                "job_id": job_id,
                "user_id": ObjectId(user_id)
            })
            
            if result.deleted_count == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Scraping job not found"
                )
            
            logger.info(f"Scraping job deleted: {job_id}")
            return True
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting scraping job: {e}")
            raise

    async def _scrape_website(self, url: str, selectors: dict) -> dict:
        """Scrape website and extract data using CSS selectors"""
        try:
            # Set headers to avoid blocking
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            scraped_data = {}
            for field_name, css_selector in selectors.items():
                try:
                    elements = soup.select(css_selector)
                    
                    if len(elements) == 1:
                        # Single element - return text
                        scraped_data[field_name] = elements[0].get_text(strip=True)
                    else:
                        # Multiple elements - return list
                        scraped_data[field_name] = [
                            elem.get_text(strip=True) for elem in elements
                        ]
                except Exception as e:
                    logger.warning(f"Error extracting selector {css_selector}: {e}")
                    scraped_data[field_name] = None
            
            logger.info(f"Successfully scraped {url}")
            return scraped_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error scraping {url}: {e}")
            raise Exception(f"Failed to fetch website: {str(e)}")
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            raise

    def _flatten_data(self, data: dict) -> list:
        """Flatten scraped data for counting items"""
        items = []
        for key, value in data.items():
            if isinstance(value, list):
                items.extend(value)
            elif value is not None:
                items.append(value)
        return items
    
    def _calculate_next_run(self, schedule: str) -> Optional[datetime]:
        """Calculate next run time based on schedule"""
        if schedule == "manual":
            return None
        
        now = datetime.now(timezone.utc)
        
        if schedule == "hourly":
            return now + timedelta(hours=1)
        elif schedule == "daily":
            return now + timedelta(days=1)
        elif schedule == "weekly":
            return now + timedelta(weeks=1)
        elif schedule == "monthly":
            return now + timedelta(days=30)
        else:
            return None
    
    async def get_jobs_due_for_run(self) -> List[Dict]:
        """Get all jobs that are due to run (for scheduled execution)"""
        try:
            now = datetime.now(timezone.utc)
            
            jobs = await self.scraping_jobs_collection.find({
                "enabled": True,
                "schedule": {"$ne": "manual"},
                "next_run": {"$lte": now}
            }).to_list(100)
            
            return jobs
            
        except Exception as e:
            logger.error(f"Error getting jobs due for run: {e}")
            raise
    
    async def toggle_job(self, job_id: str, user_id: str, enabled: bool) -> dict:
        """Enable or disable a scraping job"""
        try:
            job = await self.scraping_jobs_collection.find_one({
                "job_id": job_id,
                "user_id": ObjectId(user_id)
            })
            
            if not job:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Scraping job not found"
                )
            
            await self.scraping_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {"enabled": enabled}}
            )
            
            logger.info(f"Scraping job {job_id} {'enabled' if enabled else 'disabled'}")
            
            return {
                "job_id": job_id,
                "enabled": enabled,
                "message": f"Job {'enabled' if enabled else 'disabled'} successfully"
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error toggling scraping job: {e}")
            raise
    
    async def get_scraping_stats(self, user_id: str) -> dict:
        """Get comprehensive scraping statistics"""
        try:
            pipeline = [
                {"$match": {"user_id": ObjectId(user_id)}},
                {
                    "$group": {
                        "_id": None,
                        "total_jobs": {"$sum": 1},
                        "active_jobs": {
                            "$sum": {"$cond": [{"$eq": ["$enabled", True]}, 1, 0]}
                        },
                        "total_runs": {"$sum": "$run_count"},
                        "total_successes": {"$sum": "$success_count"},
                        "total_errors": {"$sum": "$error_count"}
                    }
                }
            ]
            
            result = await self.scraping_jobs_collection.aggregate(pipeline).to_list(1)
            
            if not result:
                return {
                    "total_jobs": 0,
                    "active_jobs": 0,
                    "total_runs": 0,
                    "total_successes": 0,
                    "total_errors": 0,
                    "success_rate": 0
                }
            
            stats = result[0]
            success_rate = (
                (stats["total_successes"] / stats["total_runs"] * 100)
                if stats["total_runs"] > 0
                else 0
            )
            
            return {
                "total_jobs": stats["total_jobs"],
                "active_jobs": stats["active_jobs"],
                "total_runs": stats["total_runs"],
                "total_successes": stats["total_successes"],
                "total_errors": stats["total_errors"],
                "success_rate": round(success_rate, 2)
            }
            
        except Exception as e:
            logger.error(f"Error getting scraping stats: {e}")
            raise
