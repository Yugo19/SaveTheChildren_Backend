from fastapi import APIRouter

from app.api.v1.endpoints import (
    auth, cases, users, analytics, files, data, admin, search, reports,
    chatbot, scraping, geospatial, kenya_api, monitoring, data_loader
)

router = APIRouter(prefix="/api/v1")

router.include_router(auth.router)
router.include_router(users.router)
router.include_router(cases.router)
router.include_router(analytics.router)
router.include_router(files.router)
router.include_router(data.router)
router.include_router(data_loader.router)
router.include_router(reports.router)
router.include_router(chatbot.router)
router.include_router(scraping.router)
router.include_router(geospatial.router)
router.include_router(kenya_api.router)
router.include_router(monitoring.router)
router.include_router(admin.router)
router.include_router(search.router)
