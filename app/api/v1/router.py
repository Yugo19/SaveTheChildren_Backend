from fastapi import APIRouter

from app.api.v1.endpoints import (
    auth, cases, users, analytics, files, data, admin, search,
    chatbot, chatbot_ws, scraping, geospatial, kenya_api, monitoring, data_loader, overpass
)

router = APIRouter(prefix="/api/v1")

router.include_router(auth.router)
router.include_router(users.router)
router.include_router(cases.router)
router.include_router(analytics.router)
router.include_router(files.router)
router.include_router(data.router)
router.include_router(data_loader.router)
router.include_router(chatbot.router)
router.include_router(chatbot_ws.router)
router.include_router(scraping.router)
router.include_router(geospatial.router)
router.include_router(kenya_api.router)
router.include_router(monitoring.router)
router.include_router(admin.router)
router.include_router(search.router)
router.include_router(overpass.router)
