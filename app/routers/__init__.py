from fastapi import APIRouter

from app.routers import movie, model

router = APIRouter(prefix="/v1")

router.include_router(movie.router, prefix="/movie", tags=["movie"])
router.include_router(model.router, prefix="/model", tags=["model"])
