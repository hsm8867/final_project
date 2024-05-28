from fastapi import APIRouter

from app.routers import movie

router = APIRouter(prefix="/v1")

router.include_router(movie.router, prefix="/movie", tags=["movie"])
