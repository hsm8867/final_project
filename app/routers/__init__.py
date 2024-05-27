from fastapi import APIRouter, Depends

from app.routers import movie
from app.core.auth import validate_api_key

router = APIRouter(prefix="/v1", dependencies=[Depends(validate_api_key)])

router.include_router(movie.router, prefix="/movie", tags=["movie"])
