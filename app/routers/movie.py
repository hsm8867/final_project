from uuid import uuid4
from typing import List

from fastapi import APIRouter
from sqlalchemy import select, insert, update, delete

from app.core.db.session import AsyncScopedSession
from app.models.schemas.common import BaseResponse, HttpResponse
from app.models.schemas.movie_ import (
    MovieResp,
)
from app.models.db.movie_model import Movie

from app.core.redis import redis_cache, key_builder
from app.core.logger import logger

router = APIRouter()


@router.get("/list", response_model=BaseResponse[List[MovieResp]])
async def movie_list_by_name(movie_name: str) -> BaseResponse[List[MovieResp]]:
    _key = key_builder("read_movie_list_by_name")

    if await redis_cache.exists(_key):
        logger.debug("Cache hit")
        result = await redis_cache.get(_key)
    else:
        logger.debug("Cache miss")
        async with AsyncScopedSession() as session:
            stmt = select(Movie).where(Movie.movienm == movie_name)
            results = (await session.execute(stmt)).scalars()

    return HttpResponse(
        content=[
            MovieResp(
                date=result.date,
                moviecd=result.moviecd,
                movienm=result.movienm,
                showcnt=result.showcnt,
                scrncnt=result.scrncnt,
                opendt=result.opendt,
                audiacc=result.audiacc,
            )
            for result in results
        ]
    )


@router.get("/{movie_name}", response_model=BaseResponse)
async def read_movie_by_name(movie_name: str) -> BaseResponse[MovieResp]:
    _key = key_builder("read_movie_by_name")

    if await redis_cache.exists(_key):
        logger.debug("Cache hit")
        result = await redis_cache.get(_key)
    else:
        logger.debug("Cache miss")
        async with AsyncScopedSession() as session:
            stmt = select(Movie).where(Movie.movienm == movie_name)
            result = (await session.execute(stmt)).scalar()

    return HttpResponse(
        content=MovieResp(
            date=result.date,
            moviecd=result.moviecd,
            movienm=result.movienm,
            showcnt=result.showcnt,
            scrncnt=result.scrncnt,
            opendt=result.opendt,
            audiacc=result.audiacc,
        )
    )
