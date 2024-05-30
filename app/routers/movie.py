from uuid import uuid4
from typing import List

from fastapi import APIRouter
from sqlalchemy import select, insert, update, delete

from app.core.db.session import AsyncScopedSession
from app.models.schemas.common import BaseResponse, HttpResponse, ErrorResponse
from app.models.schemas.movie_ import (
    MovieResp,
)
from app.models.db.movie_model import Movie, Movie_info

from app.core.redis import redis_cache, key_builder
from app.core.logger import logger
from app.core.errors import error

router = APIRouter()


@router.get(
    "/list",
    response_model=BaseResponse[List[MovieResp]],
    responses={400: {"model": ErrorResponse}},
)
async def movie_list_by_name(movie_name: str) -> BaseResponse[List[MovieResp]]:
    _key = key_builder("read_movie_list_by_name")

    if await redis_cache.exists(_key):
        logger.debug("Cache hit")
        result = await redis_cache.get(_key)
    else:
        logger.debug("Cache miss")
        async with AsyncScopedSession() as session:
            stmt = select(Movie).where(Movie.movienm == movie_name)
            result = (await session.execute(stmt)).scalars().all()

        if result:
            await redis_cache.set(_key, result, ttl=60)

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
            for result in result
        ]
    )


@router.get(
    "/{movie_name}",
    response_model=BaseResponse,
    responses={400: {"model": ErrorResponse}},
)
async def read_movie_by_name(movie_name: str) -> BaseResponse[MovieResp]:
    _key = key_builder("read_movie_by_name")

    if await redis_cache.exists(_key):
        logger.debug("Cache hit")
        result = await redis_cache.get(_key)
    else:
        logger.debug("Cache miss")
        async with AsyncScopedSession() as session:
            stmt = (
                select(Movie, Movie_info.repgenrenm)
                .outerjoin(Movie_info, Movie.moviecd == Movie_info.moviecd)
                .where(Movie.movienm == movie_name)
                .order_by(Movie.date)
                .limit(7)
            )
            result = (await session.execute(stmt)).scalar()
            print(result)

            if result:
                await redis_cache.set(_key, result, ttl=60)

    if result is None:
        raise error.MovieNotFoundException()

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
