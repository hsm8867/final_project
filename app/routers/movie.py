from uuid import uuid4
from typing import List

from fastapi import APIRouter, HTTPException
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

from datetime import datetime

router = APIRouter()


@router.get(
    "/movies_list",
    response_model=BaseResponse,
    responses={400: {"model": ErrorResponse}},
)
async def showing_movies(date: datetime) -> BaseResponse[MovieResp]:
    try:
        _key = key_builder("read_movie_by_date")

        if await redis_cache.exists(_key):
            logger.debug("Cache hit")
            result = await redis_cache.get(_key)
        else:
            logger.debug("Cache miss")
            async with AsyncScopedSession() as session:
                stmt = (
                    select(Movie, Movie_info.repgenrenm)
                    .outerjoin(Movie_info, Movie.moviecd == Movie_info.moviecd)
                    .where(Movie.date == date)
                    .order_by(Movie.date)
                    .limit(7)
                )
                result = (await session.execute(stmt)).scalars().all()

                if result:
                    await redis_cache.set(_key, result, ttl=60)

        if result is None:
            raise error.MovieNotFoundException()

        return HttpResponse(
            content=[
                MovieResp(
                    date=movie.date,
                    moviecd=movie.moviecd,
                    movienm=movie.movienm,
                    showcnt=movie.showcnt,
                    scrncnt=movie.scrncnt,
                    opendt=movie.opendt,
                    audiacc=movie.audiacc,
                    # repgenrenm=repgenrenm.repgenrenm
                )
                for movie in result
            ]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
