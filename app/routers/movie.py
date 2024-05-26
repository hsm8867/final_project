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

router = APIRouter()


@router.get("/list", response_model=BaseResponse[List[MovieResp]])
async def movie_list_by_name(movie_name: str) -> BaseResponse[List[MovieResp]]:
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
