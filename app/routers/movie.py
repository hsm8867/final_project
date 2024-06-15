from fastapi import APIRouter, Query

from app.models.schemas.common import BaseResponse, HttpResponse, ErrorResponse
from app.models.schemas.movie_ import (
    MovieListResp,
)

from app.services.movie_service import MovieService
from app import repositories

from datetime import datetime

router = APIRouter()


@router.get(
    "/movies_list",
    response_model=BaseResponse,
    responses={400: {"model": ErrorResponse}},
)
async def showing_movies(
    date: datetime,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, dexcription="Number of items per page"),
) -> BaseResponse[MovieListResp]:
    movie_service = MovieService(repositories.MovieRepository())
    result = await movie_service.read_showing_movies(date, page, limit)

    return HttpResponse(content=MovieListResp.from_dto(result))
