from fastapi import APIRouter

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
async def showing_movies(date: datetime) -> BaseResponse[MovieListResp]:
    movie_service = MovieService(repositories.MovieRepository())
    result = await movie_service.read_showing_movies(date)

    return HttpResponse(content=MovieListResp.from_dto(result))
