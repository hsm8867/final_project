from fastapi import APIRouter, Depends
from dependency_injector.wiring import Provide, inject

from app.models.schemas.common import BaseResponse, HttpResponse, ErrorResponse
from app.models.schemas.movie_ import (
    MovieListResp,
)

from app.services import MovieService
from app.core.container import Container

from datetime import datetime

router = APIRouter()


@router.get(
    "/movies_list",
    response_model=BaseResponse[MovieListResp],
    responses={400: {"model": ErrorResponse}},
)
@inject
async def showing_movies(
    date: datetime,
    movie_service: MovieService = Depends(Provide[Container.movie_service]),
) -> BaseResponse[MovieListResp]:

    result = await movie_service.read_showing_movies(date)
    return HttpResponse(content=MovieListResp.from_dto(result))
