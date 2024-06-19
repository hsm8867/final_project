from fastapi import APIRouter, Query, Depends
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
    response_model=BaseResponse,
    responses={400: {"model": ErrorResponse}},
)
@inject
async def showing_movies(
    date: datetime,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Number of items per page"),
    movie_service: MovieService = Depends(Provide[Container.movie_service]),
) -> BaseResponse[MovieListResp]:

    result = await movie_service.read_showing_movies(date, page, limit)
    return HttpResponse(content=MovieListResp.from_dto(result))
