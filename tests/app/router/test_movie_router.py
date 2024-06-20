import pytest
from datetime import datetime

from httpx import AsyncClient

from app.core.container import Container
from app.core.errors import error
from app.models.dtos.common import PageDTO
from app.models.dtos.movie_ import MovieListDTO, MovieDTO
from app.models.schemas.model_ import ModelReq

from app.services import MovieService

from datetime import datetime


@pytest.mark.parametrize("page, limit", [(1, 10)])
async def test_movielist_200(
    container: Container,
    async_client: AsyncClient,
    movie_service_mock: MovieService,
    page: int,
    limit: int,
):
    params = {
        "page": page,
        "limit": limit,
    }
    movie_dto = MovieDTO(
        movienm="movienm",
        date=datetime.now(),
        moviecd="moviecd",
        showcnt="showcnt",
        scrncnt="scrncnt",
        opendt=datetime.now(),
        audiacc="audiacc",
        repgenrenm="repgenrenm",
    )

    page_dto = PageDTO(
        page=page,
        limit=limit,
        total=1,
    )

    movielist_dto = MovieListDTO(
        page=page_dto,
        data=[movie_dto],
    )
    movie_service_mock.movie_repository.read_showing_movies.return_value = movielist_dto
    container.movie_service.override(movie_service_mock)

    url = "v1/movies/movies_list"
    response = await async_client.get(url, params=params)
    json_response = response.json()

    assert response.status_code == 200

    page = json_response["data"]["page"]
    assert page["page"] == page_dto.page
    assert page["limit"] == page_dto.limit
    assert page["total"] == page_dto.total

    results = json_response["data"]["data"]
    assert len(results) == 1
    result = results[0]
    assert result["movienm"] == movie_dto.movienm
    assert result["opendt"] == movie_dto.opendt
