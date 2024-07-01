import pytest
from datetime import datetime

from httpx import AsyncClient
from unittest.mock import AsyncMock
from app.core.container import Container
from app.models.dtos.movie_ import MovieListDTO, MovieDTO
from app.services import MovieService


@pytest.mark.parametrize("date", ["2024-01-01"])
@pytest.mark.asyncio
async def test_movielist_200(
    container: Container,
    async_client: AsyncClient,
    movie_service_mock: MovieService,
    date: str,
):
    movie_dto = MovieDTO(
        movienm="movienm",
        date=datetime.strptime(date, "%Y-%m-%d"),
        moviecd="moviecd",
        showcnt="showcnt",
        scrncnt="scrncnt",
        opendt=datetime.now(),
        audiacc="audiacc",
        repgenrenm="repgenrenm",
    )

    movielist_dto = MovieListDTO(
        data=[movie_dto],
    )
    movie_service_mock.read_showing_movies = AsyncMock(return_value=movielist_dto)
    container.movie_service.override(movie_service_mock)

    url = f"/v1/movie/movies_list/?date={date}"
    response = await async_client.get(url)
    assert response.status_code == 200

    if response.status_code == 200:
        json_response = response.json()
        results = json_response["data"]["data"]
        assert len(results) == 1
        result = results[0]
        assert result["movienm"] == movie_dto.movienm
        assert result["opendt"] == movie_dto.opendt
