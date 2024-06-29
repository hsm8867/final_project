import pytest
from unittest.mock import AsyncMock

from app.models.dtos.common import PageDTO
from app.models.dtos.movie_ import MovieDTO, MovieListDTO
from app.services.movie_service import MovieService


@pytest.mark.asyncio
async def test_showing_movies(
    movie_repository_mock: AsyncMock,
    movie_service_mock: MovieService,
):
    movie_dto = MovieDTO(
        date="date",
        moviecd="moviecd",
        movienm="movienm",
        showcnt="showcnt",
        scrncnt="scrncnt",
        opendt="opendt",
        audiacc="audiacc",
        repgenrenm="repgenrenm",
    )

    movielist_dto = MovieListDTO(
        data=[movie_dto],
    )
    movie_repository_mock.showing_movie_list.return_value = movielist_dto
    result = await movie_service_mock.read_showing_movies(date="date")

    assert result is not None

    result_data = result.data
    assert len(result_data) == 1
    result = result_data[0]
    assert result.movienm == movie_dto.movienm


@pytest.mark.asyncio
async def test_predict(
    movie_repository_mock: AsyncMock,
    movie_service_mock: MovieService,
):
    movie_dto = MovieDTO(
        date="date",
        moviecd="moviecd",
        movienm="movienm",
        showcnt="showcnt",
        scrncnt="scrncnt",
        opendt="opendt",
        audiacc="audiacc",
        repgenrenm="repgenrenm",
    )
    movie_repository_mock.get_movie_list.return_value = [movie_dto]
    result = await movie_service_mock.predict(moviename="moviename")

    assert result is not None
    assert isinstance(result, int)

    movie_service_mock.movie_repository.predict.assert_called_once_with(
        moviename="moviename"
    )
