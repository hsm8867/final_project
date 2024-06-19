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
    page = 1
    limit = 10
    total = 1
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
    page_dto = PageDTO(page=page, limit=limit, total=total)
    movielist_dto = MovieListDTO(
        data=[movie_dto],
        page=page_dto,
    )
    movie_repository_mock.showing_movie_list.return_value = movielist_dto
    result = await movie_service_mock.read_showing_movies(movielist_dto=movielist_dto)

    assert result != None

    result_page = result.page

    assert result_page.page == page_dto.page
    assert result_page.limit == page_dto.limit
    assert result_page.total == page_dto.total

    result_data = result.data
    assert len(result_data) == 1
    result = result_data[0]
    assert result.movienm == movie_dto.movienm

    movie_service_mock.movie_repository.read_showing_movies.assert_called_onece_with(
        page=page, limit=limit
    )


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
    movie_repository_mock.get_movie_list.return_value = movie_dto
    result = await movie_service_mock.predict(movie_dto=movie_dto)

    assert result != None
    assert type(result) == int

    movie_service_mock.movie_repository.predict.assert_called_once_with(
        movienm=movie_dto.movienm
    )
