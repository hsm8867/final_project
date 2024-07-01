from unittest.mock import AsyncMock
import pytest
from datetime import datetime

from httpx import AsyncClient

from app.core.container import Container
from app.models.dtos.movie_ import MovieListDTO
from app.models.schemas.model_ import ModelResp

from app.services import MovieService


@pytest.mark.parametrize("moviename", [("TestMovie")])
async def test_predict_200(
    container: Container,
    async_client: AsyncClient,
    movie_service_mock: MovieService,
    movie_repository_mock: AsyncMock,
    moviename: str,
):

    movie_repository_mock.get_movie_list.return_value = [{"moviename": moviename}]
    movie_repository_mock.showing_movie_list.return_value = MovieListDTO(data=[])

    movie_service_mock.movie_repository = movie_repository_mock

    prediction_input = {"moviename": moviename}

    movie_service_mock.predict = AsyncMock(return_value=ModelResp(result_=1))

    url = "/v1/model/predict"
    response = await async_client.post(url, json={"moviename": moviename})

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["data"]["result"] == 1

    movie_repository_mock.get_movie_list.assert_called_once_with(moviename)
    movie_service_mock.predict.assert_called_once_with(moviename)
