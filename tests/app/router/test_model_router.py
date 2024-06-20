import pytest
from datetime import datetime

from httpx import AsyncClient

from app.core.container import Container
from app.models.dtos.movie_ import MovieDTO
from app.models.schemas.model_ import ModelReq

from app.services import MovieService

from datetime import datetime


@pytest.mark.parametrize("movienm", [("movienm")])
async def test_predict_200(
    container: Container,
    async_client: AsyncClient,
    movie_service_mock: MovieService,
    movienm: str,
):

    data = ModelReq(movienm=movienm)

    movie_dto = MovieDTO(
        movienm="-",
        date=datetime.now(),
        moviecd=1,
        showcnt=1,
        scrncnt=1,
        opendt=datetime.now(),
        audiacc=1,
        repgenrenm="genre",
    )

    movie_service_mock.movie_repository.predcit.return_value = movie_dto
    container.movie_service.override(movie_service_mock)

    url = "v1/predict"
    response = await async_client.post(url, data=data.model_dump_json())
    json_response = response.json()

    assert response.status_code == 200
    assert json_response["data"]["movienm"] == movie_dto.movienm
    # assert json_response["data"]["repgenrenm"] == movie_dto.repgenrenm
