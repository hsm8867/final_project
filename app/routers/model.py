from fastapi import APIRouter, Depends
from app.models.schemas.model_ import ModelResp
from app.models.schemas.common import BaseResponse

from dependency_injector.wiring import Provide, inject

from app.core.container import Container
from app.services.movie_service import MovieService

router = APIRouter()


@router.post("/predict", response_model=BaseResponse[ModelResp])
@inject
async def predict(
    moviename: str,
    movie_service: MovieService = Depends(Provide[Container.movie_service]),
) -> BaseResponse[ModelResp]:

    result = await movie_service.predict(moviename)
    return BaseResponse(data=result)
