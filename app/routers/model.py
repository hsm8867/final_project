from fastapi import APIRouter, Path
from app.models.schemas.model_ import ModelResp, ModelReq
from app.models.schemas.common import BaseResponse
from app import repositories
from app.services.movie_service import MovieService

router = APIRouter()


@router.post("/predict", response_model=ModelResp)
async def predict(moviename: str) -> BaseResponse[ModelResp]:
    movie_service = MovieService(repositories.MovieRepository())
    result = await movie_service.predict(moviename)
    return result
