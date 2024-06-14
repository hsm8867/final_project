from fastapi import APIRouter
from app.models.schemas.model_ import ModelResp, ModelReq
from app.services.movie_service import MovieService

router = APIRouter()


@router.post("/predict", response_model=ModelResp)
async def predict(request: ModelReq):
    result = await MovieService.predict(request.movienm)
    return result
