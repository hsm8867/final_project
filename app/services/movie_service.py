from fastapi import APIRouter

from app import repositories

from app.core.model_registry import model_registry
from app.models.dtos.movie_ import MovieListDTO

from datetime import datetime

from app.models.schemas.model_ import ModelResp
from app.models.schemas.common import BaseResponse
from app import repositories

from app.utils import prepare_for_predict

router = APIRouter()


class MovieService:
    def __init__(self, movie_repository: repositories.MovieRepository):
        self.movie_repository = movie_repository

    async def read_showing_movies(
        self, date: datetime, page: int, limit: int
    ) -> MovieListDTO:
        return await self.movie_repository.showing_movie_list(
            date, page=page, limit=limit
        )

    async def predict(self, moviename: str) -> BaseResponse[ModelResp]:
        data = await self.movie_repository.get_movie_list(moviename)
        input = prepare_for_predict(data)
        prediction = model_registry.get_model("movie_model").predict(input)
        return ModelResp(result_=int(prediction))
