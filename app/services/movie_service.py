from fastapi import APIRouter

from app import repositories

from app.core.db.session import AsyncScopedSession
from app.core.errors import error
from app.core.model_registry import model_registry
from app.models.dtos.movie_ import MovieListDTO

from datetime import datetime

from app.models.schemas.model_ import ModelResp
from app.repositories.movie_repository import MovieRepository

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

    async def predict(self, moviename: str) -> ModelResp:
        input = self.movie_repository.get_movie_list(moviename)
        return await model_registry.get_model("movie_model").predict(input)
