import pytest

from unittest.mock import AsyncMock

from httpx import AsyncClient

from app.main import create_app
from app.core.container import Container
from app.services import MovieService
from app.repositories import MovieRepository


@pytest.fixture
def container() -> Container:
    return Container()


@pytest.fixture
def async_client(container) -> AsyncClient:
    app = create_app(container)
    return AsyncClient(app=app, base_url="http://test")


@pytest.fixture
def movie_repository_mock():
    return AsyncMock(spec=MovieRepository)


@pytest.fixture
def movie_service_mock(movie_repository_mock):
    return AsyncMock(movie_repository=movie_repository_mock)
