from dependency_injector import providers, containers

from app.core.config import Config

from app.services.movie_service import MovieService
from app.repositories.movie_repository import MovieRepository


class Container(containers.DeclarativeContainer):
    config: Config = providers.Configuration()

    wiring_config = containers.WiringConfiguration(
        packages=[
            "app.routers",
        ]
    )

    # repositories
    movie_repository = providers.Factory(MovieRepository)

    # services
    movie_service = providers.Factory(MovieService, movie_repository=movie_repository)
