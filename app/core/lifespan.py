from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.redis import redis_cache
from app.core.db.session import ping_db, close_db
from app.core.model_registry import model_registry

from app.utils import load_model


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ping_db()
    await redis_cache.ping()
    model_registry.register_model("movie_model", load_model())

    yield

    await close_db()
    await redis_cache.close()
    model_registry.clear()
