from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette_context.middleware import ContextMiddleware

from app.core.config import config
from app.core.lifespan import lifespan
from app.routers import router

from app.core.container import Container

from app.core.errors.error import BaseAPIException
from app.core.errors.handler import api_error_handler
from app.core.middlewares.sqlalchemy import SQLAlchemyMiddleware

from app.core.middlewares.metirc_middleware import MetricMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


def create_app(container=Container()) -> FastAPI:
    app = FastAPI(lifespan=lifespan, **config.fastapi_kwargs)

    container = Container()
    container.config.from_dict(config.model_dump())

    app.include_router(router)
    app.add_exception_handler(BaseAPIException, api_error_handler)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(SQLAlchemyMiddleware)
    app.add_middleware(ContextMiddleware)
    app.add_middleware(MetricMiddleware)

    return app


app = create_app()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/metrics")
async def get_metrics():
    headers = {"Content-Type": CONTENT_TYPE_LATEST}
    content = generate_latest().decode("utf-8")
    return Response(content=content, media_type="text/plain", headers=headers)
