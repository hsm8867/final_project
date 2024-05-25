from fastapi import FastAPI, HTTPException


from app.core.config import config
from app.core.lifespan import lifespan
from app.routers import router

app = FastAPI(lifespan=lifespan, **config.fastapi_kwargs)

app.include_router(router)


@app.get("/")
async def root():
    return {"message": "Hello World"}
