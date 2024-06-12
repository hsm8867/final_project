import pandas as pd
from fastapi import APIRouter, HTTPException

from sqlalchemy import select

from app.core.db.session import AsyncScopedSession
from app.core.model_registry import model_registry
from app.models.schemas.model_ import ModelResp, ModelReq
from app.models.db.movie_model import Movie, Movie_info
from app.services.model_service import PredictService

from app.core.logger import logger

import numpy as np

router = APIRouter()


@router.post("/predict", response_model=ModelResp)
async def predict(request: ModelReq):
    result = await PredictService.predict(request.movienm)
    return result
