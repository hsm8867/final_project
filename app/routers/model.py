import pandas as pd
from fastapi import APIRouter, HTTPException

from sqlalchemy import select

from app.core.db.session import AsyncScopedSession
from app.utils import load_model, check_movie_count, prepare_data
from app.models.schemas.model_ import ModelResp, ModelReq
from app.models.db.movie_model import Movie, Movie_info
from app.core.errors import error
from app.core.logger import logger

import numpy as np

router = APIRouter()


@router.post("/predict", response_model=ModelResp)
async def predict(request: ModelReq):
    moviename = request.movienm

    async with AsyncScopedSession() as session:
        stmt = (
            select(
                Movie.date,
                Movie.moviecd,
                Movie.movienm,
                Movie.showcnt,
                Movie.scrncnt,
                Movie.opendt,
                Movie.audiacc,
                Movie_info.repgenrenm,
            )
            .outerjoin(Movie_info, Movie.moviecd == Movie_info.moviecd)
            .where(Movie.movienm == moviename)
            .order_by(Movie.date)
            .limit(7)
        )
        result = (await session.execute(stmt)).all()
        
        if not result:
            raise error.MovieNotFoundException()
        
        if len(result) != 7:
            raise error.MovieNotEnoughException()
            

        audiacc = max(row.audiacc for row in result)
        showacc = sum(row.showcnt for row in result)
        scrnacc = sum(row.scrncnt for row in result)
        repgenrenm = result[0].repgenrenm

        df = pd.DataFrame(
            {
                "audiacc": [audiacc],
                "showAcc": [showacc],
                "scrnAcc": [scrnacc],
                "repgenrenm": [repgenrenm],
            }
        )
        
        # Load the model
        model = load_model()
        
        result = model.predict(df)
        
        return ModelResp(result_=int(result))
