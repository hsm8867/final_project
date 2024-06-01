import pandas as pd
from fastapi import APIRouter, HTTPException

from sqlalchemy import select

from app.core.db.session import AsyncScopedSession
from app.utils import load_model, check_movie_count, prepare_data
from app.models.schemas.model_ import ModelResp, ModelReq
from app.models.db.movie_model import Movie, Movie_info

from app.core.logger import logger

import numpy as np

router = APIRouter()


@router.post("/predict", response_model=ModelResp)
async def predict(request: ModelReq):

    try:
        # Extract movie name from the request
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

            print("DataFrame for prediction:")
            print(df)

            # Load the model
            model = load_model()
            try:
                result = model.predict(df)
                print(result)
                return ModelResp(result_=int(result))
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Prediction error: {str(e)}"
                )

    except Exception as e:
        # 해당 영화 존재하지 않음
        raise HTTPException(status_code=500, detail=str(e))
