import pandas as pd
from fastapi import APIRouter, HTTPException

from sqlalchemy import select

from app.core.db.session import AsyncScopedSession
from app.utils import load_model, check_movie_count, prepare_data
from app.models.schemas.model_ import ModelResp, ModelReq
from app.models.db.movie_model import Movie, Movie_info

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
            df = pd.DataFrame([movie.__dict__ for movie in result])

            # 영화 상영이 일주일 이상 됐는지 체크
            if check_movie_count(df, moviename):
                # Prepare the data
                prepared_data = prepare_data(df)

                # Load the model
                model = load_model()

                # Predict using the model with the prepared data
                result = model.predict(prepared_data)

                return ModelResp(target=result.tolist())
            else:
                raise HTTPException(
                    status_code=404,
                    detail=f"Not enough movies with the name '{moviename}'.",
                )

    except Exception as e:
        # 해당 영화 존재하지 않음
        raise HTTPException(status_code=500, detail=str(e))
