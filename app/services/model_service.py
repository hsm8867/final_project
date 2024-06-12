import pandas as pd
from app.utils import load_model
from app.models.schemas.model_ import ModelResp, ResponseModel
from app.core.errors import error
from app.repositories.model_repository import MovieRepository
from app.core.db.session import AsyncScopedSession
from app.models.schemas.common import BaseResponse, HttpResponse, ErrorResponse

class PredictService:
    @staticmethod
    async def predict(moviename: str) -> ModelResp:
        async with AsyncScopedSession() as session: 
            movie_data = await MovieRepository.get_movie_data(session, moviename)  
            
            if not movie_data:
                raise error.MovieNotFoundException()
            
            if len(movie_data) != 7:
                raise error.MovieNotEnoughException()

            audiacc = max(row.audiacc for row in movie_data)
            showacc = sum(row.showcnt for row in movie_data)
            scrnacc = sum(row.scrncnt for row in movie_data)
            repgenrenm = movie_data[0].repgenrenm

            df = pd.DataFrame(
                {
                    "audiacc": [audiacc],
                    "showAcc": [showacc],
                    "scrnAcc": [scrnacc],
                    "repgenrenm": [repgenrenm],
                }
            )

            model = load_model()
            prediction = model.predict(df)

            return ModelResp(result_=int(prediction))