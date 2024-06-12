from pydantic import BaseModel, Field
from datetime import datetime
from app.models.dtos.movie_model import MovieDTO



class ModelResp(BaseModel):
    result_: int = Field(..., description="Predicted audience")


class ModelReq(BaseModel):
    movienm: str = Field(..., example="리바운드", description="Movie name")
    
    def to_dto(self) -> MovieDTO:
        return MovieDTO(
            movienm = self.movienm
        )
        


class ResponseModel(BaseModel):
    status_code: int = Field(..., example=200, description="Status code")
    data: ModelResp = Field(..., description="Response data")
