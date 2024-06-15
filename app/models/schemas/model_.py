from pydantic import BaseModel, Field
from app.models.dtos.movie_ import MovieDTO


class ModelResp(BaseModel):
    result_: int = Field(..., description="Predicted audience")


class ModelReq(BaseModel):
    movienm: str = Field(..., description="Movie name")

    def to_dto(self) -> MovieDTO:
        return MovieDTO(moviename=self.movienm)


class ResponseModel(BaseModel):
    status_code: int = Field(..., example=200, description="Status code")
    data: ModelResp = Field(..., description="Response data")
