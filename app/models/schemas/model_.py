from pydantic import BaseModel, Field
from datetime import datetime


class ModelResp(BaseModel):
    result: int = Field(..., description="Predicted audience")


class ModelReq(BaseModel):
    movienm: str = Field(..., example="리바운드", description="Movie name")


class ResponseModel(BaseModel):
    status_code: int = Field(..., example=200, description="Status code")
    data: ModelResp = Field(..., description="Response data")
