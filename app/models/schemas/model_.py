from pydantic import BaseModel, Field
from datetime import datetime


class ModelResp(BaseModel):
    target: int = Field(..., description="Predicted audience")


class ModelReq(BaseModel):
    date: datetime = Field(..., example="2023-04-03", description="Date")
    opendt: datetime = Field(..., example="2023-04-05", description="Open date")
    moviecd: str = Field(..., example="20226489", description="Movie code")
    movienm: str = Field(..., example="리바운드", description="Movie name")
    showcnt: int = Field(..., example=13, description="Show count")
    scrncnt: int = Field(..., example=13, description="Screen count")
    audiacc: int = Field(..., example=1000, description="Accumulated audience")


class ResponseModel(BaseModel):
    status_code: int = Field(..., example=200, description="Status code")
    data: ModelResp = Field(..., description="Response data")
