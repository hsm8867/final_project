from pydantic import BaseModel, Field

from pydantic.dataclasses import dataclass

from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime


@dataclass
class MovieResp:
    date: datetime = Field(..., title="date")
    moviecd: str = Field(..., title="moviecd")
    movienm: str = Field(..., title="movienm")
    showcnt: int = Field(..., title="showcnt")
    scrncnt: int = Field(..., title="scrncnt")
    opendt: datetime = Field(..., title="opendt")
    audiacc: int = Field(..., title="audiacc")
