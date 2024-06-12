from pydantic import BaseModel, Field

from pydantic.dataclasses import dataclass

from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from app.models.dtos.movie_model import MovieDTO


@dataclass
class MovieResp:
    date: datetime = Field(..., title="date")
    moviecd: str = Field(..., title="moviecd")
    movienm: str = Field(..., title="movienm")
    showcnt: int = Field(..., title="showcnt")
    scrncnt: int = Field(..., title="scrncnt")
    opendt: datetime = Field(..., title="opendt")
    audiacc: int = Field(..., title="audiacc")
    repgenrenm: str = Field(..., title="repgenrenm")

    # @classmethod
    # def from_dto(cls, dto : MovieDTO) -> "MovieResp":
    #     return cls(
    #         date = dto.date
    #         moviecd = dto.moviecd
    #         movienm = dto.movienm
    #         showcnt = dto.showcnt
    #         scrncnt = dto.scrncnt
    #         opendt = dto.opendt
    #         audiacc = dto.audiacc
    #         # repgenrenm = dto.repgenrenm
    #     )
