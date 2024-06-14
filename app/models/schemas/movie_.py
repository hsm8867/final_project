from pydantic import Field
from pydantic.dataclasses import dataclass
from typing import List
from datetime import datetime
from app.models.schemas.common import PageResp
from app.models.dtos.movie_ import (
    MovieDTO,
    MovieListDTO,
)


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

    @classmethod
    def from_dto(cls, dto: MovieDTO) -> "MovieResp":
        return cls(
            date=dto.date,
            moviecd=dto.moviecd,
            movienm=dto.movienm,
            showcnt=dto.showcnt,
            scrncnt=dto.scrncnt,
            opendt=dto.opendt,
            audiacc=dto.audiacc,
            repgenrenm=dto.repgenrenm,
        )


class MovieListResp:
    data: List[MovieResp] = Field(..., title="Data")
    page: PageResp = Field(..., title="Page")

    @classmethod
    def from_dto(cls, dto: MovieListDTO) -> "MovieListResp":
        return cls(
            data=[MovieResp.from_dto(movie_) for movie_ in dto.data],
            page=PageResp.from_dto(dto.page),
        )
