from dataclasses import dataclass
from datetime import datetime
from typing import List

from app.models.dtos.common import PageDTO


@dataclass
class MovieDTO:
    date: datetime
    moviecd: str
    movienm: str
    showcnt: int
    scrncnt: int
    opendt: str
    audiacc: int
    repgenrenm: str


@dataclass
class MovieListDTO:
    data: List[MovieDTO]
    page: PageDTO
