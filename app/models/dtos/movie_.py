from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

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
    repgenrenm: Optional[str]


@dataclass
class MovieListDTO:
    data: List[MovieDTO]
