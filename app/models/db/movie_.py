# app/models.py
from typing import Optional
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, Date, ForeignKey
from app.core.db.session import Base


class Movie(Base):
    __tablename__ = "movies"
    __table_args__ = {"schema": "data"}

    date: Mapped[Date] = mapped_column(Date, index=True)
    moviecd: Mapped[str] = mapped_column(String(255), index=True)
    movienm: Mapped[Optional[str]] = mapped_column(
        String(255), primary_key=True, index=True
    )
    showcnt: Mapped[Optional[int]] = mapped_column(Integer)
    scrncnt: Mapped[Optional[int]] = mapped_column(Integer)
    opendt: Mapped[Optional[Date]] = mapped_column(Date)
    audiacc: Mapped[Optional[int]] = mapped_column(Integer)


class Movie_info(Base):
    __tablename__ = "movie_info"
    __table_args__ = {"schema": "data"}

    moviecd: Mapped[str] = mapped_column(String(255), index=True, primary_key=True)
    repgenrenm: Mapped[str] = mapped_column(String(255), index=True)
    opendt: Mapped[Optional[Date]] = mapped_column(Date)
