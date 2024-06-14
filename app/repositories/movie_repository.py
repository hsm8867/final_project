from sqlalchemy import select
from app.models.db.movie_ import Movie, Movie_info

from app.core.db.session import AsyncScopedSession

from app.models.dtos.common import PageDTO
from app.models.dtos.movie_ import MovieDTO, MovieListDTO

from app.core.redis import RedisCacheDecorator


from typing import List
from datetime import datetime


class MovieRepository:

    @RedisCacheDecorator
    async def get_movie_list(moviename: str):
        async with AsyncScopedSession() as session:
            stmt = (
                select(
                    Movie.date,
                    Movie.moviecd,
                    Movie.movienm,
                    Movie.showcnt,
                    Movie.scrncnt,
                    Movie.opendt,
                    Movie.audiacc,
                    Movie_info.repgenrenm,
                )
                .outerjoin(Movie_info, Movie.moviecd == Movie_info.moviecd)
                .where(Movie.movienm == moviename)
                .order_by(Movie.date)
                .limit(7)
            )
            result = (await session.execute(stmt)).all()
            return [
                MovieDTO(
                    date=row.date,
                    moviecd=row.moviecd,
                    movienm=row.movienm,
                    showcnt=row.showcnt,
                    scrncnt=row.scrncnt,
                    opendt=row.opendt,
                    audiacc=row.audiacc,
                    repgenrenm=row.repgenrenm,
                )
                for row in result
            ]

    @RedisCacheDecorator
    async def showing_movie_list(
        self, page: int, limit: int, date: datetime
    ) -> MovieListDTO:
        async with AsyncScopedSession() as session:

            stmt = (
                select(
                    Movie.date,
                    Movie.moviecd,
                    Movie.movienm,
                    Movie.showcnt,
                    Movie.scrncnt,
                    Movie.opendt,
                    Movie.audiacc,
                    Movie_info.repgenrenm,
                )
                .outerjoin(Movie_info, Movie.moviecd == Movie_info.moviecd)
                .where(Movie.date == date)
                .order_by(Movie.movienm)
            )

            results = List[List[Movie, int]] = (await session.execute(stmt)).all()

        data = []
        page = PageDTO(page=page, limit=limit, total=0)

        if results:
            for row, total in results:
                data.append(
                    MovieDTO(
                        date=row.date,
                        moviecd=row.moviecd,
                        movienm=row.movienm,
                        showcnt=row.showcnt,
                        scrncnt=row.scrncnt,
                        opendt=row.opendt,
                        audiacc=row.audiacc,
                        repregenrenm=row.repegenrenm,
                    )
                )
            page.total = total
        return MovieListDTO(data=data, page=page)
