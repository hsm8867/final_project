from sqlalchemy import select
from app.models.db.movie_model import Movie, Movie_info
from app.models.dtos.movie_model import MovieDTO

class MovieRepository:
    @staticmethod
    async def get_movie_data(session, moviename: str):
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
        return [MovieDTO(
            date=row.date,
            moviecd=row.moviecd,
            movienm=row.movienm,
            showcnt=row.showcnt,
            scrncnt=row.scrncnt,
            opendt=row.opendt,
            audiacc=row.audiacc,
            repgenrenm=row.repgenrenm
        ) for row in result]