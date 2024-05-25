from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from app.database import get_db
from app.models import Movie
from datetime import date

app = FastAPI()


@app.get("/movies/{data}")
async def read_movie_by_date(date: date, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Movie).where(Movie.date == date))
    movie = result.scalars().first()
    if movie is None:
        raise HTTPException(status_code=404, detail="Movie not found")
    return movie
