import asyncio
import aiohttp
import logging
import time
import jwt
import uuid
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from airflow.models.variable import Variable
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, DateTime, Integer, Float
from sqlalchemy import text, select, func
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy base class
Base = declarative_base()

# BtcOhlcv table definition (imported from create_table.py)
class BtcOhlcv(Base):
    __tablename__ = "btc_ohlcv"
    time = Column(DateTime, primary_key=True)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Float)

# JWT Token generation
def generate_jwt_token(access_key: str, secret_key: str) -> str:
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    authorization_token = f"Bearer {jwt_token}"
    return authorization_token

# Upbit API fetch function
async def fetch_ohlcv_data(session, market: str, to: str, count: int, minutes: int, max_retries: int = 5) -> list:
    """Fetch OHLCV data from Upbit API."""
    UPBIT_ACCESS_KEY = Variable.get("upbit_access_key")
    UPBIT_SECRET_KEY = Variable.get("upbit_secret_key")

    url = f"https://api.upbit.com/v1/candles/minutes/{minutes}?market={market}&to={to}&count={count}"
    headers = {
        "Accept": "application/json",
        "Authorization": generate_jwt_token(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY),
    }

    retries = 0
    while retries < max_retries:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                if isinstance(data, list):
                    return data
                else:
                    logger.error(f"Unexpected response format: {data}")
                    return []
            elif response.status == 429:  # Too Many Requests
                logger.warning(f"Rate limit exceeded. Retrying in 60 seconds...")
                await asyncio.sleep(60)  # Wait for 60 seconds before retrying
                retries += 1
            else:
                logger.error(f"Unexpected status code: {response.status}")
                break
    logger.error(f"Failed to fetch data after {max_retries} retries.")
    return []

# Insert data into the database
async def insert_data_into_db(data: list, session: AsyncSession) -> None:
    try:
        bulk_data = [
            {
                "time": datetime.fromisoformat(record["candle_date_time_kst"]),
                "open": record["opening_price"],
                "high": record["high_price"],
                "low": record["low_price"],
                "close": record["trade_price"],
                "volume": record["candle_acc_trade_volume"],
            }
            for record in data
        ]

        stmt = pg_insert(BtcOhlcv).values(bulk_data).on_conflict_do_nothing()
        await session.execute(stmt)
        await session.commit()
        logger.info(f"Inserted {len(data)} records successfully.")

    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to insert data: {e}")
        raise e

async def delete_old_data(session: AsyncSession) -> None:
    try:
        # Calculate the threshold to keep only the most recent 365 days of data
        threshold_date = datetime.now() - relativedelta(days=365)
        threshold_kst = threshold_date + timedelta(hours=9)
        now = datetime.now() + timedelta(hours=9)
        logger.info(f"Current time (KST): {now}")
        logger.info(f"Threshold date to keep data after: {threshold_kst}")

        # Query to get the oldest time in the table
        oldest_query = select(BtcOhlcv.time).order_by(BtcOhlcv.time).limit(1)
        oldest_result = await session.execute(oldest_query)
        oldest_time = oldest_result.scalar()

        if oldest_time and oldest_time < threshold_kst:
            # Count how many records will be deleted
            count_query = select(func.count()).where(BtcOhlcv.time < threshold_kst)
            result = await session.execute(count_query)
            delete_count = result.scalar()
            logger.info(f"Number of records to be deleted: {delete_count}")

            # Delete records older than the threshold date (keeping only the most recent 365 days)
            delete_query = BtcOhlcv.__table__.delete().where(BtcOhlcv.time < threshold_kst)
            await session.execute(delete_query)
            await session.commit()
            logger.info(f"Deleted {delete_count} old records from the database.")
        else:
            logger.info("No old data to delete; dataset already within the last 365 days.")
    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to delete old data: {e}")
        raise e

# Main function to fetch and insert data
async def collect_and_load_data():
    # Database connection details
    db_uri = Variable.get("db_uri")
    engine = create_async_engine(db_uri, future=True)
    SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with aiohttp.ClientSession() as aiohttp_session:
        async with SessionLocal() as session:
            # Fetch OHLCV data
            to_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            data = await fetch_ohlcv_data(aiohttp_session, market="KRW-BTC", to=to_time, count=200, minutes=5)

            if data:
                # Insert into the database
                await insert_data_into_db(data, session)
            else:
                logger.info("No new data to insert.")

# Entry point
if __name__ == "__main__":
    asyncio.run(collect_and_load_data())