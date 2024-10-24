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


# Exponential backoff strategy for retries
async def exponential_backoff_retry(session, url, headers, max_retries=5):
    retries = 0
    delay = 1  # Initial delay of 1 second
    while retries < max_retries:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:  # Too Many Requests
                retry_after = response.headers.get("Retry-After", 60)
                logger.warning(
                    f"Rate limit exceeded. Retrying after {retry_after} seconds..."
                )
                await asyncio.sleep(int(retry_after))
                retries += 1
            else:
                logger.error(f"Unexpected status code: {response.status}")
                break
        # Exponential backoff in case of repeated retries
        delay = min(delay * 2, 60)  # Maximum delay of 60 seconds
        await asyncio.sleep(delay)
    logger.error(f"Failed to fetch data after {max_retries} retries.")
    return []


# Fetch OHLCV data in parallel
async def fetch_ohlcv_data_in_parallel(
    session,
    market: str,
    start_time: str,
    end_time: str,
    count: int,
    minutes: int,
    max_retries: int = 5,
):
    """Fetch OHLCV data from Upbit API in parallel across multiple requests."""
    UPBIT_ACCESS_KEY = Variable.get("upbit_access_key")
    UPBIT_SECRET_KEY = Variable.get("upbit_secret_key")

    tasks = []
    headers = {
        "Accept": "application/json",
        "Authorization": generate_jwt_token(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY),
    }
    logger.info(
        f"Starting parallel data fetch for {market} from {start_time} to {end_time}"
    )

    # Break down the time range into smaller intervals and create parallel tasks
    current_time = datetime.fromisoformat(end_time)
    while current_time > datetime.fromisoformat(start_time):
        to_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        url = f"https://api.upbit.com/v1/candles/minutes/{minutes}?market={market}&to={to_time}&count={count}"
        logger.info(
            f"Fetching data for {market} up to {to_time} with count={count} and minutes={minutes}"
        )
        tasks.append(exponential_backoff_retry(session, url, headers))
        current_time -= timedelta(
            minutes=minutes * count
        )  # Move back by 'count' intervals of 'minutes'

    # Run all the tasks in parallel
    results = await asyncio.gather(*tasks)

    # Combine all results into a single list
    combined_data = [item for result in results for item in result if result]
    logger.info(
        f"Fetched {len(combined_data)} data points from {start_time} to {end_time} for {market}"
    )

    # Optional: Log some sample data points for debugging
    # if combined_data:
    #     logger.debug(f"Sample data: {combined_data[:2]}")  # Log first two data points for inspection

    return combined_data


# Insert data into the database with batching to avoid exceeding parameter limits
async def insert_data_into_db_batched(
    data: list, session: AsyncSession, batch_size: int = 1000
) -> None:
    try:
        # Split the data into batches to avoid hitting the parameter limit
        for i in range(0, len(data), batch_size):
            batch_data = data[i : i + batch_size]
            bulk_data = [
                {
                    "time": datetime.fromisoformat(record["candle_date_time_kst"]),
                    "open": record["opening_price"],
                    "high": record["high_price"],
                    "low": record["low_price"],
                    "close": record["trade_price"],
                    "volume": record["candle_acc_trade_volume"],
                }
                for record in batch_data
            ]
            stmt = pg_insert(BtcOhlcv).values(bulk_data).on_conflict_do_nothing()
            await session.execute(stmt)
            await session.commit()
            logger.info(f"Inserted {len(batch_data)} records in this batch.")
    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to insert data: {e}")
        raise e


async def fetch_one_year_of_data(
    session: AsyncSession, aiohttp_session: aiohttp.ClientSession
) -> None:
    """Fetch historical OHLCV data from start_time to end_time."""
    logger.info("Fetching one year of historical data")
    current_time = datetime.now()
    one_year_ago = current_time - relativedelta(days=365)

    end_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
    start_time = one_year_ago.strftime("%Y-%m-%dT%H:%M:%S")

    # Check if there is data in the database for at least one year ago
    threshold_query = select(func.min(BtcOhlcv.time)).where(
        BtcOhlcv.time <= one_year_ago
    )
    threshold_result = await session.execute(threshold_query)
    earliest_data_time = threshold_result.scalar()

    if earliest_data_time:
        # Data from a year ago already exists, so skip fetching
        logger.info(
            f"Data from one year ago already exists in the database (earliest record: {earliest_data_time}). Skipping fetch."
        )
        return

    # If no data from one year ago exists, proceed to fetch historical data
    logger.info(
        "No data from one year ago found. Fetching one year of historical data."
    )

    market = "KRW-BTC"
    count = 200  # Number of candles to retrieve per request
    minutes = 5  # 5-minute candles

    # Fetch data
    data = await fetch_ohlcv_data_in_parallel(
        aiohttp_session, market, start_time, end_time, count, minutes
    )

    if data:
        # Insert data into the database
        await insert_data_into_db(data, session)
    else:
        logger.info("No more data to fetch.")


async def delete_old_data(session: AsyncSession) -> None:
    try:
        # Get the most recent time in the table (latest row)
        latest_query = (
            select(func.max(BtcOhlcv.time)).order_by(BtcOhlcv.time.desc()).limit(1)
        )
        latest_result = await session.execute(latest_query)
        latest_time = latest_result.scalar()

        if not latest_time:
            logger.info("No data in the database to delete.")
            return

        # Calculate the threshold based on the latest row in the table
        threshold_date = latest_time - relativedelta(days=365)
        logger.info(f"Latest data timestamp: {latest_time}")
        logger.info(f"Threshold date for deletion: {threshold_date}")

        # Query to get the oldest time in the table for confirmation/logging purposes
        oldest_query = select(func.min(BtcOhlcv.time)).limit(1)
        oldest_result = await session.execute(oldest_query)
        oldest_time = oldest_result.scalar()

        if oldest_time and oldest_time < threshold_date:
            # Count how many records will be deleted
            count_query = select(func.count()).where(BtcOhlcv.time < threshold_date)
            result = await session.execute(count_query)
            delete_count = result.scalar()
            logger.info(f"Number of records to be deleted: {delete_count}")

            # Delete records older than the threshold date (keeping only the most recent 365 days)
            delete_query = BtcOhlcv.__table__.delete().where(
                BtcOhlcv.time < threshold_date
            )
            await session.execute(delete_query)
            await session.commit()
            logger.info(f"Deleted {delete_count} old records from the database.")
        else:
            logger.info(
                "No old data to delete; dataset already within the last 365 days."
            )
    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to delete old data: {e}")
        raise e


# Main function to fetch and insert data
async def collect_and_load_data_fn():
    # Database connection details
    db_uri = Variable.get("db_uri")
    engine = create_async_engine(db_uri, future=True)
    SessionLocal = sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )

    async with aiohttp.ClientSession() as aiohttp_session:
        async with SessionLocal() as session:
            # Check if there is a need to fetch one year of historical data
            await fetch_one_year_of_data(session, aiohttp_session)

            # After historical data, start fetching the latest 5-minute data
            to_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            data = await fetch_ohlcv_data_in_parallel(
                aiohttp_session,
                market="KRW-BTC",
                start_time=to_time,
                end_time=to_time,
                count=200,
                minutes=5,
            )
            if data:
                # Insert into the database
                await insert_data_into_db_batched(data, session)

                # After inserting new data, delete data older than one year
                await delete_old_data(session)
            else:
                logger.info("No new data to insert.")
