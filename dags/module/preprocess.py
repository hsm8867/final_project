from dags.module.create_table import (
    BtcOhlcv,
    BtcPreprocessed,
)
from sqlalchemy import select, func, text, and_, update, bindparam
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_scoped_session,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import declarative_base, sessionmaker
from contextvars import ContextVar

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from airflow.models.variable import Variable

from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, func
from datetime import timedelta

from asyncpg.exceptions import DeadlockDetectedError

import psutil
import pandas as pd
import numpy as np
import logging
import asyncio
import uvloop
import time

# uvloop를 기본 이벤트 루프로 설정
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

# ContextVar를 사용하여 세션을 관리(비동기 함수간에 컨텍스트를 안전하게 전달하도록 해줌. 세션을 여러 코루틴간에 공유 가능)
session_context = ContextVar("session_context", default=None)


# deadlock 발생 시 retry 실행 함수
async def execute_with_retry(session, query, max_retries=3, delay=2):
    retries = 0
    while retries < max_retries:
        try:
            await session.execute(query)
            await session.commit()
            logger.info("Query executed successfully")
            return
        except DeadlockDetectedError as e:
            logger.warning(
                f"Deadlock detected: {e}, retrying {retries + 1}/{max_retries}"
            )
            await asyncio.sleep(delay)
            retries += 1
            await session.rollback()  # Roll back the transaction before retrying

    # If max_retries is reached and it still fails, raise the error
    raise DeadlockDetectedError(
        f"Query failed after {max_retries} retries due to deadlock."
    )


# 현재 시간(UTC+9)으로부터 365일이 지난 데이터를 데이터베이스에서 삭제하는 함수
async def delete_old_data_from_preprocessed(session: AsyncSession) -> None:
    """Delete data from btc_preprocessed older than one year from the most recent entry."""
    try:
        # Query to get the most recent time in the btc_preprocessed table
        latest_query = select(func.max(BtcPreprocessed.time)).select_from(
            BtcPreprocessed
        )
        result = await session.execute(latest_query)
        latest_time = result.scalar()

        if latest_time is None:
            logger.info("No data in btc_preprocessed table.")
            return

        # Calculate the threshold to keep only the most recent 365 days of data
        threshold_date = latest_time - relativedelta(days=365)
        logger.info(f"Deleting data older than {threshold_date} from btc_preprocessed.")

        # Delete records older than the threshold date
        delete_query = BtcPreprocessed.__table__.delete().where(
            BtcPreprocessed.time < threshold_date
        )
        result = await session.execute(delete_query)
        await session.commit()

        logger.info(
            f"Deleted records older than {threshold_date} from btc_preprocessed."
        )
    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to delete old data: {e}")
        raise e


async def get_first_and_last_time(
    session: AsyncSession, new_time: str, past_new_time: str
):

    if new_time is None:
        # If new_time is None, retrieve the latest available time from the database
        print("new_time is None, retrieving the latest time from the database...")
        latest_query = select(func.max(text("time"))).select_from(text("btc_ohlcv"))
        result = await session.execute(latest_query)
        new_time = result.scalar()
        if new_time is None:
            raise ValueError(
                "No data found in the raw data table, and new_time is not provided."
            )
        else:
            new_time = new_time.isoformat()  # Convert it to ISO string format

    # Log the type of new_time for debugging
    print(f"new_time: {new_time}, type: {type(new_time)}")

    # Convert time strings to datetime objects
    last_time = datetime.fromisoformat(new_time)

    # Calculate one year ago from last_time
    one_year_ago = last_time - timedelta(days=365)

    # Check if there is data for one year ago
    earliest_query = select(func.min(BtcOhlcv.time)).where(
        BtcOhlcv.time >= one_year_ago
    )
    result = await session.execute(earliest_query)
    first_time = result.scalar()

    if not first_time:
        # If there's no data from one year ago, retrieve the most recent data
        earliest_query = select(func.min(BtcOhlcv.time))
        result = await session.execute(earliest_query)
        first_time = result.scalar()

    return first_time, last_time


async def add_moving_average(
    session: AsyncSession, first_time: str, last_time: str
) -> None:
    logger.info(
        f"Adding or updating moving averages between {first_time} and {last_time}"
    )

    query = text(
        f"""
        WITH subquery AS (
            SELECT
                time,
                AVG(close) OVER (ORDER BY time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_close_7,
                AVG(close) OVER (ORDER BY time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_close_14,
                AVG(close) OVER (ORDER BY time ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_close_30
            FROM btc_ohlcv
            WHERE time BETWEEN '{first_time}' AND '{last_time}'
        )
        INSERT INTO btc_preprocessed (time, ma_7, ma_14, ma_30)
        SELECT subquery.time, subquery.avg_close_7, subquery.avg_close_14, subquery.avg_close_30
        FROM subquery
        ON CONFLICT (time) DO UPDATE
        SET ma_7 = EXCLUDED.ma_7,
            ma_14 = EXCLUDED.ma_14,
            ma_30 = EXCLUDED.ma_30;
    """
    )

    await execute_with_retry(session, query)
    logger.info("Moving averages added/updated successfully")


async def add_rsi(session: AsyncSession, first_time: str, last_time: str) -> None:
    logger.info(f"Adding or updating RSI between {first_time} and {last_time}")

    query = text(
        f"""
        WITH gains_and_losses AS (
            SELECT
                time,
                CASE WHEN close - LAG(close) OVER (ORDER BY time) > 0
                    THEN close - LAG(close) OVER (ORDER BY time)
                    ELSE 0
                END AS gain,
                CASE WHEN close - LAG(close) OVER (ORDER BY time) < 0
                    THEN LAG(close) OVER (ORDER BY time) - close
                    ELSE 0
                END AS loss
            FROM btc_ohlcv
            WHERE time BETWEEN '{first_time}' AND '{last_time}'
        ),
        avg_gains_losses AS (
            SELECT
                time,
                AVG(gain) OVER (ORDER BY time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
                AVG(loss) OVER (ORDER BY time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
            FROM gains_and_losses
        )
        INSERT INTO btc_preprocessed (time, rsi_14)
        SELECT avg_gains_losses.time,
            CASE
                WHEN avg_loss = 0 THEN 100
                WHEN avg_gain = 0 THEN 0
                ELSE 100 - (100 / (1 + avg_gain / avg_loss))
            END AS rsi_14
        FROM avg_gains_losses
        ON CONFLICT (time) DO UPDATE
        SET rsi_14 = EXCLUDED.rsi_14;
    """
    )

    await execute_with_retry(session, query)
    logger.info("RSI added/updated successfully")


async def add_rsi_over(session: AsyncSession, first_time: str, last_time: str) -> None:
    logger.info(f"Adding or updating RSI_OVER between {first_time} and {last_time}")

    query = text(
        f"""
        UPDATE btc_preprocessed
        SET rsi_over = CASE
                          WHEN rsi_14 > 75 THEN 1
                          WHEN rsi_14 < 25 THEN 0
                          ELSE 2
                      END
        WHERE time BETWEEN '{first_time}' AND '{last_time}';
    """
    )

    await execute_with_retry(session, query)
    logger.info("RSI_OVER added/updated successfully")


async def update_labels(session: AsyncSession, first_time: str, last_time: str) -> None:
    logger.info(f"Updating labels between {first_time} and {last_time}")

    query = text(
        f"""
        WITH CTE AS (
            SELECT time, close,
                   LAG(close) OVER (ORDER BY time) AS prev_close
            FROM btc_preprocessed
            WHERE time BETWEEN '{first_time}' AND '{last_time}'
        )
        UPDATE btc_preprocessed
        SET label = CASE
                       WHEN CTE.close > CTE.prev_close THEN 1
                       ELSE 0
                   END
        FROM CTE
        WHERE btc_preprocessed.time = CTE.time
        AND btc_preprocessed.time BETWEEN '{first_time}' AND '{last_time}';
    """
    )

    await execute_with_retry(session, query)
    logger.info("Labels updated successfully")


async def insert_preprocessed_data(session: AsyncSession) -> None:
    logger.info(f"Inserting data from btc_ohlcv to btc_preprocessed")

    query = text(
        f"""
        INSERT INTO btc_preprocessed (time, open, high, low, close, volume)
        SELECT time, open, high, low, close, volume
        FROM btc_ohlcv
        ON CONFLICT (time) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
    """
    )

    await execute_with_retry(session, query)
    logger.info("Data inserted successfully from btc_ohlcv to btc_preprocessed")


async def preprocess_data(async_context: dict):
    db_uri = Variable.get("db_uri").replace("postgresql://", "postgresql+asyncpg://")
    new_time = async_context["new_time"]
    past_new_time = async_context["past_new_time"]

    # Database connection setup
    engine = create_async_engine(db_uri, future=True, pool_size=10, max_overflow=5)
    SessionLocal = sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )

    async with SessionLocal() as session:
        # Get the dynamic first_time and last_time based on the new_time and past_new_time
        first_time, last_time = await get_first_and_last_time(
            session, new_time, past_new_time
        )

        await insert_preprocessed_data(session)

        # Call all preprocessing functions
        await add_moving_average(session, first_time, last_time)
        await add_rsi(session, first_time, last_time)
        await add_rsi_over(session, first_time, last_time)
        await update_labels(session, first_time, last_time)

        # After preprocessing, delete data older than one year from the most recent row in btc_preprocessed
        await delete_old_data_from_preprocessed(session)


def preprocess_data_fn(**context) -> None:
    s = time.time()
    ti = context["ti"]
    db_uri = ti.xcom_pull(key="db_uri", task_ids="create_table")
    minutes = ti.xcom_pull(key="minutes", task_ids="save_raw_data_from_UPBIT_API")
    initial_insert = ti.xcom_pull(
        key="initial_insert", task_ids="save_raw_data_from_UPBIT_API"
    )

    new_time = ti.xcom_pull(key="new_time", task_ids="save_raw_data_from_UPBIT_API")

    past_new_time = ti.xcom_pull(
        key="past_new_time", task_ids="save_raw_data_from_UPBIT_API"
    )

    current_time = ti.xcom_pull(
        key="current_time", task_ids="save_raw_data_from_UPBIT_API"
    )

    # 비동기 함수 호출 시 전달할 context 생성(XCom은 JSON직렬화를 요구해서 그냥 쓸려고하면 비동기함수와는 호환이 안됨)
    async_context = {
        "db_uri": db_uri,
        "initial_insert": initial_insert,
        "new_time": new_time,
        "past_new_time": past_new_time,
        "current_time": current_time,
        "minutes": minutes,
    }

    # preprocess task의 속도, 메모리, cpu, 테스트를 위한코드
    process = psutil.Process()
    initial_memory = process.memory_info().rss
    initial_cpu = process.cpu_percent(interval=None)

    asyncio.run(preprocess_data(async_context))

    final_memory = process.memory_info().rss
    final_cpu = process.cpu_percent(interval=None)
    memory_usage = final_memory - initial_memory
    cpu_usage = final_cpu - initial_cpu
    logger.info(
        f"Memory usage: {memory_usage / (1024 * 1024):.2f} MB, CPU usage: {cpu_usage:.2f}%"
    )
    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")
