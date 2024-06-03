import requests
from datetime import datetime, timedelta
import time
import pandas as pd
import pytz
from airflow.providers.postgres.hooks.postgres import PostgresHook

# API 키 설정
api_key = "12d75fdeab5071a1fff3090078f5701e"


def get_daily_box_office(today_date, key):
    url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={key}&targetDt={today_date}"
    res = requests.get(url)
    data = res.json()
    daily_data = []
    for movie in data["boxOfficeResult"]["dailyBoxOfficeList"]:
        daily_data.append(
            [
                today_date,
                movie["movieCd"],
                movie["movieNm"],
                movie["showCnt"],
                movie["scrnCnt"],
                movie["openDt"],
                movie["audiAcc"],
            ]
        )
    return daily_data


def get(**context):
    key = "5e38bdb1f56234fc61867650667fa591"
    # 업데이트 할 데이터의 날짜(어제)
    tz_kst = pytz.timezone("Asia/Seoul")
    today_kst = datetime.now(tz_kst)
    yesterday_kst = today_kst - timedelta(days=1)
    yesterday_str = yesterday_kst.strftime("%Y%m%d")

    # 오늘 날짜에 해당하는 데이터 가져오기
    data = get_daily_box_office(yesterday_str, key)

    # 데이터프레임으로 변환
    df = pd.DataFrame(
        data,
        columns=[
            "date",
            "moviecd",
            "movienm",
            "showcnt",
            "scrncnt",
            "opendt",
            "audiacc",
        ],
    )

    # Convert date and opendt to datetime format
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["opendt"] = pd.to_datetime(df["opendt"], errors="coerce")

    # Convert showcnt, scrncnt, and audiacc to integer format
    df["showcnt"] = df["showcnt"].astype(int, errors="ignore")
    df["scrncnt"] = df["scrncnt"].astype(int, errors="ignore")
    df["audiacc"] = df["audiacc"].astype(int, errors="ignore")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_sqlalchemy_engine()

    df.to_sql("movies", conn, schema="data", if_exists="append", index=False)


def fetch_movie_data(targetYear, key):
    all_movies = []
    i = 1  # start page

    while True:
        url = f"http://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={key}&openStartDt={targetYear}&itemPerPage=100&curPage={i}"
        res = requests.get(url)

        if res.status_code != 200:
            print(f"Failed to fetch data: {res.status_code}")
            break

        data = res.json()

        if not data.get("movieListResult", {}).get("movieList", []):
            break

        for movie in data["movieListResult"]["movieList"]:
            movie_info = {
                "moviecd": movie.get("movieCd"),
                # 'movieNm': movie.get('movieNm'),
                "repgenrenm": movie.get("repGenreNm"),
                "opendt": movie.get("openDt"),
            }
            all_movies.append(movie_info)

        i += 1
        time.sleep(1)

    return all_movies


def process_movie_info(movies):
    df = pd.DataFrame(movies)

    # Convert openDt to datetime format
    df["opendt"] = pd.to_datetime(df["opendt"], errors="coerce")

    # 업데이트 할 데이터의 날짜(어제)
    tz_kst = pytz.timezone("Asia/Seoul")
    today_kst = datetime.now(tz_kst)
    yesterday_kst = today_kst - timedelta(days=1)
    yesterday_str = yesterday_kst.strftime("%Y-%m-%d")

    # 해당 데이터 추출
    movie_info = df[df["opendt"] == yesterday_str]
    movie_info = movie_info.reset_index(drop=True)

    return movie_info


def update_movie_info(**context):
    key = "12d75fdeab5071a1fff3090078f5701e"
    targetYear = 2024

    movies = fetch_movie_data(targetYear, key)
    movie_info = process_movie_info(movies)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_sqlalchemy_engine()

    movie_info.to_sql(
        "movie_info", conn, schema="data", if_exists="append", index=False
    )
