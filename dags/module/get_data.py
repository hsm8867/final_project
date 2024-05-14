import requests
from datetime import datetime, timedelta
from pytz import timezone
from sqlalchemy import create_engine
import pandas as pd

# Database connection information
USERNAME = "postgres"
PASSWORD = "mlecourse"
HOST = "34.64.174.26"
PORT = "5432"  # Default port for PostgreSQL
DATABASE = "raw"

# SQLAlchemy engine for PostgreSQL
engine = create_engine(f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")


def get_weather_data():
    while True:
        try:
            # 한국
            KST = timezone("Asia/Seoul")

            url = "http://apis.data.go.kr/1360000/VilageFcstInfoService/getUltraSrtNcst"
            service_key = "wO9XsANP1Nt9zBBV1WlwVtpfusq7uJZzkoRJjkfjIQxkgaf9X%2FbBCYOCgaS60vye6TNtlhmP1yhL%2Fm9NojPLFw%3D%3D"

            # 웹 요청할 base_date, base_time 계산
            now = datetime.now()  # 현재 시각 데이터 생성
            now = now.astimezone(KST)

            # 40분 이전이면 현재 시보다 1시간 전 `base_time`을 요청
            if now.minute <= 40:
                # 단. 00:40분 이전이라면 `base_date`는 전날이고 `base_time`은 2300
                if now.hour == 0:
                    base_date = (now - timedelta(days=1)).strftime("%Y%m%d")
                    base_time = "2300"
                else:
                    base_date = now.strftime("%Y%m%d")
                    base_time = (now - timedelta(hours=1)).strftime("%H00")
            # 40분 이후면 현재 시와 같은 `base_time`을 요청
            else:
                base_date = base_date = now.strftime("%Y%m%d")
                base_time = now.strftime("%H00")

            # 웹 요청시 같이 전달될 데이터 = 요청 메시지
            params = {
                "serviceKey": service_key,
                "numOfRows": 30,
                "pageNo": 1,
                "dataType": "JSON",
                "base_date": base_date,
                "base_time": base_time,
                "nx": 60,  # 서울 기상관측소(종로구)
                "ny": 127,
            }

            res = requests.get(url=url, params=params)
            # res.raise_for_status()

            # response 데이터 정리
            data = res.json()["response"]["body"]["items"]["item"]

            categorys = {
                "T1H": "기온",
                "RN1": "1시간 강수량",
                "UUU": "동서바람성분",
                "VVV": "남북바람성분",
                "REH": "습도",
                "PTY": "강수형태",
                "VEC": "풍향",
                "WSD": "풍속",
            }

            # 최종 데이터 생성(dict)
            results = {}
            results["date"] = datetime.strptime(
                base_date + base_time, "%Y%m%d%H%M"
            ).strftime("%Y-%m-%d %H:%M:%S")
            for d in data:
                results[categorys[d["category"]]] = d["obsrValue"]

            # 필요한 데이터 추출 및 DF 변환
            filtered_data = {
                "date": results["date"],
                "temp": results["기온"],
                "mm": results["1시간 강수량"],
                "ws": results["풍속"],
                "humidity": results["습도"],
            }
            convert_data = pd.DataFrame(filtered_data, index=[0])

            if not convert_data.empty:
                return convert_data

        except Exception as e:  # 통신 등의 이유로 에러 발생 시, 다시 시도
            print("Re-Try")


def upload_data():
    # Fetch data using the function defined in get_data.py
    weather_data = get_weather_data()

    # Insert data into PostgreSQL database
    if not weather_data.empty:
        weather_data.to_sql(
            "weather_table", con=engine, if_exists="append", index=False
        )
        print("Data uploaded successfully.")
    else:
        print("No data to upload.")
