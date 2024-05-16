import requests
import json
import time
import datetime
import pandas as pd

# 오늘 날짜 가져오기
today_date = datetime.datetime.now().strftime("%Y%m%d")

# API 키 설정
api_key = "12d75fdeab5071a1fff3090078f5701e"


def get_daily_box_office(today_date, api_key):
    url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={api_key}&targetDt={today_date}"
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


# 함수를 사용하여 오늘 날짜에 해당하는 데이터 가져오기
data = get_daily_box_office(today_date, api_key)

# 데이터프레임으로 변환
df = pd.DataFrame(
    data,
    columns=["date", "movieCd", "movieNm", "showCnt", "scrnCnt", "openDt", "audiAcc"],
)
