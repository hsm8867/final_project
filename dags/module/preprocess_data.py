import pandas as pd
import datetime
from datetime import timedelta


def preprocess(df):
    # 불필요한 공백 데이터 제거
    df = df[df["openDt"] != " "]

    # 같은 개봉일 영화 수 초기화
    df["sameOpenDtCnt"] = ""

    for i in range(len(df)):
        row = df.iloc[i, :]
        movieCd = row["movieCd"]
        openDt = row["openDt"]
        df_search = df[df["openDt"].str.match(openDt)]
        df_groupby = df_search.groupby(["movieCd"])
        row["sameOpenDtCnt"] = len(df_groupby)
        df.iloc[i] = row

    # 7일 뒤 날짜 계산 및 새로운 컬럼 생성
    df["dateAfter7Days"] = df["openDt"].apply(
        lambda x: (
            datetime.datetime.strptime(x, "%Y-%m-%d") + timedelta(days=7)
        ).strftime("%Y-%m-%d")
    )

    # showAcc, scrnAcc 초기화
    df["showAcc"] = 0
    df["scrnAcc"] = 0

    # showAcc, scrnAcc 계산
    for i in range(len(df)):
        row = df.iloc[i, :]
        movieCd = row["movieCd"]
        openDt = row["openDt"]
        df_search = df[
            (df["date"] <= df["dateAfter7Days"]) & (df["movieCd"] == movieCd)
        ]
        row["showAcc"] = df_search["showCnt"].sum()
        row["scrnAcc"] = df_search["scrnCnt"].sum()
        df.iloc[i] = row

    # 중복 제거 및 마지막 날짜 값만 남기기
    y = df.sort_values(by="date").drop_duplicates(subset="movieNm", keep="last")
    y = y[["movieCd", "audiAcc"]]
    y.rename(columns={"audiAcc": "total"}, inplace=True)
    df_search = df[df["date"] == df["dateAfter7Days"]]
    data = pd.merge(df_search, y, on="movieCd", how="left")
    data = data[
        [
            "showCnt",
            "scrnCnt",
            "audiAcc",
            "sameOpenDtCnt",
            "showAcc",
            "scrnAcc",
            "total",
        ]
    ]

    return data
