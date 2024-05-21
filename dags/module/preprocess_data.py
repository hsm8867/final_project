import pandas as pd
import datetime
from datetime import timedelta


def add_7_days(date_str):
    try:
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return (date_obj + timedelta(days=7)).date()  # 날짜 형식으로 반환
    except ValueError:
        return None


def preprocess(df_dict):

    # dictionary를 DataFrame으로 변환
    df = pd.DataFrame.from_dict(df_dict)

    # 불필요한 공백 데이터 제거 및 opendt 열의 None 값 처리
    df = df[df["opendt"].notna()]
    df["opendt"] = df["opendt"].astype(str)

    # opendt 열을 문자열로 변환
    df["opendt"] = df["opendt"].astype(str)

    # 같은 개봉일 영화 수 초기화
    df["sameOpenDtCnt"] = ""

    for i in range(len(df)):
        row = df.iloc[i, :]
        movieCd = row["moviecd"]
        opendt = row["opendt"]
        df_search = df[df["opendt"].str.match(opendt)]
        df_groupby = df_search.groupby(["moviecd"])
        row["sameOpenDtCnt"] = len(df_groupby)
        df.iloc[i] = row

    # 7일 뒤 날짜 계산 및 새로운 컬럼 생성
    df["dateAfter7Days"] = df["opendt"].apply(add_7_days)

    # showAcc, scrnAcc 초기화
    df["showAcc"] = 0
    df["scrnAcc"] = 0

    # showAcc, scrnAcc 계산
    for i in range(len(df)):
        row = df.iloc[i, :]  # apply 쓰자
        movieCd = row["moviecd"]
        openDt = row["opendt"]
        df_search = df[
            (df["date"] <= df["dateAfter7Days"]) & (df["moviecd"] == movieCd)
        ]
        row["showAcc"] = df_search["showcnt"].sum()
        row["scrnAcc"] = df_search["scrncnt"].sum()
        df.iloc[i] = row

    # 중복 제거 및 마지막 날짜 값만 남기기
    y = df.sort_values(by="date").drop_duplicates(subset="movienm", keep="last")
    y = y[["moviecd", "audiacc"]]
    y.rename(columns={"audiacc": "total"}, inplace=True)
    df_search = df[df["date"] == df["dateAfter7Days"]]
    data = pd.merge(df_search, y, on="moviecd", how="left")
    data = data[
        [
            "showcnt",
            "scrncnt",
            "audiacc",
            "sameOpenDtCnt",
            "showAcc",
            "scrnAcc",
            "total",
        ]
    ]

    return data.to_dict()
