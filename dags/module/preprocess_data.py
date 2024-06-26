import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta


def preprocess(df):
    df.replace(" ", np.nan, inplace=True)
    df.dropna(inplace=True)
    # datetime 형식 변환
    df[["date", "opendt"]] = df[["date", "opendt"]].apply(
        pd.to_datetime, format="%Y-%m-%d"
    )
    #
    df["dateAfter7Days"] = df["opendt"] + pd.Timedelta(days=7)

    # 각 moviecd별로 showcnt와 scrncnt의 합계를 계산하여 showAcc와 scrnAcc에 할당
    for moviecd in df["moviecd"].unique():
        # 해당 moviecd에 대한 데이터를 필터링
        subset = df[df["moviecd"] == moviecd]
        if not subset.empty:
            # opendt부터 dateAfter7Days까지의 기간에 대한 필터링
            mask = (df["date"] >= subset["opendt"].values[0]) & (
                df["date"] <= subset["dateAfter7Days"].values[0]
            )
            filtered_data = df[mask & (df["moviecd"] == moviecd)]

            if not filtered_data.empty:
                chowcnt_sum = filtered_data["showcnt"].sum()
                scrncnt_sum = filtered_data["scrncnt"].sum()

                df.loc[df["moviecd"] == moviecd, "showAcc"] = chowcnt_sum
                df.loc[df["moviecd"] == moviecd, "scrnAcc"] = scrncnt_sum

    df.dropna(inplace=True)

    # 총 관객 수 관련(target 변수)

    yesterday_kst = datetime.now(pytz.timezone("Asia/Seoul")) - timedelta(days=1)
    yesterday_str = yesterday_kst.strftime("%Y-%m-%d")

    # 마지막 관측값만 추출(total 변수)
    last = (
        df.sort_values("date")
        .groupby("moviecd")
        .last()
        .reset_index()[["date", "moviecd", "audiacc"]]
    )
    last = last[last["date"] != yesterday_str]
    last = last.rename(columns={"audiacc": "total"})
    last.drop(columns="date", inplace=True)

    df = pd.merge(df, last, how="left", on="moviecd")
    df = df[df["date"] == df["dateAfter7Days"]]

    df.dropna(inplace=True)

    df = df[["audiacc", "showAcc", "scrnAcc", "repgenrenm", "total"]]

    return df
