import torch
import torch.nn as nn

import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine


def preprocess_data(data):
    # 날짜 변수 변환
    data["date"] = pd.to_datetime(data["date"], format="%Y-%m-%d %H:%M")

    # 시간 변수 생성
    data["hour"] = data["date"].dt.hour

    # 계절 그룹화
    data["month_group"] = pd.cut(
        data["date"].dt.month,
        bins=[0, 3, 6, 9, 12],
        labels=["winter", "spring", "summer", "autumn"],
    )
    data["month_group"].fillna("winter", inplace=True)

    # 더미 변수 변환
    dummy_df = pd.get_dummies(data["month_group"], dtype="int")

    # 더미 변수 concat
    data = pd.concat([data, dummy_df], axis=1)

    # 원 'month_group' 열 제거
    data.drop("month_group", axis=1, inplace=True)

    # 강수량 fillna 0
    data.fillna(0, inplace=True)

    # 날짜 열 제거
    data.drop(columns="date", inplace=True)

    return data


def create_sequences(data, time_steps):
    X, Y = [], []
    for i in range(len(data) - time_steps - 24):  # 수정
        X.append(data[i : i + time_steps, :])
        Y.append(data[i + time_steps : i + time_steps + 24, 0])
    return torch.tensor(X).float(), torch.tensor(Y).float()


# LSTM 모델 정의
class LSTMModel(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers=1, output_size=24):
        super(LSTMModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])  # 마지막 시간 단계의 출력만 사용
        return out


def train_fn(**context):
    # DB 접속 정보
    USERNAME = "postgres"
    PASSWORD = "mlecourse"
    HOST = "34.64.174.26"
    PORT = "5432"  # Default port for PostgreSQL
    DATABASE = "raw"
    ENGINE_PATH = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    engine = create_engine(ENGINE_PATH)

    query = "SELECT date, temp, mm, ws, humidity FROM data.temperature"
    data = pd.read_sql(query, engine)

    # 데이터 전처리
    data = preprocess_data(data)

    temp_min = data["temp"].min()
    temp_max = data["temp"].max()

    # 데이터를 정규화
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(data)

    time_steps = 72  # 72시간을 한 묶음으로 입력
    X, Y = create_sequences(scaled_data, time_steps)

    # 데이터셋 분할 (Train/Test)
    split = int(0.8 * len(data))
    X_train, X_test = X[:split], X[split:]
    Y_train, Y_test = Y[:split], Y[split:]

    # 모델 초기화
    input_size = X.shape[2]
    hidden_size = 50
    num_layers = 1
    output_size = 24
    model = LSTMModel(input_size, hidden_size, num_layers, output_size)

    # 손실 함수 및 최적화 함수
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Early stopping 관련 변수 설정
    best_loss = float("inf")
    patience = 5  # for early stopping
    counter = 0

    # 학습 데이터와 검증 데이터 분리
    split = int(len(X) * 0.8)
    X_train, X_val = X[:split], X[split:]
    Y_train, Y_val = Y[:split], Y[split:]

    # 모델 훈련
    num_epochs = 20
    for epoch in range(num_epochs):
        model.train()
        optimizer.zero_grad()
        outputs = model(X_train)
        loss = criterion(outputs, Y_train)
        loss.backward()
        optimizer.step()

        # 검증 데이터에 대한 손실 계산
        model.eval()
        with torch.no_grad():
            val_outputs = model(X_val)
            val_loss = criterion(val_outputs, Y_val)

        # 손실이 감소하면 모델 저장
        if val_loss < best_loss:
            best_loss = val_loss
            counter = 0
            torch.save(model.state_dict(), "best_model.pt")
        else:
            counter += 1

        # 조기 종료
        if counter >= patience:
            print(f"Early stopping at epoch {epoch+1}")
            break

        if (epoch + 1) % 10 == 0:
            print(
                f"Epoch [{epoch+1}/{num_epochs}], Train Loss: {loss.item():.4f}, Val Loss: {val_loss.item():.4f}"
            )

    # 모델 로드
    model.load_state_dict(torch.load("best_model.pt"))

    # 모델 평가
    model.eval()
    with torch.no_grad():
        test_outputs = model(X_val)

    # 결과 출력
    print(test_outputs)
