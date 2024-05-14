import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# 임의의 데이터 생성
np.random.seed(0)
date_range = pd.date_range(start="2022-01-01", end="2024-01-01", freq="H")
data = pd.DataFrame(
    {
        "date": date_range,
        "temp": np.random.uniform(0, 40, len(date_range)),  # 온도
        "mm": np.random.uniform(0, 20, len(date_range)),  # 강수량
        "ws": np.random.uniform(0, 10, len(date_range)),  # 풍속
        "humidity": np.random.uniform(0, 100, len(date_range)),  # 습도
    }
)

# 데이터를 정규화
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(data[["temp", "mm", "ws", "humidity"]])


# 시계열 데이터를 LSTM 입력 형식으로 변환
def create_sequences(data, time_steps):
    X, Y = [], []
    for i in range(len(data) - time_steps):
        X.append(data[i : i + time_steps, :])
        Y.append(data[i + time_steps, 0])  # 온도는 1번째 열
    return torch.tensor(X).float(), torch.tensor(Y).float()


time_steps = 24  # 24시간을 한 묶음으로 입력
X, Y = create_sequences(scaled_data, time_steps)

# 데이터셋 분할 (Train/Test)
split = int(0.8 * len(data))
X_train, X_test = X[:split], X[split:]
Y_train, Y_test = Y[:split], Y[split:]


# LSTM 모델 정의
class LSTMModel(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers=1):
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])
        return out


# 모델 초기화
input_size = X.shape[2]
hidden_size = 50
num_layers = 1
model = LSTMModel(input_size, hidden_size, num_layers)

# 손실 함수 및 최적화 함수
criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# Early stopping 관련 변수 설정
best_loss = float("inf")
patience = 5  # for early stopping
counter = 0

# 모델 훈련
num_epochs = 10
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
        val_outputs = model(X_test)
        val_loss = criterion(val_outputs, Y_test)

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
    predicted_temp = model(X_test).numpy()

# 예측값을 실제 값의 스케일로 변환
predicted_temp = scaler.inverse_transform(
    np.hstack(
        (np.zeros((len(predicted_temp), 3)), np.array(predicted_temp).reshape(-1, 1))
    )
).T[0]

# 예측값 출력
print(predicted_temp)
