import pandas as pd
from fastapi import APIRouter, HTTPException

from app.utils import load_model, preprocess
from app.models.schemas.model_ import ModelResp, ModelReq

router = APIRouter()


@router.post("/predict", response_model=ModelResp)
async def predict(request: ModelReq):
    try:
        model = load_model()
        input = pd.DataFrame([request.model_dump()])
        processed_data = preprocess(input)
        result = model.predict(processed_data)
        return ModelResp(target=result[0])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
