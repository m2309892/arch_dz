from fastapi import APIRouter, HTTPException, status

from app.schemas.scenario import PredictionResponse
from app.db import scenario_crud

router = APIRouter(prefix="/prediction", tags=["prediction"])


@router.get(
    "/{scenario_id}/",
    response_model=PredictionResponse,
    summary="Получение результатов предсказаний"
)
async def get_predictions(scenario_id: int) -> PredictionResponse:

    # Проверяем существование сценария
    scenario = await scenario_crud.get_scenario(scenario_id=scenario_id)
    
    if not scenario:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Сценарий с id={scenario_id} не найден"
        )
    
    # Получаем предсказания
    predictions = await scenario_crud.get_predict(scenario_id=scenario_id)
    
    return PredictionResponse(
        scenario_id=scenario_id,
        predictions=predictions
    )

