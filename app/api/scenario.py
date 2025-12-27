"""Роутер для работы со сценариями."""

from fastapi import APIRouter, HTTPException, status
from typing import Dict

from app.schemas.scenario import (
    ScenarioCreate,
    ScenarioUpdate,
    ScenarioStatusResponse,
)
from app.orchestrator import Orchestrator
from app.db import ScenarioStatus

router = APIRouter(prefix="/scenario", tags=["scenario"])

# Глобальный экземпляр оркестратора (будет инициализирован в main.py)
orchestrator: Orchestrator = None


@router.post(
    "/",
    response_model=Dict[str, int],
    status_code=status.HTTP_201_CREATED,
    summary="Инициализация стейт-машины сценария"
)
async def create_scenario(scenario_data: ScenarioCreate) -> Dict[str, int]:
    """
    Инициализирует новый сценарий (стейт-машину).
    
    Создает новый сценарий с указанным начальным статусом через оркестратор.
    """
    if orchestrator is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Оркестратор не инициализирован"
        )
    
    scenario_id = await orchestrator.create_scenario(
        status=scenario_data.status
    )
    return {"scenario_id": scenario_id}


@router.post(
    "/{scenario_id}/",
    response_model=Dict[str, str],
    summary="Изменение статуса стейт-машины сценария"
)
async def update_scenario_status(
    scenario_id: int,
    scenario_update: ScenarioUpdate
) -> Dict[str, str]:
    """
    Изменяет статус стейт-машины сценария.
    
    Обновляет текущий статус сценария на новый через оркестратор.
    """
    if orchestrator is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Оркестратор не инициализирован"
        )
    
    success = await orchestrator.update_scenario_status(
        scenario_id=scenario_id,
        new_status=scenario_update.status
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Сценарий с id={scenario_id} не найден"
        )
    
    return {"status": "updated", "scenario_id": str(scenario_id)}


@router.get(
    "/{scenario_id}/",
    response_model=ScenarioStatusResponse,
    summary="Получение информации о текущем статусе сценария"
)
async def get_scenario_status(scenario_id: int) -> ScenarioStatusResponse:
    """
    Получает информацию о текущем статусе сценария.
    
    Возвращает текущий статус стейт-машины сценария.
    """
    from app.db import scenario_crud
    
    scenario = await scenario_crud.get_scenario(scenario_id=scenario_id)
    
    if not scenario:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Сценарий с id={scenario_id} не найден"
        )
    
    # Преобразуем строку из БД в enum для ответа
    status_enum = ScenarioStatus(scenario.status) if isinstance(scenario.status, str) else scenario.status
    return ScenarioStatusResponse(
        id=scenario.id,
        status=status_enum
    )
