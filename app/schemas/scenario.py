"""Схемы для работы со сценариями."""

from datetime import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel, Field
from app.db.models import ScenarioStatus


class ScenarioCreate(BaseModel):
    """Схема для создания сценария."""
    
    status: Optional[ScenarioStatus] = Field(
        default=ScenarioStatus.INIT_STARTUP,
        description="Начальный статус сценария"
    )


class ScenarioUpdate(BaseModel):
    """Схема для обновления статуса сценария."""
    
    status: ScenarioStatus = Field(
        description="Новый статус сценария"
    )


class ScenarioResponse(BaseModel):
    """Схема ответа с информацией о сценарии."""
    
    id: int
    status: ScenarioStatus
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class ScenarioStatusResponse(BaseModel):
    """Схема ответа со статусом сценария."""
    
    id: int
    status: ScenarioStatus
    
    class Config:
        from_attributes = True


class PredictionResponse(BaseModel):
    """Схема ответа с предсказаниями."""
    
    scenario_id: int
    predictions: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Результаты предсказаний"
    )

