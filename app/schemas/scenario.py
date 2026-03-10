from datetime import datetime
from typing import Optional, Dict, Any, List

from pydantic import BaseModel, Field
from app.db.models import ScenarioStatus


class ScenarioCreate(BaseModel):
    status: Optional[ScenarioStatus] = Field(default=ScenarioStatus.INIT_STARTUP)


class ScenarioUpdate(BaseModel):
    status: ScenarioStatus


class ScenarioResponse(BaseModel):
    id: int
    status: ScenarioStatus
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class ScenarioStatusResponse(BaseModel):
    id: int
    status: ScenarioStatus
    
    class Config:
        from_attributes = True


class PredictionResponse(BaseModel):
    scenario_id: int
    predictions: Optional[List[Dict[str, Any]]] = Field(default=None)

