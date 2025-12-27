from typing import Optional, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.db.database import db
from app.db.models import Scenario, ScenarioStatus


class ScenarioCRUD:
    
    @staticmethod
    async def create_scenario(
        status: ScenarioStatus = ScenarioStatus.INIT_STARTUP,
        predict: Optional[Dict[str, Any]] = None
    ) -> int:
        async with db.get_session() as session:
            status_value = status.value if isinstance(status, ScenarioStatus) else status
            new_scenario = Scenario(
                status=status_value,
                predict=predict
            )
            session.add(new_scenario)
            await session.flush()
            scenario_id = new_scenario.id
            await session.commit()
            return scenario_id
    
    @staticmethod
    async def update_status(scenario_id: int, status: ScenarioStatus) -> bool:
        async with db.get_session() as session:
            status_value = status.value if isinstance(status, ScenarioStatus) else status
            stmt = (
                update(Scenario)
                .where(Scenario.id == scenario_id)
                .values(status=status_value)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0
    
    @staticmethod
    async def get_status(scenario_id: int) -> Optional[ScenarioStatus]:
        async with db.get_session() as session:
            stmt = select(Scenario.status).where(Scenario.id == scenario_id)
            result = await session.execute(stmt)
            status_value = result.scalar_one_or_none()
            if status_value:
                return ScenarioStatus(status_value)
            return None
    
    @staticmethod
    async def get_predict(scenario_id: int) -> Optional[Dict[str, Any]]:
        async with db.get_session() as session:
            stmt = select(Scenario.predict).where(Scenario.id == scenario_id)
            result = await session.execute(stmt)
            predict = result.scalar_one_or_none()
            return predict
    
    @staticmethod
    async def get_scenario(scenario_id: int) -> Optional[Scenario]:
        async with db.get_session() as session:
            stmt = select(Scenario).where(Scenario.id == scenario_id)
            result = await session.execute(stmt)
            scenario = result.scalar_one_or_none()
            return scenario
    
    @staticmethod
    async def update_predict(
        scenario_id: int,
        predict: Dict[str, Any]
    ) -> bool:
        async with db.get_session() as session:
            stmt = (
                update(Scenario)
                .where(Scenario.id == scenario_id)
                .values(predict=predict)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0


scenario_crud = ScenarioCRUD()
