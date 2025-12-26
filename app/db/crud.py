"""CRUD операции для работы со сценариями через SQLAlchemy ORM."""

from typing import Optional, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.db.database import db
from app.db.models import Scenario, ScenarioStatus


class ScenarioCRUD:
    """Класс для выполнения CRUD операций со сценариями."""
    
    @staticmethod
    async def create_scenario(
        status: ScenarioStatus = ScenarioStatus.INIT_STARTUP,
        predict: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Создает новый сценарий.
        
        Args:
            status: Статус сценария (по умолчанию init_startup)
            predict: Словарь с предсказаниями (опционально)
        
        Returns:
            ID созданного сценария
        """
        async with db.get_session() as session:
            new_scenario = Scenario(
                status=status,
                predict=predict
            )
            session.add(new_scenario)
            await session.flush()
            scenario_id = new_scenario.id
            await session.commit()
            return scenario_id
    
    @staticmethod
    async def update_status(scenario_id: int, status: ScenarioStatus) -> bool:
        """
        Изменяет статус сценария.
        
        Args:
            scenario_id: ID сценария
            status: Новый статус
        
        Returns:
            True если обновление прошло успешно, False если сценарий не найден
        """
        async with db.get_session() as session:
            stmt = (
                update(Scenario)
                .where(Scenario.id == scenario_id)
                .values(status=status)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0
    
    @staticmethod
    async def get_status(scenario_id: int) -> Optional[ScenarioStatus]:
        """
        Получает текущий статус сценария.
        
        Args:
            scenario_id: ID сценария
        
        Returns:
            Статус сценария или None если сценарий не найден
        """
        async with db.get_session() as session:
            stmt = select(Scenario.status).where(Scenario.id == scenario_id)
            result = await session.execute(stmt)
            status_value = result.scalar_one_or_none()
            return status_value
    
    @staticmethod
    async def get_predict(scenario_id: int) -> Optional[Dict[str, Any]]:
        """
        Получает предсказания для сценария.
        
        Args:
            scenario_id: ID сценария
        
        Returns:
            Словарь с предсказаниями или None если сценарий не найден
        """
        async with db.get_session() as session:
            stmt = select(Scenario.predict).where(Scenario.id == scenario_id)
            result = await session.execute(stmt)
            predict = result.scalar_one_or_none()
            return predict
    
    @staticmethod
    async def get_scenario(scenario_id: int) -> Optional[Scenario]:
        """
        Получает полную информацию о сценарии.
        
        Args:
            scenario_id: ID сценария
        
        Returns:
            Объект Scenario или None если не найден
        """
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
        """
        Обновляет предсказания для сценария.
        
        Args:
            scenario_id: ID сценария
            predict: Словарь с предсказаниями
        
        Returns:
            True если обновление прошло успешно, False если сценарий не найден
        """
        async with db.get_session() as session:
            stmt = (
                update(Scenario)
                .where(Scenario.id == scenario_id)
                .values(predict=predict)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0


# Экземпляр для использования в других модулях
scenario_crud = ScenarioCRUD()
