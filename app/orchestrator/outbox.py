from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.db.database import db
from app.db.models import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxManager:
    @staticmethod
    async def create_event(
        scenario_id: int,
        event_type: str,
        event_data: Dict[str, Any],
        topic: str
    ) -> int:
        async with db.get_session() as session:
            event = OutboxEvent(
                scenario_id=scenario_id,
                event_type=event_type,
                event_data=event_data,
                topic=topic,
                processed=False
            )
            session.add(event)
            await session.flush()
            event_id = event.id
            await session.commit()
            logger.info(f"Создано событие в outbox: event_id={event_id}, scenario_id={scenario_id}, type={event_type}")
            return event_id
    
    @staticmethod
    async def get_unprocessed_events(limit: int = 100) -> List[OutboxEvent]:
        async with db.get_session() as session:
            stmt = (
                select(OutboxEvent)
                .where(OutboxEvent.processed == False)
                .order_by(OutboxEvent.created_at)
                .limit(limit)
            )
            result = await session.execute(stmt)
            events = result.scalars().all()
            return list(events)
    
    @staticmethod
    async def mark_as_processed(event_id: int) -> bool:
        async with db.get_session() as session:
            stmt = (
                update(OutboxEvent)
                .where(OutboxEvent.id == event_id)
                .values(processed=True, processed_at=datetime.now())
            )
            result = await session.execute(stmt)
            await session.commit()
            success = result.rowcount > 0
            if success:
                logger.info(f"Событие помечено как обработанное: event_id={event_id}")
            return success
