from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy import DateTime, JSON, func, String, Integer
from sqlalchemy.dialects.postgresql import ENUM as PostgresEnum
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase


class Base(DeclarativeBase):
    pass


class ScenarioStatus(str, Enum):
    INIT_STARTUP = "init_startup"
    INIT_STARTUP_PROCESSING = "init_startup_processing"
    ACTIVE = "active"
    INIT_SHUTDOWN = "init_shutdown"
    IN_SHUTDOWN_PROCESSING = "in_shutdown_processing"
    INACTIVE = "inactive"




class Scenario(Base):
    __tablename__ = "scenarios"
    
    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )
    status: Mapped[str] = mapped_column(
        PostgresEnum(
            'init_startup',
            'init_startup_processing',
            'active',
            'init_shutdown',
            'in_shutdown_processing',
            'inactive',
            name="scenario_status",
            create_type=False
        ),
        default='init_startup'
    )
    predict: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON,
        nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )
    
    def __repr__(self):
        return f"<Scenario(id={self.id}, status={self.status})>"


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    
    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )
    scenario_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False
    )
    event_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False
    )
    event_data: Mapped[Dict[str, Any]] = mapped_column(
        JSON,
        nullable=False
    )
    topic: Mapped[str] = mapped_column(
        String(100),
        nullable=False
    )
    processed: Mapped[bool] = mapped_column(
        default=False,
        nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )
    
    def __repr__(self):
        return f"<OutboxEvent(id={self.id}, scenario_id={self.scenario_id}, event_type={self.event_type}, processed={self.processed})>"
