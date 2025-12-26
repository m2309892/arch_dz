from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy import DateTime, JSON, Enum as SQLEnum, func
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase


class Base(DeclarativeBase):
    """Базовый класс для всех моделей."""
    pass


class ScenarioStatus(str, Enum):
    """Статусы сценария."""
    
    INIT_STARTUP = "init_startup"
    INIT_STARTUP_PROCESSING = "init_startup_processing"
    ACTIVE = "active"
    INIT_SHUTDOWN = "init_shutdown"
    IN_SHUTDOWN_PROCESSING = "in_shutdown_processing"
    INACTIVE = "inactive"


class Scenario(Base):
    """Модель сценария."""
    
    __tablename__ = "scenarios"
    
    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )
    status: Mapped[ScenarioStatus] = mapped_column(
        SQLEnum(ScenarioStatus, name="scenario_status"),
        default=ScenarioStatus.INIT_STARTUP
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
