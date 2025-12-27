"""Главный файл приложения FastAPI."""

from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.db.database import db
from app.api import scenario, prediction


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    # Startup
    await db.connect()
    yield
    # Shutdown
    await db.disconnect()


app = FastAPI(
    title="Распределенная система видео аналитики",
    description="API для управления сценариями и получения предсказаний",
    version="1.0.0",
    lifespan=lifespan
)

# Подключение роутеров
app.include_router(scenario.router)
app.include_router(prediction.router)


@app.get("/", tags=["root"])
async def root():
    """Корневой эндпоинт."""
    return {
        "message": "Распределенная система видео аналитики",
        "version": "1.0.0"
    }


@app.get("/health", tags=["health"])
async def health_check():
    """Проверка здоровья приложения."""
    return {"status": "healthy"}

