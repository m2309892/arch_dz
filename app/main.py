"""Главный файл приложения FastAPI."""

from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

from app.db.database import db
from app.api import scenario, prediction
from app.orchestrator import Orchestrator

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Глобальный экземпляр оркестратора
orchestrator: Orchestrator = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    global orchestrator
    
    # Startup
    logger.info("Запуск приложения...")
    await db.connect()
    
    # Инициализируем оркестратор
    orchestrator = Orchestrator(runner_pool_size=5)
    await orchestrator.start()
    
    # Устанавливаем оркестратор в роутер
    scenario.orchestrator = orchestrator
    
    logger.info("Приложение запущено")
    
    yield
    
    # Shutdown
    logger.info("Остановка приложения...")
    if orchestrator:
        await orchestrator.stop()
    await db.disconnect()
    logger.info("Приложение остановлено")


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
