"""Главный файл приложения FastAPI."""

import os
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

from app.db.database import db
from app.api import scenario, prediction
from app.orchestrator import Orchestrator
from app.runner.video_processor import DEFAULT_TEST_VIDEO_PATH

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Глобальный экземпляр оркестратора
orchestrator: Orchestrator = None


def _get_video_path() -> str:
    """
    Получает путь к видеофайлу из переменной окружения или использует значение по умолчанию.
    
    Returns:
        Путь к видеофайлу
    """
    video_path = os.getenv("VIDEO_PATH")
    
    if video_path:
        return video_path
    
    # Пытаемся найти первое видео в папке test_videos
    test_videos_dir = Path("test_videos")
    if test_videos_dir.exists():
        video_files = list(test_videos_dir.glob("*.mp4"))
        if video_files:
            return str(video_files[0])
    
    # Используем значение по умолчанию
    return DEFAULT_TEST_VIDEO_PATH


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    global orchestrator
    
    # Startup
    logger.info("Запуск приложения...")
    await db.connect()
    
    # Получаем путь к видео
    video_path = _get_video_path()
    logger.info(f"Используется видеофайл: {video_path}")
    
    # Инициализируем оркестратор с путем к видео
    orchestrator = Orchestrator(runner_pool_size=5, video_path=video_path)
    await orchestrator.start()
    
    # Устанавливаем оркестратор в роутер
    scenario.orchestrator = orchestrator
    
    logger.info("Приложение запущено")
    
    yield
    
    # Shutdown
    logger.info("Остановка приложения...")
    try:
        if orchestrator:
            await orchestrator.stop()
    except Exception as e:
        logger.error(f"Ошибка при остановке оркестратора: {e}", exc_info=True)
    
    # Закрываем БД после остановки оркестратора
    try:
        await db.disconnect()
    except Exception as e:
        logger.error(f"Ошибка при закрытии БД: {e}", exc_info=True)
    
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
