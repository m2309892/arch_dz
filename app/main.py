import os
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

from app.db.database import db
from app.api import scenario, prediction
from app.orchestrator import Orchestrator
from app.runner.video_processor import DEFAULT_TEST_VIDEO_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

orchestrator: Orchestrator = None


def _get_video_path() -> str:
    video_path = os.getenv("VIDEO_PATH")
    
    if video_path:
        return video_path
    
    test_videos_dir = Path("test_videos")
    if test_videos_dir.exists():
        video_files = list(test_videos_dir.glob("*.mp4"))
        if video_files:
            return str(video_files[0])
    
    return DEFAULT_TEST_VIDEO_PATH


@asynccontextmanager
async def lifespan(app: FastAPI):
    global orchestrator
    
    logger.info("Запуск приложения...")
    await db.connect()
    
    video_path = _get_video_path()
    logger.info(f"Используется видеофайл: {video_path}")
    
    orchestrator = Orchestrator(runner_pool_size=5, video_path=video_path)
    await orchestrator.start()
    
    scenario.orchestrator = orchestrator
    
    logger.info("Приложение запущено")
    
    yield
    
    logger.info("Остановка приложения...")
    try:
        if orchestrator:
            await orchestrator.stop()
    except Exception as e:
        logger.error(f"Ошибка при остановке оркестратора: {e}", exc_info=True)
    
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

app.include_router(scenario.router)
app.include_router(prediction.router)


@app.get("/", tags=["root"])
async def root():
    return {
        "message": "Распределенная система видео аналитики",
        "version": "1.0.0"
    }


@app.get("/health", tags=["health"])
async def health_check():
    return {"status": "healthy"}
