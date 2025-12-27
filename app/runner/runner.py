"""Раннер для обработки видео и работы с Inference."""

import asyncio
import logging
from typing import Optional, Dict, Any
from pathlib import Path

from app.runner.video_processor import VideoProcessor, DEFAULT_TEST_VIDEO_PATH
from app.runner.inference_client import InferenceClient
from app.db import scenario_crud, ScenarioStatus
from app.orchestrator.kafka_client import KafkaClient

logger = logging.getLogger(__name__)


class Runner:
    """Раннер для обработки видео и получения предсказаний."""
    
    def __init__(
        self,
        runner_id: str,
        video_path: str = DEFAULT_TEST_VIDEO_PATH,
        inference_url: str = "http://localhost:8001"
    ):
        """
        Инициализация раннера.
        
        Args:
            runner_id: ID раннера
            video_path: Путь к видеофайлу (по умолчанию test_videos/test_video.mp4)
            inference_url: URL Inference сервиса
        
        Примечание: Для тестирования поместите видеофайл в папку test_videos/ в корне проекта
        """
        self.runner_id = runner_id
        self.video_processor = VideoProcessor(video_path)
        self.inference_client = InferenceClient(inference_url)
        self.kafka_client = KafkaClient()
        self._running = False
        
        logger.info(f"Runner инициализирован: runner_id={runner_id}, video_path={video_path}")
    
    async def start(self):
        """Запуск раннера."""
        if self._running:
            logger.warning(f"Runner {self.runner_id} уже запущен")
            return
        
        self._running = True
        await self.kafka_client.connect()
        logger.info(f"Runner {self.runner_id} запущен")
    
    async def stop(self):
        """Остановка раннера."""
        if not self._running:
            return
        
        self._running = False
        await self.kafka_client.disconnect()
        logger.info(f"Runner {self.runner_id} остановлен")
    
    async def process_scenario(self, scenario_id: int) -> bool:
        """
        Обрабатывает сценарий: читает кадры, отправляет в Inference, сохраняет результаты.
        
        Args:
            scenario_id: ID сценария
        
        Returns:
            True если обработка прошла успешно, False при ошибке
        """
        try:
            logger.info(f"Runner {self.runner_id} начинает обработку scenario_id={scenario_id}")
            
            # 1. Чтение кадра из видео
            frame = self.video_processor.read_frame(frame_number=0)
            if not frame:
                logger.error(f"Не удалось прочитать кадр для scenario_id={scenario_id}")
                await self._handle_error(scenario_id, "failed_to_read_frame")
                return False
            
            logger.info(f"Кадр прочитан для scenario_id={scenario_id}, размер: {len(frame)} байт")
            
            # 2. Отправка кадра в Inference сервис
            try:
                predictions = await self.inference_client.predict(frame)
                logger.info(f"Получены предсказания от Inference для scenario_id={scenario_id}")
            except Exception as e:
                logger.error(f"Ошибка при обращении к Inference для scenario_id={scenario_id}: {e}")
                await self._handle_error(scenario_id, f"inference_error: {str(e)}")
                return False
            
            # 3. Получение результата и сохранение в БД
            try:
                await scenario_crud.update_predict(
                    scenario_id=scenario_id,
                    predict=predictions
                )
                logger.info(f"Предсказания сохранены в БД для scenario_id={scenario_id}")
            except Exception as e:
                logger.error(f"Ошибка при сохранении предсказаний для scenario_id={scenario_id}: {e}")
                await self._handle_error(scenario_id, f"db_save_error: {str(e)}")
                return False
            
            # 4. Организация результата - обновление статуса на active
            try:
                await scenario_crud.update_status(
                    scenario_id=scenario_id,
                    status=ScenarioStatus.ACTIVE
                )
                logger.info(f"Статус обновлен на ACTIVE для scenario_id={scenario_id}")
            except Exception as e:
                logger.error(f"Ошибка при обновлении статуса для scenario_id={scenario_id}: {e}")
                await self._handle_error(scenario_id, f"status_update_error: {str(e)}")
                return False
            
            logger.info(f"Runner {self.runner_id} успешно обработал scenario_id={scenario_id}")
            return True
            
        except Exception as e:
            logger.error(f"Неожиданная ошибка при обработке scenario_id={scenario_id}: {e}", exc_info=True)
            await self._handle_error(scenario_id, f"unexpected_error: {str(e)}")
            return False
    
    async def _handle_error(self, scenario_id: int, reason: str):
        """
        Обрабатывает ошибку: отправляет событие в retry топик Kafka.
        
        Args:
            scenario_id: ID сценария
            reason: Причина ошибки
        """
        try:
            # Получаем информацию о сценарии для retry
            scenario = await scenario_crud.get_scenario(scenario_id)
            event_data = {
                "scenario_id": scenario_id,
                "status": scenario.status.value if scenario else "unknown",
                "reason": reason,
                "runner_id": self.runner_id
            }
            
            # Отправляем в retry топик Kafka
            await self.kafka_client.send_to_retry_topic(
                scenario_id=scenario_id,
                event_data=event_data,
                reason=reason
            )
            
            logger.warning(
                f"Ошибка обработана, событие отправлено в retry топик: "
                f"scenario_id={scenario_id}, reason={reason}"
            )
        except Exception as e:
            logger.error(f"Критическая ошибка при отправке в retry топик: {e}", exc_info=True)
            # При ошибке отправки в retry топик раннер должен умереть
            self._running = False
            raise

