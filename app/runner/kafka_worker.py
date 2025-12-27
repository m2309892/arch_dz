"""Воркер для чтения задач из Kafka и обработки их раннерами."""

import asyncio
import logging
from typing import Optional

from app.runner.runner import Runner
from app.orchestrator.kafka_client import KafkaClient
from app.db.database import db

logger = logging.getLogger(__name__)


class KafkaWorker:
    """Воркер, который читает задачи из Kafka и обрабатывает их раннерами."""
    
    def __init__(
        self,
        worker_id: str,
        video_path: str,
        scenarios_topic: str = "scenarios",
        group_id: str = "runners_group",
        bootstrap_servers: str = "localhost:9092",
        inference_url: str = "http://localhost:8001"
    ):
        """
        Инициализация воркера.
        
        Args:
            worker_id: ID воркера
            video_path: Путь к видеофайлу
            scenarios_topic: Название топика для сценариев
            group_id: ID группы consumer (все раннеры в одной группе)
            bootstrap_servers: Адреса Kafka брокеров
            inference_url: URL Inference сервиса
        """
        self.worker_id = worker_id
        self.runner = Runner(
            runner_id=worker_id,
            video_path=video_path,
            inference_url=inference_url
        )
        self.kafka_client = KafkaClient(
            bootstrap_servers=bootstrap_servers,
            scenarios_topic=scenarios_topic
        )
        self.scenarios_topic = scenarios_topic
        self.group_id = group_id
        
        self._running = False
        
        logger.info(f"KafkaWorker инициализирован: worker_id={worker_id}")
    
    async def start(self):
        """Запуск воркера."""
        if self._running:
            logger.warning(f"Worker {self.worker_id} уже запущен")
            return
        
        self._running = True
        await db.connect()
        await self.kafka_client.connect()
        await self.runner.start()
        
        logger.info(f"Worker {self.worker_id} запущен")
    
    async def stop(self):
        """Остановка воркера."""
        if not self._running:
            return
        
        self._running = False
        await self.runner.stop()
        await self.kafka_client.disconnect()
        await db.disconnect()
        
        logger.info(f"Worker {self.worker_id} остановлен")
    
    async def run(self):
        """Основной цикл работы воркера."""
        consumer = None
        stop_event = asyncio.Event()
        
        try:
            # Создаем consumer для топика scenarios
            consumer = await self.kafka_client.create_consumer(
                topic=self.scenarios_topic,
                group_id=self.group_id
            )
            
            logger.info(f"Worker {self.worker_id} начал чтение из топика '{self.scenarios_topic}'")
            
            # Читаем сообщения из Kafka и обрабатываем их
            async for msg in consumer:
                if stop_event.is_set():
                    break
                
                try:
                    message_value = msg.value
                    
                    # Пропускаем служебные сообщения
                    if message_value.get("type") == "__init__":
                        continue
                    
                    scenario_id = message_value.get("scenario_id")
                    
                    if not scenario_id:
                        logger.warning(f"Сообщение без scenario_id пропущено: {message_value}")
                        continue
                    
                    logger.info(f"Worker {self.worker_id} получил задачу для scenario_id={scenario_id}")
                    
                    # Обрабатываем сценарий
                    success = await self.runner.process_scenario(scenario_id)
                    
                    if success:
                        logger.info(f"Worker {self.worker_id} успешно обработал scenario_id={scenario_id}")
                    else:
                        logger.warning(f"Worker {self.worker_id} не смог обработать scenario_id={scenario_id}")
                        # Runner уже отправил ошибку в retry топик и упал
                        break  # Воркер умирает
                        
                except Exception as e:
                    logger.error(f"Ошибка при обработке сообщения в worker {self.worker_id}: {e}", exc_info=True)
                    # При ошибке воркер умирает
                    break
            
        except Exception as e:
            logger.error(f"Ошибка в worker {self.worker_id}: {e}", exc_info=True)
        finally:
            if consumer:
                await consumer.stop()
            await self.stop()

