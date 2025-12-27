"""Главный оркестратор для управления сценариями и раннерами."""

import asyncio
import threading
from queue import Queue
from typing import Optional
import logging

from app.orchestrator.runner_pool import RunnerPool
from app.orchestrator.outbox import OutboxManager
from app.orchestrator.kafka_client import KafkaClient
from app.db import scenario_crud, ScenarioStatus

logger = logging.getLogger(__name__)


class Orchestrator:
    """Оркестратор для управления сценариями и раннерами."""
    
    def __init__(self, runner_pool_size: int = 5):
        """
        Инициализация оркестратора.
        
        Args:
            runner_pool_size: Размер пула раннеров (количество воркеров)
        """
        self.runner_pool = RunnerPool(pool_size=runner_pool_size)
        self.outbox_manager = OutboxManager()
        self.kafka_client = KafkaClient()
        
        self._running = False
        self._event_processor_thread: Optional[threading.Thread] = None
        self._retry_consumer_thread: Optional[threading.Thread] = None
        self._event_queue = Queue()  # Используем обычную Queue для межпоточной коммуникации
        
        logger.info(f"Orchestrator инициализирован с размером пула раннеров: {runner_pool_size}")
    
    async def start(self):
        """Запуск оркестратора."""
        if self._running:
            logger.warning("Оркестратор уже запущен")
            return
        
        self._running = True
        await self.kafka_client.connect()
        
        # Инициализируем пул раннеров
        self.runner_pool.ensure_pool_size()
        
        # Запускаем потоки
        self._event_processor_thread = threading.Thread(
            target=self._run_event_processor,
            daemon=True,
            name="EventProcessor"
        )
        self._retry_consumer_thread = threading.Thread(
            target=self._run_retry_consumer,
            daemon=True,
            name="RetryConsumer"
        )
        
        self._event_processor_thread.start()
        self._retry_consumer_thread.start()
        
        logger.info("Оркестратор запущен")
    
    async def stop(self):
        """Остановка оркестратора."""
        if not self._running:
            return
        
        self._running = False
        await self.kafka_client.disconnect()
        
        # Ждем завершения потоков
        if self._event_processor_thread:
            self._event_processor_thread.join(timeout=5)
        if self._retry_consumer_thread:
            self._retry_consumer_thread.join(timeout=5)
        
        logger.info("Оркестратор остановлен")
    
    async def create_scenario(self, status: Optional[ScenarioStatus] = None) -> int:
        """
        Создает новый сценарий через оркестратор.
        
        Args:
            status: Начальный статус сценария
        
        Returns:
            ID созданного сценария
        """
        # Создаем сценарий в БД
        scenario_id = await scenario_crud.create_scenario(
            status=status or ScenarioStatus.INIT_STARTUP
        )
        
        # Создаем событие в outbox для отправки в Kafka
        await self.outbox_manager.create_event(
            scenario_id=scenario_id,
            event_type="scenario_created",
            event_data={
                "scenario_id": scenario_id,
                "status": (status or ScenarioStatus.INIT_STARTUP).value
            },
            topic="scenarios"
        )
        
        # Добавляем событие в очередь для обработки
        self._event_queue.put({
            "type": "scenario_created",
            "scenario_id": scenario_id,
            "status": status or ScenarioStatus.INIT_STARTUP
        })
        
        logger.info(f"Создан сценарий через оркестратор: scenario_id={scenario_id}")
        return scenario_id
    
    async def update_scenario_status(
        self,
        scenario_id: int,
        new_status: ScenarioStatus
    ) -> bool:
        """
        Обновляет статус сценария через оркестратор.
        
        Args:
            scenario_id: ID сценария
            new_status: Новый статус
        
        Returns:
            True если обновление прошло успешно
        """
        # Обновляем статус в БД
        success = await scenario_crud.update_status(
            scenario_id=scenario_id,
            status=new_status
        )
        
        if not success:
            return False
        
        # Создаем событие в outbox для отправки в Kafka
        await self.outbox_manager.create_event(
            scenario_id=scenario_id,
            event_type="scenario_status_updated",
            event_data={
                "scenario_id": scenario_id,
                "status": new_status.value
            },
            topic="scenarios"
        )
        
        # Добавляем событие в очередь для обработки
        self._event_queue.put({
            "type": "scenario_status_updated",
            "scenario_id": scenario_id,
            "status": new_status
        })
        
        return True
    
    def _run_event_processor(self):
        """Первый поток: обработка событий от API."""
        logger.info("Поток обработки событий запущен")
        
        # Создаем новый event loop для этого потока
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self._event_processor_loop())
        except Exception as e:
            logger.error(f"Ошибка в потоке обработки событий: {e}", exc_info=True)
        finally:
            loop.close()
    
    async def _event_processor_loop(self):
        """Основной цикл обработки событий."""
        while self._running:
            try:
                # Обрабатываем события из очереди
                try:
                    event = self._event_queue.get_nowait()
                    await self._process_event(event)
                except:
                    pass  # Очередь пуста
                
                # Обрабатываем события из outbox и отправляем в Kafka
                await self._process_outbox_events()
                
                await asyncio.sleep(0.5)  # Небольшая задержка между итерациями
            except Exception as e:
                logger.error(f"Ошибка в цикле обработки событий: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _process_event(self, event: dict):
        """Обрабатывает одно событие."""
        event_type = event.get("type")
        scenario_id = event.get("scenario_id")
        
        logger.info(f"Обработка события: type={event_type}, scenario_id={scenario_id}")
        
        if event_type == "scenario_created":
            # Логика создания сценария уже выполнена
            pass
        elif event_type == "scenario_status_updated":
            # Логика обновления статуса уже выполнена
            pass
    
    async def _process_outbox_events(self):
        """Обрабатывает события из outbox и отправляет в Kafka."""
        events = await self.outbox_manager.get_unprocessed_events(limit=10)
        
        for event in events:
            try:
                # Отправляем в Kafka
                success = await self.kafka_client.send_message(
                    topic=event.topic,
                    key=str(event.scenario_id),
                    value=event.event_data
                )
                
                if success:
                    # Помечаем как обработанное
                    await self.outbox_manager.mark_as_processed(event.id)
                    logger.info(f"Событие отправлено в Kafka: event_id={event.id}")
            except Exception as e:
                logger.error(f"Ошибка при обработке события из outbox: {e}", exc_info=True)
    
    def _run_retry_consumer(self):
        """Второй поток: чтение retry топика из Kafka."""
        logger.info("Поток чтения retry топика запущен")
        
        # Создаем новый event loop для этого потока
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self._retry_consumer_loop())
        except Exception as e:
            logger.error(f"Ошибка в потоке чтения retry топика: {e}", exc_info=True)
        finally:
            loop.close()
    
    async def _retry_consumer_loop(self):
        """Основной цикл чтения retry топика."""
        # Подписываемся на retry топик
        await self.kafka_client.consume_retry_topic(callback=self._handle_retry_message)
        
        # В реальной реализации здесь будет бесконечный цикл чтения из Kafka
        # Пока это заглушка - просто проверяем периодически
        while self._running:
            try:
                # TODO: В реальной реализации здесь будет чтение из Kafka consumer
                # Пока это заглушка - просто ждем
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Ошибка в цикле чтения retry топика: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _handle_retry_message(self, message: dict):
        """
        Обрабатывает сообщение из retry топика.
        
        Args:
            message: Сообщение из retry топика
        """
        scenario_id = message.get("scenario_id")
        event_data = message.get("event_data", {})
        reason = message.get("reason", "unknown")
        
        logger.warning(
            f"Получено сообщение из retry топика: scenario_id={scenario_id}, reason={reason}"
        )
        
        # Получаем информацию о сценарии
        scenario = await scenario_crud.get_scenario(scenario_id)
        if not scenario:
            logger.error(f"Сценарий не найден: scenario_id={scenario_id}")
            return
        
        # Создаем новый раннер
        new_runner_id = self.runner_pool.create_runner()
        logger.info(f"Создан новый раннер после получения retry: runner_id={new_runner_id}")
        
        # Перезапускаем сценарий
        await scenario_crud.update_status(scenario_id, ScenarioStatus.INIT_STARTUP)
        await asyncio.sleep(0.1)  # Небольшая задержка
        await scenario_crud.update_status(scenario_id, ScenarioStatus.INIT_STARTUP_PROCESSING)
        
        # Создаем событие в outbox
        await self.outbox_manager.create_event(
            scenario_id=scenario_id,
            event_type="scenario_restarted",
            event_data={
                "scenario_id": scenario_id,
                "new_runner_id": new_runner_id,
                "reason": reason,
                "original_event": event_data
            },
            topic="scenarios"
        )
        
        logger.info(f"Сценарий перезапущен после retry: scenario_id={scenario_id}")
