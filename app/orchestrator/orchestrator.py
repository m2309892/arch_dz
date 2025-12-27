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
    
    def __init__(self, runner_pool_size: int = 5, video_path: Optional[str] = None):
        """
        Инициализация оркестратора.
        
        Args:
            runner_pool_size: Размер пула раннеров (количество воркеров)
            video_path: Путь к видеофайлу для раннеров (опционально)
        """
        self.runner_pool = RunnerPool(pool_size=runner_pool_size, video_path=video_path)
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
        
        logger.info("Начало остановки оркестратора...")
        self._running = False
        
        # Сначала останавливаем Kafka клиент
        try:
            await self.kafka_client.disconnect()
        except Exception as e:
            logger.error(f"Ошибка при отключении Kafka клиента: {e}", exc_info=True)
        
        # Останавливаем все раннеры
        try:
            self.runner_pool.stop_all_runners()
        except Exception as e:
            logger.error(f"Ошибка при остановке раннеров: {e}", exc_info=True)
        
        # Ждем завершения потоков (они должны остановиться сами, когда _running = False)
        if self._event_processor_thread and self._event_processor_thread.is_alive():
            self._event_processor_thread.join(timeout=5)
            if self._event_processor_thread.is_alive():
                logger.warning("Поток обработки событий не остановился за 5 секунд")
        
        if self._retry_consumer_thread and self._retry_consumer_thread.is_alive():
            self._retry_consumer_thread.join(timeout=5)
            if self._retry_consumer_thread.is_alive():
                logger.warning("Поток retry consumer не остановился за 5 секунд")
        
        # Небольшая задержка, чтобы дать время всем операциям завершиться
        await asyncio.sleep(0.5)
        
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
            logger.info("Запуск event loop в потоке обработки событий")
            loop.run_until_complete(self._event_processor_loop())
            logger.info("Event loop завершился нормально")
        except Exception as e:
            logger.error(f"Ошибка в потоке обработки событий: {e}", exc_info=True)
        finally:
            try:
                loop.close()
                logger.info("Event loop закрыт")
            except Exception as e:
                logger.error(f"Ошибка при закрытии event loop: {e}", exc_info=True)
    
    async def _event_processor_loop(self):
        """Основной цикл обработки событий."""
        logger.info("Цикл обработки событий начал работу")
        iteration = 0
        try:
            while self._running:
                try:
                    iteration += 1
                    if iteration == 1 or iteration % 100 == 0:  # Логируем первую и каждую 100-ю итерацию
                        logger.info(f"Цикл обработки событий работает, итерация {iteration}, _running={self._running}")
                    
                    # Обрабатываем события из очереди
                    try:
                        event = self._event_queue.get_nowait()
                        logger.info(f"Получено событие из очереди: {event}")
                        await self._process_event(event)
                    except:
                        pass  # Очередь пуста
                    
                    # Обрабатываем события из outbox и отправляем в Kafka
                    await self._process_outbox_events()
                    
                    await asyncio.sleep(0.1)  # Небольшая задержка между итерациями (уменьшено для более быстрой обработки)
                except asyncio.CancelledError:
                    logger.info("Цикл обработки событий отменен")
                    break
                except Exception as e:
                    logger.error(f"Ошибка в цикле обработки событий: {e}", exc_info=True)
                    await asyncio.sleep(1)
        finally:
            logger.info(f"Цикл обработки событий завершен, всего итераций: {iteration}")
    
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
        try:
            events = await self.outbox_manager.get_unprocessed_events(limit=10)
            
            if events:
                logger.info(f"Найдено {len(events)} необработанных событий в outbox")
            
            for event in events:
                try:
                    logger.info(f"Обработка события из outbox: event_id={event.id}, scenario_id={event.scenario_id}, type={event.event_type}, topic={event.topic}")
                    
                    # Проверяем, что Kafka клиент подключен
                    if not self.kafka_client.connected:
                        logger.warning(f"Kafka клиент не подключен, пропускаем событие event_id={event.id}")
                        continue
                    
                    logger.info(f"Попытка отправить событие в Kafka: event_id={event.id}, topic={event.topic}, key={event.scenario_id}")
                    
                    # Отправляем в Kafka
                    success = await self.kafka_client.send_message(
                        topic=event.topic,
                        key=str(event.scenario_id),
                        value=event.event_data
                    )
                    
                    logger.info(f"Результат отправки события event_id={event.id}: success={success}")
                    
                    if success:
                        # Помечаем как обработанное
                        await self.outbox_manager.mark_as_processed(event.id)
                        logger.info(f"✓ Событие отправлено в Kafka и помечено как обработанное: event_id={event.id}, scenario_id={event.scenario_id}")
                    else:
                        logger.warning(f"✗ Не удалось отправить событие в Kafka: event_id={event.id}")
                except Exception as e:
                    logger.error(f"Ошибка при обработке события из outbox event_id={event.id}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Ошибка при получении событий из outbox: {e}", exc_info=True)
    
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
        consumer = None
        stop_event = asyncio.Event()
        max_retries = 5
        retry_delay = 2  # секунды
        
        # Пытаемся создать consumer с повторными попытками
        for attempt in range(max_retries):
            try:
                # Создаем consumer для retry топика
                consumer = await self.kafka_client.create_consumer(
                    topic=self.kafka_client.retry_topic,
                    group_id="orchestrator_retry_consumer"
                )
                
                logger.info(f"Начато чтение из retry топика: {self.kafka_client.retry_topic}")
                break  # Успешно создали consumer
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Ошибка при создании consumer для retry топика (попытка {attempt + 1}/{max_retries}): {e}. "
                        f"Повторная попытка через {retry_delay} секунд..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Не удалось создать consumer для retry топика после {max_retries} попыток: {e}")
                    return
        
        if not consumer:
            logger.error("Consumer не создан, выходим из retry consumer loop")
            return
        
        try:
            # Читаем сообщения из retry топика
            async for msg in consumer:
                if not self._running:
                    logger.info("Получен сигнал остановки, завершаем чтение retry топика")
                    break
                
                try:
                    message_value = msg.value
                    # message_value уже является словарем благодаря value_deserializer
                    if not isinstance(message_value, dict):
                        logger.error(f"Неверный формат сообщения из retry топика: {type(message_value)}")
                        continue
                    
                    await self._handle_retry_message(message_value)
                except asyncio.CancelledError:
                    logger.info("Обработка сообщения отменена")
                    break
                except Exception as e:
                    logger.error(f"Ошибка при обработке сообщения из retry топика: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Цикл чтения retry топика отменен")
        except Exception as e:
            logger.error(f"Ошибка в цикле чтения retry топика: {e}", exc_info=True)
        finally:
            if consumer:
                try:
                    await consumer.stop()
                    logger.info("Consumer для retry топика остановлен")
                except Exception as e:
                    logger.error(f"Ошибка при остановке consumer: {e}", exc_info=True)
    
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
