import asyncio
from typing import Optional
import logging

import asyncpg
from sqlalchemy.exc import OperationalError

from app.orchestrator.runner_pool import RunnerPool
from app.orchestrator.outbox import OutboxManager
from app.orchestrator.kafka_client import KafkaClient
from app.db import scenario_crud, ScenarioStatus

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, runner_pool_size: int = 5, video_path: Optional[str] = None):
        self.runner_pool = RunnerPool(pool_size=runner_pool_size, video_path=video_path)
        self.outbox_manager = OutboxManager()
        self.kafka_client = KafkaClient()

        self._running = False
        self._event_task: Optional[asyncio.Task] = None
        self._retry_task: Optional[asyncio.Task] = None
        self._event_queue: asyncio.Queue = asyncio.Queue()

        logger.info(f"Orchestrator инициализирован с размером пула раннеров: {runner_pool_size}")

    async def start(self):
        if self._running:
            logger.warning("Оркестратор уже запущен")
            return

        self._running = True
        await self.kafka_client.connect()

        self.runner_pool.ensure_pool_size()

        self._event_task = asyncio.create_task(self._event_processor_loop(), name="EventProcessor")
        self._retry_task = asyncio.create_task(self._retry_consumer_loop(), name="RetryConsumer")

        logger.info("Оркестратор запущен")

    async def stop(self):
        if not self._running:
            return

        logger.info("Начало остановки оркестратора...")
        self._running = False

        for t in (self._event_task, self._retry_task):
            if t and not t.done():
                t.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(t), timeout=5.0)
                except asyncio.CancelledError:
                    pass
                except asyncio.TimeoutError:
                    logger.warning("Задача не завершилась за 5 секунд")

        self._event_task = None
        self._retry_task = None

        try:
            await self.kafka_client.disconnect()
        except Exception as e:
            logger.error(f"Ошибка при отключении Kafka клиента: {e}", exc_info=True)

        try:
            self.runner_pool.stop_all_runners()
        except Exception as e:
            logger.error(f"Ошибка при остановке раннеров: {e}", exc_info=True)

        await asyncio.sleep(0.5)

        logger.info("Оркестратор остановлен")

    async def create_scenario(self, status: Optional[ScenarioStatus] = None) -> int:
        scenario_id = await scenario_crud.create_scenario(
            status=status or ScenarioStatus.INIT_STARTUP
        )

        await self.outbox_manager.create_event(
            scenario_id=scenario_id,
            event_type="scenario_created",
            event_data={
                "scenario_id": scenario_id,
                "status": (status or ScenarioStatus.INIT_STARTUP).value
            },
            topic="scenarios"
        )

        self._event_queue.put_nowait({
            "type": "scenario_created",
            "scenario_id": scenario_id,
            "status": status or ScenarioStatus.INIT_STARTUP
        })

        payload = {"scenario_id": scenario_id, "status": (status or ScenarioStatus.INIT_STARTUP).value}
        if self.kafka_client.connected:
            direct_ok = await self.kafka_client.send_message(
                topic="scenarios",
                key=str(scenario_id),
                value=payload
            )
            logger.info("Прямая отправка в Kafka (scenarios): success=%s, scenario_id=%s", direct_ok, scenario_id)
        else:
            logger.warning("Kafka не подключен, прямая отправка пропущена, scenario_id=%s", scenario_id)

        logger.info(f"Создан сценарий через оркестратор: scenario_id={scenario_id}")
        return scenario_id

    async def update_scenario_status(
        self,
        scenario_id: int,
        new_status: ScenarioStatus
    ) -> bool:
        success = await scenario_crud.update_status(
            scenario_id=scenario_id,
            status=new_status
        )

        if not success:
            return False

        await self.outbox_manager.create_event(
            scenario_id=scenario_id,
            event_type="scenario_status_updated",
            event_data={
                "scenario_id": scenario_id,
                "status": new_status.value
            },
            topic="scenarios"
        )

        self._event_queue.put_nowait({
            "type": "scenario_status_updated",
            "scenario_id": scenario_id,
            "status": new_status
        })

        return True

    async def _event_processor_loop(self):
        logger.info("Цикл обработки событий начал работу")
        iteration = 0
        try:
            while self._running:
                try:
                    iteration += 1
                    if iteration == 1 or iteration % 100 == 0:
                        logger.info(f"Цикл обработки событий, итерация {iteration}, _running={self._running}")

                    try:
                        event = self._event_queue.get_nowait()
                        logger.info(f"Получено событие из очереди: {event}")
                        await self._process_event(event)
                    except asyncio.QueueEmpty:
                        pass

                    await self._process_outbox_events()
                    await self._process_shutdown_scenarios()

                    await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    logger.info("Цикл обработки событий отменен")
                    break
                except Exception as e:
                    logger.error(f"Ошибка в цикле обработки событий: {e}", exc_info=True)
                    await asyncio.sleep(1)
        finally:
            logger.info(f"Цикл обработки событий завершен, всего итераций: {iteration}")

    async def _process_event(self, event: dict):
        event_type = event.get("type")
        scenario_id = event.get("scenario_id")

        logger.info(f"Обработка события: type={event_type}, scenario_id={scenario_id}")

        if event_type == "scenario_created":
            pass
        elif event_type == "scenario_status_updated":
            pass

    async def _process_outbox_events(self):
        try:
            events = await self.outbox_manager.get_unprocessed_events(limit=10)

            if events:
                logger.info("Найдено %d необработанных событий в outbox", len(events))

            for event in events:
                try:
                    logger.info(
                        "Обработка события из outbox: event_id=%s, scenario_id=%s, type=%s, topic=%s",
                        event.id, event.scenario_id, event.event_type, event.topic
                    )

                    if not self.kafka_client.connected:
                        logger.warning("Kafka клиент не подключен, пропускаем событие event_id=%s", event.id)
                        continue

                    success = await self.kafka_client.send_message(
                        topic=event.topic,
                        key=str(event.scenario_id),
                        value=event.event_data
                    )

                    if success:
                        await self.outbox_manager.mark_as_processed(event.id)
                        logger.info("Событие отправлено в Kafka: event_id=%s, scenario_id=%s", event.id, event.scenario_id)
                    else:
                        logger.warning("Не удалось отправить событие в Kafka: event_id=%s", event.id)
                except Exception as e:
                    logger.error("Ошибка при обработке события из outbox event_id=%s: %s", event.id, e, exc_info=True)
        except (asyncpg.exceptions.ConnectionDoesNotExistError, ConnectionResetError, OSError, OperationalError) as e:
            logger.warning(
                "Ошибка соединения с БД при чтении outbox (пауза 5 с): %s",
                e,
                exc_info=False
            )
            await asyncio.sleep(5)
        except Exception as e:
            logger.error("Ошибка при получении событий из outbox: %s", e, exc_info=True)

    async def _process_shutdown_scenarios(self):
        try:
            scenarios = await scenario_crud.get_scenarios_by_status(
                ScenarioStatus.INIT_SHUTDOWN,
                limit=10,
            )
            for scenario in scenarios:
                try:
                    sid = scenario.id
                    await scenario_crud.update_status(sid, ScenarioStatus.IN_SHUTDOWN_PROCESSING)
                    logger.info("Сценарий scenario_id=%s: INIT_SHUTDOWN → IN_SHUTDOWN_PROCESSING", sid)
                    await asyncio.sleep(0.2)
                    await scenario_crud.update_status(sid, ScenarioStatus.INACTIVE)
                    logger.info("Сценарий scenario_id=%s: IN_SHUTDOWN_PROCESSING → INACTIVE", sid)

                    await scenario_crud.update_status(sid, ScenarioStatus.INIT_STARTUP)
                    await self.outbox_manager.create_event(
                        scenario_id=sid,
                        event_type="scenario_restarted",
                        event_data={"scenario_id": sid, "status": ScenarioStatus.INIT_STARTUP.value},
                        topic="scenarios",
                    )
                    payload = {"scenario_id": sid, "status": ScenarioStatus.INIT_STARTUP.value}
                    if self.kafka_client.connected:
                        ok = await self.kafka_client.send_message(
                            topic="scenarios",
                            key=str(sid),
                            value=payload,
                        )
                        logger.info(
                            "Сценарий scenario_id=%s перезапущен (INACTIVE → очередь): success=%s",
                            sid, ok,
                        )
                    else:
                        logger.warning(
                            "Kafka не подключен, перезапуск scenario_id=%s пропущен",
                            sid,
                        )
                except Exception as e:
                    logger.error(
                        "Ошибка при обработке shutdown сценария scenario_id=%s: %s",
                        scenario.id, e, exc_info=True
                    )
        except (asyncpg.exceptions.ConnectionDoesNotExistError, ConnectionResetError, OSError, OperationalError) as e:
            logger.warning(
                "Ошибка БД при обработке shutdown сценариев: %s",
                e, exc_info=False
            )
        except Exception as e:
            logger.error("Ошибка при обработке shutdown сценариев: %s", e, exc_info=True)

    async def _retry_consumer_loop(self):
        consumer = None
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                consumer = await self.kafka_client.create_consumer(
                    topic=self.kafka_client.retry_topic,
                    group_id="orchestrator_retry_consumer"
                )
                logger.info("Начато чтение из retry топика: %s", self.kafka_client.retry_topic)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        "Ошибка при создании consumer для retry (попытка %d/%d): %s. Повтор через %d с...",
                        attempt + 1, max_retries, e, retry_delay
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error("Не удалось создать consumer для retry после %d попыток: %s", max_retries, e)
                    return

        if not consumer:
            return

        try:
            async for msg in consumer:
                if not self._running:
                    break
                try:
                    message_value = msg.value
                    if not isinstance(message_value, dict):
                        logger.error("Неверный формат сообщения из retry: %s", type(message_value))
                        continue
                    await self._handle_retry_message(message_value)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error("Ошибка при обработке сообщения из retry: %s", e, exc_info=True)
        except asyncio.CancelledError:
            logger.info("Цикл чтения retry топика отменен")
            raise
        except Exception as e:
            logger.error("Ошибка в цикле чтения retry топика: %s", e, exc_info=True)
        finally:
            if consumer:
                try:
                    await asyncio.wait_for(asyncio.shield(consumer.stop()), timeout=5.0)
                    logger.info("Consumer для retry топика остановлен")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning("Ошибка при остановке retry consumer: %s", e)

    async def _handle_retry_message(self, message: dict):
        scenario_id = message.get("scenario_id")
        reason = message.get("reason", "unknown")

        logger.warning(
            "Получено сообщение из retry топика (падение раннера): scenario_id=%s, reason=%s",
            scenario_id, reason
        )

        scenario = await scenario_crud.get_scenario(scenario_id)
        if not scenario:
            logger.error("Сценарий не найден: scenario_id=%s", scenario_id)
            return

        await scenario_crud.update_status(scenario_id, ScenarioStatus.INIT_SHUTDOWN)
        logger.info(
            "Инициирован shutdown сценария после падения раннера: scenario_id=%s, reason=%s → INIT_SHUTDOWN",
            scenario_id, reason
        )
