"""Заглушка для Kafka клиента."""

import json
from datetime import datetime
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class KafkaClient:
    """Заглушка для работы с Kafka."""
    
    def __init__(self):
        """Инициализация Kafka клиента (заглушка)."""
        self.connected = False
        logger.info("KafkaClient инициализирован (заглушка)")
    
    async def connect(self):
        """Подключение к Kafka (заглушка)."""
        self.connected = True
        logger.info("Подключение к Kafka (заглушка)")
    
    async def disconnect(self):
        """Отключение от Kafka (заглушка)."""
        self.connected = False
        logger.info("Отключение от Kafka (заглушка)")
    
    async def send_message(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any]
    ) -> bool:
        """
        Отправка сообщения в Kafka (заглушка).
        
        Args:
            topic: Название топика
            key: Ключ сообщения
            value: Значение сообщения (словарь)
        
        Returns:
            True если отправка успешна
        """
        logger.info(
            f"[KAFKA STUB] Отправка сообщения в топик '{topic}' с ключом '{key}': "
            f"{json.dumps(value, ensure_ascii=False, indent=2)}"
        )
        # В реальной реализации здесь будет отправка в Kafka
        return True
    
    async def send_to_retry_topic(
        self,
        scenario_id: int,
        event_data: Dict[str, Any],
        reason: str
    ) -> bool:
        """
        Отправка сообщения в retry топик (заглушка).
        
        Args:
            scenario_id: ID сценария
            event_data: Данные события
            reason: Причина отправки в retry
        
        Returns:
            True если отправка успешна
        """
        message = {
            "scenario_id": scenario_id,
            "event_data": event_data,
            "reason": reason,
            "timestamp": str(datetime.now())
        }
        logger.warning(
            f"[KAFKA STUB RETRY] Отправка в retry топик для scenario_id={scenario_id}, "
            f"причина: {reason}"
        )
        return await self.send_message(
            topic="retry_topic",
            key=str(scenario_id),
            value=message
        )
    
    async def consume_retry_topic(self, callback) -> None:
        """
        Чтение сообщений из retry топика (заглушка).
        
        Args:
            callback: Функция-обработчик для каждого сообщения
        """
        # TODO: В реальной реализации здесь будет подписка на retry_topic
        # Пока это заглушка - в реальности здесь будет Kafka consumer
        logger.info("[KAFKA STUB] Подписка на retry_topic (заглушка)")
        # В реальной реализации:
        # async for message in kafka_consumer:
        #     await callback(message.value)

