"""Клиент для работы с Kafka."""

import json
import os
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, Callable, Awaitable
import logging
from dotenv import load_dotenv

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError

load_dotenv()

logger = logging.getLogger(__name__)


class KafkaClient:
    """Клиент для работы с Kafka."""
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        scenarios_topic: str = "scenarios",
        retry_topic: str = "retry_topic"
    ):
        """
        Инициализация Kafka клиента.
        
        Args:
            bootstrap_servers: Адреса Kafka брокеров (по умолчанию из переменной окружения KAFKA_BOOTSTRAP_SERVERS или localhost:9092)
            scenarios_topic: Название топика для сценариев
            retry_topic: Название топика для retry
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.scenarios_topic = scenarios_topic
        self.retry_topic = retry_topic
        
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        
        self.connected = False
        logger.info(f"KafkaClient инициализирован, bootstrap_servers={bootstrap_servers}")
    
    async def connect(self):
        """Подключение к Kafka."""
        try:
            # Создаем producer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            await self.producer.start()
            
            self.connected = True
            logger.info("Подключение к Kafka установлено")
        except Exception as e:
            logger.error(f"Ошибка при подключении к Kafka: {e}", exc_info=True)
            raise
    
    async def disconnect(self):
        """Отключение от Kafka."""
        try:
            if self.producer:
                await self.producer.stop()
                self.producer = None
            
            if self.consumer:
                await self.consumer.stop()
                self.consumer = None
            
            self.connected = False
            logger.info("Отключение от Kafka")
        except Exception as e:
            logger.error(f"Ошибка при отключении от Kafka: {e}", exc_info=True)
    
    async def send_message(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any]
    ) -> bool:
        """
        Отправка сообщения в Kafka.
        
        Args:
            topic: Название топика
            key: Ключ сообщения
            value: Значение сообщения (словарь)
        
        Returns:
            True если отправка успешна
        """
        if not self.producer:
            logger.error("Producer не инициализирован")
            return False
        
        try:
            await self.producer.send_and_wait(
                topic=topic,
                key=key,
                value=value
            )
            logger.info(f"Сообщение отправлено в топик '{topic}' с ключом '{key}'")
            return True
        except KafkaError as e:
            logger.error(f"Ошибка при отправке сообщения в Kafka: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке сообщения: {e}", exc_info=True)
            return False
    
    async def send_to_retry_topic(
        self,
        scenario_id: int,
        event_data: Dict[str, Any],
        reason: str
    ) -> bool:
        """
        Отправка сообщения в retry топик.
        
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
            "timestamp": datetime.now().isoformat()
        }
        logger.warning(
            f"Отправка в retry топик для scenario_id={scenario_id}, причина: {reason}"
        )
        return await self.send_message(
            topic=self.retry_topic,
            key=str(scenario_id),
            value=message
        )
    
    async def create_consumer(
        self,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "earliest"
    ) -> AIOKafkaConsumer:
        """
        Создает consumer для чтения из топика.
        
        Args:
            topic: Название топика
            group_id: ID группы consumer
            auto_offset_reset: Откуда начинать читать (earliest/latest)
        
        Returns:
            AIOKafkaConsumer
        """
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True
        )
        await consumer.start()
        logger.info(f"Consumer создан для топика '{topic}', group_id='{group_id}'")
        return consumer
    
    async def consume_messages(
        self,
        consumer: AIOKafkaConsumer,
        callback: Callable[[Dict[str, Any]], Awaitable[None]],
        stop_event: Optional[asyncio.Event] = None
    ):
        """
        Читает сообщения из consumer и вызывает callback.
        
        Args:
            consumer: Kafka consumer
            callback: Асинхронная функция-обработчик сообщений
            stop_event: Событие для остановки чтения
        """
        try:
            async for msg in consumer:
                if stop_event and stop_event.is_set():
                    break
                
                try:
                    await callback(msg.value)
                except Exception as e:
                    logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Ошибка при чтении сообщений из Kafka: {e}", exc_info=True)
        finally:
            await consumer.stop()
