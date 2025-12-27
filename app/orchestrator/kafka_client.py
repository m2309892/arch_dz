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
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        scenarios_topic: str = "scenarios",
        retry_topic: str = "retry_topic"
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.scenarios_topic = scenarios_topic
        self.retry_topic = retry_topic
        
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        
        self.connected = False
        logger.info(f"KafkaClient инициализирован, bootstrap_servers={bootstrap_servers}")
    
    async def connect(self):
        try:
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
        try:
            if self.producer:
                try:
                    await self.producer.stop()
                except Exception as e:
                    logger.warning(f"Ошибка при остановке producer: {e}")
                finally:
                    self.producer = None
            
            if self.consumer:
                try:
                    await self.consumer.stop()
                except Exception as e:
                    logger.warning(f"Ошибка при остановке consumer: {e}")
                finally:
                    self.consumer = None
            
            self.connected = False
            logger.info("Отключение от Kafka завершено")
        except Exception as e:
            logger.error(f"Ошибка при отключении от Kafka: {e}", exc_info=True)
    
    async def send_message(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any]
    ) -> bool:
        if not self.producer:
            logger.error("Producer не инициализирован")
            return False
        
        try:
            logger.info(f"Отправка сообщения в Kafka: topic={topic}, key={key}")
            logger.debug(f"Значение сообщения: {value}")
            
            if hasattr(self.producer, '_closed'):
                closed = self.producer._closed
                if isinstance(closed, bool) and closed:
                    logger.error("Producer закрыт!")
                    return False
                elif hasattr(closed, 'is_set') and closed.is_set():
                    logger.error("Producer закрыт!")
                    return False
            
            try:
                result = await asyncio.wait_for(
                    self.producer.send_and_wait(
                        topic=topic,
                        key=key,
                        value=value
                    ),
                    timeout=10.0
                )
                logger.info(f"✓ Сообщение успешно отправлено в топик '{topic}' с ключом '{key}'")
                return True
            except asyncio.TimeoutError:
                logger.error(f"✗ Таймаут при отправке сообщения в топик '{topic}' (10 секунд)")
                return False
        except KafkaError as e:
            logger.error(f"✗ Kafka ошибка при отправке сообщения в топик '{topic}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"✗ Неожиданная ошибка при отправке сообщения в топик '{topic}': {e}", exc_info=True)
            return False
    
    async def send_to_retry_topic(
        self,
        scenario_id: int,
        event_data: Dict[str, Any],
        reason: str
    ) -> bool:
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
