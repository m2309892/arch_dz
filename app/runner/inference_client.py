"""Заглушка для Inference сервиса."""

import logging
from typing import Dict, Any, Optional
import base64
import io

logger = logging.getLogger(__name__)


class InferenceClient:
    """Заглушка для работы с Inference сервисом."""
    
    def __init__(self, inference_url: str = "http://localhost:8001"):
        """
        Инициализация Inference клиента.
        
        Args:
            inference_url: URL Inference сервиса
        """
        self.inference_url = inference_url
        logger.info(f"InferenceClient инициализирован (заглушка), URL: {inference_url}")
    
    async def predict(self, frame: bytes) -> Dict[str, Any]:
        """
        Отправляет кадр в Inference сервис и получает предсказания (заглушка).
        
        Args:
            frame: Байты кадра (изображение)
        
        Returns:
            Словарь с предсказаниями
        """
        # TODO: В реальной реализации здесь будет HTTP запрос к Inference сервису
        # Пока возвращаем заглушку с предсказаниями
        
        logger.info(f"[INFERENCE STUB] Получен кадр размером {len(frame)} байт")
        
        # Заглушка предсказаний
        predictions = {
            "predictions": [
                {
                    "class": "person",
                    "confidence": 0.95,
                    "bbox": [100, 100, 200, 300]
                },
                {
                    "class": "car",
                    "confidence": 0.87,
                    "bbox": [300, 150, 450, 350]
                }
            ],
            "timestamp": "2024-01-01T12:00:00"
        }
        
        logger.info(f"[INFERENCE STUB] Возвращены предсказания: {len(predictions['predictions'])} объектов")
        return predictions

