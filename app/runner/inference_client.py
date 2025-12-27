"""Клиент для работы с Inference сервисом."""

import logging
import httpx
from typing import Dict, Any
import io

logger = logging.getLogger(__name__)


class InferenceClient:
    """Клиент для работы с Inference сервисом."""
    
    def __init__(self, inference_url: str = "http://localhost:8001"):
        """
        Инициализация Inference клиента.
        
        Args:
            inference_url: URL Inference сервиса
        """
        self.inference_url = inference_url
        self.predict_endpoint = f"{inference_url}/predict"
        logger.info(f"InferenceClient инициализирован, URL: {inference_url}")
    
    async def predict(self, frame: bytes) -> Dict[str, Any]:
        """
        Отправляет кадр в Inference сервис и получает предсказания.
        
        Args:
            frame: Байты кадра (изображение)
        
        Returns:
            Словарь с предсказаниями
        
        Raises:
            Exception: При ошибке обращения к Inference сервису
        """
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Отправляем кадр как файл
                files = {"frame": ("frame.jpg", io.BytesIO(frame), "image/jpeg")}
                
                response = await client.post(
                    self.predict_endpoint,
                    files=files
                )
                
                response.raise_for_status()
                
                result = response.json()
                logger.info(
                    f"Получены предсказания от Inference: "
                    f"{len(result.get('predictions', []))} объектов"
                )
                
                return result
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP ошибка при обращении к Inference: {e.response.status_code} - {e.response.text}")
            raise Exception(f"Inference сервис вернул ошибку: {e.response.status_code}")
        except httpx.TimeoutException:
            logger.error("Таймаут при обращении к Inference сервису")
            raise Exception("Таймаут при обращении к Inference сервису")
        except Exception as e:
            logger.error(f"Ошибка при обращении к Inference сервису: {e}", exc_info=True)
            raise Exception(f"Ошибка при обращении к Inference сервису: {str(e)}")
