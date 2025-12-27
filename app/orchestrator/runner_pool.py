"""Пул раннеров (воркеров) для обработки задач из Kafka."""

import uuid
from typing import Dict, Set
import logging

logger = logging.getLogger(__name__)


class RunnerPool:
    """Пул раннеров (воркеров) для обработки задач из Kafka."""
    
    def __init__(self, pool_size: int = 5):
        """
        Инициализация пула раннеров.
        
        Args:
            pool_size: Размер пула раннеров (количество воркеров)
        """
        self.pool_size = pool_size
        self.runners: Dict[str, dict] = {}  # runner_id -> runner_info
        logger.info(f"RunnerPool инициализирован с размером пула: {pool_size}")
    
    def create_runner(self) -> str:
        """
        Создает новый раннер (воркер).
        
        Returns:
            runner_id созданного раннера
        """
        runner_id = f"runner_{uuid.uuid4().hex[:12]}"
        
        runner_info = {
            "runner_id": runner_id,
            "status": "created"
        }
        
        self.runners[runner_id] = runner_info
        logger.info(f"Создан раннер: runner_id={runner_id}")
        return runner_id
    
    def remove_runner(self, runner_id: str) -> bool:
        """
        Удаляет раннер из пула.
        
        Args:
            runner_id: ID раннера
        
        Returns:
            True если удаление прошло успешно
        """
        if runner_id in self.runners:
            del self.runners[runner_id]
            logger.info(f"Раннер удален из пула: runner_id={runner_id}")
            return True
        return False
    
    def get_runner_count(self) -> int:
        """
        Получает текущее количество раннеров в пуле.
        
        Returns:
            Количество раннеров
        """
        return len(self.runners)
    
    def get_all_runner_ids(self) -> Set[str]:
        """
        Получает все ID раннеров в пуле.
        
        Returns:
            Множество ID раннеров
        """
        return set(self.runners.keys())
    
    def ensure_pool_size(self):
        """
        Обеспечивает, что в пуле есть нужное количество раннеров.
        Создает новые раннеры, если их меньше pool_size.
        """
        current_count = len(self.runners)
        if current_count < self.pool_size:
            needed = self.pool_size - current_count
            for _ in range(needed):
                self.create_runner()
            logger.info(f"Создано {needed} новых раннеров для поддержания размера пула")
