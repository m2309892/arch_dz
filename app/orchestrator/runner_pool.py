"""Пул раннеров (воркеров) для обработки задач из Kafka."""

import uuid
import subprocess
import sys
import os
from pathlib import Path
from typing import Dict, Set, Optional
import logging

logger = logging.getLogger(__name__)


class RunnerPool:
    """Пул раннеров (воркеров) для обработки задач из Kafka."""
    
    def __init__(self, pool_size: int = 5, video_path: Optional[str] = None):
        """
        Инициализация пула раннеров.
        
        Args:
            pool_size: Размер пула раннеров (количество воркеров)
            video_path: Путь к видеофайлу для раннеров (опционально)
        """
        self.pool_size = pool_size
        self.video_path = video_path
        self.runners: Dict[str, dict] = {}  # runner_id -> runner_info (включая process)
        logger.info(f"RunnerPool инициализирован с размером пула: {pool_size}")
    
    def create_runner(self) -> str:
        """
        Создает новый раннер (воркер) и запускает его как отдельный процесс.
        
        Returns:
            runner_id созданного раннера
        """
        runner_id = f"runner_{uuid.uuid4().hex[:12]}"
        
        # Запускаем процесс раннера
        process = self._start_runner_process(runner_id)
        
        runner_info = {
            "runner_id": runner_id,
            "status": "running",
            "process": process
        }
        
        self.runners[runner_id] = runner_info
        logger.info(f"Создан и запущен раннер: runner_id={runner_id}, pid={process.pid}")
        return runner_id
    
    def _start_runner_process(self, runner_id: str) -> subprocess.Popen:
        """
        Запускает процесс раннера через subprocess.
        
        Args:
            runner_id: ID раннера
            
        Returns:
            Объект subprocess.Popen
        """
        # Путь к скрипту run_worker.py (относительно корня проекта)
        script_path = Path(__file__).parent.parent.parent / "run_worker.py"
        
        # Формируем команду для запуска
        cmd = [sys.executable, str(script_path), runner_id]
        
        # Добавляем путь к видео, если указан
        if self.video_path:
            cmd.append(self.video_path)
        
        # Запускаем процесс
        # Используем None для stdout/stderr, чтобы видеть логи в консоли
        # Или можно перенаправить в файлы для отладки
        log_dir = Path(__file__).parent.parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"runner_{runner_id}.log"
        with open(log_file, "w") as f:
            process = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,  # Объединяем stderr с stdout
                cwd=Path(__file__).parent.parent.parent,
                env=os.environ.copy(),
                text=True
            )
        
        logger.info(f"Процесс раннера запущен: runner_id={runner_id}, pid={process.pid}, log_file={log_file}")
        
        return process
    
    def remove_runner(self, runner_id: str) -> bool:
        """
        Удаляет раннер из пула и останавливает его процесс.
        
        Args:
            runner_id: ID раннера
        
        Returns:
            True если удаление прошло успешно
        """
        if runner_id in self.runners:
            runner_info = self.runners[runner_id]
            process = runner_info.get("process")
            
            # Останавливаем процесс, если он запущен
            if process and process.poll() is None:  # Процесс еще работает
                try:
                    process.terminate()
                    process.wait(timeout=5)
                    logger.info(f"Процесс раннера остановлен: runner_id={runner_id}, pid={process.pid}")
                except subprocess.TimeoutExpired:
                    # Если не остановился за 5 секунд, принудительно завершаем
                    process.kill()
                    logger.warning(f"Процесс раннера принудительно завершен: runner_id={runner_id}, pid={process.pid}")
                except Exception as e:
                    logger.error(f"Ошибка при остановке процесса раннера: {e}")
            
            del self.runners[runner_id]
            logger.info(f"Раннер удален из пула: runner_id={runner_id}")
            return True
        return False
    
    def stop_all_runners(self):
        """Останавливает все раннеры в пуле."""
        runner_ids = list(self.runners.keys())
        for runner_id in runner_ids:
            self.remove_runner(runner_id)
        logger.info("Все раннеры остановлены")
    
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
        Создает новые раннеры (процессы), если их меньше pool_size.
        Также проверяет, что существующие процессы еще работают.
        """
        # Проверяем "мертвые" процессы и удаляем их
        dead_runners = []
        for runner_id, runner_info in self.runners.items():
            process = runner_info.get("process")
            if process and process.poll() is not None:  # Процесс завершился
                dead_runners.append(runner_id)
                logger.warning(f"Обнаружен завершенный процесс раннера: runner_id={runner_id}")
        
        # Удаляем "мертвые" раннеры
        for runner_id in dead_runners:
            del self.runners[runner_id]
        
        # Создаем новые раннеры, если нужно
        current_count = len(self.runners)
        if current_count < self.pool_size:
            needed = self.pool_size - current_count
            for _ in range(needed):
                self.create_runner()
            logger.info(f"Создано {needed} новых раннеров для поддержания размера пула")
