"""Обработка видео для извлечения кадров."""

import os
import logging
from typing import Optional, Generator
from pathlib import Path

logger = logging.getLogger(__name__)

# Путь по умолчанию для тестового видео
DEFAULT_TEST_VIDEO_PATH = "test_videos/test_video.mp4"


class VideoProcessor:
    """Класс для обработки видео и извлечения кадров."""
    
    def __init__(self, video_path: str = DEFAULT_TEST_VIDEO_PATH):
        """
        Инициализация видео процессора.
        
        Args:
            video_path: Путь к видеофайлу (по умолчанию test_videos/test_video.mp4)
        
        Примечание: Для тестирования поместите видеофайл в папку test_videos/ в корне проекта
        """
        self.video_path = Path(video_path)
        if not self.video_path.exists():
            logger.warning(
                f"Видеофайл не найден: {video_path}. Будет использована заглушка.\n"
                f"Для тестирования создайте папку 'test_videos/' и поместите туда видеофайл."
            )
        logger.info(f"VideoProcessor инициализирован для файла: {video_path}")
    
    def read_frame(self, frame_number: int = 0) -> Optional[bytes]:
        """
        Читает кадр из видео (заглушка).
        
        Args:
            frame_number: Номер кадра (по умолчанию 0 - первый кадр)
        
        Returns:
            Байты кадра или None если файл не существует
        """
        if not self.video_path.exists():
            logger.warning(f"[VIDEO STUB] Видеофайл не найден, возвращаем заглушку кадра")
            # Возвращаем пустые байты как заглушку
            return b"fake_frame_data"
        
        # TODO: В реальной реализации здесь будет чтение кадра из видео
        # Например, используя OpenCV:
        # import cv2
        # cap = cv2.VideoCapture(str(self.video_path))
        # cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        # ret, frame = cap.read()
        # if ret:
        #     _, buffer = cv2.imencode('.jpg', frame)
        #     return buffer.tobytes()
        
        logger.info(f"Чтение кадра {frame_number} из видео: {self.video_path}")
        # Заглушка - возвращаем фиктивные данные
        return b"frame_data"
    
    def get_frame_generator(self) -> Generator[bytes, None, None]:
        """
        Генератор кадров из видео (заглушка).
        
        Yields:
            Байты кадров
        """
        # TODO: В реальной реализации здесь будет генератор кадров
        # Пока заглушка - возвращаем один фиктивный кадр
        logger.info("Генератор кадров запущен (заглушка)")
        yield b"frame_data"

