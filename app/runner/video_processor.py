import os
import logging
from typing import Optional, Generator
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_TEST_VIDEO_PATH = "test_videos/test_video.mp4"


class VideoProcessor:
    def __init__(self, video_path: str = DEFAULT_TEST_VIDEO_PATH):
        self.video_path = Path(video_path)
        if not self.video_path.exists():
            logger.warning(
                f"Видеофайл не найден: {video_path}. Будет использована заглушка.\n"
                f"Для тестирования создайте папку 'test_videos/' и поместите туда видеофайл."
            )
        logger.info(f"VideoProcessor инициализирован для файла: {video_path}")
    
    def read_frame(self, frame_number: int = 0) -> Optional[bytes]:
        if not self.video_path.exists():
            logger.warning(f"[VIDEO STUB] Видеофайл не найден, возвращаем заглушку кадра")
            return b"fake_frame_data"
        
        logger.info(f"Чтение кадра {frame_number} из видео: {self.video_path}")
        return b"frame_data"
    
    def get_frame_generator(self) -> Generator[bytes, None, None]:
        logger.info("Генератор кадров запущен (заглушка)")
        yield b"frame_data"
