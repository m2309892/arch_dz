import asyncio
import sys
import logging
from pathlib import Path

from app.runner.kafka_worker import KafkaWorker
from app.runner.video_processor import DEFAULT_TEST_VIDEO_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def main():
    worker_id = sys.argv[1] if len(sys.argv) > 1 else f"worker_{asyncio.get_event_loop().time()}"
    video_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TEST_VIDEO_PATH
    
    logger.info(f"Запуск воркера: worker_id={worker_id}, video_path={video_path}")
    
    worker = KafkaWorker(
        worker_id=worker_id,
        video_path=video_path
    )
    
    try:
        await worker.start()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
