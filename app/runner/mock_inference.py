import random
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

POSSIBLE_CLASSES = ["person", "car", "bicycle", "dog", "cat", "truck", "bus"]


class MockInference:
    async def predict(self, frame: bytes) -> List[Dict[str, Any]]:
        num_objects = random.randint(1, 4)
        out = []
        for _ in range(num_objects):
            cls = random.choice(POSSIBLE_CLASSES)
            conf = round(random.uniform(0.6, 0.99), 2)
            x1, y1 = random.randint(0, 500), random.randint(0, 500)
            w, h = random.randint(50, 200), random.randint(50, 200)
            x2 = min(x1 + w, 800)
            y2 = min(y1 + h, 600)
            out.append({"class": cls, "confidence": conf, "bbox": [x1, y1, x2, y2]})
        logger.info("MockInference: сгенерировано %d предсказаний для кадра %d байт", len(out), len(frame))
        return out
