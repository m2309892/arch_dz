import random
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from typing import List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Inference Service",
    description="Сервис для обработки кадров и генерации предсказаний",
    version="1.0.0"
)


class MockMLModel:
    POSSIBLE_CLASSES = ["person", "car", "bicycle", "dog", "cat", "truck", "bus"]
    
    @staticmethod
    def predict(frame_bytes: bytes) -> List[Dict[str, Any]]:
        num_objects = random.randint(1, 4)
        
        predictions = []
        for _ in range(num_objects):
            class_name = random.choice(MockMLModel.POSSIBLE_CLASSES)
            confidence = round(random.uniform(0.6, 0.99), 2)
            
            x1 = random.randint(0, 500)
            y1 = random.randint(0, 500)
            width = random.randint(50, 200)
            height = random.randint(50, 200)
            x2 = min(x1 + width, 800)
            y2 = min(y1 + height, 600)
            
            predictions.append({
                "class": class_name,
                "confidence": confidence,
                "bbox": [x1, y1, x2, y2]
            })
        
        logger.info(f"Сгенерировано {len(predictions)} предсказаний для кадра размером {len(frame_bytes)} байт")
        return predictions


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "inference"}


@app.post("/predict")
async def predict(frame: UploadFile = File(...)):
    try:
        frame_bytes = await frame.read()
        
        if not frame_bytes:
            raise HTTPException(status_code=400, detail="Пустой кадр")
        
        logger.info(f"Получен кадр от Runner: размер={len(frame_bytes)} байт, тип={frame.content_type}")
        
        predictions = MockMLModel.predict(frame_bytes)
        
        result = {
            "predictions": predictions,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Возвращено {len(predictions)} предсказаний")
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при обработке кадра: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при обработке кадра: {str(e)}")


@app.get("/")
async def root():
    return {
        "message": "Inference Service",
        "version": "1.0.0",
        "status": "running"
    }
