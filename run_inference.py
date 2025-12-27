"""Скрипт для запуска Inference сервиса."""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.inference.main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )

