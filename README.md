# Распределенная система видео аналитики

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Настройте переменные окружения (создайте файл `.env`):
```
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/video_analytics
```

3. Создайте базу данных PostgreSQL и выполните миграции Alembic:
```bash
# Применить миграции
alembic upgrade head

# Или вручную через SQL (альтернативный способ)
psql -U postgres -d video_analytics -f app/db/migrations/001_create_scenarios_table.sql
```

## Структура проекта

```
app/
  api/                   # API роутеры
    __init__.py
    scenario.py          # Роутер для работы со сценариями
    prediction.py        # Роутер для работы с предсказаниями
  db/
    __init__.py          # Экспорт основных компонентов
    models.py            # SQLAlchemy модели и enum типы
    database.py          # Подключение к БД через SQLAlchemy
    crud.py              # CRUD операции через ORM
    migrations/          # SQL миграции (legacy)
      001_create_scenarios_table.sql
  schemas/               # Pydantic схемы
    __init__.py
    scenario.py          # Схемы для работы со сценариями
  main.py                # Главный файл FastAPI приложения

alembic/                 # Миграции Alembic
  versions/              # Файлы миграций
  env.py                 # Конфигурация Alembic
  script.py.mako         # Шаблон миграций
alembic.ini              # Конфигурация Alembic
run.py                   # Скрипт для запуска приложения
```

## Использование

### Подключение к базе данных

```python
import asyncio
from app.db import db, scenario_crud, ScenarioStatus, Scenario

async def main():
    # Подключение к БД
    await db.connect()
    
    # Опционально: создание таблиц программно (лучше использовать миграции)
    # await db.create_tables()
    
    try:
        # Создание сценария
        scenario_id = await scenario_crud.create_scenario(
            status=ScenarioStatus.INIT_STARTUP
        )
        print(f"Создан сценарий: {scenario_id}")
        
        # Изменение статуса
        success = await scenario_crud.update_status(
            scenario_id, 
            ScenarioStatus.INIT_STARTUP_PROCESSING
        )
        print(f"Статус обновлен: {success}")
        
        # Получение статуса
        status = await scenario_crud.get_status(scenario_id)
        print(f"Статус: {status}")
        
        # Обновление предсказаний
        await scenario_crud.update_predict(
            scenario_id,
            {"predictions": [{"class": "person", "confidence": 0.95}]}
        )
        
        # Получение предсказаний
        predict = await scenario_crud.get_predict(scenario_id)
        print(f"Предсказания: {predict}")
        
        # Получение полной информации о сценарии
        scenario = await scenario_crud.get_scenario(scenario_id)
        if scenario:
            print(f"Сценарий: {scenario.id}, статус: {scenario.status}")
    
    finally:
        # Закрытие подключения
        await db.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

### Работа с сессиями напрямую

```python
from app.db import db
from app.db.models import Scenario, ScenarioStatus

async with db.get_session() as session:
    # Создание
    new_scenario = Scenario(status=ScenarioStatus.INIT_STARTUP)
    session.add(new_scenario)
    await session.commit()
    
    # Чтение
    from sqlalchemy import select
    stmt = select(Scenario).where(Scenario.id == new_scenario.id)
    result = await session.execute(stmt)
    scenario = result.scalar_one()
    
    # Обновление
    scenario.status = ScenarioStatus.ACTIVE
    await session.commit()
```

## Миграции Alembic

### Создание новой миграции
```bash
alembic revision --autogenerate -m "описание изменений"
```

### Применение миграций
```bash
alembic upgrade head
```

### Откат миграции
```bash
alembic downgrade -1
```

### Просмотр истории миграций
```bash
alembic history
```

## Запуск API

### Запуск сервера разработки
```bash
python run.py
```

Или через uvicorn напрямую:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

После запуска API будет доступно по адресу:
- API: http://localhost:8000
- Документация: http://localhost:8000/docs
- Альтернативная документация: http://localhost:8000/redoc

## API Эндпоинты

### POST /scenario/
Инициализация стейт-машины сценария.

**Запрос:**
```json
{
  "status": "init_startup"
}
```

**Ответ:**
```json
{
  "scenario_id": 1
}
```

### POST /scenario/{scenario_id}/
Изменение статуса стейт-машины сценария.

**Запрос:**
```json
{
  "status": "active"
}
```

**Ответ:**
```json
{
  "status": "updated",
  "scenario_id": "1"
}
```

### GET /scenario/{scenario_id}/
Получение информации о текущем статусе сценария.

**Ответ:**
```json
{
  "id": 1,
  "status": "active"
}
```

### GET /prediction/{scenario_id}/
Получение результатов предсказаний.

**Ответ:**
```json
{
  "scenario_id": 1,
  "predictions": {
    "predictions": [
      {"class": "person", "confidence": 0.95}
    ]
  }
}
```

## Запуск Kafka

Для работы системы требуется Kafka. Запустите Kafka через Docker Compose:

```bash
# Запуск Kafka, Zookeeper и Kafka UI
docker-compose up -d

# Проверка статуса
docker-compose ps

# Просмотр логов
docker-compose logs -f kafka

# Остановка
docker-compose down
```

После запуска Kafka будет доступна на `localhost:9092`, а Kafka UI - на `http://localhost:8080`

## Запуск раннеров (воркеров)

Раннеры должны запускаться отдельно и читать задачи из Kafka:

```bash
# Запуск одного воркера
python run_worker.py [worker_id] [video_path]

# Например:
python run_worker.py worker_1 test_videos/test_video.mp4

# Для запуска нескольких воркеров (в разных терминалах):
python run_worker.py worker_1 test_videos/test_video.mp4
python run_worker.py worker_2 test_videos/test_video.mp4
python run_worker.py worker_3 test_videos/test_video.mp4
```

## Архитектура работы с Kafka

1. **Топик `scenarios`** - основной топик для событий сценариев:
   - Оркестратор отправляет события в этот топик
   - Раннеры читают из этого топика (кто свободен - тот берет задачу)
   - Группа consumer: `runners_group`

2. **Топик `retry_topic`** - топик для повторной обработки:
   - Раннеры отправляют события в этот топик при ошибках
   - Оркестратор читает из этого топика и перезапускает сценарии

## Технологии

- **FastAPI** - современный веб-фреймворк для создания API
- **SQLAlchemy 2.0** - ORM для работы с базой данных
- **asyncpg** - асинхронный драйвер PostgreSQL
- **Alembic** - система миграций базы данных
- **Pydantic** - валидация данных и схемы
- **Uvicorn** - ASGI сервер
- **Kafka** - распределенная система обмена сообщениями
- **aiokafka** - асинхронный клиент для Kafka
