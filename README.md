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
  db/
    __init__.py          # Экспорт основных компонентов
    models.py            # SQLAlchemy модели и enum типы
    database.py          # Подключение к БД через SQLAlchemy
    crud.py              # CRUD операции через ORM
    migrations/          # SQL миграции (legacy)
      001_create_scenarios_table.sql

alembic/                 # Миграции Alembic
  versions/              # Файлы миграций
  env.py                 # Конфигурация Alembic
  script.py.mako         # Шаблон миграций
alembic.ini              # Конфигурация Alembic
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

## Технологии

- **SQLAlchemy 2.0** - ORM для работы с базой данных
- **asyncpg** - асинхронный драйвер PostgreSQL
- **Alembic** - система миграций базы данных
