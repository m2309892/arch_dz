# Распределённая система видеоаналитики

Сервис управления сценариями обработки видео, генерации предсказаний (мок) и обработки ошибок раннеров через Kafka, outbox и state machine.

---

## Архитектура

- **FastAPI** — HTTP API (сценарии, предсказания, health).
- **Orchestrator** — создание/обновление сценариев, outbox, Kafka producer, цикл событий, обработка shutdown, retry consumer.
- **RunnerPool** — пул воркеров (subprocess) `run_worker.py`; при старте поднимает N раннеров, пишет логи в `logs/`.
- **Kafka** — топики `scenarios` (задачи для воркеров) и `retry_topic` (ошибки раннеров).
- **PostgreSQL** — сценарии (`scenarios`), outbox (`outbox_events`). Таблицы создаются при старте без миграций.
- **MockInference** — локальный мок: по байтам кадра возвращает 1–4 случайных «объекта» (class, confidence, bbox). Реальное видео не используется.

---

## Машина состояний сценария

Статусы: `init_startup` → `init_startup_processing` → `active` → `inactive`, а также `init_shutdown` → `in_shutdown_processing` → `inactive`.

### Успешная обработка

1. **POST /scenario/** — создаётся сценарий со статусом `init_startup`, событие в outbox и **сразу** отправка в Kafka `scenarios`.
2. Outbox-цикл оркестратора забирает необработанные события и дублирует отправку в Kafka (на случай сбоя прямой отправки).
3. Воркер (Kafka consumer, `runners_group`) читает из `scenarios`, берёт `scenario_id`, вызывает **Runner**.
4. Runner: чтение кадра (VideoProcessor, заглушка), **MockInference.predict** → список объектов, сохранение в `Scenario.predict`, обновление статуса **ACTIVE**, затем **INACTIVE**.
5. **GET /prediction/{id}/** — отдаёт `predict` из БД (список объектов).

### Ошибка раннера (retry)

При любой ошибке в `process_scenario` (нет кадра, ошибка inference, БД, смена статуса) Runner вызывает `_handle_error`:

- формирует `event_data` (scenario_id, status, reason, runner_id);
- отправляет сообщение в **retry_topic**.

Оркестратор слушает `retry_topic`:

- при получении сообщения переводит сценарий в **INIT_SHUTDOWN**;
- цикл `_process_shutdown_scenarios` обрабатывает сценарии в `init_shutdown`.

### Shutdown и перезапуск

1. Находятся сценарии в статусе **INIT_SHUTDOWN**.
2. Для каждого: **IN_SHUTDOWN_PROCESSING** → пауза → **INACTIVE**.
3. Сразу после **INACTIVE**: сценарий переводится в **INIT_STARTUP**, создаётся outbox-событие `scenario_restarted` и **прямая отправка** в `scenarios`. Сценарий снова попадает в очередь на обработку воркерами.

То есть после падения раннера: retry → shutdown → **INACTIVE** → **сразу** перезапуск (init_startup + Kafka).

### Явный shutdown по API

**POST /scenario/{id}/** с `{"status": "init_shutdown"}` переводит сценарий в `init_shutdown`. Дальше тот же `_process_shutdown_scenarios`: `in_shutdown_processing` → `inactive` → перезапуск в очередь.

### INACTIVE после успешной обработки

По завершении успешной обработки Runner выставляет **ACTIVE**, затем **INACTIVE**. Сценарий остаётся в `inactive` и **не** перезапускается автоматически (перезапуск только из shutdown-цепочек выше).

---

## Retry при жёстком падении процесса

Воркеры читают `scenarios` с **manual commit** (`enable_auto_commit=False`):

- **успех** — commit;
- **ошибка** (retry отправлен) — commit, затем выход воркера;
- **исключение / краш** (OOM, kill -9 и т.п.) — **без** commit.

Если воркер умер до commit, Kafka при ребалансе группы передоставляет то же сообщение другому воркеру. Тем самым обеспечивается retry при жёстком падении процесса.

---

## Пул раннеров

- **ensure_pool_size** вызывается при старте оркестратора: проверяет мёртвые процессы (`process.poll()`), удаляет их из пула, при нехватке — создаёт новых.
- Раннеры — отдельные процессы `run_worker.py` (KafkaWorker + Runner), логи в `logs/runner_<id>.log`.

---

## API

| Метод | Путь | Описание |
|-------|------|----------|
| GET | / | Информация о сервисе |
| GET | /health | Health check |
| POST | /scenario/ | Создать сценарий. Тело: `{"status": "init_startup"}` (опционально). Ответ: `{"scenario_id": int}` |
| POST | /scenario/{id}/ | Обновить статус. Тело: `{"status": "init_startup" \| "init_shutdown" \| ...}` |
| GET | /scenario/{id}/ | Текущий статус сценария |
| GET | /prediction/{id}/ | Предсказания по сценарию (`predictions` — список `{class, confidence, bbox}` или `null`) |

---

## Окружение

- **DATABASE_URL** — `postgresql://user:pass@host:port/db`. По умолчанию `postgresql://postgres:postgres@127.0.0.1:5432/video_analytics`. Для Docker задаётся `postgresql://...@postgres:5432/video_analytics_scenarios`.
- **KAFKA_BOOTSTRAP_SERVERS** — брокеры Kafka. По умолчанию `localhost:9092`; в Docker — `kafka:29092`.
- **VIDEO_PATH** — путь к видео (опционально). Иначе используется `test_videos/*.mp4` или заглушка.

---

## Развёртывание и запуск

### Docker Compose (рекомендуется)

Собрать и поднять все сервисы:

```bash
docker compose up -d --build
```

Поднимаются: **postgres**, **zookeeper**, **kafka**, **kafka-ui**, **app**. Приложение слушает порт **8000**, логи приложения — `docker compose logs -f app`. Логи воркеров — в `./logs/runner_*.log` (каталог `logs` смонтирован в контейнер).

Проверка:

```bash
curl -X POST http://localhost:8000/scenario/ -H "Content-Type: application/json" -d "{}"
curl http://localhost:8000/scenario/1/
curl http://localhost:8000/prediction/1/
```

Остановка:

```bash
docker compose down
```

### Локальный запуск (без Docker)

1. Запустить PostgreSQL и Kafka (например, через Docker только для postgres/kafka).
2. Создать БД, при необходимости:
   ```bash
   createdb -U postgres video_analytics
   ```
3. Установить зависимости:
   ```bash
   pip install -r requirements.txt
   ```
4. Задать переменные окружения (или `.env`):
   - `DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/video_analytics`
   - `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
5. Запуск API (оркестратор и воркеры поднимаются внутри приложения):
   ```bash
   python run.py
   ```
   либо через uvicorn:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

Воркеры стартуют как дочерние процессы оркестратора; при локальном запуске они должны иметь доступ к тем же `DATABASE_URL` и `KAFKA_BOOTSTRAP_SERVERS` (через env процесса).

---

## Структура проекта

```
app/
  main.py           # FastAPI, lifespan, роуты /, /health
  api/
    scenario.py     # POST/GET /scenario/, POST /scenario/{id}/
    prediction.py   # GET /prediction/{id}/
  db/
    database.py     # AsyncEngine, warmup, create_tables
    models.py       # Scenario, OutboxEvent, ScenarioStatus
    crud.py         # ScenarioCRUD, get_scenarios_by_status
  orchestrator/
    orchestrator.py # Оркестратор, циклы, shutdown, retry consumer
    kafka_client.py # Producer, consumer, send_message, send_to_retry_topic
    outbox.py       # OutboxManager
    runner_pool.py  # RunnerPool, ensure_pool_size
  runner/
    runner.py       # Runner, process_scenario, _handle_error
    kafka_worker.py # KafkaWorker, manual commit, consume scenarios
    mock_inference.py # MockInference.predict
    video_processor.py # VideoProcessor, read_frame (заглушка)
  schemas/
    scenario.py     # ScenarioCreate, ScenarioUpdate, PredictionResponse, ...
run.py              # uvicorn с reload
run_worker.py       # Точка входа воркера (KafkaWorker)
docker-compose.yml  # postgres, zookeeper, kafka, kafka-ui, app
Dockerfile          # Python 3.12, uvicorn app.main:app
```

---

## Топики Kafka

- **scenarios** — сообщения `{"scenario_id": int, "status": "..."}`. Воркеры потребляют группой `runners_group`, один топик — одна партиция, один потребитель обрабатывает.
- **retry_topic** — сообщения при ошибках раннера. Оркестратор потребляет группой `orchestrator_retry_consumer`, выставляет `init_shutdown` и запускает shutdown-цепочку с перезапуском в очередь.
