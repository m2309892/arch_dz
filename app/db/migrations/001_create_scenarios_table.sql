-- Создание enum типа для статусов сценария
DO $$ BEGIN
    CREATE TYPE scenario_status AS ENUM (
        'init_startup',
        'init_startup_processing',
        'active',
        'init_shutdown',
        'in_shutdown_processing',
        'inactive'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Создание таблицы scenarios
CREATE TABLE IF NOT EXISTS scenarios (
    id SERIAL PRIMARY KEY,
    status scenario_status NOT NULL DEFAULT 'init_startup',
    predict JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Создание индекса для быстрого поиска по статусу
CREATE INDEX IF NOT EXISTS idx_scenarios_status ON scenarios(status);

-- Создание индекса для быстрого поиска по id
CREATE INDEX IF NOT EXISTS idx_scenarios_id ON scenarios(id);

-- Триггер для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_scenarios_updated_at
    BEFORE UPDATE ON scenarios
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

