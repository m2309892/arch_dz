"""Initial migration

Revision ID: 001_initial
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Создание enum типа для статусов сценария
    op.execute("""
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
    """)
    
    # Создание таблицы scenarios
    # Используем postgresql.ENUM с create_type=False, т.к. тип уже создан выше
    scenario_status_enum = postgresql.ENUM(
        'init_startup', 'init_startup_processing', 'active', 
        'init_shutdown', 'in_shutdown_processing', 'inactive',
        name='scenario_status',
        create_type=False  # Тип уже создан выше, не создавать заново
    )
    
    op.create_table(
        'scenarios',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False, primary_key=True),
        sa.Column('status', scenario_status_enum, nullable=False, server_default='init_startup'),
        sa.Column('predict', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    
    # Создание индексов
    op.create_index('idx_scenarios_status', 'scenarios', ['status'])
    op.create_index('idx_scenarios_id', 'scenarios', ['id'])
    
    # Создание триггера для автоматического обновления updated_at
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
    """)
    
    op.execute("""
        CREATE TRIGGER update_scenarios_updated_at
            BEFORE UPDATE ON scenarios
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    """)


def downgrade() -> None:
    # Удаление триггера
    op.execute("DROP TRIGGER IF EXISTS update_scenarios_updated_at ON scenarios;")
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column();")
    
    # Удаление индексов
    op.drop_index('idx_scenarios_id', table_name='scenarios')
    op.drop_index('idx_scenarios_status', table_name='scenarios')
    
    # Удаление таблицы
    op.drop_table('scenarios')
    
    # Удаление enum типа
    op.execute("DROP TYPE IF EXISTS scenario_status;")

