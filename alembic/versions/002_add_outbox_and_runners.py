"""Add outbox table

Revision ID: 002_outbox
Revises: 001_initial
Create Date: 2024-01-02 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '002_outbox'
down_revision: Union[str, None] = '001_initial'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Создание таблицы outbox_events для transactional outbox
    op.create_table(
        'outbox_events',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False, primary_key=True),
        sa.Column('scenario_id', sa.Integer(), nullable=False),
        sa.Column('event_type', sa.String(length=100), nullable=False),
        sa.Column('event_data', postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column('topic', sa.String(length=100), nullable=False),
        sa.Column('processed', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
    )
    
    # Создание индексов для outbox_events
    op.create_index('idx_outbox_events_scenario_id', 'outbox_events', ['scenario_id'])
    op.create_index('idx_outbox_events_processed', 'outbox_events', ['processed'])
    op.create_index('idx_outbox_events_created_at', 'outbox_events', ['created_at'])


def downgrade() -> None:
    # Удаление индексов outbox_events
    op.drop_index('idx_outbox_events_created_at', table_name='outbox_events')
    op.drop_index('idx_outbox_events_processed', table_name='outbox_events')
    op.drop_index('idx_outbox_events_scenario_id', table_name='outbox_events')
    
    # Удаление таблицы outbox_events
    op.drop_table('outbox_events')

