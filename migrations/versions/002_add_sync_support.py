"""Add sync support - outbox pattern and indexes

Revision ID: 002
Revises: 001
Create Date: 2026-02-10

"""
from alembic import op
import sqlalchemy as sa

revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None

def upgrade():
    # Add deleted_at for soft deletes
    op.execute("""
        ALTER TABLE logbook_entries 
        ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP
    """)
    
    # Add indexes for sync queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_entries_updated_at 
        ON logbook_entries(updated_at)
    """)
    
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_entries_created_at 
        ON logbook_entries(created_at)
    """)
    
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_entries_user_updated 
        ON logbook_entries(user_id, updated_at)
    """)
    
    # Create outbox table for reliable event publishing
    op.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            payload JSONB NOT NULL,
            processed BOOLEAN DEFAULT FALSE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            processed_at TIMESTAMP
        )
    """)
    
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_outbox_processed 
        ON outbox(processed, created_at)
    """)

def downgrade():
    op.execute("DROP INDEX IF EXISTS idx_outbox_processed")
    op.execute("DROP TABLE IF EXISTS outbox")
    op.execute("DROP INDEX IF EXISTS idx_logbook_entries_user_updated")
    op.execute("DROP INDEX IF EXISTS idx_logbook_entries_created_at")
    op.execute("DROP INDEX IF EXISTS idx_logbook_entries_updated_at")
    op.execute("ALTER TABLE logbook_entries DROP COLUMN IF EXISTS deleted_at")
