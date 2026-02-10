"""Add day and night full stop landing fields

Revision ID: 003
Revises: 002
Create Date: 2026-02-10

This migration adds separate day_full_stop_landings and night_full_stop_landings fields
to comply with FAA night currency requirements (3 night full stop landings in 90 days)
and tailwheel currency tracking (3 day full stop landings in 90 days).

These replace the legacy full_stop_landings field which remains in the database schema
for backward compatibility with old data, but is no longer used by the application.

"""
from alembic import op

revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None

def upgrade():
    # Add day_full_stop_landings and night_full_stop_landings columns
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS day_full_stop_landings INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS night_full_stop_landings INTEGER DEFAULT 0
    """)
    
    print("✓ Added day_full_stop_landings and night_full_stop_landings columns")
    print("  These replace the legacy full_stop_landings field")

def downgrade():
    # Remove the columns
    op.execute("""
        ALTER TABLE logbook_entries
        DROP COLUMN IF EXISTS day_full_stop_landings,
        DROP COLUMN IF EXISTS night_full_stop_landings
    """)
    
    print("✓ Removed day_full_stop_landings and night_full_stop_landings columns")
