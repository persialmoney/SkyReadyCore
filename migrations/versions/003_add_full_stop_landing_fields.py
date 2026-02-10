"""Replace full_stop_landings with day and night specific fields

Revision ID: 003
Revises: 002
Create Date: 2026-02-10

This migration replaces the generic full_stop_landings field with separate 
day_full_stop_landings and night_full_stop_landings fields to comply with:
- FAA night currency requirements (3 night full stop landings in 90 days)
- Tailwheel currency tracking (3 day full stop landings in 90 days)

"""
from alembic import op

revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None

def upgrade():
    # Add new columns
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS day_full_stop_landings INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS night_full_stop_landings INTEGER DEFAULT 0
    """)
    
    # Drop the legacy full_stop_landings column
    op.execute("""
        ALTER TABLE logbook_entries
        DROP COLUMN IF EXISTS full_stop_landings
    """)
    
    print("✓ Added day_full_stop_landings and night_full_stop_landings columns")
    print("✓ Removed legacy full_stop_landings column")

def downgrade():
    # Restore legacy column
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS full_stop_landings INTEGER DEFAULT 0
    """)
    
    # Remove the new columns
    op.execute("""
        ALTER TABLE logbook_entries
        DROP COLUMN IF EXISTS day_full_stop_landings,
        DROP COLUMN IF EXISTS night_full_stop_landings
    """)
    
    print("✓ Restored legacy full_stop_landings column")
    print("✓ Removed day_full_stop_landings and night_full_stop_landings columns")
