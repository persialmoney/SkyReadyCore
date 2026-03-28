"""Add signature workflow columns to logbook_entries.

Revision ID: 006
Revises: 005
Create Date: 2026-03-28

Adds return_note and void_reason to support the CFI digital signature
workflow (returnEntry / voidEntry mutations). Both columns are nullable TEXT
and default to NULL so existing rows are unaffected.
"""
from alembic import op

revision = '006'
down_revision = '005'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE logbook_entries
            ADD COLUMN IF NOT EXISTS return_note TEXT,
            ADD COLUMN IF NOT EXISTS void_reason TEXT
    """)
    print("✓ Added return_note and void_reason to logbook_entries")


def downgrade():
    op.execute("""
        ALTER TABLE logbook_entries
            DROP COLUMN IF EXISTS return_note,
            DROP COLUMN IF EXISTS void_reason
    """)
