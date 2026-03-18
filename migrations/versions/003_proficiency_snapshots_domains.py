"""Add per-domain score columns to proficiency_snapshots.

Revision ID: 003
Revises: 002
Create Date: 2026-03-16

Adds five per-domain score columns (0–100, NULL when domain is inactive)
and an active_domains TEXT column (JSON array of active domain keys)
to support the domain-based proficiency system.

Existing rows will have NULL for all new columns; the client will repopulate
them on the next snapshot write.
"""
from alembic import op

revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE proficiency_snapshots
            ADD COLUMN IF NOT EXISTS score_core_vfr  INTEGER,
            ADD COLUMN IF NOT EXISTS score_night     INTEGER,
            ADD COLUMN IF NOT EXISTS score_ifr       INTEGER,
            ADD COLUMN IF NOT EXISTS score_tailwheel INTEGER,
            ADD COLUMN IF NOT EXISTS score_multi     INTEGER,
            ADD COLUMN IF NOT EXISTS active_domains  TEXT
    """)

    print("✓ Added per-domain score columns to proficiency_snapshots")


def downgrade():
    op.execute("""
        ALTER TABLE proficiency_snapshots
            DROP COLUMN IF EXISTS score_core_vfr,
            DROP COLUMN IF EXISTS score_night,
            DROP COLUMN IF EXISTS score_ifr,
            DROP COLUMN IF EXISTS score_tailwheel,
            DROP COLUMN IF EXISTS score_multi,
            DROP COLUMN IF EXISTS active_domains
    """)
