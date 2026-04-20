"""Add seaplane and rotorcraft domain score columns to proficiency_snapshots.

Revision ID: 010
Revises: 009
Create Date: 2026-04-17

Adds score_seaplane and score_rotorcraft INTEGER columns (0–100, NULL when
domain is inactive), matching the client-side domain model.
"""

from alembic import op

revision = '010'
down_revision = '009'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE proficiency_snapshots
            ADD COLUMN IF NOT EXISTS score_seaplane   INTEGER,
            ADD COLUMN IF NOT EXISTS score_rotorcraft INTEGER
    """)
    print("✓ Added score_seaplane and score_rotorcraft to proficiency_snapshots")


def downgrade():
    op.execute("""
        ALTER TABLE proficiency_snapshots
            DROP COLUMN IF EXISTS score_seaplane,
            DROP COLUMN IF EXISTS score_rotorcraft
    """)
