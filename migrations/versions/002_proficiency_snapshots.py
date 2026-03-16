"""Add proficiency_snapshots table.

Revision ID: 002
Revises: 001
Create Date: 2026-03-16

Stores one row per user per calendar day containing the computed proficiency
score and its four pillar sub-scores. The UNIQUE (user_id, snapshot_date)
constraint allows upsert-by-business-key semantics from the sync-push Lambda.
The computed_at BIGINT column serves as the sync cursor (same pattern as
readiness_assessments).
"""
from alembic import op

revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE IF NOT EXISTS proficiency_snapshots (
            id            TEXT PRIMARY KEY,
            user_id       TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,      -- YYYY-MM-DD calendar date
            score         INTEGER NOT NULL,   -- 0-100 final weighted score
            recency       INTEGER NOT NULL,   -- Recency pillar (35%)
            exposure      INTEGER NOT NULL,   -- Exposure pillar (30%)
            envelope      INTEGER NOT NULL,   -- Envelope Practice pillar (20%)
            consistency   INTEGER NOT NULL,   -- Consistency pillar (15%)
            computed_at   BIGINT NOT NULL,    -- epoch ms; used as the sync cursor column
            _sync_pending BOOLEAN NOT NULL DEFAULT FALSE,
            UNIQUE (user_id, snapshot_date)
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_proficiency_snapshots_user_date
        ON proficiency_snapshots (user_id, snapshot_date DESC)
    """)

    print("✓ Created proficiency_snapshots table with UNIQUE (user_id, snapshot_date) and index")


def downgrade():
    op.execute("DROP TABLE IF EXISTS proficiency_snapshots")
