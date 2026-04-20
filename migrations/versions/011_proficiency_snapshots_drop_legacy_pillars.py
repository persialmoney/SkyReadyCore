"""Drop legacy pillar columns from proficiency_snapshots.

Revision ID: 011
Revises: 010
Create Date: 2026-04-17

The previous proficiency engine wrote four "pillar" subscores
(`recency`, `exposure`, `envelope`, `consistency`) alongside the overall
score. The redesigned engine (three-layer continuous decay + seven
per-domain scores) does not produce those subscores — they were kept on
the row for backward compat during the transition.

Pre-launch (no production users) and the engine has been fully migrated,
so these columns are dead weight. Dropping them keeps the table aligned
with the GraphQL schema and the client model.
"""

from alembic import op

revision = '011'
down_revision = '010'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE proficiency_snapshots
            DROP COLUMN IF EXISTS recency,
            DROP COLUMN IF EXISTS exposure,
            DROP COLUMN IF EXISTS envelope,
            DROP COLUMN IF EXISTS consistency
    """)
    print("✓ Dropped legacy pillar columns from proficiency_snapshots")


def downgrade():
    op.execute("""
        ALTER TABLE proficiency_snapshots
            ADD COLUMN IF NOT EXISTS recency     INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS exposure    INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS envelope    INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS consistency INTEGER NOT NULL DEFAULT 0
    """)
