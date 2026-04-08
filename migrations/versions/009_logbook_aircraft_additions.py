"""Add touch-and-go landings, Hobbs/Tach, block times, on/off duty to logbook_entries.

Revision ID: 009
Revises: 008
Create Date: 2026-04-07

Adds the following to logbook_entries:
  - day_touch_and_go_landings / night_touch_and_go_landings (INTEGER)
  - hobbs_start / hobbs_end / tach_start / tach_end (DECIMAL)
  - block_out / block_in (TEXT HH:MM — aircraft block times; already in GraphQL schema, now persisted)
  - on_duty / off_duty (TEXT HH:MM Zulu — crew duty period, distinct from block times)

Aircraft flags (pressurized, taa, gear_type) are stored inside the aircraft JSONB snapshot
on logbook_entries and in the DynamoDB user record, so no Postgres column changes are needed
for those fields.
"""
from alembic import op

revision = '009'
down_revision = '008'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE logbook_entries
            ADD COLUMN IF NOT EXISTS day_touch_and_go_landings    INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS night_touch_and_go_landings   INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS hobbs_start                   DECIMAL(7,1),
            ADD COLUMN IF NOT EXISTS hobbs_end                     DECIMAL(7,1),
            ADD COLUMN IF NOT EXISTS tach_start                    DECIMAL(7,1),
            ADD COLUMN IF NOT EXISTS tach_end                      DECIMAL(7,1),
            ADD COLUMN IF NOT EXISTS on_duty                       TEXT,
            ADD COLUMN IF NOT EXISTS off_duty                      TEXT
    """)
    print("✓ Added touch-and-go, Hobbs/Tach, and duty time columns to logbook_entries")


def downgrade():
    op.execute("""
        ALTER TABLE logbook_entries
            DROP COLUMN IF EXISTS day_touch_and_go_landings,
            DROP COLUMN IF EXISTS night_touch_and_go_landings,
            DROP COLUMN IF EXISTS hobbs_start,
            DROP COLUMN IF EXISTS hobbs_end,
            DROP COLUMN IF EXISTS tach_start,
            DROP COLUMN IF EXISTS tach_end,
            DROP COLUMN IF EXISTS on_duty,
            DROP COLUMN IF EXISTS off_duty
    """)
