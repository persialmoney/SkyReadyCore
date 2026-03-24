"""Add norm_first_name to faa_airmen_basic for name-based CFI verification.

Revision ID: 005
Revises: 004
Create Date: 2026-03-24

The FAA Airmen Releasable Database does not include certificate numbers.
Verification must be done by matching first name + last name + expiry date.
This migration adds:
  - norm_first_name column to faa_airmen_basic and its staging table
  - Composite index (norm_last_name, norm_first_name) for the lookup query
  - Drops the misleading idx_faa_verify_lookup index on norm_cert_number
    (UNIQUE_ID is an internal FAA key, not the number on a pilot's certificate)
"""
from alembic import op

revision = '005'
down_revision = '004'
branch_labels = None
depends_on = None


def upgrade():
    # ── Add norm_first_name to production table ───────────────────────────────
    op.execute("""
        ALTER TABLE faa_airmen_basic
        ADD COLUMN IF NOT EXISTS norm_first_name TEXT NOT NULL DEFAULT ''
    """)

    # ── Add norm_first_name to staging table (same shape) ────────────────────
    op.execute("""
        ALTER TABLE faa_airmen_basic_staging
        ADD COLUMN IF NOT EXISTS norm_first_name TEXT NOT NULL DEFAULT ''
    """)

    # ── Composite index for the primary lookup query ──────────────────────────
    # Lookup is: WHERE norm_last_name = %s AND norm_first_name LIKE %s
    # with is_flight_instructor = true on the joined cert table.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_faa_basic_name_lookup
            ON faa_airmen_basic (norm_last_name, norm_first_name)
    """)

    # ── Drop the misleading norm_cert_number index ───────────────────────────
    # norm_cert_number was populated from UNIQUE_ID (an internal FAA DB key),
    # which is not the certificate number printed on a pilot's certificate.
    # Users cannot enter this value, so the index was never useful for lookup.
    op.execute("""
        DROP INDEX IF EXISTS idx_faa_verify_lookup
    """)

    print("✓ Added norm_first_name to faa_airmen_basic and staging")
    print("✓ Created idx_faa_basic_name_lookup")
    print("✓ Dropped idx_faa_verify_lookup (norm_cert_number was not a real cert number)")


def downgrade():
    op.execute("DROP INDEX IF EXISTS idx_faa_basic_name_lookup")
    op.execute("""
        ALTER TABLE faa_airmen_basic_staging
        DROP COLUMN IF EXISTS norm_first_name
    """)
    op.execute("""
        ALTER TABLE faa_airmen_basic
        DROP COLUMN IF EXISTS norm_first_name
    """)
    print("✓ Removed norm_first_name from faa_airmen_basic and staging")
