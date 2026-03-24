"""Add FAA airmen tables for CFI certificate verification.

Revision ID: 004
Revises: 003
Create Date: 2026-03-23

Adds five tables that support the CFI verification flow:

  faa_airmen_basic              — FAA identity rows (name, city/state, normalized last name)
  faa_airmen_certificates       — FAA certificate/rating rows (cert type, expiry, normalized cert#)
  faa_airmen_basic_staging      — Staging counterpart; swapped atomically at ingest end
  faa_airmen_certificates_staging
  cfi_verification_attempts     — Audit trail of every verifyCfiCertificate API call
  faa_ingest_metadata           — Single-row record of last successful FAA ingest run

Both raw and normalized columns are stored so that support can debug
"why didn't this instructor verify?" without re-running normalization.

The staging tables are populated by the faa-airmen-sync Lambda and swapped
into production inside a single transaction after row-count validation.
"""
from alembic import op

revision = '004'
down_revision = '003'
branch_labels = None
depends_on = None


def upgrade():
    # ── faa_airmen_basic ──────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS faa_airmen_basic (
            unique_id             TEXT        PRIMARY KEY,
            first_middle_name     TEXT,
            last_name_suffix      TEXT        NOT NULL,
            city                  TEXT,
            state                 TEXT,
            country               TEXT,
            medical_class         TEXT,
            medical_date          TEXT,
            norm_last_name        TEXT        NOT NULL DEFAULT '',
            raw_source_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_faa_basic_norm_last
            ON faa_airmen_basic (norm_last_name)
    """)

    print("✓ Created faa_airmen_basic")

    # ── faa_airmen_certificates ───────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS faa_airmen_certificates (
            id                      BIGSERIAL   PRIMARY KEY,
            unique_id               TEXT        NOT NULL
                                        REFERENCES faa_airmen_basic (unique_id)
                                        ON DELETE CASCADE,
            record_type             TEXT,
            certificate_type        TEXT,
            certificate_level       TEXT,
            certificate_expire_date TEXT,
            ratings_raw             TEXT,
            is_flight_instructor    BOOLEAN     NOT NULL DEFAULT false,
            norm_cert_number        TEXT        NOT NULL DEFAULT '',
            raw_source_updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_faa_cert_unique_id
            ON faa_airmen_certificates (unique_id)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_faa_verify_lookup
            ON faa_airmen_certificates (norm_cert_number, is_flight_instructor)
    """)

    print("✓ Created faa_airmen_certificates")

    # ── Staging tables (same shape) ───────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS faa_airmen_basic_staging
            (LIKE faa_airmen_basic INCLUDING ALL)
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS faa_airmen_certificates_staging
            (LIKE faa_airmen_certificates INCLUDING ALL)
    """)

    print("✓ Created staging tables")

    # ── cfi_verification_attempts ─────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS cfi_verification_attempts (
            id                   BIGSERIAL    PRIMARY KEY,
            user_id              TEXT         NOT NULL,
            entered_cert_number  TEXT         NOT NULL,
            entered_last_name    TEXT         NOT NULL,
            entered_first_name   TEXT,
            matched_unique_id    TEXT,
            verification_status  TEXT         NOT NULL
                                     CHECK (verification_status IN ('verified', 'partial', 'unverified')),
            match_reason         TEXT,
            verified_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
            source_snapshot_date DATE
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_cfi_attempts_user
            ON cfi_verification_attempts (user_id)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_cfi_attempts_status
            ON cfi_verification_attempts (verification_status, verified_at DESC)
    """)

    print("✓ Created cfi_verification_attempts")

    # ── faa_ingest_metadata ───────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS faa_ingest_metadata (
            id                   INT          PRIMARY KEY DEFAULT 1,
            source_snapshot_date DATE         NOT NULL,
            ingested_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
            basic_row_count      INT,
            cert_row_count       INT
        )
    """)

    print("✓ Created faa_ingest_metadata")


def downgrade():
    op.execute("DROP TABLE IF EXISTS faa_ingest_metadata")
    op.execute("DROP TABLE IF EXISTS cfi_verification_attempts")
    op.execute("DROP TABLE IF EXISTS faa_airmen_certificates_staging")
    op.execute("DROP TABLE IF EXISTS faa_airmen_basic_staging")
    op.execute("DROP TABLE IF EXISTS faa_airmen_certificates CASCADE")
    op.execute("DROP TABLE IF EXISTS faa_airmen_basic CASCADE")

    print("✓ Dropped FAA airmen tables")
