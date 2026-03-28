"""Create student_cfi_shares consent table.

Revision ID: 007
Revises: 006
Create Date: 2026-03-28

Adds a lightweight consent gate so students can choose, per CFI, whether
to share their proficiency snapshot data. The proficiency data itself stays
in proficiency_snapshots — this table is purely "is sharing allowed?".
"""
from alembic import op

revision = '007'
down_revision = '006'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE IF NOT EXISTS student_cfi_shares (
            student_id   TEXT NOT NULL,
            cfi_user_id  TEXT NOT NULL,
            sharing      BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (student_id, cfi_user_id)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_student_cfi_shares_cfi
            ON student_cfi_shares (cfi_user_id)
    """)
    print("✓ Created student_cfi_shares table with index on cfi_user_id")


def downgrade():
    op.execute("DROP TABLE IF EXISTS student_cfi_shares")
