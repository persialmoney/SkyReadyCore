"""Add student_snapshot column to student_cfi_shares.

Revision ID: 008
Revises: 007
Create Date: 2026-03-28

Adds a JSONB snapshot of the student's basic profile (name, certificateType)
so the CFI can display a student list without needing to look them up separately.
"""
from alembic import op

revision = '008'
down_revision = '007'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE student_cfi_shares
            ADD COLUMN IF NOT EXISTS student_snapshot JSONB
    """)
    print("✓ Added student_snapshot to student_cfi_shares")


def downgrade():
    op.execute("""
        ALTER TABLE student_cfi_shares
            DROP COLUMN IF EXISTS student_snapshot
    """)
