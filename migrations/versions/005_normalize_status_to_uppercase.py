"""Normalize status values to uppercase enum format

Revision ID: 005
Revises: 004
Create Date: 2026-02-12

This migration:
1. Updates all existing status values from lowercase to uppercase to match GraphQL schema
2. Changes the default value from 'draft' to 'DRAFT'

Status value mappings:
- 'draft' -> 'DRAFT'
- 'saved' -> 'SAVED'
- 'pending-signature' -> 'PENDING_SIGNATURE'
- 'signed' -> 'SIGNED'
- 'returned' -> 'RETURNED'

This ensures consistency between:
- Frontend (React Native)
- Backend (Python Lambdas)
- Database (PostgreSQL)
- GraphQL Schema (AppSync)

"""
from alembic import op

revision = '005'
down_revision = '004'
branch_labels = None
depends_on = None

def upgrade():
    # Update existing status values to uppercase
    op.execute("""
        UPDATE logbook_entries
        SET status = CASE 
            WHEN LOWER(status) = 'draft' THEN 'DRAFT'
            WHEN LOWER(status) = 'saved' THEN 'SAVED'
            WHEN LOWER(status) = 'pending-signature' OR LOWER(status) = 'pending_signature' THEN 'PENDING_SIGNATURE'
            WHEN LOWER(status) = 'signed' THEN 'SIGNED'
            WHEN LOWER(status) = 'returned' THEN 'RETURNED'
            ELSE 'DRAFT'
        END
        WHERE status IS NOT NULL
    """)
    
    # Update the default value for the status column
    op.execute("""
        ALTER TABLE logbook_entries
        ALTER COLUMN status SET DEFAULT 'DRAFT'
    """)
    
    print("✓ Updated all existing status values to uppercase")
    print("✓ Changed default status from 'draft' to 'DRAFT'")

def downgrade():
    # Revert status values to lowercase
    op.execute("""
        UPDATE logbook_entries
        SET status = CASE 
            WHEN status = 'DRAFT' THEN 'draft'
            WHEN status = 'SAVED' THEN 'saved'
            WHEN status = 'PENDING_SIGNATURE' THEN 'pending-signature'
            WHEN status = 'SIGNED' THEN 'signed'
            WHEN status = 'RETURNED' THEN 'returned'
            ELSE 'draft'
        END
        WHERE status IS NOT NULL
    """)
    
    # Revert the default value
    op.execute("""
        ALTER TABLE logbook_entries
        ALTER COLUMN status SET DEFAULT 'draft'
    """)
    
    print("✓ Reverted status values to lowercase")
    print("✓ Changed default status from 'DRAFT' back to 'draft'")
