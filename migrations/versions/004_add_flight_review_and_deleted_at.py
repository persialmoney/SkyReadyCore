"""Add is_flight_review and deleted_at columns

Revision ID: 004
Revises: 003
Create Date: 2026-02-12

This migration adds:
- is_flight_review: Boolean field to mark flight review entries (BFR/IPC)
- deleted_at: Timestamp for soft deletes (sync protocol support)

These fields are required for:
1. Flight review currency tracking (FAR 61.56)
2. Proper sync protocol implementation with conflict resolution
3. Frontend-backend schema alignment

"""
from alembic import op

revision = '004'
down_revision = '003'
branch_labels = None
depends_on = None

def upgrade():
    # Add is_flight_review column
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS is_flight_review BOOLEAN DEFAULT FALSE
    """)
    
    # Add deleted_at column for soft deletes
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP DEFAULT NULL
    """)
    
    # Add index for deleted_at to improve sync query performance
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_deleted_at 
        ON logbook_entries(deleted_at) 
        WHERE deleted_at IS NOT NULL
    """)
    
    # Add index for is_flight_review for currency queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_flight_review 
        ON logbook_entries(user_id, is_flight_review) 
        WHERE is_flight_review = TRUE
    """)
    
    print("✓ Added is_flight_review column")
    print("✓ Added deleted_at column")
    print("✓ Created idx_logbook_deleted_at index")
    print("✓ Created idx_logbook_flight_review index")

def downgrade():
    # Remove indexes
    op.execute("""
        DROP INDEX IF EXISTS idx_logbook_deleted_at
    """)
    
    op.execute("""
        DROP INDEX IF EXISTS idx_logbook_flight_review
    """)
    
    # Remove columns
    op.execute("""
        ALTER TABLE logbook_entries
        DROP COLUMN IF EXISTS is_flight_review,
        DROP COLUMN IF EXISTS deleted_at
    """)
    
    print("✓ Removed is_flight_review and deleted_at columns")
    print("✓ Removed associated indexes")
