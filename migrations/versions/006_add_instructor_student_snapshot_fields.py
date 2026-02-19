"""Add instructor/student snapshot fields and CFI mirror tracking

Revision ID: 006
Revises: 005
Create Date: 2026-02-18

This migration adds:
- instructor_user_id: Text field for instructor user ID reference
- instructor_snapshot: JSONB field for immutable instructor data snapshot
- student_user_id: Text field for student user ID reference  
- student_snapshot: JSONB field for immutable student data snapshot
- mirrored_from_entry_id: Text field for CFI mirror tracking
- mirrored_from_user_id: Text field for source user tracking

These fields replace the old JSONB instructor/student columns with a cleaner
architecture that separates user IDs from snapshot data and enables proper
CFI mirror tracking for dual instruction scenarios.

"""
from alembic import op

revision = '006'
down_revision = '005'
branch_labels = None
depends_on = None

def upgrade():
    # Add instructor snapshot fields (replacing old JSONB instructor field)
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS instructor_user_id TEXT
    """)
    
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS instructor_snapshot JSONB
    """)
    
    # Add student snapshot fields (replacing old JSONB student field)
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS student_user_id TEXT
    """)
    
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS student_snapshot JSONB
    """)
    
    # Add CFI mirror tracking fields
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS mirrored_from_entry_id TEXT
    """)
    
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS mirrored_from_user_id TEXT
    """)
    
    # Create index on mirrored_from_entry_id for efficient lookups (idempotency checks)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_mirrored_from 
        ON logbook_entries(mirrored_from_entry_id) 
        WHERE mirrored_from_entry_id IS NOT NULL
    """)
    
    # Create index on instructor_user_id for efficient queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_instructor_user 
        ON logbook_entries(instructor_user_id) 
        WHERE instructor_user_id IS NOT NULL
    """)
    
    # Create index on student_user_id for efficient queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_student_user 
        ON logbook_entries(student_user_id) 
        WHERE student_user_id IS NOT NULL
    """)
    
    # CLEAN BREAK: Drop old columns (no users exist, no migration needed)
    # The old 'instructor' and 'student' columns stored instructor data as JSONB
    # New approach: instructor_user_id + instructor_snapshot (immutable)
    op.execute("""
        ALTER TABLE logbook_entries
        DROP COLUMN IF EXISTS instructor
    """)
    
    op.execute("""
        ALTER TABLE logbook_entries
        DROP COLUMN IF EXISTS student
    """)
    
    print("✓ Added instructor_user_id and instructor_snapshot columns")
    print("✓ Added student_user_id and student_snapshot columns")
    print("✓ Added mirrored_from_entry_id and mirrored_from_user_id columns")
    print("✓ Created idx_logbook_mirrored_from index")
    print("✓ Created idx_logbook_instructor_user index")
    print("✓ Created idx_logbook_student_user index")
    print("✓ Dropped old instructor and student columns")

def downgrade():
    # Remove indexes
    op.execute("""
        DROP INDEX IF EXISTS idx_logbook_mirrored_from
    """)
    
    op.execute("""
        DROP INDEX IF EXISTS idx_logbook_instructor_user
    """)
    
    op.execute("""
        DROP INDEX IF EXISTS idx_logbook_student_user
    """)
    
    # Remove new columns
    op.execute("""
        ALTER TABLE logbook_entries
        DROP COLUMN IF EXISTS instructor_user_id,
        DROP COLUMN IF EXISTS instructor_snapshot,
        DROP COLUMN IF EXISTS student_user_id,
        DROP COLUMN IF EXISTS student_snapshot,
        DROP COLUMN IF EXISTS mirrored_from_entry_id,
        DROP COLUMN IF EXISTS mirrored_from_user_id
    """)
    
    # Restore old columns (as empty JSONB)
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS instructor JSONB
    """)
    
    op.execute("""
        ALTER TABLE logbook_entries
        ADD COLUMN IF NOT EXISTS student JSONB
    """)
    
    print("✓ Removed instructor/student snapshot fields")
    print("✓ Removed CFI mirror tracking fields")
    print("✓ Removed associated indexes")
    print("✓ Restored old instructor and student columns")
