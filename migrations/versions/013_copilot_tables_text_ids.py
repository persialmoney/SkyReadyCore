"""Change copilot table ID columns from UUID to TEXT.

Revision ID: 013
Revises: 012
Create Date: 2026-04-23

WatermelonDB generates 16-char alphanumeric IDs (e.g. "zhqrc1SWyUq2W71W"),
not UUIDs. The client sends this as conversation_id; storing it in a UUID
column causes a type error. TEXT preserves the 1:1 mapping without any
server-side ID transformation.
"""

from alembic import op

revision = '013'
down_revision = '012'
branch_labels = None
depends_on = None


def upgrade():
    # Drop FK first, recreate after column type change
    op.execute("ALTER TABLE copilot_messages DROP CONSTRAINT IF EXISTS copilot_messages_conversation_id_fkey")

    op.execute("ALTER TABLE copilot_conversations ALTER COLUMN id TYPE TEXT")
    op.execute("ALTER TABLE copilot_messages      ALTER COLUMN id              TYPE TEXT")
    op.execute("ALTER TABLE copilot_messages      ALTER COLUMN conversation_id TYPE TEXT")

    # Remove gen_random_uuid() default — messages.id is now supplied by the caller (uuid4)
    op.execute("ALTER TABLE copilot_messages ALTER COLUMN id DROP DEFAULT")

    op.execute("""
        ALTER TABLE copilot_messages
            ADD CONSTRAINT copilot_messages_conversation_id_fkey
            FOREIGN KEY (conversation_id) REFERENCES copilot_conversations(id) ON DELETE CASCADE
    """)
    print("✓ Converted copilot ID columns to TEXT")


def downgrade():
    op.execute("ALTER TABLE copilot_messages DROP CONSTRAINT IF EXISTS copilot_messages_conversation_id_fkey")

    op.execute("ALTER TABLE copilot_conversations ALTER COLUMN id              TYPE UUID USING id::uuid")
    op.execute("ALTER TABLE copilot_messages      ALTER COLUMN id              TYPE UUID USING id::uuid")
    op.execute("ALTER TABLE copilot_messages      ALTER COLUMN conversation_id TYPE UUID USING conversation_id::uuid")

    op.execute("ALTER TABLE copilot_messages ALTER COLUMN id SET DEFAULT gen_random_uuid()")

    op.execute("""
        ALTER TABLE copilot_messages
            ADD CONSTRAINT copilot_messages_conversation_id_fkey
            FOREIGN KEY (conversation_id) REFERENCES copilot_conversations(id) ON DELETE CASCADE
    """)
    print("✓ Reverted copilot ID columns to UUID")
