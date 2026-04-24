"""Create copilot_conversations and copilot_messages tables for AgentCore persistence.

Revision ID: 012
Revises: 011
Create Date: 2026-04-22

The AgentCore orchestrator writes full conversation history to PostgreSQL at the
end of every graph execution (persist_memory node). This gives us:
  - Cross-device sync foundation (future: sync pull returns these tables)
  - Audit trail for safety-critical pilot queries
  - Per-user conversation history that survives device reinstalls

The client continues to write optimistically to WatermelonDB (local-only for now).
The agent uses the same conversation UUID the client passes in, so IDs are stable.
"""

from alembic import op

revision = '012'
down_revision = '011'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE IF NOT EXISTS copilot_conversations (
            id          UUID PRIMARY KEY,
            user_id     TEXT NOT NULL,
            title       TEXT NOT NULL DEFAULT '',
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS copilot_conversations_user_updated
            ON copilot_conversations (user_id, updated_at DESC)
    """)
    print("✓ Created copilot_conversations table")

    op.execute("""
        CREATE TABLE IF NOT EXISTS copilot_messages (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_id     UUID NOT NULL REFERENCES copilot_conversations(id) ON DELETE CASCADE,
            role                TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
            content             TEXT NOT NULL,
            model_id            TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS copilot_messages_conversation
            ON copilot_messages (conversation_id, created_at ASC)
    """)
    print("✓ Created copilot_messages table")


def downgrade():
    op.execute("DROP TABLE IF EXISTS copilot_messages")
    op.execute("DROP TABLE IF EXISTS copilot_conversations")
    print("✓ Dropped copilot tables")
