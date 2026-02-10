"""Initial logbook schema with pgvector

Revision ID: 001
Revises: 
Create Date: 2026-02-03

"""
from alembic import op

revision = '001'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    
    # Create logbook_entries table
    op.execute("""
        CREATE TABLE IF NOT EXISTS logbook_entries (
            entry_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            
            aircraft JSONB,
            tail_number VARCHAR(50),
            
            route_legs JSONB,
            route TEXT,
            flight_types TEXT[],
            
            total_time DECIMAL(5,2),
            block_out VARCHAR(20),
            block_in VARCHAR(20),
            
            pic DECIMAL(5,2),
            sic DECIMAL(5,2),
            dual_received DECIMAL(5,2),
            dual_given DECIMAL(5,2),
            solo DECIMAL(5,2),
            cross_country DECIMAL(5,2),
            night DECIMAL(5,2),
            actual_imc DECIMAL(5,2),
            simulated_instrument DECIMAL(5,2),
            
            day_takeoffs INTEGER,
            day_landings INTEGER,
            night_takeoffs INTEGER,
            night_landings INTEGER,
            day_full_stop_landings INTEGER DEFAULT 0,
            night_full_stop_landings INTEGER DEFAULT 0,
            
            approaches INTEGER,
            holds BOOLEAN DEFAULT FALSE,
            tracking BOOLEAN DEFAULT FALSE,
            
            instructor JSONB,
            student JSONB,
            lesson_topic TEXT,
            ground_instruction TEXT,
            maneuvers TEXT[],
            
            remarks TEXT,
            safety_notes TEXT,
            safety_relevant BOOLEAN DEFAULT FALSE,
            
            status VARCHAR(50) DEFAULT 'draft',
            signature JSONB,
            
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    
    # Create embeddings table
    op.execute("""
        CREATE TABLE IF NOT EXISTS logbook_entry_embeddings (
            entry_id UUID PRIMARY KEY REFERENCES logbook_entries(entry_id) ON DELETE CASCADE,
            embedding vector(1536),
            searchable_text TEXT,
            created_at TIMESTAMP NOT NULL
        )
    """)
    
    # Create indexes
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_entries_user_id ON logbook_entries(user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_entries_date ON logbook_entries(date)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_entries_status ON logbook_entries(status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_entries_user_date ON logbook_entries(user_id, date DESC)")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_entry_embeddings_embedding 
        ON logbook_entry_embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS logbook_entry_embeddings CASCADE")
    op.execute("DROP TABLE IF EXISTS logbook_entries CASCADE")
    op.execute("DROP EXTENSION IF EXISTS vector")
