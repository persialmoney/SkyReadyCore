"""Complete initial schema — all tables, indexes, and extensions from scratch.

Revision ID: 001
Revises:
Create Date: 2026-02-28

Single-migration baseline. No incremental history needed; there are no users.
"""
from alembic import op

revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # =========================================================================
    # EXTENSIONS
    # =========================================================================
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    print("✓ Enabled pgvector extension")

    # =========================================================================
    # LOGBOOK ENTRIES
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS logbook_entries (
            entry_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id         TEXT NOT NULL,
            date            DATE NOT NULL,

            -- Aircraft
            aircraft        JSONB,
            tail_number     TEXT,

            -- Route
            route_legs      JSONB,
            route           TEXT,
            flight_types    TEXT[],

            -- Time
            total_time              DECIMAL(5,2),
            block_out               TEXT,
            block_in                TEXT,
            pic                     DECIMAL(5,2),
            sic                     DECIMAL(5,2),
            dual_received           DECIMAL(5,2),
            dual_given              DECIMAL(5,2),
            solo                    DECIMAL(5,2),
            cross_country           DECIMAL(5,2),
            night                   DECIMAL(5,2),
            actual_imc              DECIMAL(5,2),
            simulated_instrument    DECIMAL(5,2),

            -- Takeoffs & landings
            day_takeoffs                INTEGER,
            day_landings                INTEGER,
            night_takeoffs              INTEGER,
            night_landings              INTEGER,
            day_full_stop_landings      INTEGER DEFAULT 0,
            night_full_stop_landings    INTEGER DEFAULT 0,

            -- Instrument
            approaches  INTEGER,
            holds       BOOLEAN DEFAULT FALSE,
            tracking    BOOLEAN DEFAULT FALSE,

            -- Crew / instruction (snapshot + live reference)
            instructor_user_id      TEXT,
            instructor_snapshot     JSONB,
            student_user_id         TEXT,
            student_snapshot        JSONB,
            lesson_topic            TEXT,
            ground_instruction      DECIMAL(5,2),
            maneuvers               TEXT[],

            -- Notes
            remarks             TEXT,
            safety_notes        TEXT,
            safety_relevant     BOOLEAN DEFAULT FALSE,

            -- Meta
            status              TEXT NOT NULL DEFAULT 'DRAFT',
            signature           JSONB,
            is_flight_review    BOOLEAN DEFAULT FALSE,

            -- CFI mirror tracking
            mirrored_from_entry_id  TEXT,
            mirrored_from_user_id   TEXT,

            -- Timestamps (TIMESTAMPTZ for logbook; used with datetime comparisons in sync)
            created_at  TIMESTAMPTZ NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL,
            deleted_at  TIMESTAMPTZ
        )
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_user_id     ON logbook_entries(user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_date        ON logbook_entries(date)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_status      ON logbook_entries(status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_user_date   ON logbook_entries(user_id, date DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_updated_at  ON logbook_entries(updated_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_created_at  ON logbook_entries(created_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_logbook_user_updated ON logbook_entries(user_id, updated_at)")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_deleted_at
        ON logbook_entries(deleted_at) WHERE deleted_at IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_flight_review
        ON logbook_entries(user_id, is_flight_review) WHERE is_flight_review = TRUE
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_mirrored_from
        ON logbook_entries(mirrored_from_entry_id) WHERE mirrored_from_entry_id IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_instructor_user
        ON logbook_entries(instructor_user_id) WHERE instructor_user_id IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_student_user
        ON logbook_entries(student_user_id) WHERE student_user_id IS NOT NULL
    """)

    print("✓ Created logbook_entries table and indexes")

    # =========================================================================
    # LOGBOOK EMBEDDINGS (pgvector AI search)
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS logbook_entry_embeddings (
            entry_id        UUID PRIMARY KEY REFERENCES logbook_entries(entry_id) ON DELETE CASCADE,
            embedding       vector(1536),
            searchable_text TEXT,
            created_at      TIMESTAMPTZ NOT NULL
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_logbook_embeddings_vec
        ON logbook_entry_embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)
    """)

    print("✓ Created logbook_entry_embeddings table")

    # =========================================================================
    # OUTBOX (sync event publishing)
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            id          SERIAL PRIMARY KEY,
            event_type  TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            payload     JSONB NOT NULL,
            processed   BOOLEAN NOT NULL DEFAULT FALSE,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_outbox_processed ON outbox(processed, created_at)
    """)

    print("✓ Created outbox table")

    # =========================================================================
    # MISSIONS
    # All timestamps are epoch milliseconds (BIGINT) to match WatermelonDB.
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS missions (
            -- Identity
            id              TEXT PRIMARY KEY,   -- WatermelonDB row id
            mission_id      TEXT NOT NULL,      -- stable business id (same as id for now)
            user_id         TEXT NOT NULL,

            -- Lifecycle
            status          TEXT NOT NULL DEFAULT 'SCHEDULED',
            is_operational  BOOLEAN NOT NULL DEFAULT FALSE,
            mission_type    TEXT NOT NULL DEFAULT 'LOCAL',

            -- Schedule
            scheduled_time_utc  BIGINT,         -- epoch ms; NULL for operational checks
            time_precision      TEXT,           -- EXACT | DATE_ONLY

            -- Aircraft
            tail_number     TEXT,
            aircraft_label  TEXT,

            -- Misc
            notes                   TEXT,
            forecast_reviewed_at    BIGINT,     -- epoch ms

            -- Route versioning
            route_hash      TEXT,

            -- Denormalized latest readiness summary (keeps list queries fast)
            latest_assessment_id    TEXT,
            latest_result           TEXT,       -- GO | CAUTION | NO_GO | UNKNOWN
            latest_checked_time     BIGINT,     -- epoch ms
            latest_reason_short     TEXT,
            latest_reason_long      TEXT,

            -- Timestamps (epoch ms)
            created_at  BIGINT NOT NULL,
            updated_at  BIGINT NOT NULL,
            deleted_at  BIGINT,

            -- Sync
            _sync_pending   BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_missions_user_id      ON missions(user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_missions_user_updated  ON missions(user_id, updated_at)")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_missions_user_status
        ON missions(user_id, status) WHERE deleted_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_missions_scheduled_time
        ON missions(user_id, scheduled_time_utc) WHERE deleted_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_missions_deleted_at
        ON missions(deleted_at) WHERE deleted_at IS NOT NULL
    """)

    print("✓ Created missions table and indexes")

    # =========================================================================
    # MISSION AIRPORTS
    # No FK constraint — offline-first sync; airports may arrive before mission.
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS mission_airports (
            id          TEXT PRIMARY KEY,   -- WatermelonDB row id
            mission_id  TEXT NOT NULL,      -- references missions.id (no FK for offline sync)

            icao        TEXT NOT NULL,
            role        TEXT NOT NULL,      -- DEPARTURE | DESTINATION | STOP | ALTERNATE
            order_index INTEGER NOT NULL DEFAULT 0,
            display_name TEXT,

            _sync_pending BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_mission_airports_mission_id  ON mission_airports(mission_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_mission_airports_role        ON mission_airports(mission_id, role)")

    print("✓ Created mission_airports table and indexes")

    # =========================================================================
    # READINESS ASSESSMENTS
    # Immutable events — append-only, soft-delete only.
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS readiness_assessments (
            -- Identity
            id          TEXT PRIMARY KEY,   -- WatermelonDB row id
            mission_id  TEXT NOT NULL,      -- references missions.id

            -- When + what was checked
            assessment_time_utc BIGINT NOT NULL,    -- epoch ms
            target_time_utc     BIGINT,             -- epoch ms; scheduled departure or NOW
            target_time_kind    TEXT NOT NULL DEFAULT 'NOW',  -- NOW | SCHEDULED | USER_SELECTED

            -- Route snapshot (self-contained for auditability)
            route_hash          TEXT,
            route_airports_json JSONB,  -- [{icao, role, orderIndex}]

            -- Minimums profile used
            minimums_profile_id     TEXT,
            minimums_profile_name   TEXT,

            -- Data source
            -- TAF_AT_SCHEDULED_TIME | TAF_LAST_PERIOD | DATA_MISSING | CURRENT_METAR | MOS
            data_source TEXT,

            -- Aggregate result
            aggregate_result        TEXT NOT NULL,  -- GO | CAUTION | NO_GO | UNKNOWN
            aggregate_reason_short  TEXT,
            aggregate_reason_long   TEXT,

            -- UI explanation strings
            dominant_factor     TEXT,
            dominant_message    TEXT,
            margin_message      TEXT,

            -- Summary / analytics index columns
            airports_evaluated_count    INTEGER DEFAULT 0,
            airports_missing_data_count INTEGER DEFAULT 0,
            flags_summary               TEXT,   -- pipe-delimited: "CROSSWIND_PROXY|GUST_SPREAD"
            max_severity_score          INTEGER DEFAULT 0,
            near_limit_airport_count    INTEGER DEFAULT 0,
            no_go_airport_count         INTEGER DEFAULT 0,

            -- Per-airport detail (immutable snapshot)
            airport_checks_json JSONB,

            -- Stale evaluation inputs
            stale_threshold_minutes INTEGER DEFAULT 360,

            -- Timestamps (epoch ms)
            created_at  BIGINT NOT NULL,
            deleted_at  BIGINT,

            -- Sync
            _sync_pending BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_assessments_mission_id   ON readiness_assessments(mission_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_assessments_user_created ON readiness_assessments(mission_id, created_at)")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_assessments_mission_time
        ON readiness_assessments(mission_id, assessment_time_utc) WHERE deleted_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_assessments_result
        ON readiness_assessments(aggregate_result) WHERE deleted_at IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_assessments_deleted_at
        ON readiness_assessments(deleted_at) WHERE deleted_at IS NOT NULL
    """)

    print("✓ Created readiness_assessments table and indexes")


def downgrade():
    op.execute("DROP TABLE IF EXISTS readiness_assessments")
    op.execute("DROP TABLE IF EXISTS mission_airports")
    op.execute("DROP TABLE IF EXISTS missions")
    op.execute("DROP TABLE IF EXISTS outbox")
    op.execute("DROP TABLE IF EXISTS logbook_entry_embeddings CASCADE")
    op.execute("DROP TABLE IF EXISTS logbook_entries CASCADE")
    op.execute("DROP EXTENSION IF EXISTS vector")

    print("✓ Dropped all tables and extensions")
