"""Add missions, mission_airports, and readiness_assessments tables

Revision ID: 007
Revises: 006
Create Date: 2026-02-28

This migration adds:
- missions: Planned flights with route, schedule, aircraft, and cached readiness summary
- mission_airports: Ordered airport list for each mission (one-to-many)
- readiness_assessments: Immutable readiness check events attached to missions

Design decisions:
- All timestamps are epoch milliseconds (BIGINT) to match WatermelonDB / client convention
- airport_checks_json / route_airports_json stored as JSONB for now; index columns
  (flags_summary, max_severity_score, etc.) enable analytics without full JSON parsing
- Soft deletes via deleted_at on missions and assessments (airports follow mission)
- route_hash on both missions and assessments allows staleness detection without joins
- No foreign key constraints to match offline-first sync pattern (client may push
  airports before or after the parent mission; server reconciles on pull)

"""
from alembic import op

revision = '007'
down_revision = '006'
branch_labels = None
depends_on = None


def upgrade():
    # =========================================================================
    # MISSIONS
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS missions (
            -- Identity
            id                      TEXT PRIMARY KEY,   -- WatermelonDB row id
            mission_id              TEXT NOT NULL,      -- stable business id (same as id for now)
            user_id                 TEXT NOT NULL,

            -- Lifecycle
            status                  TEXT NOT NULL DEFAULT 'SCHEDULED',
            is_operational          BOOLEAN NOT NULL DEFAULT FALSE,
            mission_type            TEXT NOT NULL DEFAULT 'LOCAL',

            -- Schedule
            scheduled_time_utc      BIGINT,             -- epoch ms; NULL for operational checks
            time_precision          TEXT,               -- EXACT | DATE_ONLY

            -- Aircraft
            tail_number             TEXT,
            aircraft_label          TEXT,

            -- Misc
            notes                   TEXT,
            forecast_reviewed_at    BIGINT,             -- epoch ms

            -- Route versioning
            route_hash              TEXT,

            -- Denormalized latest readiness summary (keeps list queries fast)
            latest_assessment_id    TEXT,
            latest_result           TEXT,               -- GO | CAUTION | NO_GO | UNKNOWN
            latest_checked_time     BIGINT,             -- epoch ms
            latest_reason_short     TEXT,
            latest_reason_long      TEXT,

            -- Timestamps (epoch ms)
            created_at              BIGINT NOT NULL,
            updated_at              BIGINT NOT NULL,
            deleted_at              BIGINT,

            -- Sync
            _sync_pending           BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_missions_user_id ON missions(user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_missions_user_status ON missions(user_id, status) WHERE deleted_at IS NULL")
    op.execute("CREATE INDEX IF NOT EXISTS idx_missions_user_updated ON missions(user_id, updated_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_missions_deleted_at ON missions(deleted_at) WHERE deleted_at IS NOT NULL")
    op.execute("CREATE INDEX IF NOT EXISTS idx_missions_scheduled_time ON missions(user_id, scheduled_time_utc) WHERE deleted_at IS NULL")

    print("✓ Created missions table and indexes")

    # =========================================================================
    # MISSION AIRPORTS
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS mission_airports (
            -- Identity
            id                      TEXT PRIMARY KEY,   -- WatermelonDB row id
            mission_id              TEXT NOT NULL,      -- references missions.id (no FK for offline sync)

            -- Airport
            icao                    TEXT NOT NULL,
            role                    TEXT NOT NULL,      -- DEPARTURE | DESTINATION | STOP | ALTERNATE
            order_index             INTEGER NOT NULL DEFAULT 0,
            display_name            TEXT,

            -- Sync
            _sync_pending           BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_mission_airports_mission_id ON mission_airports(mission_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_mission_airports_user_mission ON mission_airports(mission_id, role)")

    print("✓ Created mission_airports table and indexes")

    # =========================================================================
    # READINESS ASSESSMENTS
    # =========================================================================
    op.execute("""
        CREATE TABLE IF NOT EXISTS readiness_assessments (
            -- Identity
            id                      TEXT PRIMARY KEY,   -- WatermelonDB row id
            mission_id              TEXT NOT NULL,      -- references missions.id

            -- When + what was checked
            assessment_time_utc     BIGINT NOT NULL,    -- epoch ms
            target_time_utc         BIGINT,             -- epoch ms; scheduled departure or NOW
            target_time_kind        TEXT NOT NULL DEFAULT 'NOW',  -- NOW | SCHEDULED | USER_SELECTED

            -- Route snapshot (self-contained for auditability)
            route_hash              TEXT,
            route_airports_json     JSONB,              -- [{icao, role, orderIndex}]

            -- Minimums profile used
            minimums_profile_id     TEXT,
            minimums_profile_name   TEXT,

            -- Data source
            data_source             TEXT,               -- TAF_AT_SCHEDULED_TIME | TAF_LAST_PERIOD | DATA_MISSING | CURRENT_METAR | MOS

            -- Aggregate result
            aggregate_result        TEXT NOT NULL,      -- GO | CAUTION | NO_GO | UNKNOWN
            aggregate_reason_short  TEXT,
            aggregate_reason_long   TEXT,

            -- UI explanation strings
            dominant_factor         TEXT,
            dominant_message        TEXT,
            margin_message          TEXT,

            -- Summary / analytics index columns
            airports_evaluated_count    INTEGER DEFAULT 0,
            airports_missing_data_count INTEGER DEFAULT 0,
            flags_summary               TEXT,   -- pipe-delimited: "CROSSWIND_PROXY|GUST_SPREAD"
            max_severity_score          INTEGER DEFAULT 0,
            near_limit_airport_count    INTEGER DEFAULT 0,
            no_go_airport_count         INTEGER DEFAULT 0,

            -- Per-airport detail (append-only immutable snapshot)
            airport_checks_json     JSONB,

            -- Stale evaluation inputs
            stale_threshold_minutes INTEGER DEFAULT 360,

            -- Timestamps (epoch ms)
            created_at              BIGINT NOT NULL,
            deleted_at              BIGINT,

            -- Sync
            _sync_pending           BOOLEAN NOT NULL DEFAULT FALSE
        )
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_assessments_mission_id ON readiness_assessments(mission_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_assessments_mission_time ON readiness_assessments(mission_id, assessment_time_utc) WHERE deleted_at IS NULL")
    op.execute("CREATE INDEX IF NOT EXISTS idx_assessments_user_created ON readiness_assessments(mission_id, created_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_assessments_result ON readiness_assessments(aggregate_result) WHERE deleted_at IS NULL")
    op.execute("CREATE INDEX IF NOT EXISTS idx_assessments_deleted_at ON readiness_assessments(deleted_at) WHERE deleted_at IS NOT NULL")

    print("✓ Created readiness_assessments table and indexes")


def downgrade():
    op.execute("DROP TABLE IF EXISTS readiness_assessments")
    op.execute("DROP TABLE IF EXISTS mission_airports")
    op.execute("DROP TABLE IF EXISTS missions")

    print("✓ Dropped readiness_assessments table")
    print("✓ Dropped mission_airports table")
    print("✓ Dropped missions table")
