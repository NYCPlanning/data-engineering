"""Initial dispatch schema: work_items, work_attempts, workers, events.

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE workers (
            id          text PRIMARY KEY,
            caps        text[] NOT NULL DEFAULT '{}',
            first_seen  timestamptz NOT NULL DEFAULT now(),
            last_seen   timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE work_items (
            id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            run_id           text,
            asset_key        text,
            partition_key    text,
            idempotency_key  text UNIQUE,
            required_caps    text[] NOT NULL DEFAULT '{}',
            priority         integer NOT NULL DEFAULT 0,
            status           text NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued', 'claimed', 'done', 'failed', 'dead', 'cancelled')),
            -- Number of claims so far. (id, attempt) is the fencing token for one claim.
            attempt          integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
            max_attempts     integer NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
            worker_id        text,
            lease_until      timestamptz,
            cancel_requested boolean NOT NULL DEFAULT false,
            enqueued_at      timestamptz NOT NULL DEFAULT now(),
            claimed_at       timestamptz,
            finished_at      timestamptz,
            updated_at       timestamptz NOT NULL DEFAULT now(),
            payload          jsonb NOT NULL DEFAULT '{}',
            result           jsonb,
            error            text,
            CHECK (attempt <= max_attempts),
            CHECK (
                (status = 'claimed') = (worker_id IS NOT NULL AND lease_until IS NOT NULL)
            )
        );
        CREATE INDEX work_items_required_caps_gin ON work_items USING gin (required_caps);
        CREATE INDEX work_items_queued_order ON work_items (priority DESC, enqueued_at, id)
            WHERE status = 'queued';
        CREATE INDEX work_items_claimed_lease ON work_items (lease_until)
            WHERE status = 'claimed';
        CREATE INDEX work_items_run_id ON work_items (run_id);

        -- One row per claim. The primary key is the double-claim backstop: if two claims
        -- ever got the same (item_id, attempt), the second claim statement fails outright.
        CREATE TABLE work_attempts (
            item_id     bigint NOT NULL REFERENCES work_items (id) ON DELETE CASCADE,
            attempt     integer NOT NULL,
            worker_id   text NOT NULL,
            claimed_at  timestamptz NOT NULL DEFAULT now(),
            ended_at    timestamptz,
            outcome     text CHECK (outcome IN ('done', 'failed', 'expired', 'cancelled')),
            error       text,
            PRIMARY KEY (item_id, attempt)
        );

        CREATE TABLE events (
            id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            item_id    bigint NOT NULL REFERENCES work_items (id) ON DELETE CASCADE,
            attempt    integer,
            worker_id  text,
            kind       text NOT NULL,
            data       jsonb NOT NULL DEFAULT '{}',
            at         timestamptz NOT NULL DEFAULT now()
        );
        CREATE INDEX events_item ON events (item_id, id);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE events, work_attempts, work_items, workers")
