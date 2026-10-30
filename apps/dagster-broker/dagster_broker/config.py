from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    valkey_url: str = "redis://localhost:6379/0"
    # Every key this service writes starts with this, so one Valkey can be shared by tests.
    key_prefix: str = "dagster-broker:"
    # Shared key for the X-Dagster-Broker-Key header. None disables auth (local dev only).
    shared_key: str | None = None
    lease_seconds: float = 60.0
    # How often the in-process reaper runs. 0 disables it (e.g. if it runs elsewhere).
    reap_interval_seconds: float = 15.0
    # How long a finished item (and its events, attempts and idempotency key) is kept.
    terminal_ttl_seconds: int = 24 * 3600
    # Most events one attempt may append; more are refused (429), never silently trimmed.
    max_events_per_item: int = 100_000
    pool_max_size: int = 32

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get
        return cls(
            valkey_url=env("DAGSTER_BROKER_VALKEY_URL", cls.valkey_url),
            key_prefix=env("DAGSTER_BROKER_KEY_PREFIX", cls.key_prefix),
            shared_key=env("DAGSTER_BROKER_SHARED_KEY") or None,
            lease_seconds=float(env("DAGSTER_BROKER_LEASE_SECONDS", cls.lease_seconds)),
            reap_interval_seconds=float(
                env("DAGSTER_BROKER_REAP_INTERVAL_SECONDS", cls.reap_interval_seconds)
            ),
            terminal_ttl_seconds=int(
                env("DAGSTER_BROKER_TERMINAL_TTL_SECONDS", cls.terminal_ttl_seconds)
            ),
            max_events_per_item=int(
                env("DAGSTER_BROKER_MAX_EVENTS_PER_ITEM", cls.max_events_per_item)
            ),
            pool_max_size=int(env("DAGSTER_BROKER_POOL_MAX_SIZE", cls.pool_max_size)),
        )
