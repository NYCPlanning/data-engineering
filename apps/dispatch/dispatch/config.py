from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    valkey_url: str = "redis://localhost:6379/0"
    # Every key this service writes starts with this, so one Valkey can be shared by tests.
    key_prefix: str = "dispatch:"
    # Shared key for the X-Dispatch-Key header. None disables auth (local dev only).
    shared_key: str | None = None
    lease_seconds: float = 60.0
    # How often the in-process reaper runs. 0 disables it (e.g. if it runs elsewhere).
    reap_interval_seconds: float = 15.0
    # How long a finished item (and its events, attempts and idempotency key) is kept.
    terminal_ttl_seconds: int = 7 * 24 * 3600
    # Most events one attempt may append; more are refused (429), never silently trimmed.
    max_events_per_item: int = 100_000
    pool_max_size: int = 32

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get
        return cls(
            valkey_url=env("DISPATCH_VALKEY_URL", cls.valkey_url),
            key_prefix=env("DISPATCH_KEY_PREFIX", cls.key_prefix),
            shared_key=env("DISPATCH_SHARED_KEY") or None,
            lease_seconds=float(env("DISPATCH_LEASE_SECONDS", cls.lease_seconds)),
            reap_interval_seconds=float(
                env("DISPATCH_REAP_INTERVAL_SECONDS", cls.reap_interval_seconds)
            ),
            terminal_ttl_seconds=int(
                env("DISPATCH_TERMINAL_TTL_SECONDS", cls.terminal_ttl_seconds)
            ),
            max_events_per_item=int(
                env("DISPATCH_MAX_EVENTS_PER_ITEM", cls.max_events_per_item)
            ),
            pool_max_size=int(env("DISPATCH_POOL_MAX_SIZE", cls.pool_max_size)),
        )
