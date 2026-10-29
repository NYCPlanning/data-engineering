from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    # Shared key for the X-Dispatch-Key header. None disables auth (local dev only).
    shared_key: str | None = None
    lease_seconds: float = 60.0
    # How often the in-process reaper runs. 0 disables it (e.g. if it runs elsewhere).
    reap_interval_seconds: float = 15.0
    pool_max_size: int = 10

    @classmethod
    def from_env(cls) -> Settings:
        url = os.environ.get("DISPATCH_DATABASE_URL")
        if not url:
            raise RuntimeError("DISPATCH_DATABASE_URL must be set")
        return cls(
            database_url=url,
            shared_key=os.environ.get("DISPATCH_SHARED_KEY") or None,
            lease_seconds=float(os.environ.get("DISPATCH_LEASE_SECONDS", 60)),
            reap_interval_seconds=float(
                os.environ.get("DISPATCH_REAP_INTERVAL_SECONDS", 15)
            ),
            pool_max_size=int(os.environ.get("DISPATCH_POOL_MAX_SIZE", 10)),
        )
