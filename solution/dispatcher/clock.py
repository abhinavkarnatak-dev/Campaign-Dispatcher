"""The provider's simulated clock.

The provider runs a simulated clock faster than real time, and every business rule
(calling windows, retry waits, callbacks) uses that simulated time, never the
machine's clock. The provider defines it as:

    sim_now = sim_epoch + (real_now - real_epoch) * speed

We read sim_epoch, real_epoch and speed once from GET /v1/clock and then do the maths
ourselves, so we never need to ask the provider for the time again.

Inside the Temporal workflow, `real_now` must come from `workflow.now()`, never from
the machine clock. Temporal replays the workflow from history after a crash, and
workflow.now() returns the same values on replay, so the same decisions are made.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class SimClock:
    """Maps real time to the provider's simulated time.

    Fields are plain types (str, float) so Temporal can store this in workflow
    history and give back exactly the same values on replay.
    """

    sim_epoch_iso: str  # simulated time at real_epoch, ISO-8601 with offset
    real_epoch: float  # real unix time (seconds) when the provider's clock started
    speed: float  # simulated seconds per real second

    def __post_init__(self) -> None:
        if self.speed <= 0:
            raise ValueError(f"clock speed must be positive, got {self.speed}")
        if datetime.fromisoformat(self.sim_epoch_iso).tzinfo is None:
            raise ValueError("sim_epoch_iso must include a timezone offset")

    @property
    def sim_epoch(self) -> datetime:
        return datetime.fromisoformat(self.sim_epoch_iso)

    def sim_at(self, real_now: datetime) -> datetime:
        """Simulated time (in IST) at the given real moment. real_now must be tz-aware."""
        if real_now.tzinfo is None:
            raise ValueError("real_now must be timezone-aware")
        elapsed_real = real_now.timestamp() - self.real_epoch
        return (self.sim_epoch + timedelta(seconds=elapsed_real * self.speed)).astimezone(IST)

    def real_seconds(self, sim_seconds: float) -> float:
        """How many real seconds a simulated duration takes."""
        return sim_seconds / self.speed

    def real_delay_until(self, sim_target: datetime, real_now: datetime) -> float:
        """Real seconds to wait from real_now until the simulated clock reaches
        sim_target. Zero if that moment has already passed."""
        sim_gap = (sim_target - self.sim_at(real_now)).total_seconds()
        return max(0.0, self.real_seconds(sim_gap))


def fetch_clock(provider_url: str, timeout: float = 5.0) -> SimClock:
    """Read the provider's clock parameters from GET /v1/clock.

    Called once at startup, outside the workflow. httpx is imported here, not at the
    top, so the workflow module can import this file without pulling in network code.
    """
    import httpx

    resp = httpx.get(f"{provider_url.rstrip('/')}/v1/clock", timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    return SimClock(
        sim_epoch_iso=body["sim_epoch"],
        real_epoch=float(body["real_epoch"]),
        speed=float(body["speed"]),
    )
