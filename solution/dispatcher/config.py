"""Loads campaign config, leads and runtime settings.

Two kinds of config live here:

1. Campaign data (tenants, campaigns, windows, retry policy, leads) read from the
   fixtures folder. This is the business input for a run.
2. Runtime settings (provider URL, Temporal connection, webhook port...) read from
   environment variables, with defaults that work locally with no setup.

Everything is a frozen dataclass so it can be passed into the Temporal workflow as
input and saved in its history exactly as it was at the start of the run.
"""
from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# repo_root/solution/dispatcher/config.py -> repo_root
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FIXTURES_DIR = REPO_ROOT / "assignment-b-workflow" / "fixtures"

DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


@dataclass(frozen=True)
class Window:
    """A daily calling window in IST, [start, end), on the listed days.

    start and end are "HH:MM". days is None for a window that applies every day
    (used for the regulatory 09:00-21:00 window).
    """

    start: str
    end: str
    days: tuple[str, ...] | None = None


@dataclass(frozen=True)
class Tenant:
    tenant_id: str
    max_concurrent_calls: int


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    tenant_id: str
    window: Window
    max_attempts: int


@dataclass(frozen=True)
class Lead:
    """One lead to call. Customer names are deliberately not loaded: we never need
    them, and anything the workflow holds is stored in Temporal history."""

    campaign_id: str
    lead_id: str
    phone: str


@dataclass(frozen=True)
class RetryPolicy:
    """Minutes to wait after a call ends with this status, before the next attempt."""

    no_answer_minutes: int
    busy_minutes: int
    failed_minutes: int

    def wait_minutes(self, status: str) -> int | None:
        return {
            "no_answer": self.no_answer_minutes,
            "busy": self.busy_minutes,
            "failed": self.failed_minutes,
        }.get(status)


@dataclass(frozen=True)
class CampaignConfig:
    """Everything a run needs to know about the business rules and leads."""

    regulatory_window: Window
    window_grace_seconds: int
    retry_policy: RetryPolicy
    webhook_secret: str
    tenants: tuple[Tenant, ...]
    campaigns: tuple[Campaign, ...]
    leads: tuple[Lead, ...] = field(default_factory=tuple)

    def tenant(self, tenant_id: str) -> Tenant:
        for t in self.tenants:
            if t.tenant_id == tenant_id:
                return t
        raise KeyError(f"unknown tenant {tenant_id}")

    def campaign(self, campaign_id: str) -> Campaign:
        for c in self.campaigns:
            if c.campaign_id == campaign_id:
                return c
        raise KeyError(f"unknown campaign {campaign_id}")


def _parse_window(raw: dict, require_days: bool) -> Window:
    days = raw.get("days")
    if days is None:
        if require_days:
            raise ValueError(f"campaign window is missing 'days': {raw}")
        return Window(start=raw["start"], end=raw["end"], days=None)
    days = tuple(d.lower() for d in days)
    bad = [d for d in days if d not in DAYS]
    if bad:
        raise ValueError(f"unknown day names in window: {bad}")
    return Window(start=raw["start"], end=raw["end"], days=days)


def load_campaign_config(fixtures_dir: str | Path = DEFAULT_FIXTURES_DIR) -> CampaignConfig:
    """Read campaigns.json and every campaign's leads CSV from the fixtures folder."""
    fixtures_dir = Path(fixtures_dir)
    raw = json.loads((fixtures_dir / "campaigns.json").read_text(encoding="utf-8"))

    tenants = tuple(
        Tenant(tenant_id=t["tenant_id"], max_concurrent_calls=int(t["max_concurrent_calls"]))
        for t in raw["tenants"]
    )
    tenant_ids = {t.tenant_id for t in tenants}

    campaigns: list[Campaign] = []
    leads: list[Lead] = []
    seen_leads: set[tuple[str, str]] = set()
    for c in raw["campaigns"]:
        if c["tenant_id"] not in tenant_ids:
            raise ValueError(f"campaign {c['campaign_id']} points to unknown tenant {c['tenant_id']}")
        campaigns.append(
            Campaign(
                campaign_id=c["campaign_id"],
                tenant_id=c["tenant_id"],
                window=_parse_window(c["window"], require_days=True),
                max_attempts=int(c["max_attempts"]),
            )
        )
        with open(fixtures_dir / c["leads_file"], newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = (c["campaign_id"], row["lead_id"].strip())
                if key in seen_leads:
                    raise ValueError(f"duplicate lead {key} in {c['leads_file']}")
                seen_leads.add(key)
                leads.append(Lead(campaign_id=key[0], lead_id=key[1], phone=row["phone"].strip()))

    rp = raw["retry_policy"]
    return CampaignConfig(
        regulatory_window=_parse_window(raw["regulatory_window"], require_days=False),
        window_grace_seconds=int(raw.get("window_grace_seconds", 0)),
        retry_policy=RetryPolicy(
            no_answer_minutes=int(rp["no_answer_minutes"]),
            busy_minutes=int(rp["busy_minutes"]),
            failed_minutes=int(rp["failed_minutes"]),
        ),
        webhook_secret=raw["webhook_secret"],
        tenants=tenants,
        campaigns=tuple(campaigns),
        leads=tuple(leads),
    )


@dataclass(frozen=True)
class Settings:
    """Runtime settings. Defaults work locally with `temporal server start-dev`."""

    provider_url: str
    fixtures_dir: Path
    temporal_address: str
    temporal_namespace: str
    temporal_api_key: str | None
    task_queue: str
    webhook_host: str
    webhook_port: int
    output_path: Path

    @property
    def callback_url(self) -> str:
        return f"http://{self.webhook_host}:{self.webhook_port}/webhooks/telephony"

    @property
    def uses_temporal_cloud(self) -> bool:
        return bool(self.temporal_api_key)


def load_settings() -> Settings:
    """Read settings from the environment. A `.env` file is loaded first if present."""
    load_dotenv(REPO_ROOT / "solution" / ".env")

    def env(name: str, default: str) -> str:
        # Treat an empty value (e.g. a blank line copied from .env.example) as unset.
        return os.environ.get(name) or default

    return Settings(
        provider_url=env("PROVIDER_URL", "http://127.0.0.1:8099").rstrip("/"),
        fixtures_dir=Path(env("FIXTURES_DIR", str(DEFAULT_FIXTURES_DIR))),
        temporal_address=env("TEMPORAL_ADDRESS", "localhost:7233"),
        temporal_namespace=env("TEMPORAL_NAMESPACE", "default"),
        temporal_api_key=os.environ.get("TEMPORAL_API_KEY") or None,
        task_queue=env("TASK_QUEUE", "campaign-dispatcher"),
        webhook_host=env("WEBHOOK_HOST", "127.0.0.1"),
        webhook_port=int(env("WEBHOOK_PORT", "8200")),
        output_path=Path(env("OUTPUT_PATH", str(REPO_ROOT / "final_state.csv"))),
    )
