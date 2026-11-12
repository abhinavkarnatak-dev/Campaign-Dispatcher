import json
from pathlib import Path

import pytest

from dispatcher.config import DEFAULT_FIXTURES_DIR, load_campaign_config, load_settings


def test_loads_the_provided_fixtures():
    cfg = load_campaign_config(DEFAULT_FIXTURES_DIR)
    assert {t.tenant_id: t.max_concurrent_calls for t in cfg.tenants} == {
        "acme_insurance": 4,
        "zen_retail": 3,
    }
    assert {c.campaign_id for c in cfg.campaigns} == {
        "c_acme_renewals",
        "c_acme_feedback",
        "c_zen_winback",
    }
    assert len(cfg.leads) == 200
    assert cfg.regulatory_window.start == "09:00" and cfg.regulatory_window.end == "21:00"
    assert cfg.regulatory_window.days is None
    assert cfg.window_grace_seconds == 120
    assert cfg.retry_policy.wait_minutes("no_answer") == 60
    assert cfg.retry_policy.wait_minutes("failed") == 15
    assert cfg.retry_policy.wait_minutes("completed") is None


def test_campaign_window_days_are_kept():
    cfg = load_campaign_config(DEFAULT_FIXTURES_DIR)
    zen = cfg.campaign("c_zen_winback")
    assert zen.window.days == ("mon", "tue", "wed", "thu", "fri")
    assert zen.tenant_id == "zen_retail"
    assert zen.max_attempts == 3


def test_customer_names_are_not_loaded():
    cfg = load_campaign_config(DEFAULT_FIXTURES_DIR)
    assert not hasattr(cfg.leads[0], "customer_name")


def test_some_phones_appear_in_more_than_one_campaign():
    # R3 and R8 matter because of this, so make sure the fixtures really have it.
    cfg = load_campaign_config(DEFAULT_FIXTURES_DIR)
    campaigns_by_phone: dict[str, set[str]] = {}
    for lead in cfg.leads:
        campaigns_by_phone.setdefault(lead.phone, set()).add(lead.campaign_id)
    assert any(len(c) > 1 for c in campaigns_by_phone.values())


def _write_fixtures(tmp_path: Path, campaigns_json: dict, csv_text: str) -> Path:
    (tmp_path / "campaigns.json").write_text(json.dumps(campaigns_json))
    (tmp_path / "leads.csv").write_text(csv_text)
    return tmp_path


BASE = {
    "regulatory_window": {"start": "09:00", "end": "21:00"},
    "window_grace_seconds": 120,
    "retry_policy": {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15},
    "webhook_secret": "s",
    "tenants": [{"tenant_id": "t1", "max_concurrent_calls": 2}],
    "campaigns": [
        {
            "campaign_id": "c1",
            "tenant_id": "t1",
            "leads_file": "leads.csv",
            "window": {"start": "10:00", "end": "18:00", "days": ["mon"]},
            "max_attempts": 3,
        }
    ],
}


def test_rejects_duplicate_lead_ids(tmp_path):
    d = _write_fixtures(tmp_path, BASE, "lead_id,phone,customer_name\nl1,+919999999999,A\nl1,+918888888888,B\n")
    with pytest.raises(ValueError, match="duplicate lead"):
        load_campaign_config(d)


def test_rejects_unknown_tenant(tmp_path):
    bad = json.loads(json.dumps(BASE))
    bad["campaigns"][0]["tenant_id"] = "nope"
    d = _write_fixtures(tmp_path, bad, "lead_id,phone\nl1,+919999999999\n")
    with pytest.raises(ValueError, match="unknown tenant"):
        load_campaign_config(d)


def test_rejects_unknown_day_name(tmp_path):
    bad = json.loads(json.dumps(BASE))
    bad["campaigns"][0]["window"]["days"] = ["monday"]
    d = _write_fixtures(tmp_path, bad, "lead_id,phone\nl1,+919999999999\n")
    with pytest.raises(ValueError, match="unknown day"):
        load_campaign_config(d)


def test_settings_defaults(monkeypatch):
    for var in ("PROVIDER_URL", "TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_API_KEY",
                "WEBHOOK_HOST", "WEBHOOK_PORT"):
        monkeypatch.delenv(var, raising=False)
    s = load_settings()
    assert s.provider_url == "http://127.0.0.1:8099"
    assert s.temporal_address == "localhost:7233"
    assert s.temporal_namespace == "default"
    assert not s.uses_temporal_cloud
    assert s.callback_url == "http://127.0.0.1:8200/webhooks/telephony"


def test_settings_switch_to_temporal_cloud(monkeypatch):
    monkeypatch.setenv("TEMPORAL_ADDRESS", "my-ns.abcde.tmprl.cloud:7233")
    monkeypatch.setenv("TEMPORAL_NAMESPACE", "my-ns.abcde")
    monkeypatch.setenv("TEMPORAL_API_KEY", "test-key")
    s = load_settings()
    assert s.uses_temporal_cloud
    assert s.temporal_namespace == "my-ns.abcde"


def test_empty_env_values_fall_back_to_defaults(monkeypatch):
    # A .env copied straight from .env.example has blank values; they must not win.
    monkeypatch.setenv("TEMPORAL_ADDRESS", "")
    monkeypatch.setenv("TEMPORAL_NAMESPACE", "")
    monkeypatch.setenv("TEMPORAL_API_KEY", "")
    s = load_settings()
    assert s.temporal_address == "localhost:7233"
    assert s.temporal_namespace == "default"
    assert not s.uses_temporal_cloud
