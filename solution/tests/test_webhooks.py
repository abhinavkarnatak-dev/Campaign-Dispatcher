"""Tests for the webhook receiver, without starting a server."""
import hashlib
import hmac
import json

from fastapi.testclient import TestClient

from dispatcher.clock import SimClock
from dispatcher.main import workflow_id_for
from dispatcher.webhooks import create_app, valid_signature

SECRET = "whsec_test"
BODY = {
    "event": "call.completed", "call_sid": "CA1", "request_id": "c1:l1:d1",
    "tenant_id": "t1", "campaign_id": "c1", "lead_id": "l1", "phone": "+919000000001",
    "status": "completed", "disposition": "interested", "callback_at": None,
    "started_at": "2026-11-12T17:31:00+05:30", "ended_at": "2026-11-12T17:35:00+05:30",
    "duration_sec": 200,
}


def sign(raw: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


def make_client(fail: bool = False, status=None):
    received = []

    async def send_signal(result):
        if fail:
            raise ConnectionError("temporal down")
        received.append(result)

    async def get_status():
        return status or {"finished": 3}

    return TestClient(create_app(SECRET, send_signal, get_status)), received


def post(client, raw: bytes, signature: str | None):
    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["X-AU-Signature"] = signature
    return client.post("/webhooks/telephony", content=raw, headers=headers)


def test_valid_webhook_is_signalled():
    client, received = make_client()
    raw = json.dumps(BODY).encode()
    resp = post(client, raw, sign(raw))
    assert resp.status_code == 200
    assert len(received) == 1
    assert received[0].call_sid == "CA1" and received[0].disposition == "interested"


def test_wrong_signature_is_rejected():
    client, received = make_client()
    raw = json.dumps(BODY).encode()
    assert post(client, raw, sign(raw, "other_secret")).status_code == 401
    assert received == []


def test_missing_signature_is_rejected():
    client, received = make_client()
    assert post(client, json.dumps(BODY).encode(), None).status_code == 401
    assert received == []


def test_signature_is_over_the_exact_raw_bytes():
    # Same JSON, different spacing: the signature for one does not fit the other.
    client, received = make_client()
    raw = json.dumps(BODY).encode()
    reformatted = json.dumps(BODY, indent=2).encode()
    assert post(client, reformatted, sign(raw)).status_code == 401
    assert received == []


def test_unreadable_body_is_400():
    client, received = make_client()
    raw = b"not json"
    assert post(client, raw, sign(raw)).status_code == 400
    raw = json.dumps({"call_sid": "CA1"}).encode()  # missing fields
    assert post(client, raw, sign(raw)).status_code == 400
    assert received == []


def test_temporal_unreachable_is_503_so_provider_retries():
    client, _ = make_client(fail=True)
    raw = json.dumps(BODY).encode()
    assert post(client, raw, sign(raw)).status_code == 503


def test_duplicate_deliveries_are_all_accepted():
    # The receiver passes every copy on; the workflow ignores the repeats by call_sid.
    client, received = make_client()
    raw = json.dumps(BODY).encode()
    assert post(client, raw, sign(raw)).status_code == 200
    assert post(client, raw, sign(raw)).status_code == 200
    assert len(received) == 2


def test_status_endpoint():
    client, _ = make_client(status={"finished": 7, "leads_total": 200})
    assert client.get("/status").json() == {"finished": 7, "leads_total": 200}
    assert client.get("/health").json() == {"ok": True}


def test_valid_signature_helper():
    raw = b"{}"
    assert valid_signature(SECRET, raw, sign(raw))
    assert valid_signature(SECRET, raw, sign(raw).upper())
    assert not valid_signature(SECRET, raw, "")
    assert not valid_signature(SECRET, raw, None)


def test_workflow_id_is_stable_per_provider_run():
    a = SimClock("2026-11-12T17:30:00+05:30", 1794397432.60, 600)
    same_run = SimClock("2026-11-12T17:30:00+05:30", 1794397432.60, 600)
    new_run = SimClock("2026-11-12T17:30:00+05:30", 1794399000.00, 600)
    assert workflow_id_for(a) == workflow_id_for(same_run) == "campaign-dispatch-1794397432"
    assert workflow_id_for(a) != workflow_id_for(new_run)
