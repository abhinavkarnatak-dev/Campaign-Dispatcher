"""Tests for final_state.csv and the verify check."""
import os

import httpx
import pytest

from dispatcher import output
from dispatcher.output import render_final_state, verify, write_final_state
from dispatcher.workflow import LeadOutcome

LEADS = [
    LeadOutcome("c_acme_feedback", "fb_0002", "exhausted", 3, "no_answer", ""),
    LeadOutcome("c_acme_renewals", "ren_0001", "dnd_blocked", 0, "", ""),
    LeadOutcome("c_acme_feedback", "fb_0001", "completed", 2, "completed", "interested"),
]

EXPECTED = (
    "campaign_id,lead_id,final_state,attempts,last_status,last_disposition\n"
    "c_acme_feedback,fb_0001,completed,2,completed,interested\n"
    "c_acme_feedback,fb_0002,exhausted,3,no_answer,\n"
    "c_acme_renewals,ren_0001,dnd_blocked,0,,\n"
)


def test_render_matches_the_contract_exactly():
    assert render_final_state(LEADS) == EXPECTED


def test_written_file_has_plain_newlines(tmp_path):
    path = write_final_state(LEADS, tmp_path / "final_state.csv")
    assert path.read_bytes() == EXPECTED.encode()
    assert b"\r" not in path.read_bytes()


def test_failed_write_keeps_the_old_file_and_leaves_no_temp(tmp_path, monkeypatch):
    path = tmp_path / "final_state.csv"
    path.write_text("old contents\n")

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(output.os, "replace", boom)
    with pytest.raises(OSError):
        write_final_state(LEADS, path)
    assert path.read_text() == "old contents\n"
    assert os.listdir(tmp_path) == ["final_state.csv"]


def _provider(leads, violations=0):
    rows = [{"campaign_id": c, "lead_id": l, "expected_state": s, "attempts": a,
             "last_status": st or None, "last_disposition": d or None}
            for c, l, s, a, st, d in leads]

    def handler(request):
        if request.url.path == "/admin/leads":
            return httpx.Response(200, json=rows)
        return httpx.Response(200, json={"violation_count": violations, "violations_by_type": {}})

    return httpx.Client(transport=httpx.MockTransport(handler))


def _as_tuples(leads):
    return [(l.campaign_id, l.lead_id, l.final_state, l.attempts, l.last_status, l.last_disposition)
            for l in leads]


def test_verify_passes_when_everything_matches(tmp_path):
    path = write_final_state(LEADS, tmp_path / "final_state.csv")
    result = verify(path, "http://p.test", client=_provider(_as_tuples(LEADS)))
    assert result.ok and result.mismatches == []


def test_verify_points_at_the_lead_that_differs(tmp_path):
    path = write_final_state(LEADS, tmp_path / "final_state.csv")
    theirs = _as_tuples(LEADS)
    theirs[0] = ("c_acme_feedback", "fb_0002", "exhausted", 2, "no_answer", "")  # attempts differ
    result = verify(path, "http://p.test", client=_provider(theirs))
    assert not result.ok
    assert [m[0] for m in result.mismatches] == [("c_acme_feedback", "fb_0002")]


def test_verify_fails_on_violations_or_missing_leads(tmp_path):
    path = write_final_state(LEADS[:2], tmp_path / "final_state.csv")
    result = verify(path, "http://p.test", client=_provider(_as_tuples(LEADS), violations=1))
    assert not result.ok
    assert [m[0] for m in result.mismatches] == [("c_acme_feedback", "fb_0001")]
