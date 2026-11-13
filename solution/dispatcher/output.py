"""Writes final_state.csv and checks it against the provider.

Output contract (from the assignment README), sorted by campaign_id, lead_id:

    campaign_id,lead_id,final_state,attempts,last_status,last_disposition
    c_acme_feedback,fb_0001,completed,2,completed,interested
    c_acme_feedback,fb_0002,exhausted,3,no_answer,
"""
from __future__ import annotations

import csv
import io
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .workflow import LeadOutcome

COLUMNS = ("campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition")


def render_final_state(leads: Iterable[LeadOutcome]) -> str:
    """The CSV text. Lines end with "\\n" (the csv module defaults to "\\r\\n")."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(COLUMNS)
    for lead in sorted(leads, key=lambda l: (l.campaign_id, l.lead_id)):
        writer.writerow([lead.campaign_id, lead.lead_id, lead.final_state, lead.attempts,
                         lead.last_status or "", lead.last_disposition or ""])
    return buf.getvalue()


def write_final_state(leads: Iterable[LeadOutcome], path: str | Path) -> Path:
    """Write the file atomically: a temp file in the same folder, then a rename.

    If the process is killed while writing, the old file (if any) is left as it
    was, never half written."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = render_final_state(leads)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise
    return path


# --------------------------------------------------------------------------- verify
@dataclass
class VerifyReport:
    rows_ours: int
    rows_provider: int
    mismatches: list[tuple[tuple[str, str], tuple | None, tuple | None]]
    report: dict

    @property
    def ok(self) -> bool:
        return not self.mismatches and self.report.get("violation_count") == 0


def _key_row(row: dict) -> tuple[tuple[str, str], tuple]:
    return ((row["campaign_id"], row["lead_id"]),
            (row["final_state"], str(row["attempts"]), row["last_status"] or "",
             row["last_disposition"] or ""))


def verify(csv_path: str | Path, provider_url: str, client=None) -> VerifyReport:
    """Compare final_state.csv with the provider's /admin/leads, line by line."""
    import httpx

    http = client or httpx.Client(timeout=10.0)
    try:
        theirs_raw = http.get(f"{provider_url}/admin/leads").json()
        report = http.get(f"{provider_url}/admin/report").json()
    finally:
        if client is None:
            http.close()

    with open(csv_path, newline="", encoding="utf-8") as f:
        ours = dict(_key_row(r) for r in csv.DictReader(f))
    theirs = dict(_key_row({**l, "final_state": l["expected_state"]}) for l in theirs_raw)

    mismatches = [(k, ours.get(k), theirs.get(k))
                  for k in sorted(set(ours) | set(theirs)) if ours.get(k) != theirs.get(k)]
    return VerifyReport(rows_ours=len(ours), rows_provider=len(theirs),
                        mismatches=mismatches, report=report)


def print_verify(result: VerifyReport) -> None:
    r = result.report
    print(f"final_state.csv rows: {result.rows_ours}   provider leads: {result.rows_provider}")
    if result.mismatches:
        print(f"MISMATCHES: {len(result.mismatches)}")
        for key, ours, theirs in result.mismatches[:20]:
            print(f"  {key[0]},{key[1]}: ours={ours} provider={theirs}")
    else:
        print("final_state.csv matches /admin/leads: every lead")
    print(f"violation_count: {r.get('violation_count')}  {r.get('violations_by_type') or ''}")
    print(f"calls_total: {r.get('calls_total')}   max_concurrency_seen: {r.get('max_concurrency_seen')}")
    print(f"callback_lateness_minutes: {r.get('callback_lateness_minutes')}")
    print(f"webhooks: {r.get('webhooks')}")
    print("RESULT:", "PASS" if result.ok else "FAIL")
