#!/usr/bin/env python3
"""
Synthetic data generator for Assignment A (Interaction Ledger & Reporting).

Generates two "drops" of data, the way they would arrive at Agentic Universe:

    <out>/drop_1/leads/<workflow_id>/batch_001.csv ...
    <out>/drop_1/events/events_0001.jsonl ...
    <out>/drop_2/...

The data is deliberately messy (see assignment-a-data/README.md, "What makes
this data messy"). Same --seed => byte-identical output.

Usage:
    python tools/generate_data.py --seed 42 --scale small --out data
    python tools/generate_data.py --seed 42 --scale large --out data_large

Standard library only.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

FIRST = ["Aarav", "Vivaan", "Aditya", "Ananya", "Diya", "Ishaan", "Kavya", "Meera", "Rohan",
         "Saanvi", "Arjun", "Priya", "Karthik", "Lakshmi", "Nikhil", "Pooja", "Rahul", "Sneha",
         "Varun", "Zoya", "Farhan", "Gurpreet", "Harini", "Imran", "Jyoti", "Manoj", "Nandini"]
LAST = ["Sharma", "Iyer", "Reddy", "Nair", "Patel", "Gupta", "Khan", "Singh", "Menon", "Rao",
        "Das", "Joshi", "Kulkarni", "Banerjee", "Pillai", "Verma", "Hegde", "Shetty", "Bose"]
PRODUCTS = ["Phone", "Laptop", "Tablet", "Smartwatch", "Earbuds"]
LANGS = ["en", "hi", "ta", "kn", "te", "mr"]

DISPOSITIONS = {
    # code: (is_terminal, is_success, weight)
    "motor_renewal": {
        "renewed": (True, True, 18),
        "payment_link_sent": (False, False, 14),
        "callback_requested": (False, False, 22),
        "not_interested": (True, False, 14),
        "wrong_number": (True, False, 5),
        "already_renewed_elsewhere": (True, False, 6),
        "language_barrier": (False, False, 6),
        "dnd": (True, False, 3),
    },
    "policy_feedback": {
        "feedback_captured": (True, True, 30),
        "partial_feedback": (False, False, 15),
        "callback_requested": (False, False, 20),
        "refused": (True, False, 12),
        "wrong_number": (True, False, 5),
        "dnd": (True, False, 3),
    },
}

SCALES = {"small": 1000, "large": 70000}  # leads per workflow


class Gen:
    def __init__(self, seed: int, leads_per_wf: int):
        self.r = random.Random(seed)
        self.n = leads_per_wf
        self.used_phones: set[str] = set()
        self.event_seq = 0
        self.call_seq = 0

    # ---------- primitives ----------
    def phone10(self) -> str:
        while True:
            p = str(self.r.choice("6789")) + "".join(str(self.r.randint(0, 9)) for _ in range(9))
            if p not in self.used_phones:
                self.used_phones.add(p)
                return p

    def raw_phone(self, p: str) -> str:
        f = self.r.random()
        if f < 0.30:
            return p
        if f < 0.50:
            return f"+91 {p[:5]} {p[5:]}"
        if f < 0.65:
            return f"0{p}"
        if f < 0.78:
            return f"91-{p[:5]}-{p[5:]}"
        if f < 0.88:
            return f"(+91) {p}"
        return f"+91{p}"

    def bad_phone(self) -> str:
        return self.r.choice([
            "".join(str(self.r.randint(0, 9)) for _ in range(9)),            # 9 digits
            "5" + "".join(str(self.r.randint(0, 9)) for _ in range(9)),      # bad prefix
            f"022-{self.r.randint(2000, 2999)}-{self.r.randint(1000, 9999)}",  # landline
            "N/A",
            "+1 415 555 0134",
        ])

    def fmt_date(self, d: datetime) -> str:
        f = self.r.random()
        if f < 0.45:
            return d.strftime("%Y-%m-%d")
        if f < 0.75:
            return d.strftime("%d/%m/%Y")
        if f < 0.88:
            return d.strftime("%d-%m-%Y")
        return d.strftime("%d-%b-%Y")

    def fmt_amount(self, a: float) -> str:
        f = self.r.random()
        if f < 0.45:
            return f"{a:.2f}" if self.r.random() < 0.5 else str(int(a))
        if f < 0.70:
            return f"₹{int(a):,}"
        if f < 0.85:
            return f"Rs. {int(a)}"
        return f"INR {a:.1f}"

    def fmt_lang(self, l: str) -> str:
        f = self.r.random()
        if f < 0.55:
            return l
        if f < 0.70:
            return l.upper()
        if f < 0.80:
            return f" {l} "
        return ""

    def fmt_ts(self, t: datetime) -> str:
        """t is an aware IST datetime."""
        f = self.r.random()
        if f < 0.45:
            return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if f < 0.80:
            return t.strftime("%Y-%m-%dT%H:%M:%S+05:30")
        return t.strftime("%Y-%m-%d %H:%M:%S")  # naive => IST per spec

    def eid(self) -> str:
        self.event_seq += 1
        return f"evt_{self.event_seq:08d}"

    def cid(self) -> str:
        self.call_seq += 1
        return f"call_{self.call_seq:08d}"

    # ---------- leads ----------
    def make_leads(self, wf: str):
        leads = []
        for i in range(self.n):
            p = self.phone10()
            lead = {
                "phone": p,
                "customer_name": f"{self.r.choice(FIRST)} {self.r.choice(LAST)}",
                "language": self.r.choice(LANGS),
            }
            if wf == "motor_renewal":
                lead["ref"] = f"MOT/{2026}/{100000 + i:06d}"
                lead["date"] = datetime(2026, 10, 1) + timedelta(days=self.r.randint(5, 90))
                lead["amount"] = round(self.r.uniform(4000, 38000), 2)
            else:
                lead["ref"] = f"ORD-{700000 + i}"
                lead["date"] = datetime(2026, 9, 1) + timedelta(days=self.r.randint(0, 29))
                lead["product"] = self.r.choice(PRODUCTS)
            # dirt flags
            x = self.r.random()
            lead["defect"] = None
            if x < 0.02:
                lead["defect"] = "bad_phone"
            elif x < 0.035:
                lead["defect"] = "no_name"
            elif x < 0.045:
                lead["defect"] = "no_ref"
            elif x < 0.050:
                lead["defect"] = "no_phone"
            leads.append(lead)
        return leads

    def lead_row(self, wf: str, lead: dict, alias_headers: bool) -> dict:
        phone = self.raw_phone(lead["phone"])
        name = lead["customer_name"]
        ref = lead["ref"]
        if lead["defect"] == "bad_phone":
            phone = self.bad_phone()
        elif lead["defect"] == "no_name":
            name = self.r.choice(["", "   "])
        elif lead["defect"] == "no_ref":
            ref = ""
        elif lead["defect"] == "no_phone":
            phone = ""

        date_s = self.fmt_date(lead["date"])
        if self.r.random() < 0.02:
            date_s = self.r.choice(["31/02/2026", "soon", "2026-13-01", "TBD"])
        lang = self.fmt_lang(lead["language"])
        if self.r.random() < 0.03:
            lang = self.r.choice(["Hindi", "english", "tamil"])

        if wf == "motor_renewal":
            amount = self.fmt_amount(lead["amount"])
            if self.r.random() < 0.02:
                amount = self.r.choice(["N/A", "-", "twelve thousand"])
            if alias_headers:
                return {"Language": lang, "Insured Name": name, "Policy No": ref,
                        "Mobile No": phone, "Expiry Date": date_s, "Premium": amount,
                        "agent_notes": self.r.choice(["", "VIP", "called before", ""])}
            return {"customer_name": name, "phone": phone, "policy_number": ref,
                    "policy_expiry": date_s, "premium_amount": amount, "preferred_language": lang}
        else:
            if alias_headers:
                return {"Order ID": ref, "Name": name, "Contact": phone, "Product": lead["product"],
                        "Order Date": date_s, "Language": lang, "agent_notes": ""}
            return {"customer_name": name, "phone": phone, "order_id": ref,
                    "product": lead["product"], "order_date": date_s, "preferred_language": lang}

    # ---------- calls / events ----------
    def make_calls(self, wf: str, lead_phone: str, phone_is_valid: bool):
        """Returns list of (event_dict, is_override) for one lead."""
        events = []
        codes = list(DISPOSITIONS[wf].keys())
        weights = [DISPOSITIONS[wf][c][2] for c in codes]
        n_calls = 0 if self.r.random() < 0.25 else self.r.randint(1, 4)
        day0 = datetime(2026, 10, 1, tzinfo=IST)
        t = day0 + timedelta(days=self.r.randint(0, 9), hours=self.r.randint(9, 19),
                             minutes=self.r.randint(0, 59), seconds=self.r.randint(0, 59))
        for _ in range(n_calls):
            call = self.cid()

            def ev(etype, when, payload=None, include_phone=True):
                e = {"event_id": self.eid(), "event_type": etype, "workflow_id": wf,
                     "call_id": call, "occurred_at": self.fmt_ts(when)}
                if include_phone:
                    e["phone"] = self.raw_phone(lead_phone) if phone_is_valid else lead_phone
                e["payload"] = payload or {}
                return e

            events.append(ev("call.initiated", t))
            o = self.r.random()
            terminal = False
            if o < 0.12:
                events.append(ev("call.telephony_failed", t + timedelta(seconds=self.r.randint(1, 8)),
                                 {"reason": self.r.choice(["carrier_rejected", "sip_503", "invalid_route"])}))
            elif o < 0.42:
                events.append(ev("call.ended", t + timedelta(seconds=self.r.randint(25, 45)),
                                 {"duration_sec": 0, "end_reason": self.r.choice(["no_answer", "busy"])}))
            else:
                conn = t + timedelta(seconds=self.r.randint(5, 25))
                dur = self.r.randint(20, 400)
                end = conn + timedelta(seconds=dur)
                events.append(ev("call.connected", conn))
                events.append(ev("call.ended", end, {"duration_sec": dur, "end_reason": "completed"}))
                if self.r.random() < 0.97:
                    code = self.r.choices(codes, weights)[0]
                    sent = code
                    if self.r.random() < 0.01:
                        sent = self.r.choice(["RENEWD", "call_back", "unknown"])
                    events.append(ev("disposition.set", end + timedelta(seconds=self.r.randint(5, 90)),
                                     {"disposition_code": sent}))
                    eff = code if sent in DISPOSITIONS[wf] else None
                    if self.r.random() < 0.05:
                        newc = self.r.choice([c for c in codes if c != code])
                        when = end + timedelta(hours=self.r.randint(1, 72))
                        events.append(ev("disposition.override", when,
                                         {"disposition_code": newc, "overridden_by": f"supervisor_{self.r.randint(1, 9)}",
                                          "reason": self.r.choice(["qa_review", "customer_complaint", "misclassified"])},
                                         include_phone=False))
                        eff = newc
                    if eff and DISPOSITIONS[wf][eff][0]:
                        terminal = True
            if terminal:
                break
            t = t + timedelta(hours=self.r.randint(3, 70), minutes=self.r.randint(0, 59))
        return events


def write_csv(path, rows, header):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            if row is None:
                f.write("," * (len(header) - 1) + "\r\n")  # blank row
            else:
                w.writerow(row)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--scale", choices=SCALES.keys(), default="small")
    ap.add_argument("--out", default="data")
    args = ap.parse_args()

    g = Gen(args.seed, SCALES[args.scale])
    r = g.r
    all_events = []  # (drop, event_json_line)
    batch_plan = [("drop_1", "batch_001"), ("drop_1", "batch_002"), ("drop_2", "batch_003")]

    for wf in ["motor_renewal", "policy_feedback"]:
        leads = g.make_leads(wf)
        batches = {b: [] for _, b in batch_plan}
        for lead in leads:
            x = r.random()
            b = "batch_001" if x < 0.40 else ("batch_002" if x < 0.75 else "batch_003")
            lead["batch"] = b
            batches[b].append(("row", lead))
        # duplicates: re-emit ~3% of leads in same or a later batch
        order = [b for _, b in batch_plan]
        for lead in leads:
            if r.random() < 0.03 and lead["defect"] is None:
                later = order[order.index(lead["batch"]):]
                tgt = r.choice(later)
                dup = dict(lead)
                if "amount" in dup:
                    dup["amount"] = round(dup["amount"] * r.uniform(0.95, 1.10), 2)
                if r.random() < 0.3:
                    dup["language"] = r.choice(LANGS)
                batches[tgt].append(("row", dup))
        for drop, b in batch_plan:
            rows = batches[b]
            r.shuffle(rows)
            alias = (b == "batch_003")
            out_rows = []
            for _, lead in rows:
                out_rows.append(g.lead_row(wf, lead, alias))
                if r.random() < 0.004:
                    out_rows.append(None)
            header = list(out_rows[0].keys()) if out_rows[0] else list(out_rows[1].keys())
            write_csv(os.path.join(args.out, drop, "leads", wf, f"{b}.csv"), out_rows, header)

        # events
        for lead in leads:
            valid_phone = lead["defect"] not in ("bad_phone", "no_phone")
            if lead["defect"] == "no_phone":
                continue
            phone = lead["phone"] if valid_phone else g.bad_phone()
            if lead["defect"] in ("no_name", "no_ref") and r.random() < 0.5:
                pass  # rejected lead, still gets calls -> orphan events
            for e in g.make_calls(wf, phone, valid_phone):
                all_events.append(e)
        # pure orphans: calls to numbers that were never uploaded
        for _ in range(max(3, g.n // 100)):
            for e in g.make_calls(wf, g.phone10(), True):
                all_events.append(e)

    # assign events to drops by time (overrides may land later), add duplicates
    cutoff = datetime(2026, 10, 7, tzinfo=IST)

    def parse_any(s):
        if s.endswith("Z"):
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if "T" in s:
            return datetime.fromisoformat(s)
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)

    drops = {"drop_1": [], "drop_2": []}
    for e in all_events:
        d = "drop_1" if parse_any(e["occurred_at"]) < cutoff else "drop_2"
        line = json.dumps(e, ensure_ascii=False)
        drops[d].append(line)
        if r.random() < 0.03:
            # duplicate delivery; sometimes crosses into the next drop
            dd = d if r.random() < 0.7 else "drop_2"
            drops[dd].append(line)

    for d, lines in drops.items():
        r.shuffle(lines)
        # malformed lines
        n_bad = max(2, len(lines) // 1000)
        for _ in range(n_bad):
            i = r.randrange(len(lines))
            lines.insert(i, lines[i][: r.randint(10, 40)])
        per_file = 5000
        start = 1 if d == "drop_1" else 1001
        os.makedirs(os.path.join(args.out, d, "events"), exist_ok=True)
        for k in range(0, len(lines), per_file):
            fn = os.path.join(args.out, d, "events", f"events_{start + k // per_file:04d}.jsonl")
            with open(fn, "w", encoding="utf-8") as f:
                f.write("\n".join(lines[k:k + per_file]) + "\n")

    print(f"Generated seed={args.seed} scale={args.scale} into {args.out}/")
    for d in drops:
        print(f"  {d}: {len(drops[d])} event lines")


if __name__ == "__main__":
    main()
