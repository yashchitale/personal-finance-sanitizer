#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def hash_value(value: str | None, salt: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return None
    payload = f"{salt}{text.lower()}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:8]


def to_iso_week_bucket(raw_date: str) -> str:
    dt = pd.to_datetime(raw_date, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        # fallback without dayfirst assumption
        dt = pd.to_datetime(raw_date, errors="coerce")
    if pd.isna(dt):
        raise ValueError(f"Unable to parse date: {raw_date}")

    iso = dt.isocalendar()
    return f"{int(iso.year):04d}-W{int(iso.week):02d}"


def sanitize_dataframe(df: pd.DataFrame, salt: str, hash_notes: bool = False):
    required = [
        "Date",
        "Counter Party",
        "Reference",
        "Type",
        "Amount (GBP)",
        "Spending Category",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    df = df.copy()
    df["Amount (GBP)"] = pd.to_numeric(df["Amount (GBP)"], errors="raise")

    records = []
    for _, row in df.iterrows():
        out = {
            "date_bucket": to_iso_week_bucket(row["Date"]),
            "counter_party_hash": hash_value(row["Counter Party"], salt),
            "reference_hash": hash_value(row["Reference"], salt),
            "type": str(row["Type"]).strip(),
            "amount": float(row["Amount (GBP)"]),
            "category": str(row["Spending Category"]).strip(),
        }

        if hash_notes and "Notes" in row:
            out["notes_hash"] = hash_value(row["Notes"], salt)

        records.append(out)

    return records


def build_summary(records: list[dict]) -> dict:
    spends = [r for r in records if r["amount"] < 0]

    spend_by_category = {}
    for r in spends:
        cat = r.get("category") or "UNKNOWN"
        spend_by_category[cat] = round(spend_by_category.get(cat, 0.0) + r["amount"], 2)

    return {
        "txn_count": len(records),
        "spend_txn_count": len(spends),
        "total_spend_by_category": spend_by_category,
    }


def parse_args():
    p = argparse.ArgumentParser(description="Sanitize Starling CSV exports into anonymized JSON.")
    p.add_argument("input", help="Path to input Starling CSV")
    p.add_argument("--output", "-o", required=True, help="Path to output JSON")
    p.add_argument("--salt", default="finance-salt-2026", help="Salt used for deterministic hashing")
    p.add_argument("--dry-run", action="store_true", help="Preview output and summary without writing output file")
    p.add_argument("--hash-notes", action="store_true", help="Include notes_hash field instead of discarding Notes")
    return p.parse_args()


def main():
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    df = pd.read_csv(input_path)
    records = sanitize_dataframe(df, salt=args.salt, hash_notes=args.hash_notes)
    summary = build_summary(records)

    if args.dry_run:
        print(json.dumps(records, indent=2))
        print("\nSummary:")
        print(json.dumps(summary, indent=2))
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

    print(f"Wrote {len(records)} sanitized transactions to {output_path}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
