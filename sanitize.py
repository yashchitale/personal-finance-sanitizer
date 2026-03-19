#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
from pathlib import Path

import pandas as pd
import pdfplumber

try:
    import tabula
except Exception:  # pragma: no cover - optional dependency runtime failures
    tabula = None


DATE_RE = re.compile(
    r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})\b"
)
AMOUNT_RE = re.compile(r"\(?[+-]?£?\s?\d{1,3}(?:,\d{3})*(?:\.\d{2})\)?")


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
        dt = pd.to_datetime(raw_date, errors="coerce")
    if pd.isna(dt):
        raise ValueError(f"Unable to parse date: {raw_date}")

    iso = dt.isocalendar()
    return f"{int(iso.year):04d}-W{int(iso.week):02d}"


def parse_amount(raw_amount: str) -> float:
    text = str(raw_amount).strip().replace(",", "")
    is_negative = text.startswith("(") and text.endswith(")")
    text = text.replace("£", "").replace("(", "").replace(")", "").replace(" ", "")
    value = float(text)
    return -abs(value) if is_negative else value


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


def infer_pdf_source_type(path: Path) -> str:
    stem = path.stem.lower()
    if "trading212" in stem or "trading" in stem:
        return "Trading212"
    if "p60" in stem:
        return "P60"
    if "payslip" in stem or "salary" in stem:
        return "Payslip"
    return "PDF"


def build_pdf_record(
    raw_date: str,
    raw_description: str,
    raw_amount: str,
    salt: str,
    source_type: str,
    category: str | None = None,
):
    out = {
        "date_bucket": to_iso_week_bucket(raw_date),
        "description_hash": hash_value(raw_description, salt),
        "amount": parse_amount(raw_amount),
        "source_type": source_type,
    }
    if category:
        out["category"] = category
    return out


def parse_line_candidates(line: str) -> list[tuple[str, str, str]]:
    date_matches = list(DATE_RE.finditer(line))
    if not date_matches:
        return []

    candidates: list[tuple[str, str, str]] = []
    for i, date_match in enumerate(date_matches):
        start = date_match.start()
        end = date_matches[i + 1].start() if i + 1 < len(date_matches) else len(line)
        segment = line[start:end].strip()

        amount_matches = list(AMOUNT_RE.finditer(segment))
        if not amount_matches:
            continue

        raw_date = date_match.group(1)
        raw_amount = amount_matches[-1].group(0)

        segment_wo_date = segment.replace(raw_date, " ", 1)
        segment_wo_amount = segment_wo_date.replace(raw_amount, " ")
        raw_description = " ".join(segment_wo_amount.split())
        if not raw_description:
            raw_description = "transaction"

        candidates.append((raw_date, raw_description, raw_amount))

    return candidates


def words_to_lines(words: list[dict], y_tol: float = 3.0) -> list[str]:
    if not words:
        return []

    rows: list[list[dict]] = []
    for w in sorted(words, key=lambda x: (x.get("top", 0.0), x.get("x0", 0.0))):
        top = float(w.get("top", 0.0))
        if not rows:
            rows.append([w])
            continue
        prev_top = float(rows[-1][0].get("top", 0.0))
        if abs(top - prev_top) <= y_tol:
            rows[-1].append(w)
        else:
            rows.append([w])

    lines: list[str] = []
    for row in rows:
        text = " ".join(str(w.get("text", "")).strip() for w in sorted(row, key=lambda x: x.get("x0", 0.0)))
        text = " ".join(text.split())
        if text:
            lines.append(text)
    return lines


def extract_candidate_lines(page) -> list[str]:
    lines: list[str] = []

    text = page.extract_text() or ""
    lines.extend(text.splitlines())

    for angle in (0, 90, 180, 270):
        try:
            candidate_page = page if angle == 0 else page.rotate(angle)
            words = candidate_page.extract_words(use_text_flow=False, keep_blank_chars=False) or []
            lines.extend(words_to_lines(words))
        except Exception:
            continue

    deduped = []
    seen = set()
    for line in lines:
        key = " ".join(str(line).split()).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(line)

    return deduped


def extract_records_from_row_cells(row: list, source_type: str, salt: str) -> list[dict]:
    cells = [str(c).strip() for c in row if c is not None and str(c).strip()]
    if not cells:
        return []

    full_line = " ".join(cells)
    records: list[dict] = []
    for raw_date, raw_description, raw_amount in parse_line_candidates(full_line):
        category = None
        if len(cells) >= 4:
            maybe_category = cells[-1]
            if maybe_category and not DATE_RE.search(maybe_category) and not AMOUNT_RE.search(maybe_category):
                category = maybe_category
        try:
            records.append(build_pdf_record(raw_date, raw_description, raw_amount, salt, source_type, category=category))
        except Exception:
            continue

    return records


def extract_pdf_records(pdf_path: Path, salt: str) -> list[dict]:
    source_type = infer_pdf_source_type(pdf_path)
    records: list[dict] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for line in extract_candidate_lines(page):
                for raw_date, raw_description, raw_amount in parse_line_candidates(line):
                    try:
                        records.append(build_pdf_record(raw_date, raw_description, raw_amount, salt, source_type))
                    except Exception:
                        continue

            for table in page.extract_tables() or []:
                for row in table:
                    if not row:
                        continue
                    records.extend(extract_records_from_row_cells(row, source_type, salt))

    if tabula is not None:
        try:
            tabula_tables = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True)
            for table_df in tabula_tables or []:
                for _, row in table_df.fillna("").iterrows():
                    row_cells = [str(x) for x in row.tolist() if str(x).strip()]
                    records.extend(extract_records_from_row_cells(row_cells, source_type, salt))
        except Exception:
            pass

    deduped = {}
    for record in records:
        key = (
            record["date_bucket"],
            record["description_hash"],
            record["amount"],
            record.get("source_type"),
            record.get("category"),
        )
        deduped[key] = record

    return list(deduped.values())


def build_summary(records: list[dict]) -> dict:
    spends = [r for r in records if r["amount"] < 0]

    spend_by_category = {}
    for r in spends:
        cat = r.get("category") or r.get("source_type") or "UNKNOWN"
        spend_by_category[cat] = round(spend_by_category.get(cat, 0.0) + r["amount"], 2)

    return {
        "txn_count": len(records),
        "spend_txn_count": len(spends),
        "total_spend_by_category": spend_by_category,
    }


def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Sanitize Starling CSV exports and finance PDFs into anonymized JSON. "
            "Single-file mode: provide INPUT and --output. "
            "Batch mode: omit INPUT to process ./raw/*.(csv|pdf) into ./sanitised/*.json."
        )
    )
    p.add_argument("input", nargs="?", help="Path to input Starling CSV or PDF (omit to run batch mode)")
    p.add_argument("--output", "-o", help="Path to output JSON (required in single-file mode)")
    p.add_argument("--salt", default="finance-salt-2026", help="Salt used for deterministic hashing")
    p.add_argument("--dry-run", action="store_true", help="Preview output and summary without writing output file")
    p.add_argument("--hash-notes", action="store_true", help="Include notes_hash field instead of discarding Notes")
    return p.parse_args()


def process_file(input_path: Path, output_path: Path | None, salt: str, hash_notes: bool, dry_run: bool) -> None:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(input_path)
        records = sanitize_dataframe(df, salt=salt, hash_notes=hash_notes)
    elif suffix == ".pdf":
        records = extract_pdf_records(input_path, salt=salt)
    else:
        raise ValueError(f"Unsupported input type: {input_path}")

    summary = build_summary(records)

    if dry_run:
        print(f"=== {input_path} ===")
        print(json.dumps(records, indent=2))
        print("\nSummary:")
        print(json.dumps(summary, indent=2))
        return

    if output_path is None:
        raise ValueError("output_path is required when dry_run is False")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

    print(f"Wrote {len(records)} sanitized transactions to {output_path}")
    print(json.dumps(summary, indent=2))


def main():
    args = parse_args()

    if args.input:
        input_path = Path(args.input)
        if not args.dry_run and not args.output:
            raise SystemExit("In single-file mode, --output/-o is required unless --dry-run is set")
        output_path = Path(args.output) if args.output else None
        process_file(input_path, output_path, args.salt, args.hash_notes, args.dry_run)
        return

    raw_dir = Path("raw")
    sanitised_dir = Path("sanitised")

    raw_dir.mkdir(parents=True, exist_ok=True)
    sanitised_dir.mkdir(parents=True, exist_ok=True)

    input_files = sorted(list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.pdf")))
    if not input_files:
        print(f"No CSV/PDF files found in {raw_dir.resolve()} (expected raw/*.csv or raw/*.pdf)")
        return

    for input_file in input_files:
        output_path = sanitised_dir / f"{input_file.stem}.json"
        process_file(input_file, output_path, args.salt, args.hash_notes, args.dry_run)


if __name__ == "__main__":
    main()
