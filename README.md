# Personal Finance Data Sanitizer

Anonymizes personal finance exports into deterministic JSON for agent analysis.

Supports:
- Starling CSV exports
- PDF statements (Trading212 / P60 / payslips)

## Install

```bash
pip install -r requirements.txt
```

`tabula-py` requires Java for table extraction. If Java is unavailable, the script still parses PDFs with `pdfplumber` text/table extraction.

## Features

- Deterministic hashing (SHA256[:8]) with user-provided salt
- Date bucketing to ISO week for privacy
- Amounts preserved as numeric values
- CSV sanitization keeps type/category fields
- PDF sanitization emits date/amount/description-hash records
- Trading212 PDFs: handles rotated pages, multi-column text, and table-style rows more robustly
- Batch mode over `raw/*.csv` and `raw/*.pdf`

## Usage

### Single-file mode

```bash
python sanitize.py INPUT.csv --output OUT.json [--salt SALT] [--hash-notes] [--dry-run]
python sanitize.py INPUT.pdf --output OUT.json [--salt SALT] [--dry-run]
```

### Batch mode

```bash
python sanitize.py [--salt SALT] [--hash-notes] [--dry-run]
```

Batch mode scans:
- `raw/*.csv`
- `raw/*.pdf`

and writes matching JSON files to `sanitised/*.json`.

The script will create `raw/` and `sanitised/` folders if they do not exist.

## Sample commands

```bash
python sanitize.py sample-starling.csv --output sample-sanitized.json --salt finance-salt-2026
python sanitize.py raw/sample-trading212.pdf --output sanitised/sample-trading212.json --salt finance-salt-2026
```

Safe for sharing with agents — no PII leakage.
