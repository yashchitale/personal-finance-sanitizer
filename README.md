# Personal Finance Data Sanitizer

Anonymizes Starling Bank CSV exports into deterministic JSON for agent analysis.

## Quick Start (Windows)

1. **Install Python 3.10+** + pandas:  
   ```
   pip install pandas
   ```

2. **Run**:
   ```
   python sanitize.py sample-starling.csv --output sanitized.json --salt my-salt-123
   ```

## Features
- Hashes counter-party/reference deterministically (SHA256[:8])
- Buckets dates to ISO week (privacy)
- Numeric amounts preserved
- Category/type preserved verbatim
- Optional notes hashing

## Sample
```
python sanitize.py sample-starling.csv --output out.json --salt finance-salt-2026
```

Output: `out.json` with anonymized txns + summary stats.

## Full Usage
Single-file mode:
```
python sanitize.py INPUT.csv --output OUT.json [--salt SALT] [--hash-notes] [--dry-run]
```

Batch mode (no input arg):
```
python sanitize.py [--salt SALT] [--hash-notes] [--dry-run]
```
This scans `raw/*.csv` and writes matching files to `sanitised/*.json`.
The script will create `raw/` and `sanitised/` folders if they do not exist.

Safe for sharing with agents — no PII leakage.
