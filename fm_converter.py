#!/usr/bin/env python3
"""
FM Converter – builds fixed‑width Family Physician Integrated Care upload (FM.txt)
from the pair of source CSVs provided by Taiwan NHI.

Usage
-----
$ python fm_converter.py --long long.CSV --short short.csv [options]

The script will prompt the operator for the constant parameters that apply to
**all** rows in the output file and then generate one or more fixed‑width text
files ready for upload.

Requirements (extracted from QM_UploadFormatFM.pdf)
--------------------------------------------------
• Output encoding: default Big‑5 (override with --utf8)
• Record length: 208 bytes, 15 fields
• File name: [BRANCH][HOSP_ID][MM][NN]FM.txt
  – BRANCH, MM, NN are provided by the operator
• CASE_TYPE mapping (per screenshot):
  – numeric 1‑5,7  → “A”
  – numeric 6      → “C”
  – other / unknown → “B” (rare, operator will be warned)

The script also creates fm_converter.log with warnings about bad or
incomplete rows. Rows that cannot be converted are dropped from the output.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List
import io  # <--- Ensure this import is here

import chardet  # type: ignore
import pandas as pd

###############################################################################
# Utility helpers                                                             #
###############################################################################

RECORD_LEN = 208  # bytes
ENCODING = "utf-8"  # default output encoding
BIG5 = "cp950"  # Changed from "big5" to "cp950" for consistency and robustness

FIELD_SPECS = [
    ("SEGMENT", 1),
    ("PLAN_NO", 2),
    ("BRANCH_CODE", 1),
    ("HOSP_ID", 10),
    ("ID", 10),
    ("BIRTHDAY", 8),
    ("NAME", 12),
    ("SEX", 1),
    ("INFORM_ADDR", 120),
    ("TEL", 15),
    ("PRSN_ID", 10),
    ("CASE_TYPE", 1),
    ("CASE_DATE", 8),
    ("CLOSE_DATE", 8),
    ("CLOSE_RSN", 1),
]

CASE_TYPE_MAP = {
    1: "A",
    2: "A",
    3: "A",
    4: "A",
    5: "A",
    7: "A",
    6: "C",
}


def detect_encoding(path: Path) -> str:
    """Detect file encoding using chardet (fallback utf‑8)."""
    with path.open("rb") as fh:
        raw = fh.read(4096)  # Read a chunk to detect encoding
    res = chardet.detect(raw)
    return res["encoding"] or "utf-8"


def roc_to_gregorian(roc_date: str) -> str:
    """Convert ROC YYYYMMDD (year may be 2‑3 digits) → Gregorian YYYYMMDD."""
    roc_date = roc_date.replace("/", "").replace("-", "")
    if len(roc_date) not in (6, 7):
        raise ValueError(f"Unexpected ROC date format: {roc_date}")
    year = int(roc_date[:-4]) + 1911
    return f"{year:04d}{roc_date[-4:]}"


def pad(field: str, length: int, encoding: str = ENCODING) -> bytes:
    """Left align (byte count) and space‑pad/truncate to length."""
    encoded = field.encode(encoding, errors="ignore")
    if len(encoded) > length:
        encoded = encoded[:length]
    return encoded.ljust(length, b" ")


def _fw(value: str, width: int, align: str = "l", enc: str = ENCODING) -> bytes:
    # Ensure value is always a string to prevent 'float' object has no attribute 'encode'
    s_value = str(value)

    # Encode and handle errors by replacing unknown characters with '?'
    # This is the proper place for errors='replace'
    raw = s_value.encode(enc, errors="replace")[:width]
    pad = b" " * (width - len(raw))
    return raw + pad if align == "l" else pad + raw


def build_record(row: pd.Series, fixed: Dict[str, str], encoding: str = ENCODING) -> bytes:
    """Assemble one 208‑byte record from merged DataFrame row + fixed fields."""

    try:
        # Ensure all values from row.get() are explicitly converted to string
        birthday_roc = str(row.get("生日", "")).strip()
        first_visit = str(row.get("看診日期", "")).strip()
    except Exception as err:
        raise ValueError(f"Missing birthday or or visit date: {err}")
    _raw_case = str(row.get("個案類別", "")).strip().lstrip("'")
    case_num = int(_raw_case) if _raw_case.isdigit() else 0
    values: Dict[str, str] = {
        "SEGMENT": "A",  # all new/open cases for now
        "PLAN_NO": fixed["PLAN_NO"],
        "BRANCH_CODE": fixed["BRANCH_CODE"],
        "HOSP_ID": fixed["HOSP_ID"],
        "ID": str(row.get("身分證號", "")),  # Ensure string
        "BIRTHDAY": roc_to_gregorian(birthday_roc),
        "NAME": str(row.get("姓名", "")),  # Ensure string
        "SEX": str(row.get("身分證號", ""))[1] if str(row.get("身分證號", "")) else "",  # Ensure string
        "INFORM_ADDR": str(row.get("住址", "")),  # Ensure string
        "TEL": _clean_tel(str(row.get("電話", ""))),  # _clean_tel already does str(), but double check for robustness
        "PRSN_ID": fixed["PRSN_ID"],
        "CASE_TYPE": CASE_TYPE_MAP.get(case_num, "B"),
        "CASE_DATE": roc_to_gregorian(first_visit[:7]),  # first visit of year
        "CLOSE_DATE": "",  # not used in segment A
        "CLOSE_RSN": "",  # not used in segment A
    }

    # Build fixed‑width line

    record = b"".join([
        _fw(values["SEGMENT"], 1, enc=encoding),
        _fw(values["PLAN_NO"], 2, "r", enc=encoding),
        _fw(values["BRANCH_CODE"], 1, enc=encoding),
        _fw(values["HOSP_ID"], 10, "r", enc=encoding),
        _fw(values["ID"], 10, enc=encoding),
        _fw(values["BIRTHDAY"], 8, "r", enc=encoding),
        _fw(values["NAME"], 12, enc=encoding),
        _fw(values["SEX"], 1, enc=encoding),
        _fw(values["INFORM_ADDR"], 120, enc=encoding),
        _fw(values["TEL"], 15, enc=encoding),
        _fw(values["PRSN_ID"], 10, "r", enc=encoding),
        _fw(values["CASE_TYPE"], 1, enc=encoding),
        _fw(values["CASE_DATE"], 8, "r", enc=encoding),
        _fw(values["CLOSE_DATE"], 8, "r", enc=encoding),
        _fw(values["CLOSE_RSN"], 1, enc=encoding),
    ])
    assert len(record) == RECORD_LEN, f"Record len {len(record)} ≠ 208"
    return record


###############################################################################
# Main converter logic                                                        #
###############################################################################

def load_csv(path: Path) -> pd.DataFrame:
    """Read CSV file, attempting to decode with fallbacks and error handling."""
    encodings_to_try = [
        "utf-8-sig",  # Prioritize UTF-8 with BOM (common from Excel)
        "utf-8",  # Then plain UTF-8
        "cp950",  # Big-5 for Traditional Chinese
        "big5",  # Python's alias for big5 (often cp950)
        detect_encoding(path),  # Chardet's best guess as a fallback
        "gbk",  # Simplified Chinese
        "latin-1",  # Last resort
    ]

    try:
        with path.open("rb") as f_raw:
            raw_bytes = f_raw.read()
    except Exception as e:
        raise ValueError(f"Error reading raw bytes from {path.name}: {e}")

    attempted_encodings = set()
    for encoding in encodings_to_try:
        if not encoding or encoding in attempted_encodings:
            continue
        attempted_encodings.add(encoding)

        print(f"Trying to decode {path.name} with encoding: '{encoding}'")
        try:
            decoded_string = raw_bytes.decode(encoding, errors='replace')
            data_io = io.StringIO(decoded_string)

            df = pd.read_csv(data_io)
            # --- ADD THIS LINE ---
            df.columns = df.columns.str.strip()  # Strip whitespace from all column names
            # --- END ADDITION ---
            return df
        except UnicodeDecodeError as e:
            print(f"  Failed to decode raw bytes with '{encoding}': {e}. Trying next encoding.")
            continue
        except Exception as e:
            print(
                f"  An unexpected error occurred while parsing CSV after decoding with '{encoding}': {e}. Trying next encoding.")
            continue

    raise ValueError(f"Could not decode or parse {path.name} with any of the attempted encodings. "
                     "Please check the file's actual content for corruption.")


def _clean_id(value: str) -> str:
    """
    Strip whitespace & leading / trailing apostrophes, make uppercase.
    """
    if not isinstance(value, str):
        return ""
    return value.strip().lstrip("'").rstrip("'").upper()


def _clean_tel(value: str) -> str:
    """Ensure telephone numbers keep their leading zero if missing."""
    if value is None:
        return ""
    s = str(value).strip()
    # Remove any trailing .0 from numbers that may have been parsed as floats
    if s.endswith(".0"):
        head = s[:-2]
        if head.isdigit():
            s = head
    if s and not s.startswith("0"):
        s = "0" + s
    return s


def merge_sources(long_df, short_df):
    # Rename '身分證字號' to '身分證號' to prepare for merge
    if "身分證字號" in long_df.columns:
        long_df.rename(columns={"身分證字號": "身分證號"}, inplace=True)

    long_df["ID_CLEAN"] = long_df["身分證號"].apply(_clean_id)
    short_df["ID_CLEAN"] = short_df["身分證號"].apply(_clean_id)

    merged = pd.merge(short_df, long_df, on="ID_CLEAN", how="inner")

    if merged.empty:
        raise ValueError("No matching IDs …")

    return merged


def chunks(lst: List[pd.Series], n: int):
    """Yield successive n‑sized chunks from list."""
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def convert(
        long_path: Path,
        short_path: Path,
        fixed: Dict[str, str],
        upload_month: str,
        seq_start: int,
        out_encoding: str = ENCODING,
        outdir: Path = Path("output"),
) -> List[Path]:
    """Convert CSVs and write FM.txt file(s)."""

    long_df = load_csv(long_path)
    short_df = load_csv(short_path)
    merged = merge_sources(long_df, short_df)

    # --- NEW, CORRECTED BLOCK FOR CLEANING UP AFTER MERGE ---

    # After merging, pandas may add suffixes. We need to consolidate these.
    # We'll prioritize the columns from the "long" file (which gets suffix _y)
    # for demographics, and keep the original "short" file columns (suffix _x) where applicable.

    # 1. Handle the '身分證號' and '生日' columns specifically
    # Keep the version from the short file (which has _x suffix) and rename it back.
    if '身分證號_x' in merged.columns:
        merged.rename(columns={'身分證號_x': '身分證號'}, inplace=True)
        # Drop the redundant _y column from the long file
        if '身分證號_y' in merged.columns:
            merged.drop(columns=['身分證號_y'], inplace=True)

    if '生日_x' in merged.columns:
        merged.rename(columns={'生日_x': '生日'}, inplace=True)
        if '生日_y' in merged.columns:
            merged.drop(columns=['生日_y'], inplace=True)

    # 2. General cleanup of other suffixed columns (_x is from short, _y is from long)
    # The original keep_map was slightly incorrect. Let's make it more explicit.
    # For '姓名', '住址', '電話', we want the version from the long file (_y)
    # For '看診日期', it also comes from the long file (_y).
    rename_suffixed = {
        '姓名_y': '姓名',
        '住址_y': '住址',
        '電話_y': '電話',
        '看診日期_y': '看診日期'  # Assuming '看診日期' might also get a suffix
    }
    for old, new in rename_suffixed.items():
        if old in merged.columns:
            merged.rename(columns={old: new}, inplace=True)

    # 3. Drop all remaining columns that have suffixes, as they are now redundant.
    cols_to_drop = [c for c in merged.columns if c.endswith(('_x', '_y'))]
    merged.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    # --- END OF NEW, CORRECTED BLOCK ---

    # Now that we have a clean '身分證號' column, we can drop duplicates.
    # This line should now work correctly.
    merged.drop_duplicates(subset="身分證號", inplace=True)

    # Sort by ID for deterministic output
    merged.sort_values("身分證號", inplace=True)
    records: List[bytes] = []

    # ... (rest of the convert function remains the same)

    # --- Sample data print for debugging ---
    print("\n--- Sample of Merged and Cleaned Data ---")
    if not merged.empty:
        sample_cols = ["姓名", "住址", "身分證號", "生日"]
        for col in sample_cols:
            if col in merged.columns:
                print(f"Column '{col}':")
                for i in range(min(5, len(merged))):
                    print(f"  Row {i}: {str(merged.iloc[i][col])[:50]}")
            else:
                print(f"Column '{col}' not found in merged data.")
    print("----------------------------------------------------------\n")
    # --- End sample data print ---

    for _, row in merged.iterrows():
        try:
            rec = build_record(row, fixed, out_encoding)
            records.append(rec)
        except Exception as e:
            logging.warning(f"Skipping {row.get('身分證號')}: {e}")

    if not records:
        logging.error("No valid rows – nothing to write!")
        raise ValueError("No valid rows to write")

    outdir.mkdir(parents=True, exist_ok=True)

    written: List[Path] = []
    CHUNK_SIZE = 9999
    for idx, chunk in enumerate(chunks(records, CHUNK_SIZE), start=seq_start):
        fname = f"{fixed['BRANCH_CODE']}{fixed['HOSP_ID']}{upload_month}{idx:02d}FM.txt"
        fpath = outdir / fname
        with fpath.open("wb") as fh:
            for rec in chunk:
                fh.write(rec + b"\r\n")  # CRLF per spec
        written.append(fpath)
        print(f"Wrote {len(chunk):,} rows to {fname}")
    # --- ADDED DEBUG PRINT FOR FINAL OUTPUT ENCODING ---
    print(f"Writing output file(s) with encoding: {out_encoding}")
    # --- END DEBUG ---

    return written


def main(argv: List[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Convert Family Physician CSVs to FM.txt upload format")
    p.add_argument("--long", required=True, type=Path, help="Path to long.CSV (demographics)")
    p.add_argument("--short", required=True, type=Path, help="Path to short.csv (case meta)")
    p.add_argument("--big5", action="store_true", help="Write output in Big-5 instead of UTF-8")
    p.add_argument("--outdir", type=Path, default=Path("output"), help="Destination directory")

    args = p.parse_args(argv)
    out_encoding = BIG5 if args.big5 else ENCODING

    logging.basicConfig(
        filename="fm_converter.log",
        level=logging.INFO,
        format="%(levelname)s:%(message)s",
    )

    # === Operator prompts ====================================================
    fixed: Dict[str, str] = {}
    fixed["PLAN_NO"] = input("Enter PLAN_NO (e.g. 09): ").zfill(2)
    fixed["BRANCH_CODE"] = input("Enter BRANCH_CODE (1‑6): ")
    fixed["HOSP_ID"] = input("Enter HOSP_ID (10 digits): ").zfill(10)
    fixed["PRSN_ID"] = input("Enter PRSN_ID (10 digits physician ID): ").zfill(10)
    upload_month = input("Enter upload month MM (01‑12): ")
    seq_start = int(input("Start sequence NN (01‑99) [default 1]: ") or 1)

    convert(
        args.long,
        args.short,
        fixed,
        upload_month,
        seq_start,
        out_encoding=out_encoding,
        outdir=args.outdir,
    )


if __name__ == "__main__":
    main()
