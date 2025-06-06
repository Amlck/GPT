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

import chardet  # type: ignore
import pandas as pd

###############################################################################
# Utility helpers                                                             #
###############################################################################

RECORD_LEN = 208  # bytes
BIG5 = "big5"

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
        raw = fh.read(4096)
    res = chardet.detect(raw)
    return res["encoding"] or "utf‑8"


def roc_to_gregorian(roc_date: str) -> str:
    """Convert ROC YYYYMMDD (year may be 2‑3 digits) → Gregorian YYYYMMDD."""
    roc_date = roc_date.replace("/", "").replace("-", "")
    if len(roc_date) not in (6, 7):
        raise ValueError(f"Unexpected ROC date format: {roc_date}")
    year = int(roc_date[:-4]) + 1911
    return f"{year:04d}{roc_date[-4:]}"


def pad(field: str, length: int, encoding: str = BIG5) -> bytes:
    """Left align (Big‑5 byte count) and space‑pad/truncate to length."""
    encoded = field.encode(encoding, errors="ignore")
    if len(encoded) > length:
        encoded = encoded[:length]
    return encoded.ljust(length, b" ")


def build_record(row: pd.Series, fixed: Dict[str, str], encoding: str = BIG5) -> bytes:
    """Assemble one 208‑byte record from merged DataFrame row + fixed fields."""

    values: Dict[str, str] = {
        "SEGMENT": "A",  # all new/open cases for now
        "PLAN_NO": fixed["PLAN_NO"],
        "BRANCH_CODE": fixed["BRANCH_CODE"],
        "HOSP_ID": fixed["HOSP_ID"],
        "ID": row["身分證號"],
        "BIRTHDAY": roc_to_gregorian(str(row["生日"])),
        "NAME": row["姓名"],
        "SEX": str(row["身分證號"])[1],
        "INFORM_ADDR": row["住址"],
        "TEL": str(row["電話"]),
        "PRSN_ID": fixed["PRSN_ID"],
        "CASE_TYPE": CASE_TYPE_MAP.get(int(row["個案類別"]), "B"),
        "CASE_DATE": roc_to_gregorian(str(row["看診日期"][:7])),  # first visit of year
        "CLOSE_DATE": "",  # not used in segment A
        "CLOSE_RSN": "",  # not used in segment A
    }

    # Build fixed‑width line
    parts: List[bytes] = []
    for name, size in FIELD_SPECS:
        parts.append(pad(values.get(name, ""), size, encoding))
    record = b"".join(parts)
    assert len(record) == RECORD_LEN, f"Record len {len(record)} ≠ 208"
    return record


###############################################################################
# Main converter logic                                                        #
###############################################################################

def load_csv(path: Path) -> pd.DataFrame:
    enc = detect_encoding(path)
    return pd.read_csv(path, encoding=enc)


def merge_sources(long_df: pd.DataFrame, short_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(short_df, long_df, left_on="身分證號", right_on="身分證字號", how="left")
    return merged


def chunks(lst: List[pd.Series], n: int):
    """Yield successive n‑sized chunks from list."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def convert(
    long_path: Path,
    short_path: Path,
    fixed: Dict[str, str],
    upload_month: str,
    seq_start: int,
    out_encoding: str = BIG5,
    outdir: Path = Path("output"),
) -> List[Path]:
    """Convert CSVs and write FM.txt file(s)."""

    long_df = load_csv(long_path)
    short_df = load_csv(short_path)
    merged = merge_sources(long_df, short_df)

    # Sort by ID for deterministic output
    merged.sort_values("身分證號", inplace=True)
    records: List[bytes] = []

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

    return written


def main(argv: List[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Convert Family Physician CSVs to FM.txt upload format")
    p.add_argument("--long", required=True, type=Path, help="Path to long.CSV (demographics)")
    p.add_argument("--short", required=True, type=Path, help="Path to short.csv (case meta)")
    p.add_argument("--utf8", action="store_true", help="Write output in UTF‑8 instead of Big‑5")
    p.add_argument("--outdir", type=Path, default=Path("output"), help="Destination directory")

    args = p.parse_args(argv)
    out_encoding = "utf-8" if args.utf8 else BIG5

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
