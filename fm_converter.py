#!/usr/bin/env python3
"""
FM Converter – builds fixed‑width Family Physician Integrated Care upload (FM.txt)
from the pair of source CSVs provided by Taiwan NHI.
"""
# ... (imports and other functions remain the same) ...
import argparse
import logging
from pathlib import Path
from typing import Dict, List
import io

import chardet  # type: ignore
import pandas as pd

###############################################################################
# Utility helpers                                                             #
###############################################################################

RECORD_LEN = 208
ENCODING = "utf-8"
BIG5 = "cp950"

# ... (FIELD_SPECS, CASE_TYPE_MAP, detect_encoding, roc_to_gregorian, pad, _fw remain the same) ...
# ...
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
    1: "C",
    2: "C",
    3: "C",
    4: "C",
    5: "C",
    7: "C",
    6: "A",
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
    s_value = str(value)
    raw = s_value.encode(enc, errors="replace")[:width]
    pad_bytes = b" " * (width - len(raw))
    return raw + pad_bytes if align == "l" else pad_bytes + raw


# --- MODIFIED: build_record signature and logic ---
def build_record(
        row: pd.Series,
        fixed: Dict[str, str],
        start_date: str,
        end_date: str,
        segment_type: str,
        close_reason: str,
        encoding: str = ENCODING
) -> bytes:
    """Assemble one 208-byte record from merged DataFrame row + fixed fields."""
    birthday_roc = str(row.get("生日", "")).strip()
    if not birthday_roc:
        raise ValueError("Missing birthday")

    _raw_case = str(row.get("個案類別", "")).strip().lstrip("'")
    case_num = int(_raw_case) if _raw_case.isdigit() else 0

    formatted_start_date = start_date.replace("/", "").replace("-", "")

    # Use end_date only if it's a closed case, otherwise it's blank
    formatted_end_date = end_date.replace("/", "").replace("-", "") if segment_type == "B" else ""
    # Use close_reason only if it's a closed case
    final_close_reason = close_reason if segment_type == "B" else ""

    values: Dict[str, str] = {
        "SEGMENT": segment_type,
        "PLAN_NO": fixed["PLAN_NO"],
        "BRANCH_CODE": fixed["BRANCH_CODE"],
        "HOSP_ID": fixed["HOSP_ID"],
        "ID": row.get("身分證號", ""),
        "BIRTHDAY": roc_to_gregorian(birthday_roc),
        "NAME": row.get("姓名", ""),
        "SEX": str(row.get("身分證號", ""))[1] if row.get("身分證號", "") else "",
        "INFORM_ADDR": row.get("住址", ""),
        "TEL": _clean_tel(row.get("電話", "")),
        "PRSN_ID": fixed["PRSN_ID"],
        "CASE_TYPE": CASE_TYPE_MAP.get(case_num, "B"),
        "CASE_DATE": formatted_start_date,
        "CLOSE_DATE": formatted_end_date,
        "CLOSE_RSN": final_close_reason,
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

# ... (load_csv, _clean_id, _clean_tel, merge_sources, chunks functions remain the same) ...
# ...
def load_csv(path: Path) -> pd.DataFrame:
    """Read CSV file, attempting to decode with fallbacks and error handling."""
    encodings_to_try = [
        "utf-8-sig",
        "utf-8",
        "cp950",
        "big5",
        detect_encoding(path),
        "gbk",
        "latin-1",
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
            df.columns = df.columns.str.strip()
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
    if s.endswith(".0"):
        head = s[:-2]
        if head.isdigit():
            s = head
    if s and not s.startswith("0"):
        s = "0" + s
    return s


def merge_sources(long_df, short_df):
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


# --- MODIFIED: convert function signature ---
def convert(
        long_path: Path,
        short_path: Path,
        fixed: Dict[str, str],
        upload_month: str,
        start_date: str,
        end_date: str,
        segment_type: str,
        close_reason: str,
        seq_start: int,
        out_encoding: str = ENCODING,
        outdir: Path = Path("output"),
) -> List[Path]:
    """Convert CSVs and write FM.txt file(s)."""

    long_df = load_csv(long_path)
    short_df = load_csv(short_path)
    merged = merge_sources(long_df, short_df)

    # Cleanup merged columns
    rename_suffixed = {
        '姓名_y': '姓名', '住址_y': '住址', '電話_y': '電話',
        '看診日期_y': '看診日期', '身分證號_x': '身分證號', '生日_x': '生日',
        '個案類別_x': '個案類別',  # <--- ADD THIS LINE
    }
    for old, new in rename_suffixed.items():
        if old in merged.columns:
            merged.rename(columns={old: new}, inplace=True)

    # Now this line will not delete the renamed '個案類別' column
    cols_to_drop = [c for c in merged.columns if c.endswith(('_x', '_y'))]
    merged.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    merged.drop_duplicates(subset="身分證號", inplace=True)
    merged.sort_values("身分證號", inplace=True)
    records: List[bytes] = []

    for _, row in merged.iterrows():
        try:
            # --- MODIFIED: Pass all new parameters to build_record ---
            rec = build_record(
                row, fixed, start_date, end_date, segment_type, close_reason, out_encoding
            )
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
    print(f"Writing output file(s) with encoding: {out_encoding}")

    return written


# --- MODIFIED: main function for CLI to handle new logic ---
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

    # CLI-specific logic for use case
    segment_type = ""
    while segment_type.upper() not in ["A", "B"]:
        segment_type = input("Enter Use Case (A: New/Open, B: Closed): ").upper()

    start_date = input("Enter Case Start Date (YYYYMMDD): ")
    end_date = ""
    close_reason = ""
    if segment_type == "B":
        end_date = input("Enter Case End Date (YYYYMMDD): ")
        close_reason = input("Enter Close Reason (1-3): ")

    convert(
        long_path=args.long,
        short_path=args.short,
        fixed=fixed,
        upload_month=upload_month,
        start_date=start_date,
        end_date=end_date,
        segment_type=segment_type,
        close_reason=close_reason,
        seq_start=seq_start,
        out_encoding=out_encoding,
        outdir=args.outdir,
    )


if __name__ == "__main__":
    main()