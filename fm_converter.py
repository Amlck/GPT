#!/usr/bin/env python3
"""
FM Converter – builds fixed‑width Family Physician Integrated Care upload (FM.txt)
from the pair of source CSVs provided by Taiwan NHI.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List
import io

import chardet  # type: ignore
import pandas as pd

# --- Constants and Field Specs (Unchanged) ---
RECORD_LEN = 208
ENCODING = "utf-8"
BIG5 = "cp950"

FIELD_SPECS = [
    ("SEGMENT", 1), ("PLAN_NO", 2), ("BRANCH_CODE", 1), ("HOSP_ID", 10),
    ("ID", 10), ("BIRTHDAY", 8), ("NAME", 12), ("SEX", 1), ("INFORM_ADDR", 120),
    ("TEL", 15), ("PRSN_ID", 10), ("CASE_TYPE", 1), ("CASE_DATE", 8),
    ("CLOSE_DATE", 8), ("CLOSE_RSN", 1),
]

CASE_TYPE_MAP = {
    1: "A", 2: "A", 3: "A", 4: "A", 5: "A", 7: "A", 6: "C",
}


# --- Utility Helpers (Unchanged) ---
def detect_encoding(path: Path) -> str:
    with path.open("rb") as fh:
        raw = fh.read(4096)
    res = chardet.detect(raw)
    return res["encoding"] or "utf-8"


def roc_to_gregorian(roc_date: str) -> str:
    roc_date = roc_date.replace("/", "").replace("-", "")
    if len(roc_date) not in (6, 7):
        raise ValueError(f"Unexpected ROC date format: {roc_date}")
    year = int(roc_date[:-4]) + 1911
    return f"{year:04d}{roc_date[-4:]}"


def _fw(value: str, width: int, align: str = "l", enc: str = ENCODING) -> bytes:
    s_value = str(value)
    raw = s_value.encode(enc, errors="replace")[:width]
    pad_bytes = b" " * (width - len(raw))
    return raw + pad_bytes if align == "l" else pad_bytes + raw


def _map_sex(id_number: str) -> str:
    if not isinstance(id_number, str) or len(id_number) < 2: return ""
    gender_digit = id_number[1]
    if gender_digit in ('1', '8'): return '1'
    if gender_digit in ('2', '9'): return '2'
    logging.warning(f"Unexpected gender digit '{gender_digit}' in ID {id_number}. Leaving SEX field blank.")
    return ""


def _clean_id(value: str) -> str:
    if not isinstance(value, str): return ""
    return value.strip().lstrip("'").rstrip("'").upper()


def _clean_tel(value: str) -> str:
    if value is None: return ""
    s = str(value).strip()
    if s.endswith(".0"):
        head = s[:-2]
        if head.isdigit(): s = head
    if s and not s.startswith("0"): s = "0" + s
    return s


def build_record(row: pd.Series, fixed: Dict[str, str], start_date: str, end_date: str, segment_type: str,
                 close_reason: str, encoding: str = ENCODING) -> bytes:
    birthday_roc = str(row.get("生日", "")).strip()
    if not birthday_roc: raise ValueError("Missing birthday")
    _raw_case = str(row.get("個案類別", "")).strip().lstrip("'")
    case_num = int(_raw_case) if _raw_case.isdigit() else 0
    formatted_start_date = start_date.replace("/", "").replace("-", "")
    formatted_end_date = end_date.replace("/", "").replace("-", "") if segment_type == "B" else ""
    final_close_reason = close_reason if segment_type == "B" else ""
    id_number = row.get("身分證號", "")
    values: Dict[str, str] = {
        "SEGMENT": segment_type, "PLAN_NO": fixed["PLAN_NO"], "BRANCH_CODE": fixed["BRANCH_CODE"],
        "HOSP_ID": fixed["HOSP_ID"], "ID": id_number, "BIRTHDAY": roc_to_gregorian(birthday_roc),
        "NAME": row.get("姓名", ""), "SEX": _map_sex(id_number), "INFORM_ADDR": row.get("住址", ""),
        "TEL": _clean_tel(row.get("電話", "")), "PRSN_ID": fixed["PRSN_ID"],
        "CASE_TYPE": CASE_TYPE_MAP.get(case_num, "B"),
        "CASE_DATE": formatted_start_date, "CLOSE_DATE": formatted_end_date, "CLOSE_RSN": final_close_reason,
    }
    record = b"".join([
        _fw(values["SEGMENT"], 1, enc=encoding), _fw(values["PLAN_NO"], 2, "r", enc=encoding),
        _fw(values["BRANCH_CODE"], 1, enc=encoding), _fw(values["HOSP_ID"], 10, "r", enc=encoding),
        _fw(values["ID"], 10, enc=encoding), _fw(values["BIRTHDAY"], 8, "r", enc=encoding),
        _fw(values["NAME"], 12, enc=encoding), _fw(values["SEX"], 1, enc=encoding),
        _fw(values["INFORM_ADDR"], 120, enc=encoding), _fw(values["TEL"], 15, enc=encoding),
        _fw(values["PRSN_ID"], 10, "r", enc=encoding), _fw(values["CASE_TYPE"], 1, enc=encoding),
        _fw(values["CASE_DATE"], 8, "r", enc=encoding), _fw(values["CLOSE_DATE"], 8, "r", enc=encoding),
        _fw(values["CLOSE_RSN"], 1, enc=encoding),
    ])
    assert len(record) == RECORD_LEN, f"Record len {len(record)} ≠ 208"
    return record


def chunks(lst: List, n: int):
    for i in range(0, len(lst), n): yield lst[i: i + n]


# --- Core Logic ---
def load_csv(path: Path) -> pd.DataFrame:
    # ... (Unchanged)
    encodings_to_try = ["utf-8-sig", "utf-8", "cp950", "big5", detect_encoding(path)]
    try:
        with path.open("rb") as f_raw:
            raw_bytes = f_raw.read()
    except Exception as e:
        raise ValueError(f"Error reading raw bytes from {path.name}: {e}")
    for encoding in encodings_to_try:
        try:
            decoded_string = raw_bytes.decode(encoding, errors='replace')
            df = pd.read_csv(io.StringIO(decoded_string))
            df.columns = df.columns.str.strip()
            return df
        except Exception:
            continue
    raise ValueError(f"Could not decode or parse {path.name}.")


def merge_sources(long_df, short_df):
    long_df["ID_CLEAN"] = long_df["身分證號"].apply(_clean_id)
    short_df["ID_CLEAN"] = short_df["身分證號"].apply(_clean_id)
    merged = pd.merge(short_df, long_df, on="ID_CLEAN", how="inner")
    if merged.empty: raise ValueError("No matching IDs…")
    return merged


def _get_eligible_candidates(long_df: pd.DataFrame, short_df: pd.DataFrame) -> pd.DataFrame:
    """Performs all initial filtering to find theoretically eligible B-class cases."""
    print("Filtering for eligible, unmatched patients...")
    required_cols = ['身分證號', '姓名', '生日', '住址', '電話']
    if not all(col in long_df.columns for col in required_cols):
        raise ValueError("Long CSV is missing one or more required columns: " + str(required_cols))
    if '身分證號' not in short_df.columns:
        raise ValueError("Short CSV is missing the '身分證號' column.")

    for df in [long_df, short_df]:
        df['身分證號'] = df['身分證號'].astype(str).str.strip()
        df.dropna(subset=['身分證號'], inplace=True)
        df.drop(df[df['身分證號'] == ''].index, inplace=True)

    long_df.dropna(subset=['電話', '姓名', '生日'], inplace=True)
    long_df = long_df[long_df['電話'].astype(str).str.strip() != '']

    short_ids = set(short_df["身分證號"].apply(_clean_id))
    short_ids.discard("")
    eligible_df = long_df[~long_df["身分證號"].apply(_clean_id).isin(short_ids)].copy()

    eligible_df['sex_check'] = eligible_df['身分證號'].apply(_map_sex)
    eligible_df = eligible_df[eligible_df['sex_check'] != '']
    eligible_df.drop(columns=['sex_check'], inplace=True)

    if eligible_df.empty:
        logging.warning("No eligible unmatched patients found after applying all filters.")
        return pd.DataFrame()

    return eligible_df


def convert(
        long_path: Path, short_path: Path, fixed: Dict[str, str], upload_month: str,
        start_date: str, end_date: str, segment_type: str, close_reason: str,
        seq_start: int, out_encoding: str = ENCODING, outdir: Path = Path("output"),
        mode: str = "matched", rejection_path: Path | None = None
) -> List[Path]:
    """Main conversion function handling all operational modes."""
    long_df = load_csv(long_path)
    short_df = load_csv(short_path)

    if "身分證字號" in long_df.columns: long_df.rename(columns={"身分證字號": "身分證號"}, inplace=True)
    if "身分證字號" in short_df.columns: short_df.rename(columns={"身分證字號": "身分證號"}, inplace=True)

    if mode in ["unmatched", "next_batch"]:
        eligible_visits_df = _get_eligible_candidates(long_df, short_df)
        if eligible_visits_df.empty: return []

        if mode == "next_batch":
            if not rejection_path or not rejection_path.exists():
                raise ValueError("Rejection file is required for 'Next Batch' mode.")
            print(f"Loading rejection file: {rejection_path.name}")
            rejection_df = load_csv(rejection_path)
            if '身分證號' not in rejection_df.columns:
                raise ValueError("Rejection file must contain a '身分證號' column.")
            rejection_df['身分證號'] = rejection_df['身分證號'].astype(str).str.strip()
            rejected_ids = set(rejection_df["身分證號"].apply(_clean_id))
            initial_count = len(eligible_visits_df)
            eligible_visits_df = eligible_visits_df[~eligible_visits_df["身分證號"].apply(_clean_id).isin(rejected_ids)]
            print(f"Removed {initial_count - len(eligible_visits_df)} rejected patients from the candidate pool.")

        if eligible_visits_df.empty:
            logging.warning("No eligible candidates remain after filtering.")
            return []

        print("Aggregating final candidate list...")
        aggregated_df = eligible_visits_df.groupby("身分證號").agg(
            visit_count=('身分證號', 'size'),
            **{col: (col, 'first') for col in ['姓名', '生日', '住址', '電話']}
        ).reset_index()
        aggregated_df['生日'] = aggregated_df['生日'].astype(str)
        sorted_df = aggregated_df.sort_values(by=['visit_count', '生日'], ascending=[False, False])
        selected_df = sorted_df.head(200)
        print(f"Selected top {len(selected_df)} patients from the final candidate pool.")
        df_to_process = selected_df

    else:  # mode == "matched"
        merged = merge_sources(long_df, short_df)
        rename_suffixed = {
            '姓名_y': '姓名', '住址_y': '住址', '電話_y': '電話',
            '看診日期_y': '看診日期', '身分證號_x': '身分證號', '生日_x': '生日',
            '個案類別_x': '個案類別',
        }
        for old, new in rename_suffixed.items():
            if old in merged.columns: merged.rename(columns={old: new}, inplace=True)
        cols_to_drop = [c for c in merged.columns if c.endswith(('_x', '_y'))]
        merged.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        merged.drop_duplicates(subset="身分證號", inplace=True)
        merged.sort_values("身分證號", inplace=True)
        df_to_process = merged

    records: List[bytes] = []
    for _, row in df_to_process.iterrows():
        try:
            rec = build_record(row, fixed, start_date, end_date, segment_type, close_reason, out_encoding)
            records.append(rec)
        except Exception as e:
            logging.warning(f"Skipping {row.get('身分證號')}: {e}")

    if not records:
        logging.error("No valid rows to process – nothing to write!")
        return []

    outdir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    file_suffix = "FM_B.txt" if mode in ["unmatched", "next_batch"] else "FM.txt"
    CHUNK_SIZE = 9999 if mode == "matched" else 200
    for idx, chunk in enumerate(chunks(records, CHUNK_SIZE), start=seq_start):
        fname = f"{fixed['BRANCH_CODE']}{fixed['HOSP_ID']}{upload_month}{idx:02d}{file_suffix}"
        fpath = outdir / fname
        with fpath.open("wb") as fh:
            for rec in chunk:
                fh.write(rec + b"\r\n")
        written.append(fpath)
        print(f"Wrote {len(chunk):,} rows to {fname}")
    print(f"Writing output file(s) with encoding: {out_encoding}")

    return written


def main(argv: List[str] | None = None) -> None:
    print("This script is intended to be run via its GUI. Please run fm_converter_gui.py")


if __name__ == "__main__":
    main()