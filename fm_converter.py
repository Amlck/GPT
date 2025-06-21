#!/usr/bin/env python3
"""
FM Converter – builds fixed‑width Family Physician Integrated Care upload (FM.txt)
from the pair of source CSVs provided by Taiwan NHI.
"""
import logging
from pathlib import Path
from typing import Dict, List, Set

import chardet  # type: ignore
import pandas as pd

# --- Constants and Specs ---
RECORD_LEN = 208
ENCODING = "utf-8"
BIG5 = "cp950"
HISTORY_FILE_NAME = "fm_submission_history.txt"
FIELD_SPECS_DEF = {"SEGMENT": 1, "PLAN_NO": 2, "BRANCH_CODE": 1, "HOSP_ID": 10, "ID": 10, "BIRTHDAY": 8, "NAME": 12,
                   "SEX": 1, "INFORM_ADDR": 120, "TEL": 15, "PRSN_ID": 10, "CASE_TYPE": 1, "CASE_DATE": 8,
                   "CLOSE_DATE": 8, "CLOSE_RSN": 1}
CASE_TYPE_MAP = {1: "A", 2: "A", 3: "A", 4: "A", 5: "A", 7: "A", 6: "C"}


# --- Utility Helpers ---
def detect_encoding(path: Path) -> str:
    with path.open("rb") as fh: raw = fh.read(4096)
    return chardet.detect(raw)["encoding"] or "utf-8"


def roc_to_gregorian(roc_date: str) -> str:
    roc_date = str(roc_date).replace("/", "").replace("-", "")
    if len(roc_date) not in (6, 7): raise ValueError(f"Bad ROC date: {roc_date}")
    return f"{int(roc_date[:-4]) + 1911:04d}{roc_date[-4:]}"


def _fw(value: str, width: int, align: str = "l", enc: str = ENCODING) -> bytes:
    raw = str(value).encode(enc, errors="replace")[:width]
    return raw.ljust(width, b" ") if align == "l" else raw.rjust(width, b" ")


def _map_sex(id_num: str) -> str:
    if not isinstance(id_num, str) or len(id_num) < 2: return ""
    digit = id_num[1]
    if digit in ('1', '8'): return '1'
    if digit in ('2', '9'): return '2'
    return ""


def _clean_id(value: str) -> str:
    return str(value).strip().strip("'").upper() if isinstance(value, str) else ""


def build_record_from_csv(row: pd.Series, fixed: Dict, start: str, end: str, seg: str, rsn: str, enc: str,
                          case_type: str) -> bytes:
    """Builds a fixed-width record ONLY from a CSV-derived DataFrame row."""
    bday_roc = str(row.get("生日", "")).strip()
    if not bday_roc: raise ValueError("Missing birthday")

    id_num = str(row.get("身分證號", ""))
    tel_str = str(row.get("電話", "")).strip()

    values = {
        "SEGMENT": seg,
        "PLAN_NO": fixed["PLAN_NO"],
        "BRANCH_CODE": fixed["BRANCH_CODE"],
        "HOSP_ID": fixed["HOSP_ID"],
        "ID": id_num,
        "BIRTHDAY": roc_to_gregorian(bday_roc),
        "NAME": row.get("姓名", ""),
        "SEX": _map_sex(id_num),
        "INFORM_ADDR": row.get("住址", ""),
        "TEL": tel_str,  # Receives the already-corrected phone number
        "PRSN_ID": fixed["PRSN_ID"],
        "CASE_TYPE": case_type,
        "CASE_DATE": start.replace("/", "").replace("-", ""),
        "CLOSE_DATE": end.replace("/", "").replace("-", "") if seg == "B" else "",
        "CLOSE_RSN": rsn if seg == "B" else "",
    }

    record = b"".join([_fw(values[k], FIELD_SPECS_DEF[k],
                           "r" if k in ["PLAN_NO", "HOSP_ID", "BIRTHDAY", "PRSN_ID", "CASE_DATE",
                                        "CLOSE_DATE"] else "l", enc) for k in FIELD_SPECS_DEF])
    assert len(record) == RECORD_LEN, f"Record len {len(record)} is not 208"
    return record


def chunks(lst: List, n: int):
    for i in range(0, len(lst), n): yield lst[i: i + n]


def load_csv(path: Path) -> pd.DataFrame:
    """
    Loads a CSV file, forcing columns that might be misinterpreted as numbers (like IDs and phone numbers)
    to be read as strings to preserve leading zeros and formatting.
    """
    sensitive_cols = ['身分證號', '身分證字號', '電話', 'HOSP_ID', '院所代碼', 'PRSN_ID', '醫師身分證字號']
    dtype_map = {col: str for col in sensitive_cols}

    for enc in ["utf-8-sig", "utf-8", "cp950", detect_encoding(path)]:
        try:
            df = pd.read_csv(path, encoding=enc, dtype=dtype_map, index_col=False)
            df.columns = [col.strip().lstrip('\ufeff') for col in df.columns]
            return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    raise ValueError(f"Could not decode or parse {path.name}.")


def merge_sources(long_df, short_df):
    long_df["ID_CLEAN"] = long_df["身分證號"].apply(_clean_id)
    short_df["ID_CLEAN"] = short_df["身分證號"].apply(_clean_id)
    return pd.merge(short_df, long_df, on="ID_CLEAN", how="inner")


def _get_eligible_candidates(long_df: pd.DataFrame, short_df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds all eligible VISITS. The main convert function handles creating a unique, stable patient list.
    """
    print("Finding all eligible visits for B-class cases...")
    req_cols = ['身分證號', '姓名', '生日', '住址', '電話']
    if not all(c in long_df.columns for c in req_cols): raise ValueError(
        f"Long CSV missing required columns: {req_cols}")
    if '身分證號' not in short_df.columns: raise ValueError("Short CSV missing '身分證號'.")

    for df in [long_df, short_df]:
        df.dropna(subset=['身分證號'], inplace=True)
        df['ID_CLEAN'] = df['身分證號'].astype(str).str.strip().str.upper()
        df.drop(df[df['ID_CLEAN'] == ''].index, inplace=True)

    unique_long = long_df.drop_duplicates(subset=['ID_CLEAN'])
    merged = pd.merge(unique_long[['ID_CLEAN']], short_df[['ID_CLEAN']], on='ID_CLEAN', how='left', indicator=True)
    unmatched_ids = set(merged[merged['_merge'] == 'left_only']['ID_CLEAN'])

    if not unmatched_ids: return pd.DataFrame()

    eligible_visits = long_df[long_df['ID_CLEAN'].isin(unmatched_ids)].copy()
    eligible_visits.dropna(subset=['電話', '姓名', '生日'], inplace=True)
    eligible_visits = eligible_visits[eligible_visits['電話'].astype(str).str.strip() != '']
    eligible_visits = eligible_visits[eligible_visits['身分證號'].apply(_map_sex) != '']

    return eligible_visits


def _load_history(history_path: Path) -> Set[str]:
    if not history_path.exists(): return set()
    with history_path.open("r", encoding="utf-8") as f:
        return {_clean_id(line) for line in f if line.strip()}


def _update_history(history_path: Path, new_ids_to_add: Set[str]):
    existing_ids = _load_history(history_path)
    combined_ids = existing_ids.union(new_ids_to_add)
    print(f"Updating history: {len(existing_ids)} existing, {len(new_ids_to_add)} new, {len(combined_ids)} total.")
    with history_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(sorted(list(combined_ids))))


def convert(long_path: Path, short_path: Path, fixed: Dict, upload_month: str, start_date: str, end_date: str,
            segment_type: str, close_reason: str, seq_start: int, out_encoding: str, outdir: Path, mode: str,
            rejection_path: Path | None, submitted_path: Path | None) -> List[Path]:
    long_df, short_df = load_csv(long_path), load_csv(short_path)
    if "身分證字號" in long_df.columns: long_df.rename(columns={"身分證字號": "身分證號"}, inplace=True)
    if "身分證字號" in short_df.columns: short_df.rename(columns={"身分證字號": "身分證號"}, inplace=True)

    outdir.mkdir(parents=True, exist_ok=True)
    history_path = outdir / HISTORY_FILE_NAME
    records = []

    if mode in ["unmatched", "refine"]:
        eligible_visits = _get_eligible_candidates(long_df, short_df)

        if eligible_visits.empty:
            if mode == "refine":
                pass
            else:
                logging.warning("No eligible B-class candidates found.")
                return []

        agg_df = eligible_visits.groupby("身分證號").agg(
            visit_count=('身分證號', 'size'),
            姓名=('姓名', 'first'),
            生日=('生日', 'first'),
            住址=('住址', 'first'),
            電話=('電話', 'first'),
        ).reset_index()

        # --- FINAL BUGFIX: Repair the phone number for B/C modes right after it's corrupted. ---
        if '電話' in agg_df.columns:
            tel_series = agg_df['電話'].astype(str).str.replace(r'\.0$', '', regex=True)
            agg_df['電話'] = tel_series.apply(lambda x: '0' + x if len(x) == 9 else x)

        master_candidate_pool = agg_df.sort_values(by=['visit_count', '生日'], ascending=[False, False])

        if mode == "refine":
            if not rejection_path or not submitted_path: raise ValueError("Submitted and rejection files are required.")

            print(f"Reading raw bytes from previous submission: {submitted_path.name}")
            with submitted_path.open('rb') as f:
                submitted_lines_raw = f.readlines()

            rejection_df = load_csv(rejection_path)
            if '上傳序號' not in rejection_df.columns: raise ValueError("Rejection file must have '上傳序號' column.")

            cleaned_rows_text = rejection_df['上傳序號'].str.extract(r'(\d+)', expand=False)
            rejected_indices = {int(r) - 1 for r in
                                pd.to_numeric(cleaned_rows_text, errors='coerce').dropna().astype(int)}

            accepted_lines = [line for i, line in enumerate(submitted_lines_raw) if i not in rejected_indices]
            num_needed = len(submitted_lines_raw) - len(accepted_lines)
            print(
                f"Analysis: {len(accepted_lines)} accepted patients preserved. Finding {num_needed} new replacements.")

            ever_tried_ids = _load_history(history_path)
            new_candidates_pool = master_candidate_pool[
                ~master_candidate_pool['身分證號'].apply(_clean_id).isin(ever_tried_ids)]
            replacements = new_candidates_pool.head(num_needed)

            if len(replacements) < num_needed:
                logging.warning(f"Needed {num_needed} replacements but only found {len(replacements)} new candidates.")

            replacement_records = []
            if not replacements.empty:
                newly_added_ids = set(replacements['身分證號'].apply(_clean_id))
                _update_history(history_path, newly_added_ids)
                for _, row in replacements.iterrows():
                    replacement_records.append(
                        build_record_from_csv(row, fixed, start_date, end_date, segment_type, close_reason,
                                              out_encoding, "B"))

            records = [line.strip(b'\r\n') for line in accepted_lines] + replacement_records

        else:  # mode == "unmatched"
            if history_path.exists():
                logging.warning(f"Starting a new 'unmatched' batch. Deleting old history file: {history_path.name}")
                history_path.unlink()
            df_to_process = master_candidate_pool.head(200)
            if not df_to_process.empty:
                initial_ids = set(df_to_process['身分證號'].apply(_clean_id))
                _update_history(history_path, initial_ids)
                for _, row in df_to_process.iterrows():
                    records.append(build_record_from_csv(row, fixed, start_date, end_date, segment_type, close_reason,
                                                         out_encoding, "B"))

    else:  # mode == "matched"
        merged = merge_sources(long_df, short_df)
        rename_map = {'姓名_y': '姓名', '住址_y': '住址', '電話_y': '電話', '看診日期_y': '看診日期',
                      '身分證號_x': '身分證號', '生日_x': '生日', '個案類別_x': '個案類別'}
        for old, new in rename_map.items():
            if old in merged.columns: merged.rename(columns={old: new}, inplace=True)
        merged.drop(columns=[c for c in merged.columns if c.endswith(('_x', '_y'))], inplace=True, errors='ignore')
        df_to_process = merged.drop_duplicates(subset="身分證號").sort_values("身分證號")

        # --- FINAL BUGFIX: Apply the same robust phone number fix to the Mode A dataframe. ---
        if '電話' in df_to_process.columns:
            tel_series = df_to_process['電話'].astype(str).str.replace(r'\.0$', '', regex=True)
            df_to_process['電話'] = tel_series.apply(lambda x: '0' + x if len(x) == 9 else x)

        for _, row in df_to_process.iterrows():
            case_num = int(str(row.get("個案類別", "0")).strip("'") or "0")
            row_case_type = CASE_TYPE_MAP.get(case_num, "B")
            records.append(
                build_record_from_csv(row, fixed, start_date, end_date, segment_type, close_reason, out_encoding,
                                      row_case_type))

    if not records:
        logging.error("No valid rows to process.")
        return []

    written = []
    suffix = "FM_B.txt" if mode in ["unmatched", "refine"] else "FM.txt"
    chunk_size = 200 if mode in ["unmatched", "refine"] else 9999
    for i, chunk in enumerate(chunks(records, chunk_size), start=seq_start):
        fpath = outdir / f"{fixed['BRANCH_CODE']}{fixed['HOSP_ID']}{upload_month}{i:02d}{suffix}"
        with fpath.open("wb") as fh: fh.writelines(rec + b"\r\n" for rec in chunk)
        written.append(fpath)
        print(f"Wrote {len(chunk)} rows to {fpath.name}")
    return written


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    print("This script is intended to be run via its GUI. Please run fm_converter_gui.py")


if __name__ == "__main__":
    main()