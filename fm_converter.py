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

# --- Constants and Specs ---
RECORD_LEN = 208
ENCODING = "utf-8"
BIG5 = "cp950"
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
    """
    Robustly cleans an ID string by normalizing full-width characters,
    stripping whitespace, and forcing uppercase.
    """
    if not isinstance(value, str):
        return ""
    full_width = "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ０１２３４５６７８９"
    half_width = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    translation_table = str.maketrans(full_width, half_width)
    normalized_id = value.translate(translation_table)
    return normalized_id.strip().upper()


def build_record(row: pd.Series, fixed: Dict, start: str, end: str, seg: str, rsn: str, enc: str) -> bytes:
    bday_roc = str(row.get("生日", "")).strip()
    if not bday_roc: raise ValueError("Missing birthday")
    case_raw = str(row.get("個案類別", "")).strip("'")
    case_num = int(case_raw) if case_raw.isdigit() else 0

    # --- THIS IS THE FIX ---
    # Clean the ID at the last possible moment before it's used, ensuring
    # the output file contains the standardized, half-width, uppercase ID.
    id_num = _clean_id(row.get("身分證號", ""))

    values = {
        "SEGMENT": seg, "PLAN_NO": fixed["PLAN_NO"], "BRANCH_CODE": fixed["BRANCH_CODE"],
        "HOSP_ID": fixed["HOSP_ID"], "ID": id_num, "BIRTHDAY": roc_to_gregorian(bday_roc),
        "NAME": row.get("姓名", ""), "SEX": _map_sex(id_num),  # This now uses the cleaned ID
        "INFORM_ADDR": row.get("住址", ""), "TEL": str(row.get("電話", "")).strip(),
        "PRSN_ID": fixed["PRSN_ID"], "CASE_TYPE": CASE_TYPE_MAP.get(case_num, "B"),
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
    for enc in ["utf-8-sig", "utf-8", "cp950", detect_encoding(path)]:
        try:
            df = pd.read_csv(path, encoding=enc, dtype=str, index_col=False)
            df.columns = [col.strip().lstrip('\ufeff') for col in df.columns]
            return df
        except Exception:
            continue
    raise ValueError(f"Could not decode or parse {path.name}.")


def merge_sources(long_df, short_df):
    long_df["ID_CLEAN"] = long_df["身分證號"].apply(_clean_id)
    short_df["ID_CLEAN"] = short_df["身分證號"].apply(_clean_id)
    return pd.merge(short_df, long_df, on="ID_CLEAN", how="inner")


def _get_eligible_candidates(long_df: pd.DataFrame, short_df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds truly unmatched patients using a robust left anti-join. This function now
    benefits from the powerful _clean_id normalization.
    """
    print("Finding eligible candidates using robust merge method...")
    req_cols = ['身分證號', '姓名', '生日', '住址', '電話']
    if not all(c in long_df.columns for c in req_cols): raise ValueError(
        f"Long CSV missing required columns: {req_cols}")
    if '身分證號' not in short_df.columns: raise ValueError("Short CSV missing '身分證號'.")

    for df in [long_df, short_df]:
        df.dropna(subset=['身分證號'], inplace=True)
        # Use the NEW _clean_id function to create a truly standardized ID
        df['ID_CLEAN'] = df['身分證號'].apply(_clean_id)
        df.drop(df[df['ID_CLEAN'] == ''].index, inplace=True)

    unique_long = long_df.drop_duplicates(subset=['ID_CLEAN'])
    merged = pd.merge(unique_long[['ID_CLEAN']], short_df[['ID_CLEAN']], on='ID_CLEAN', how='left', indicator=True)
    unmatched_ids = set(merged[merged['_merge'] == 'left_only']['ID_CLEAN'])

    if not unmatched_ids:
        logging.warning("No unmatched patients found.")
        return pd.DataFrame()

    eligible_visits = long_df[long_df['ID_CLEAN'].isin(unmatched_ids)].copy()
    eligible_visits.dropna(subset=['電話', '姓名', '生日'], inplace=True)
    eligible_visits = eligible_visits[eligible_visits['電話'].astype(str).str.strip() != '']
    eligible_visits = eligible_visits[eligible_visits['ID_CLEAN'].apply(_map_sex) != '']

    if eligible_visits.empty:
        logging.warning("No eligible unmatched patients found after data quality filters.")
        return pd.DataFrame()
    return eligible_visits


def _get_ids_from_fixed_width(path: Path, enc: str) -> List[str]:
    print(f"Reading previously generated file: {path.name}")
    id_start, id_len = 14, 10
    return [line[id_start:id_start + id_len].decode(enc, 'ignore').strip() for line in path.open("rb") if
            len(line) >= id_start + id_len]


def convert(
        long_path: Path, short_path: Path, fixed: Dict, upload_month: str,
        start_date: str, end_date: str, segment_type: str, close_reason: str,
        seq_start: int, out_encoding: str, outdir: Path, mode: str,
        rejection_path: Path | None, submitted_path: Path | None
) -> List[Path]:
    long_df, short_df = load_csv(long_path), load_csv(short_path)
    if "身分證字號" in long_df.columns: long_df.rename(columns={"身分證字號": "身分證號"}, inplace=True)
    if "身分證字號" in short_df.columns: short_df.rename(columns={"身分證字號": "身分證號"}, inplace=True)

    if mode in ["unmatched", "refine"]:
        eligible_df = _get_eligible_candidates(long_df, short_df)
        if eligible_df.empty: return []

        # Group by the clean ID to ensure correct visit aggregation
        agg_df = eligible_df.groupby("ID_CLEAN").agg(
            visit_count=('ID_CLEAN', 'size'),
            身分證號=('身分證號', 'first'),
            **{c: (c, 'first') for c in ['姓名', '生日', '住址', '電話']}
        ).reset_index(drop=True)

        master_candidate_pool = agg_df.sort_values(by=['visit_count', '生日'], ascending=[False, False])

        if mode == "refine":
            if not rejection_path or not submitted_path: raise ValueError(
                "Submitted and rejection files are required for refine mode.")

            submitted_ids = _get_ids_from_fixed_width(submitted_path, out_encoding)
            rejection_df = load_csv(rejection_path)
            if '上傳序號' not in rejection_df.columns: raise ValueError("Rejection file must have '上傳序號' column.")

            cleaned_rows_text = rejection_df['上傳序號'].str.extract(r'(\d+)', expand=False)
            rejected_rows = pd.to_numeric(cleaned_rows_text, errors='coerce').dropna().astype(int).tolist()
            if not rejected_rows:
                logging.warning("Could not find any valid row numbers in the rejection file. No changes will be made.")
                return []
            rejected_ids_clean = {_clean_id(submitted_ids[row - 1]) for row in rejected_rows if
                                  0 < row <= len(submitted_ids)}

            submitted_ids_clean = {_clean_id(id) for id in submitted_ids}
            accepted_ids_clean = submitted_ids_clean - rejected_ids_clean
            num_needed = len(submitted_ids) - len(accepted_ids_clean)
            print(f"Found {len(accepted_ids_clean)} accepted patients. Finding {num_needed} new replacements.")

            new_candidates = master_candidate_pool[
                ~master_candidate_pool['身分證號'].apply(_clean_id).isin(submitted_ids_clean)]
            replacements = new_candidates.head(num_needed)

            if len(replacements) < num_needed:
                logging.warning(f"Needed {num_needed} replacements but only found {len(replacements)} new candidates.")

            accepted_df = master_candidate_pool[
                master_candidate_pool['身分證號'].apply(_clean_id).isin(accepted_ids_clean)]
            df_to_process = pd.concat([accepted_df, replacements], ignore_index=True)

        else:  # mode == "unmatched"
            df_to_process = master_candidate_pool.head(200)

    else:  # mode == "matched"
        merged = merge_sources(long_df, short_df)
        rename_map = {'姓名_y': '姓名', '住址_y': '住址', '電話_y': '電話', '看診日期_y': '看診日期',
                      '身分證號_x': '身分證號', '生日_x': '生日', '個案類別_x': '個案類別'}
        for old, new in rename_map.items():
            if old in merged.columns: merged.rename(columns={old: new}, inplace=True)
        merged.drop(columns=[c for c in merged.columns if c.endswith(('_x', '_y'))], inplace=True, errors='ignore')
        df_to_process = merged.drop_duplicates(subset="身分證號").sort_values("身分證號")

    records = [build_record(row, fixed, start_date, end_date, segment_type, close_reason, out_encoding) for _, row in
               df_to_process.iterrows()]
    if not records: logging.error("No valid rows to process."); return []
    outdir.mkdir(parents=True, exist_ok=True);
    written = []
    suffix = "FM_B.txt" if mode in ["unmatched", "refine"] else "FM.txt"
    chunk_size = 200 if mode in ["unmatched", "refine"] else 9999
    for i, chunk in enumerate(chunks(records, chunk_size), start=seq_start):
        fpath = outdir / f"{fixed['BRANCH_CODE']}{fixed['HOSP_ID']}{upload_month}{i:02d}{suffix}"
        with fpath.open("wb") as fh: fh.writelines(rec + b"\r\n" for rec in chunk)
        written.append(fpath);
        print(f"Wrote {len(chunk)} rows to {fpath.name}")
    return written


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    print("This script is intended to be run via its GUI. Please run fm_converter_gui.py")


if __name__ == "__main__":
    main()