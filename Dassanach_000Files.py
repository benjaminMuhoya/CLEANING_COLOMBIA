#!/usr/bin/env python3
import os, re, glob
import pandas as pd
import numpy as np
from pathlib import Path

# -------------------- CONFIG --------------------
INPUT_DIR   = "./normalized_files"            # normalized CSVs live here
NORMAL_LOG  = "normalization_log_SECOND.csv"  # same mapping you used before
OUT_CSV     = "DASSANACH_combined.csv"     # final wide table
ERROR_LOG   = "extract_00xx_errors.log"       # human-readable issues
LABEL_SCAN_COLS = 8                           # scan first N columns for labels
DEFAULT_OFFSETS  = [4, 5, 6, 7]               # general offsets to probe
PH_OFFSETS       = [5, 7, 6, 4]               # pH quirk observed in zero-led files
# ------------------------------------------------

# ------------ helpers: result detection ------------
num_like = re.compile(r"^[\s]*[+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?(?:[eE][+-]?\d+)?")
qual_set = {"NEGATIVE","POSITIVE","TRACE","NEG","POS","NIL","NONE","ABSENT","PRESENT"}

def is_resultish(s):
    if s is None: return False
    if isinstance(s, float) and np.isnan(s): return False
    s2 = str(s).strip()
    if not s2 or s2.lower() == "nan": return False
    if num_like.match(s2): return True           # numeric or numeric+unit
    if s2.upper() in qual_set: return True       # qualitative
    # rescue: first token numeric (e.g. "5.6 mmol/L")
    try:
        float(s2.split()[0].replace(",",""))
        return True
    except Exception:
        return False

def canonicalize_label(lbl: str) -> str:
    """Collapse trivial suffix variants (e.g., __Urine_) and tidy underscores."""
    s = str(lbl).strip()
    s = re.sub(r"__(Urine|Serum)_?$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"_+", "_", s)
    return s

# ------------ unit split (matches your previous logic) ------------
num_regex = re.compile(r"""
    ^\s*
    (?P<num>[+-]?\d{1,3}(?:,\d{3})*|\d+)
    (?:\.\d+)?          # decimals
    (?:[eE][+-]?\d+)?   # scientific
""", re.VERBOSE)

UNIT_MAP = {
    "mmol/l": "mmol/L",
    "μmol/l": "umol/L",
    "umol/l": "umol/L",
    "iu/l":   "IU/L",
    "mg/dl":  "mg/dL",
    "g/dl":   "g/dL",
    "g/l":    "g/L",
    "mosmol/kg": "mOsmol/kg",
    "mosm/kg":   "mOsmol/kg",
    "mosmolkg":  "mOsmol/kg",
}

def clean_numeric(s):
    if pd.isna(s): return np.nan
    s = str(s).strip()
    m = num_regex.match(s)
    if not m:
        # ratio like "1.97 :1"
        ratio_match = re.match(r"^\s*([+-]?\d+(?:\.\d+)?)\s*:?\s*1\s*$", s)
        if ratio_match:
            return float(ratio_match.group(1))
        return np.nan
    num = m.group(0).replace(",", "")
    try:
        return float(num)
    except Exception:
        return np.nan

def normalize_unit(col_name, raw_value_str):
    if raw_value_str is None: return "no_units"
    unit = ""
    s = str(raw_value_str).strip()
    m = num_regex.match(s)
    unit = s[m.end():] if m else s

    unit = unit.strip()
    unit = unit.replace(".", " ").replace("·", " ")
    unit = re.sub(r"\s+", " ", unit)
    unit = unit.replace(" /", "/").replace("/ ", "/")
    unit = unit.replace(" l", "/L").replace(" L", "/L")
    unit = unit.lower().replace("µ","u").replace("umol/ l","umol/l").strip(" :;")

    # special columns
    if col_name.lower().startswith("ph"):
        return "unitless"
    if "ratio" in col_name.lower() or re.search(r"\d\s*:\s*1$", s):
        return "ratio"
    if unit in ("", None):
        return "no_units"

    # common rescues
    unit = unit.replace("mg dl", "mg/dl").replace("g dl","g/dl")

    if unit in UNIT_MAP: return UNIT_MAP[unit]
    if "umol" in unit: return "umol/L"
    if "mmol" in unit: return "mmol/L"
    if "iu"   in unit: return "IU/L"
    if "mg" in unit and "dl" in unit: return "mg/dL"
    if "g"  in unit and "/l" in unit: return "g/L"
    if "osmol" in unit: return "mOsmol/kg"

    if re.fullmatch(r"(negative|pos|positive|trace|tr|nil|none)", unit.strip(), flags=re.I):
        return "qual"

    return unit

def split_value_and_unit(col_name, s):
    if pd.isna(s):
        return np.nan, "no_units"
    s = str(s).strip()
    val = clean_numeric(s)
    unit = normalize_unit(col_name, s)
    if pd.isna(val):
        if unit == "unitless":
            return np.nan, "unitless"
        return np.nan, "qual" if unit not in ("no_units",) else "no_units"
    if unit in ("", "no_units"):
        unit = "no_units"
    return val, unit

# ------------- metadata extraction (Name / Age / Gender) -------------
META_LABELS = {
    "Name":   ["name"],
    "Age":    ["age"],
    "Gender": ["gender","sex"]
}

def grab_meta(df: pd.DataFrame, key: str):
    """
    Find Name/Age/Gender in first LABEL_SCAN_COLS columns.
    Try same-row offsets [1..8], then next-row anywhere.
    """
    rows, cols = df.shape
    keys = META_LABELS[key]
    for r in range(rows):
        for c in range(min(LABEL_SCAN_COLS, cols)):
            raw = df.iat[r, c]
            if raw is None or (isinstance(raw, float) and np.isnan(raw)): 
                continue
            s = str(raw).strip()
            if not s: continue
            if any(k in s.lower() for k in keys) and len(s) <= 20:
                # same row to right
                for dc in range(1, 9):
                    cc = c + dc
                    if cc < cols:
                        v = df.iat[r, cc]
                        if v is not None and (not (isinstance(v, float) and np.isnan(v))) and str(v).strip():
                            return str(v).strip()
                # next row anywhere
                if r+1 < rows:
                    for cc in range(cols):
                        v = df.iat[r+1, cc]
                        if v is not None and (not (isinstance(v, float) and np.isnan(v))) and str(v).strip():
                            return str(v).strip()
    return None

# ------------- core extraction per file -------------
def extract_from_one_file(path: str, expected_tests: list, warn_list: list):
    """
    expected_tests: list of 'New Name' strings from normalization log for this file.
    Returns meta dict + {biomarker: (value_str)} raw (split later).
    """
    try:
        df = pd.read_csv(path, header=None, dtype=str, engine="python", on_bad_lines="skip")
    except Exception as e:
        warn_list.append(f"READ_FAIL: {os.path.basename(path)} -> {e}")
        return {"file_name": os.path.basename(path), "Name": None, "Age": None, "Gender": None}, {}

    rows, cols = df.shape
    meta = {
        "file_name": os.path.basename(path),
        "Name":   grab_meta(df, "Name"),
        "Age":    grab_meta(df, "Age"),
        "Gender": grab_meta(df, "Gender"),
    }
    for k in ["Name","Age","Gender"]:
        if meta[k] is None:
            warn_list.append(f"META_MISSING: {os.path.basename(path)} -> {k} not found")

    # quick detect: some zero-led files have two empty leading columns; but since we scan
    # fixed first LABEL_SCAN_COLS for labels, this is robust either way.

    # build a fast lookup set for labels we expect
    exp = [t for t in expected_tests if isinstance(t, str)]
    exp_canon = {canonicalize_label(t): t for t in exp}  # canon -> original

    found = {}  # canon -> value string

    for r in range(rows):
        for c in range(min(LABEL_SCAN_COLS, cols)):
            raw = df.iat[r, c]
            if raw is None or (isinstance(raw, float) and np.isnan(raw)): 
                continue
            lbl = str(raw).strip()
            if not lbl: 
                continue
            canon = canonicalize_label(lbl)
            if canon not in exp_canon:
                continue  # only pick labels that normalization log says we expect

            # offsets choice (pH special-case)
            offsets = PH_OFFSETS if canon.lower().startswith("ph") else DEFAULT_OFFSETS

            val = None
            used_offset = None
            # same row
            for dc in offsets:
                cc = c + dc
                if cc < cols and is_resultish(df.iat[r, cc]):
                    val = str(df.iat[r, cc]).strip()
                    used_offset = dc
                    break
            # next-row fallback
            if val is None and r+1 < rows:
                for cc in range(cols):
                    if is_resultish(df.iat[r+1, cc]):
                        val = str(df.iat[r+1, cc]).strip()
                        used_offset = None
                        break

            if val is None:
                warn_list.append(f"NO_VALUE: {os.path.basename(path)} label='{lbl}' row={r} col={c}")
                continue

            # keep first found
            if canon not in found:
                found[canon] = val

    # special pH warning
    if not any(k.lower().startswith("ph") for k in found.keys()):
        warn_list.append(f"PH_MISSING: {os.path.basename(path)} -> no pH entry found")

    return meta, found

# ------------- main -------------
def main():
    # files: strictly names that start with '0' and end with .csv
    files = sorted([p for p in glob.glob(os.path.join(INPUT_DIR, "*.csv"))
                    if os.path.basename(p).startswith("0")])

    if not files:
        print(f"No files starting with '0' found in {INPUT_DIR}")
        return

    # load normalization log
    if not os.path.exists(NORMAL_LOG):
        print(f"Missing {NORMAL_LOG}. Please place it next to this script.")
        return
    log = pd.read_csv(NORMAL_LOG, dtype=str)
    # normalize column names
    log.columns = [c.strip() for c in log.columns]
    fn_col = next((c for c in log.columns if c.lower() in ("file name","file_name")), None)
    new_col = next((c for c in log.columns if c.lower() in ("new name","new_name")), None)
    if fn_col is None or new_col is None:
        print("Normalization log must have 'File Name' and 'New Name' columns.")
        return

    # for each file, pull the set of expected test names
    per_file_expected = {}
    for f in files:
        fn = os.path.basename(f)
        mask = log[fn_col].astype(str).str.contains(fn, na=False)
        per_file_expected[fn] = sorted(log.loc[mask, new_col].dropna().unique().tolist())

    # first pass: gather universe of biomarkers from expected names (canon)
    biomarker_canon = set()
    for fn, tests in per_file_expected.items():
        biomarker_canon.update(canonicalize_label(t) for t in tests)

    # build output columns: metadata + each biomarker value + units
    biomarker_list = sorted(biomarker_canon)
    cols = ["file_name", "Name", "Age", "Gender"]
    for b in biomarker_list:
        cols.append(b)
        cols.append(f"{b}_UNITS")

    all_rows = []
    warns = []

    for f in files:
        fn = os.path.basename(f)
        meta, found_raw = extract_from_one_file(f, per_file_expected.get(fn, []), warns)

        row = {c: np.nan for c in cols}
        row.update(meta)  # file_name, Name, Age, Gender

        for b in biomarker_list:
            v_raw = found_raw.get(b)
            if v_raw is not None:
                val, unit = split_value_and_unit(b, v_raw)
                row[b] = val
                row[f"{b}_UNITS"] = unit
            else:
                row[b] = np.nan
                row[f"{b}_UNITS"] = "no_units"

        all_rows.append(row)

    out = pd.DataFrame(all_rows, columns=cols).sort_values("file_name")
    out.to_csv(OUT_CSV, index=False)

    with open(ERROR_LOG, "w") as fh:
        for w in warns:
            fh.write(w + "\n")

    print(f"✅ Wrote {OUT_CSV} with {len(out)} files.")
    print(f"🧾 Error log: {ERROR_LOG} ({len(warns)} lines)")

if __name__ == "__main__":
    main()

