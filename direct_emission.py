#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
direct_emission.py  (LGA-only, no lat/lon)

Outputs per-LGA CSVs with columns:
  lga_name, lga_code, total_emission

- Annual CSV:
    Direct_Emission/Annual_Emissions/{SCENARIO}/{YEAR}_{SCENARIO}_{POLLUTANT}_direct_annual.csv
- Daily CSV (rep-day for given Month/DayType):
    Direct_Emission/Daily_Emissions/{SCENARIO}/{YEAR}_{SCENARIO}_{MON}_{DayType}_{POLLUTANT}_daily_lga.csv
"""

from __future__ import annotations
import os
import argparse
from typing import Dict, List, Optional, Set
import numpy as np
import pandas as pd
from netCDF4 import Dataset
import glob
import logging
logger = logging.getLogger(__name__)
SENTINEL_CODES: Set[int] = {997, 9999, 99999}  # treat these as invalid placeholders

# ---------------------------
# Helpers
# ---------------------------

def _ensure_dir(path: str) -> None:
    if not path:
        return
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)
        try:
            os.chmod(d, 0o777)
        except Exception:
            pass

def _pick_sheet(sheet_names) -> str:
    names = {s.lower().strip(): s for s in sheet_names}
    for cand in ["emission factors", "emission factors 2025", "emission_factors", "emission_factors_new"]:
        if cand in names:
            return names[cand]
    return list(sheet_names)[0]

def _load_emfac_map(inventory_xlsx: str, year: int, pollutants: List[str], whtypes: List[str]) -> Dict[str, Dict[str, float]]:
    xl = pd.ExcelFile(inventory_xlsx)
    sheet = _pick_sheet(xl.sheet_names)
    df = pd.read_excel(inventory_xlsx, sheet_name=sheet)

    cols = {c.lower().strip(): c for c in df.columns}
    need = {"pollutant", "whtype", "year", "emfac"}
    if not need.issubset(set(cols.keys())):
        raise ValueError(f"Sheet '{sheet}' must have columns: pollutant, Whtype, Year, Emfac")

    df = df.rename(columns={
        cols["pollutant"]: "pollutant",
        cols["whtype"]: "Whtype",
        cols["year"]: "Year",
        cols["emfac"]: "Emfac"
    })
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df = df[df["Year"] == year].copy()
    if df.empty:
        raise ValueError(f"No emission factors for Year={year} in sheet '{sheet}'")

    df["pollutant"] = df["pollutant"].astype(str)
    df["Whtype"] = df["Whtype"].astype(str)
    df["Emfac"] = pd.to_numeric(df["Emfac"], errors="coerce").fillna(0.0)

    base = df.groupby(["pollutant", "Whtype"], as_index=False)["Emfac"].mean()
    out = {p: {} for p in pollutants}

    base["_WHT_UP"] = base["Whtype"].astype(str).str.strip().str.upper()
    for p in pollutants:
        sub = base[base["pollutant"] == p]
        for w in whtypes:
            w_up = str(w).strip().upper()
            row = sub[sub["_WHT_UP"] == w_up]
            if row.empty:
                row = sub[sub["_WHT_UP"].str.contains(w_up, na=False)]
            out[p][w] = float(row["Emfac"].iloc[0]) if not row.empty else 0.0

    return out

def _load_monthly_factor(csv_path: str, month3: str) -> float:
    df = pd.read_csv(csv_path)
    mon3 = month3.upper()[:3]
    month_col = next((c for c in df.columns if df[c].astype(str).str.upper().str[:3].isin(
        ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]).mean() > 0.7), None)
    if month_col is None:
        month_col = next((c for c in ["Month", "Month_Name", "MonthID", "Month_ID"] if c in df.columns), None)
    if month_col is None:
        raise ValueError(f"Could not find month column in {csv_path}")

    factor_col = next((c for c in ["Proportion", "Factor", "Share", "Frac", "Weight"] if c in df.columns), None)
    if factor_col is None:
        cand = [c for c in df.columns if c != month_col and pd.api.types.is_numeric_dtype(df[c])]
        if not cand:
            raise ValueError(f"No numeric factor column in {csv_path}")
        factor_col = cand[0]

    df["_MON"] = df[month_col].astype(str).str.upper().str[:3]
    row = df[df["_MON"] == mon3]
    if row.empty:
        raise ValueError(f"Month {month3} not found in {csv_path}")
    return max(float(row.iloc[0][factor_col]), 0.0)

def _load_week_split(csv_path: str, daytype: str) -> float:
    df = pd.read_csv(csv_path)
    cols = {c.lower(): c for c in df.columns}
    if "proportion" not in cols:
        raise ValueError(f"No 'Proportion' column in {csv_path}")

    row = pd.DataFrame()
    if "type" in cols:
        row = df[df[cols["type"]].astype(str).str.lower().str.startswith(
            "weekdays" if daytype.lower().startswith("weekd") else "weekends"
        )]
    if row.empty and "isweekday" in cols:
        row = df[df[cols["isweekday"]] == (1 if daytype.lower().startswith("weekd") else 0)]
    if row.empty:
        row = df.iloc[[0]]
    return max(float(row.iloc[0][cols["proportion"]]), 0.0)

def _load_days_of_type(days_csv: str, year: int, month3: str, daytype: str) -> int:
    df = pd.read_csv(days_csv)
    year_col = next((c for c in df.columns if str(c).lower().strip() == "year"), None)
    if year_col is None:
        raise ValueError(f"No 'Year' column in {days_csv}")

    mon_map = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
               "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
    month_col = next((c for c in df.columns if df[c].astype(str).str.upper().str[:3].isin(mon_map.keys()).mean() > 0.7), None)
    if month_col is None:
        month_col = next((c for c in ["Month_Name", "Month"] if c in df.columns), None)
    if month_col is None:
        raise ValueError(f"No month name/number column in {days_csv}")

    target_mon3 = month3.upper()[:3]
    row = df[(df[year_col] == year)]
    if str(df[month_col].dtype) == "object":
        row = row[row[month_col].astype(str).str.upper().str[:3] == target_mon3]
    else:
        row = row[row[month_col] == mon_map.get(target_mon3, -1)]
    if row.empty:
        raise ValueError(f"No row for {year}/{month3} in {days_csv}")

    if daytype.lower().startswith("weekd"):
        cand = {"weekday_count","Weekdays"}
    else:
        cand = {"weekend_count","Weekends"}
    count_col = next((c for c in df.columns if str(c).lower().strip() in {k.lower() for k in cand}), None)
    if count_col is None:
        raise ValueError(f"No column for day count ({cand}) in {days_csv}")
    return max(int(row.iloc[0][count_col]), 1)

def _find_lga_code_col(df: pd.DataFrame) -> Optional[str]:
    prefer = [
        "LGA_CODE21", "lga_code21", "lga_code_21",
        "lga_code", "LGA_CODE",
        "L_CODE", "l_code",
        "LGACODE", "lgaid", "LGA_ID"
    ]
    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    for key in prefer:
        if key.lower() in cols_lower:
            return cols_lower[key.lower()]
    for c in df.columns:
        s = str(c).strip().lower()
        if "lga" in s and "code" in s:
            return c
    return None

def _normalize_lga_code(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series([], dtype="Int64")
    s = series.copy()
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").round(0).astype("Int64")
    s = s.astype(str).str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "None": pd.NA})
    s_no_dot0 = s.str.replace(r"\.0+$", "", regex=True)
    only_digits = s_no_dot0.dropna().str.fullmatch(r"\d+").all()
    if only_digits:
        return pd.to_numeric(s_no_dot0, errors="coerce").astype("Int64")
    has_any_digits = s.dropna().str.contains(r"\d", regex=True).any()
    if has_any_digits:
        digits = s.str.extract(r"(\d+)", expand=False)
        return pd.to_numeric(digits, errors="coerce").astype("Int64")
    return s

def _canon_name(x: str) -> str:
    import re
    x = re.sub(r"\s*\([^)]+\)", "", str(x))
    x = x.lower().replace("&", "and")
    x = re.sub(r"[^a-z0-9]+", "", x)
    return x

def _lga_name_to_code_from_nc(nc_path: str) -> Optional[pd.DataFrame]:
    """
    Build a mapping from lga_name -> most frequent LGA_CODE21 using the AWC NC grids.
    Requires 'lga_name' and 'LGA_CODE21' variables in the NC.
    Returns DataFrame with columns ['lga_name','lga_code'] (Int64).
    """
    if not os.path.exists(nc_path):
        return None
    with Dataset(nc_path, "r") as nc:
        if "LGA_CODE21" not in nc.variables or "lga_name" not in nc.variables:
            return None

        def _guess_lat_lon_varnames(nc):
            lat_cands = ["lat", "latitude", "y", "LAT", "Latitude", "Y"]
            lon_cands = ["lon", "longitude", "x", "LON", "Longitude", "X"]
            varnames = set(nc.variables.keys()); dimnames = set(nc.dimensions.keys())
            lat_name = next((v for v in lat_cands if v in varnames), None)
            lon_name = next((v for v in lon_cands if v in varnames), None)
            if lat_name is None: lat_name = next((d for d in lat_cands if d in dimnames), None)
            if lon_name is None: lon_name = next((d for d in lon_cands if d in dimnames), None)
            if lat_name is None or lon_name is None:
                dims = list(nc.dimensions.keys()); lat_name, lon_name = dims[-2], dims[-1]
            return lat_name, lon_name

        lat_name, lon_name = _guess_lat_lon_varnames(nc)

        codes = np.array(nc.variables["LGA_CODE21"][:])
        names = np.array(nc.variables["lga_name"][:], dtype=object)

        def _maybe_T(arr, vname):
            dims = getattr(nc.variables[vname], "dimensions", ())
            if len(dims) == 2 and dims[-2] == lon_name and dims[-1] == lat_name:
                return arr.T
            return arr

        codes = _maybe_T(codes, "LGA_CODE21")
        names = _maybe_T(names, "lga_name")

        df = pd.DataFrame({"lga_name": names.ravel(), "code": codes.ravel()})
        df = df.dropna(subset=["lga_name"])
        df["lga_name"] = df["lga_name"].astype(str)
        df["canon"] = df["lga_name"].apply(_canon_name)
        df = df[df["canon"] != ""]
        df["code"] = pd.to_numeric(df["code"], errors="coerce").astype("Int64")

        mode_df = (
            df.dropna(subset=["code"])
              .groupby(["canon", "code"], as_index=False)
              .size()
              .sort_values(["canon", "size"], ascending=[True, False])
              .drop_duplicates(subset=["canon"])
              .rename(columns={"code": "lga_code"})
        )
        disp = df.groupby("canon", as_index=False)["lga_name"].first()
        out = disp.merge(mode_df[["canon", "lga_code"]], on="canon", how="left")
        out = out.drop(columns=["canon"])
        out["lga_code"] = out["lga_code"].astype("Int64")
        return out.rename(columns={"lga_name": "lga_name"})

def _build_name_to_code_map_from_shapefile(shapefile_path: str,
                                           act_code_override: Optional[int]) -> Optional[pd.DataFrame]:
    """
    Build mapping from shapefile: canonical lga_name -> LGA_CODE21.
    Adds aliases so 'act' and 'australiancapitalterritory' resolve to the same code
    as 'unincorporatedact' when present (or vice versa).
    If act_code_override is provided, force ACT aliases to that code.
    Returns DataFrame with columns ['canon','lga_code'].
    """
    if shapefile_path is None or not os.path.exists(shapefile_path):
        return None
    try:
        import geopandas as gpd  # optional dependency
    except Exception as e:
        logger.warning("Cannot import geopandas for shapefile mapping: %s", e)
        return None
    try:
        gdf = gpd.read_file(shapefile_path)
    except Exception as e:
        logger.warning("Failed to read shapefile: %s", e)
        return None

    cols = {c.lower(): c for c in gdf.columns}
    name_col = cols.get("lga_name21") or cols.get("lga_name") or "LGA_NAME21"
    code_col = cols.get("lga_code21") or cols.get("lga_code") or "LGA_CODE21"
    if name_col not in gdf.columns or code_col not in gdf.columns:
        logger.warning("Shapefile must have LGA_NAME21 and LGA_CODE21 (or equivalent).")
        return None

    df = pd.DataFrame({
        "canon": gdf[name_col].astype(str).map(_canon_name),
        "lga_code": pd.to_numeric(gdf[code_col], errors="coerce").astype("Int64")
    }).dropna(subset=["canon"])
    df = df.drop_duplicates(subset=["canon"], keep="first")

    have = set(df["canon"].tolist())
    # Determine ACT code from shapefile if present
    act_keys = ["unincorporatedact", "australiancapitalterritory", "act"]
    shp_act_code = None
    for key in act_keys:
        if key in have:
            shp_act_code = df.loc[df["canon"] == key, "lga_code"].iloc[0]
            break

    # Inject aliases using shapefile ACT code (if found)
    if shp_act_code is not None:
        for alias in act_keys:
            if alias not in have:
                df = pd.concat([df, pd.DataFrame([{"canon": alias, "lga_code": shp_act_code}])], ignore_index=True)
                have.add(alias)

    # Apply override if provided
    if act_code_override is not None:
        if shp_act_code is not None and int(shp_act_code) != int(act_code_override):
            logger.warning("Shapefile ACT code = %d, but overriding to %d as requested.", int(shp_act_code), int(act_code_override))
        for alias in act_keys:
            if alias in have:
                df.loc[df["canon"] == alias, "lga_code"] = int(act_code_override)
            else:
                df = pd.concat([df, pd.DataFrame([{"canon": alias, "lga_code": int(act_code_override)}])], ignore_index=True)
                have.add(alias)

    return df

def _clean_codes_for_priority(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype("Int64")
    invalid = s.isna() | (s <= 0) | s.isin(list(SENTINEL_CODES))
    s = s.mask(invalid, pd.NA)
    return s

def _derive_or_patch_codes_for_awc_df(
    awc_df: pd.DataFrame,
    nc_map: Optional[pd.DataFrame],
    csv_code_series: pd.Series,
    shapefile_path: Optional[str],
    act_code_override: Optional[int],
    debug: bool = False
) -> pd.Series:
    canon = awc_df["lga_name"].astype(str).map(_canon_name)
    act_aliases = {"australiancapitalterritory", "unincorporatedact", "act"}

    code_nc = pd.Series([pd.NA] * len(awc_df), dtype="Int64")
    if nc_map is not None and not nc_map.empty:
        tmp = nc_map.copy()
        tmp["_canon"] = tmp["lga_name"].astype(str).map(_canon_name)
        tmp = tmp.drop_duplicates(subset=["_canon"], keep="first")
        look = pd.Series(tmp["lga_code"].values, index=tmp["_canon"]).astype("Int64")
        code_nc = canon.map(look).astype("Int64")
    code_nc = _clean_codes_for_priority(code_nc)

    code_csv_raw = csv_code_series.astype("Int64") if csv_code_series is not None else pd.Series([pd.NA]*len(awc_df), dtype="Int64")
    code_csv = _clean_codes_for_priority(code_csv_raw)

    code_pref = code_nc.where(code_nc.notna(), code_csv)

    code_final = code_pref.copy()
    shp_used = pd.Series([False] * len(awc_df), dtype=bool)
    if shapefile_path:
        shp_map = _build_name_to_code_map_from_shapefile(shapefile_path, act_code_override=act_code_override)
        if shp_map is not None and not shp_map.empty:
            look2 = pd.Series(shp_map["lga_code"].values, index=shp_map["canon"]).astype("Int64")
            fill_vals = canon.map(look2).astype("Int64")
            fill_vals = _clean_codes_for_priority(fill_vals)
            need = code_final.isna()
            code_final = code_final.where(~need, fill_vals)
            shp_used = need & fill_vals.notna()

    if act_code_override is not None:
        mask_act = canon.isin(act_aliases)
        if mask_act.any():
            code_final.loc[mask_act] = int(act_code_override)

    if debug:
        n_total = len(code_final)
        n_nc = int(code_nc.notna().sum())
        n_csv = int((code_pref.notna() & code_nc.isna()).sum())
        n_shp = int(shp_used.sum())
        n_missing = int(code_final.isna().sum())
        logger.info("[DEBUG] LGA code sources: NC=%d  CSV=%d  Shapefile=%d  Missing=%d/%d", n_nc, n_csv, n_shp, n_missing, n_total)

    return code_final.astype("Int64")

# ---------------------------
# New inline resolver to tolerate different file names
# ---------------------------
def _resolve_awc_lga_csv_path(expected_path: str, scenario_key: str, year: int) -> Optional[str]:
    if expected_path and os.path.exists(expected_path):
        return expected_path

    parent = os.path.dirname(expected_path) if expected_path else ""
    if not parent:
        parent = os.getcwd()

    scen_variants = {scenario_key, scenario_key.replace("-", "_"), scenario_key.replace("_", "-")}
    patterns = []
    for scen in scen_variants:
        patterns.extend([
            f"{scen}_{year}_awc_lga*.csv",
            f"*{scen}*_{year}_awc_lga*.csv",
            f"*{scen}*{year}*awc_lga*.csv",
        ])
    patterns.extend([
        f"{year}_*awc_lga*.csv",
        f"*_{year}_*_awc_lga*.csv",
        f"*{year}*awc_lga*.csv",
    ])

    # search immediate parent
    if os.path.isdir(parent):
        for pat in patterns:
            matches = glob.glob(os.path.join(parent, pat))
            if matches:
                logger.info("Resolved AWC LGA CSV by pattern '%s': %s", pat, matches[0])
                return matches[0]
        # fallback: any file in parent with 'awc' and year or scenario token
        for fname in os.listdir(parent):
            ln = fname.lower()
            if "awc" not in ln:
                continue
            if str(year) in ln or any(s.lower() in ln for s in scen_variants):
                logger.info("Resolved AWC LGA CSV by heuristic in folder %s: %s", parent, fname)
                return os.path.join(parent, fname)

    # also try parent.parent
    grand = os.path.dirname(parent)
    if grand and os.path.isdir(grand):
        for pat in patterns:
            matches = glob.glob(os.path.join(grand, pat))
            if matches:
                logger.info("Resolved AWC LGA CSV in parent folder by pattern '%s': %s", pat, matches[0])
                return matches[0]
        for fname in os.listdir(grand):
            ln = fname.lower()
            if "awc" not in ln:
                continue
            if str(year) in ln or any(s.lower() in ln for s in scen_variants):
                logger.info("Resolved AWC LGA CSV by heuristic in parent folder %s: %s", grand, fname)
                return os.path.join(grand, fname)

    logger.debug("Could not resolve AWC LGA CSV for expected path %s", expected_path)
    return None

def _resolve_awc_nc_path(awc_nc_path: str, scenario_key: str, year: int) -> Optional[str]:
    if awc_nc_path and os.path.exists(awc_nc_path):
        return awc_nc_path

    base = os.path.dirname(awc_nc_path) if awc_nc_path else ""
    scen_variants = {scenario_key, scenario_key.replace("-", "_"), scenario_key.replace("_", "-")}
    search_dirs = [base or ".", os.path.dirname(base or ".")]

    for root in search_dirs:
        for scen in scen_variants:
            for pat in [f"**/*{year}*{scen}*awc_all_whtypes*.nc", f"**/*{scen}*{year}*awc_all_whtypes*.nc", f"**/*{year}*awc*.nc"]:
                matches = glob.glob(os.path.join(root, pat), recursive=True)
                if matches:
                    logger.info("Found AWC NC by recursive search: %s", matches[0])
                    return matches[0]
    return None

# ---------------------------
# Core calculator (per-LGA only, no grids)
# ---------------------------

def compute_direct_emission_lga_only(
    *,
    awc_nc: str,                 # used for lga_name↔LGA_CODE21 mapping if needed
    inventory_xlsx: str,
    monthly_factors_csv: str,
    weekly_factors_csv: str,
    days_csv: str,
    awc_lga_csv: str,           # per-LGA AWC CSV (wide with AWC_<WHTYPE> columns)
    scenario_key: str,
    year: int,
    month3: str,
    daytype: str,               # "WeekDay" or "WeekEnd"
    pollutant: str,
    out_per_lga: str,           # placeholder path; output roots are resolved internally
    emitted_csv_for_crosscheck: Optional[str] = None,  # ignored
    debug: bool = False,
    lga_shapefile: Optional[str] = None,  # optional shapefile to derive/patch LGA codes (ACT-aware)
    act_code_override: Optional[int] = 89399  # e.g., 89399 to force ACT code (default enforced)
) -> Dict[str, str]:
    month3 = month3.upper()[:3]
    daytype = "WeekDay" if daytype.lower().startswith("weekd") else "WeekEnd"

    # 1) Resolve and load canonical AWC per-LGA
    resolved_awc_path = _resolve_awc_lga_csv_path(awc_lga_csv, scenario_key, int(year))
    if resolved_awc_path is None:
        raise FileNotFoundError(f"AWC LGA CSV not found: {awc_lga_csv}")
    if resolved_awc_path != awc_lga_csv:
        logger.info("Using resolved AWC LGA CSV: %s (requested: %s)", resolved_awc_path, awc_lga_csv)
    awc_lga_csv = resolved_awc_path

    awc_df = pd.read_csv(awc_lga_csv)

    # lga_name
    if "lga_name" not in awc_df.columns:
        lga_col = next((c for c in awc_df.columns if str(c).strip().lower() in {"lga_name", "lga", "lga_name21"}), None)
        if lga_col:
            awc_df = awc_df.rename(columns={lga_col: "lga_name"})
        else:
            raise ValueError("AWC LGA CSV missing 'lga_name' column")

    # 1a) get lga_code from CSV if present
    csv_code_col = _find_lga_code_col(awc_df)
    csv_code_series = _normalize_lga_code(awc_df[csv_code_col]) if csv_code_col else pd.Series([pd.NA]*len(awc_df), dtype="Int64")

    # 1b) build name→code lookup from NC (preferred)
    resolved_awc_nc = _resolve_awc_nc_path(awc_nc, scenario_key, int(year))
    if resolved_awc_nc is None:
        logger.warning("AWC NetCDF not found or unreadable; name↔code from NC will not be used: %s", awc_nc)
        nc_map = None
    else:
        if resolved_awc_nc != awc_nc:
            logger.info("Using resolved AWC NC for name→code mapping: %s (requested: %s)", resolved_awc_nc, awc_nc)
        nc_map = _lga_name_to_code_from_nc(resolved_awc_nc)

    # 1c) derive/patch final codes (NC → CSV → Shapefile → ACT override).
    awc_df["lga_code"] = _derive_or_patch_codes_for_awc_df(
        awc_df=awc_df,
        nc_map=nc_map,
        csv_code_series=csv_code_series,
        shapefile_path=lga_shapefile,
        act_code_override=act_code_override,
        debug=debug
    )

    # 2) WHTYPE columns (prefer AWC_<WHTYPE>)
    whtypes = []
    col_map = {}
    for c in awc_df.columns:
        cl = str(c).strip()
        if cl.lower().startswith("awc_"):
            w = cl[4:]
            whtypes.append(w)
            col_map[w] = c
    if not whtypes:
        for c in awc_df.columns:
            if str(c).isupper() and len(str(c)) <= 12 and c not in {"LGA_NAME", "LGA_NAME21", "lga_name", "lga_code"}:
                whtypes.append(c)
                col_map[c] = c
    if not whtypes:
        raise ValueError("Could not infer any WHTYPE columns from AWC LGA CSV (expected AWC_<WHTYPE>).")

    # 3) Emission factors
    emfac_map = _load_emfac_map(inventory_xlsx, year, [pollutant], whtypes)
    ef = emfac_map[pollutant]  # {whtype: emfac}

    # 4) Annual emission per LGA = sum_w (AWC_w * EF[w])
    ann = np.zeros(len(awc_df), dtype=np.float64)
    for wht in whtypes:
        col = col_map.get(wht)
        if col is None or col not in awc_df.columns:
            continue
        vals = pd.to_numeric(awc_df[col], errors="coerce").fillna(0.0).values.astype(np.float64)
        ann += vals * float(ef.get(wht, 0.0))

    # 5) Temporalize → daily(rep-day)
    m_frac = _load_monthly_factor(monthly_factors_csv, month3)
    dt_frac = _load_week_split(weekly_factors_csv, daytype)
    n_days = _load_days_of_type(days_csv, year, month3, daytype)

    monthly_total_lga = ann * float(m_frac)
    type_total_lga    = monthly_total_lga * float(dt_frac)
    typical_day_lga   = type_total_lga / max(n_days, 1)

    # 6) Output folders (use DIRECT_EMIS_OUTROOT provided by orchestrator)
    # Save outputs under subfolders named like: {scenario_key}_{year}
    direct_emis_outroot = os.environ["DIRECT_EMIS_OUTROOT"]
    paths: Dict[str, str] = {}
    folder_name = f"{scenario_key}_{year}"
    annual_dir = os.path.join(direct_emis_outroot, "Annual_Emissions", folder_name)
    daily_dir = os.path.join(direct_emis_outroot, "Daily_Emissions", folder_name)
    os.makedirs(annual_dir, exist_ok=True)
    os.makedirs(daily_dir, exist_ok=True)

    # 7) Annual per-LGA CSV — SINGLE code column
    annual_out = os.path.join(annual_dir, f"{year}_{scenario_key}_{pollutant}_direct_annual.csv")
    df_ann = pd.DataFrame({
        "lga_name": awc_df["lga_name"].astype(str),
        "lga_code": awc_df["lga_code"].astype("Int64"),
        "direct_emission": ann
    })
    _ensure_dir(annual_out)
    df_ann.to_csv(annual_out, index=False)
    logger.info("Saved annual emissions to: %s", annual_out)
    paths["annual_csv"] = annual_out

    # 8) Daily (rep-day) per-LGA CSV — SINGLE code column
    daily_out = os.path.join(daily_dir, f"{year}_{scenario_key}_{month3}_{daytype}_{pollutant}_daily_lga.csv")
    df_day = pd.DataFrame({
        "lga_name": awc_df["lga_name"].astype(str),
        "lga_code": awc_df["lga_code"].astype("Int64"),
        "total_emission": typical_day_lga
    })
    _ensure_dir(daily_out)
    df_day.to_csv(daily_out, index=False)
    logger.info("Saved daily (rep-day) emissions to: %s", daily_out)
    paths["daily_csv"] = daily_out

    return paths

# ---------------------------
# CLI
# ---------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Direct per-LGA emission for a single (year, month, daytype, pollutant) without lat/lon. Writes only Annual & Daily outputs."
    )
    p.add_argument("--awc-nc", required=True)
    p.add_argument("--inventory-xlsx", required=True)
    p.add_argument("--monthly-factors-csv", required=True)
    p.add_argument("--weekly-factors-csv", required=True)
    p.add_argument("--days-csv", required=True)
    p.add_argument("--awc-lga-csv", required=True)
    p.add_argument("--scenario-key", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", required=True)
    p.add_argument("--daytype", required=True, choices=["WeekDay","WeekEnd","weekday","weekend"])
    p.add_argument("--pollutant", required=True)
    p.add_argument("--emitted-csv", default=None)
    p.add_argument("--out-per-lga", required=True)
    p.add_argument("--debug", action="store_true")
    p.add_argument("--lga-shapefile", default=None)
    p.add_argument("--act-code-override", type=int, default=89399)
    return p

def main(argv=None):
    args = _build_parser().parse_args(argv)
    logger.info("▶ Direct LGA emission for %s • %s • %s • %s • %s", args.scenario_key, args.year, args.month.upper(), args.daytype, args.pollutant)
    compute_direct_emission_lga_only(
        awc_nc=args.awc_nc,
        inventory_xlsx=args.inventory_xlsx,
        monthly_factors_csv=args.monthly_factors_csv,
        weekly_factors_csv=args.weekly_factors_csv,
        days_csv=args.days_csv,
        awc_lga_csv=args.awc_lga_csv,
        scenario_key=args.scenario_key,
        year=args.year,
        month3=args.month.upper()[:3],
        daytype=args.daytype,
        pollutant=args.pollutant,
        out_per_lga=args.out_per_lga,
        emitted_csv_for_crosscheck=args.emitted_csv,
        debug=args.debug,
        lga_shapefile=args.lga_shapefile,
        act_code_override=args.act_code_override
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()