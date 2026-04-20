#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Streamlit dashboard for Direct_Emission LGA-based outputs.

Designed for SDC / HPC / VS Code Remote SSH use.

What this app does
- Scans direct-emission annual and daily CSV outputs automatically.
- Lets the user choose:
    * emission type: Annual or Daily
    * year
    * scenario/case key
    * pollutant
    * month/daytype (for daily)
- Displays NSW LGA choropleth map, summary metrics, top-LGA bar chart, and data table.

Why this version is easier on SDC
- Streamlit is simpler to run remotely than Dash for many users.
- It works well with VS Code port forwarding.
- You open the forwarded localhost URL in your browser.

Run on SDC:
    streamlit run direct_emission_streamlit_sdc.py --server.address 127.0.0.1 --server.port 8501

If port 8501 is busy, change it to another port, e.g. 8502.

Expected file structure
- Annual CSVs:
    Direct_Emission/Annual_Emissions/.../{YEAR}_{SCENARIO_KEY}_{POLLUTANT}_direct_annual.csv
- Daily CSVs:
    Direct_Emission/Daily_Emissions/.../{YEAR}_{SCENARIO_KEY}_{MON}_{DayType}_{POLLUTANT}_daily_lga.csv
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import plotly.express as px
import streamlit as st
from shapely.geometry import Point

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
BASE_ROOT = Path("/mnt/scratch_lustre/ar_ai4ba_scratch/Ai4MEMS/NSW_WHEM-INVENTORY")
DIRECT_EMISSION_ROOT = BASE_ROOT / "Outputs" / "WHEM-Emission-Dashboard" / "Direct_Emission"
SHAPEFILE_PATH = BASE_ROOT / "Inputs" / "ShapeFile" / "LGA_2021_AUST_GDA2020_SHP" / "LGA_2021_AUST_GDA2020.shp"

INCLUDE_ACT = False
ACT_NAMES = {
    "Australian Capital Territory",
    "Unincorporated ACT",
    "Unincorporated Australian Capital Territory",
    "ACT",
}

DAYTYPE_ORDER = ["WeekDay", "WeekEnd"]
MONTH_ORDER = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
EMISSION_TYPE_ORDER = ["Annual", "Daily"]

ANNUAL_PATTERN = re.compile(
    r"^(?P<year>\d{4})_(?P<scenario_key>.+)_(?P<pollutant>[^_]+)_direct_annual\.csv$",
    re.IGNORECASE,
)
DAILY_PATTERN = re.compile(
    r"^(?P<year>\d{4})_(?P<scenario_key>.+)_(?P<month>[A-Za-z]{3})_(?P<daytype>WeekDay|WeekEnd)_(?P<pollutant>[^_]+)_daily_lga\.csv$",
    re.IGNORECASE,
)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def canon_name(x: object) -> str:
    s = str(x or "")
    s = re.sub(r"\s*\([^)]+\)", "", s)
    s = s.lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def normalize_daytype(x: str) -> str:
    x = str(x or "").strip().lower()
    return "WeekDay" if x.startswith("weekd") else "WeekEnd"


def normalize_month3(x: str) -> str:
    return str(x or "").strip().upper()[:3]


def find_lga_code_col(df: pd.DataFrame) -> Optional[str]:
    prefs = ["LGA_CODE21", "lga_code21", "lga_code", "LGACODE", "lga_code_21", "LGA_CODE"]
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for p in prefs:
        if p.lower() in lower_map:
            return lower_map[p.lower()]
    for c in df.columns:
        s = str(c).strip().lower()
        if "lga" in s and "code" in s:
            return c
    return None


def find_lga_name_col(df: pd.DataFrame) -> Optional[str]:
    prefs = ["LGA_NAME21", "lga_name21", "lga_name", "LGA_NAME", "lga"]
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for p in prefs:
        if p.lower() in lower_map:
            return lower_map[p.lower()]
    for c in df.columns:
        s = str(c).strip().lower()
        if "lga" in s and "name" in s:
            return c
    return None


def find_emission_col(df: pd.DataFrame) -> str:
    for c in ["emission", "direct_emission", "total_emission"]:
        if c in df.columns:
            return c
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    exclude = {"lga_code", "lga_code21", "lgacode"}
    numeric_cols = [c for c in numeric_cols if c.lower() not in exclude]
    if not numeric_cols:
        raise ValueError("No suitable emission column found.")
    return numeric_cols[-1]


def split_scenario_key(scenario_key: str) -> Tuple[str, Optional[str]]:
    parts = str(scenario_key).split("_", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return scenario_key, None


def sorted_unique(series: pd.Series, custom_order: Optional[List[str]] = None) -> List[object]:
    vals = [v for v in series.dropna().unique().tolist() if v is not None]
    if custom_order is None:
        return sorted(vals)
    order_map = {v: i for i, v in enumerate(custom_order)}
    return sorted(vals, key=lambda x: (order_map.get(x, 999), str(x)))


# -----------------------------------------------------------------------------
# Data discovery
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def scan_direct_emission_files(root_str: str) -> pd.DataFrame:
    root = Path(root_str)
    rows: List[Dict[str, object]] = []

    annual_root = root / "Annual_Emissions"
    daily_root = root / "Daily_Emissions"

    if annual_root.exists():
        for fp in annual_root.rglob("*.csv"):
            m = ANNUAL_PATTERN.match(fp.name)
            if not m:
                continue
            gd = m.groupdict()
            scenario, case = split_scenario_key(gd["scenario_key"])
            rows.append(
                {
                    "emission_type": "Annual",
                    "year": int(gd["year"]),
                    "scenario_key": gd["scenario_key"],
                    "scenario": scenario,
                    "case": case,
                    "month": None,
                    "daytype": None,
                    "pollutant": gd["pollutant"],
                    "path": str(fp),
                }
            )

    if daily_root.exists():
        for fp in daily_root.rglob("*.csv"):
            m = DAILY_PATTERN.match(fp.name)
            if not m:
                continue
            gd = m.groupdict()
            scenario, case = split_scenario_key(gd["scenario_key"])
            rows.append(
                {
                    "emission_type": "Daily",
                    "year": int(gd["year"]),
                    "scenario_key": gd["scenario_key"],
                    "scenario": scenario,
                    "case": case,
                    "month": normalize_month3(gd["month"]),
                    "daytype": normalize_daytype(gd["daytype"]),
                    "pollutant": gd["pollutant"],
                    "path": str(fp),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.drop_duplicates().sort_values(
        by=["emission_type", "year", "scenario_key", "month", "daytype", "pollutant"]
    ).reset_index(drop=True)
    return df


# -----------------------------------------------------------------------------
# Shapefile prep
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_lga_geometries(shapefile_path_str: str, include_act: bool = False):
    shapefile_path = Path(shapefile_path_str)
    if not shapefile_path.exists():
        raise FileNotFoundError(f"Shapefile not found: {shapefile_path}")

    gdf = gpd.read_file(shapefile_path)
    if gdf.crs is None:
        gdf = gdf.set_crs(7844)
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    required = ["LGA_CODE21", "LGA_NAME21", "STE_NAME21", "geometry"]
    missing = [c for c in required if c not in gdf.columns]
    if missing:
        raise ValueError(f"Shapefile missing required columns: {missing}")

    if include_act:
        gdf = gdf[gdf["STE_NAME21"].eq("New South Wales") | gdf["LGA_NAME21"].isin(ACT_NAMES)].copy()
    else:
        gdf = gdf[gdf["STE_NAME21"].eq("New South Wales")].copy()

    gdf = gdf[~gdf.geometry.isna() & ~gdf.geometry.is_empty].copy()
    gdf["lga_code"] = pd.to_numeric(gdf["LGA_CODE21"], errors="coerce").astype("Int64")
    gdf["lga_name"] = gdf["LGA_NAME21"].astype(str)
    gdf["canon_name"] = gdf["lga_name"].map(canon_name)
    gdf = gdf[["lga_code", "lga_name", "canon_name", "geometry"]].copy()

    geojson = json.loads(gdf.to_json())

    try:
        gdf_proj = gdf.to_crs(epsg=3577)
        cent = gdf_proj.geometry.centroid
        mean_x, mean_y = float(cent.x.mean()), float(cent.y.mean())
        pt = gpd.GeoSeries([Point(mean_x, mean_y)], crs=3577).to_crs(gdf.crs).geometry[0]
        center = (float(pt.y), float(pt.x))
    except Exception:
        center = (float(gdf.geometry.centroid.y.mean()), float(gdf.geometry.centroid.x.mean()))

    zoom = 4
    return gdf, geojson, center, zoom


# -----------------------------------------------------------------------------
# Reading selected emission CSV
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def read_emission_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        return df

    code_col = find_lga_code_col(df)
    name_col = find_lga_name_col(df)
    emis_col = find_emission_col(df)

    out = df.copy()
    if code_col is not None:
        out["lga_code"] = pd.to_numeric(out[code_col], errors="coerce").astype("Int64")
    else:
        out["lga_code"] = pd.Series([pd.NA] * len(out), dtype="Int64")

    if name_col is not None:
        out["lga_name"] = out[name_col].astype(str)
    else:
        out["lga_name"] = ""

    out["canon_name"] = out["lga_name"].map(canon_name)
    out["emission"] = pd.to_numeric(out[emis_col], errors="coerce").fillna(0.0)
    return out[["lga_code", "lga_name", "canon_name", "emission"]].copy()


# -----------------------------------------------------------------------------
# Join emissions to geometries
# -----------------------------------------------------------------------------
def join_emission_to_lga(geom_gdf: gpd.GeoDataFrame, emis_df: pd.DataFrame) -> gpd.GeoDataFrame:
    geom = geom_gdf.copy()
    emis = emis_df.copy()

    usable_codes = emis["lga_code"].notna().any()
    if usable_codes:
        merged = geom.merge(
            emis[["lga_code", "emission", "lga_name"]].rename(columns={"lga_name": "source_lga_name"}),
            on="lga_code",
            how="left",
        )
    else:
        merged = geom.copy()
        merged["emission"] = pd.NA
        merged["source_lga_name"] = pd.NA

    need_fill = merged["emission"].isna()
    if need_fill.any():
        by_name = (
            emis.groupby("canon_name", as_index=False)["emission"]
            .sum(min_count=1)
            .rename(columns={"emission": "emission_by_name"})
        )
        merged = merged.merge(by_name, on="canon_name", how="left")
        merged.loc[need_fill, "emission"] = merged.loc[need_fill, "emission_by_name"]
        merged.drop(columns=["emission_by_name"], inplace=True)

    merged["emission"] = pd.to_numeric(merged["emission"], errors="coerce").fillna(0.0)
    return merged


# -----------------------------------------------------------------------------
# Figures
# -----------------------------------------------------------------------------
def make_map_figure(gdf: gpd.GeoDataFrame, geojson: dict, center: Tuple[float, float], zoom: int, title: str):
    fig = px.choropleth_mapbox(
        gdf,
        geojson=geojson,
        locations="lga_code",
        featureidkey="properties.lga_code",
        color="emission",
        color_continuous_scale="YlOrRd",
        hover_name="lga_name",
        hover_data={"lga_code": True, "emission": ":,.4f"},
        center={"lat": center[0], "lon": center[1]},
        zoom=zoom,
        opacity=0.75,
        mapbox_style="carto-positron",
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=45, b=0),
        title=title,
        coloraxis_colorbar=dict(title="Emission"),
        height=750,
    )
    return fig


def make_top_lga_bar(gdf: gpd.GeoDataFrame, n: int = 15):
    top = gdf[["lga_name", "emission"]].sort_values("emission", ascending=False).head(n)
    fig = px.bar(
        top.iloc[::-1],
        x="emission",
        y="lga_name",
        orientation="h",
        text="emission",
        title=f"Top {min(n, len(top))} LGAs by emission",
    )
    fig.update_traces(texttemplate="%{text:,.3f}", textposition="outside")
    fig.update_layout(margin=dict(l=10, r=10, t=45, b=10), yaxis_title="", xaxis_title="Emission", height=750)
    return fig


# -----------------------------------------------------------------------------
# Streamlit app
# -----------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="NSW Direct Emission Dashboard", layout="wide")
    st.title("NSW Direct Emission Dashboard")
    st.caption("Streamlit version for SDC / VS Code Remote SSH.")

    with st.expander("Runtime paths", expanded=False):
        st.write(f"Direct emission root: `{DIRECT_EMISSION_ROOT}`")
        st.write(f"Shapefile path: `{SHAPEFILE_PATH}`")
        st.write(f"Direct emission root exists: `{DIRECT_EMISSION_ROOT.exists()}`")
        st.write(f"Shapefile exists: `{SHAPEFILE_PATH.exists()}`")
        st.info("Run with: streamlit run this_file.py --server.address 127.0.0.1 --server.port 8501")

    try:
        files_df = scan_direct_emission_files(str(DIRECT_EMISSION_ROOT))
        geom_gdf, geojson, map_center, map_zoom = load_lga_geometries(str(SHAPEFILE_PATH), include_act=INCLUDE_ACT)
    except Exception as e:
        st.error(f"Startup error: {e}")
        st.stop()

    if files_df.empty:
        st.warning(f"No direct-emission CSV files were found under: {DIRECT_EMISSION_ROOT}")
        st.stop()

    st.sidebar.header("Filters")

    emission_type = st.sidebar.selectbox(
        "Emission type",
        EMISSION_TYPE_ORDER,
        index=0 if "Annual" in files_df["emission_type"].values else 1,
    )

    df1 = files_df[files_df["emission_type"] == emission_type].copy()

    years = sorted_unique(df1["year"])
    year = st.sidebar.selectbox("Year", years, index=0)
    df2 = df1[df1["year"] == year].copy()

    scenarios = sorted_unique(df2["scenario_key"])
    scenario_key = st.sidebar.selectbox("Scenario / case", scenarios, index=0)
    df3 = df2[df2["scenario_key"] == scenario_key].copy()

    pollutants = sorted_unique(df3["pollutant"])
    pollutant = st.sidebar.selectbox("Pollutant", pollutants, index=0)
    df4 = df3[df3["pollutant"] == pollutant].copy()

    month = None
    daytype = None
    if emission_type == "Daily":
        months = sorted_unique(df4["month"], MONTH_ORDER)
        month = st.sidebar.selectbox("Month", months, index=0)
        df5 = df4[df4["month"] == month].copy()

        daytypes = sorted_unique(df5["daytype"], DAYTYPE_ORDER)
        daytype = st.sidebar.selectbox("Day type", daytypes, index=0)
        df_sel = df5[df5["daytype"] == daytype].copy()
    else:
        df_sel = df4.copy()

    if df_sel.empty:
        st.warning("No file matches the selected filters.")
        st.stop()

    selected_path = df_sel.iloc[0]["path"]
    emis_df = read_emission_csv(selected_path)
    merged = join_emission_to_lga(geom_gdf, emis_df)

    title = f"{emission_type} direct emission by NSW LGA — {scenario_key} — {year} — {pollutant}"
    if emission_type == "Daily":
        title += f" — {month} — {daytype}"

    total = float(merged["emission"].sum())
    mean_v = float(merged["emission"].mean())
    max_idx = merged["emission"].idxmax()
    max_name = str(merged.loc[max_idx, "lga_name"]) if len(merged) else "-"
    max_val = float(merged.loc[max_idx, "emission"]) if len(merged) else 0.0
    covered = int((merged["emission"] > 0).sum())
    total_lgas = int(len(merged))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total emission", f"{total:,.3f}")
    c2.metric("Mean across LGAs", f"{mean_v:,.3f}")
    c3.metric("Maximum LGA", f"{max_val:,.3f}", delta=max_name)
    c4.metric("LGAs with non-zero emission", f"{covered:,}", delta=f"of {total_lgas:,}")

    col_map, col_bar = st.columns([7, 5])

    with col_map:
        map_fig = make_map_figure(merged, geojson, map_center, map_zoom, title)
        st.plotly_chart(map_fig, use_container_width=True)

    with col_bar:
        bar_fig = make_top_lga_bar(merged, n=15)
        st.plotly_chart(bar_fig, use_container_width=True)

    st.subheader("Selected output file")
    st.code(selected_path, language=None)

    st.subheader("LGA emission table")
    table_df = merged[["lga_code", "lga_name", "emission"]].sort_values("emission", ascending=False).copy()
    st.dataframe(table_df, use_container_width=True, hide_index=True)

    csv_bytes = table_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download current LGA table as CSV",
        data=csv_bytes,
        file_name=f"direct_emission_{emission_type.lower()}_{year}_{pollutant}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
