import os
import sys
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import re

# ----------------------------
# Config (adjust as needed)
# ----------------------------
ENV = os.getenv("ENV", "DEV")  # "DEV" or "PROD"
REGPATH = Path(os.getenv("REGPATH", Path.home()))  # mimic &regpath
BASE_LOG_DIR = REGPATH / "CBG" / "log" / "product_appropriateness" / "client360"
BASE_OUT_DIR = REGPATH / "CBG" / "output" / "product_appropriateness" / "client360"

# Where we persist the "ac.pa_client360_autocomplete" equivalent
AC_DIR = BASE_OUT_DIR  # keeping it simple: use same base out dir
AC_MASTER_FILE = AC_DIR / "pa_client360_autocomplete.csv"  # simple CSV-based "table"

TODAY = date.today()
RUNDAY = TODAY.strftime("%Y%m%d")  # like SAS &runday
OUT_DIR_TODAY = BASE_OUT_DIR / RUNDAY

# Input sources (replace with real paths or DB pulls)
# In SAS these were "upstream" datasets:
#   c360_detail_pre, c360_detail_more_in_pre, c360_detail
# You can either point these to CSV/Parquet files OR swap in DB reads below.
INPUTS = {
    "c360_detail_pre": AC_DIR / "c360_detail_pre.csv",
    "c360_detail_more_in_pre": AC_DIR / "c360_detail_more_in_pre.csv",
    "c360_detail": AC_DIR / "c360_detail.csv",
}

# Teradata connection info (only needed if you run the tracking/AOT queries live)
TERADATA_CFG = {
    "host": os.getenv("TD_HOST", "td.example.com"),
    "user": os.getenv("TD_USER", "username"),
    "password": os.getenv("TD_PASS", "password"),
    # "database": "ddw01"  # not required by teradatasql, but kept for clarity
}

# Week anchor assumptions:
# - SAS used intnx('week.4', ...) and intnx('week.7', ...)
# - Here we parameterize with simple weekday numbers: Monday=0 ... Sunday=6
#   We assume:
#       week.4  ~ Thursday-start (weekday=3)
#       week.7  ~ Sunday-start   (weekday=6)
WEEK4_START_WEEKDAY = 3  # Thursday
WEEK7_END_WEEKDAY = 6    # Sunday (we take end-of-week = Sunday)
LAUNCH_DT = date(2023, 5, 7)
LAUNCH_DT_MINI4 = date(2023, 4, 23)

# ----------------------------
# Logging setup
# ----------------------------
BASE_LOG_DIR.mkdir(parents=True, exist_ok=True)
BASE_OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR_TODAY.mkdir(parents=True, exist_ok=True)

logfile = BASE_LOG_DIR / f"cbg_pa_client360_{TODAY.strftime('%y%m%d')}.log"
logging.basicConfig(
    filename=logfile,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logging.getLogger().addHandler(console)

logging.info(">>>>>>>> Start: %s <<<<<<<<", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
logging.info(">>>>>>>> Env: %s <<<<<<<<", ENV)
logging.info("Script running from: %s", Path.cwd())

# ----------------------------
# Helpers
# ----------------------------
def beginning_of_week(anchor_weekday: int, d: date) -> date:
    """Return the beginning of week for date d, where week starts on anchor_weekday (Mon=0..Sun=6)."""
    # Simple logic: go backwards to anchor_weekday
    delta = (d.weekday() - anchor_weekday) % 7
    return d - timedelta(days=delta)

def end_of_week(anchor_weekday: int, d: date) -> date:
    """Return end of the week for a start anchored by anchor_weekday. If anchor is Sunday, end is Saturday, etc.
    Here we define 'end' as start + 6 days."""
    start = beginning_of_week(anchor_weekday, d)
    return start + timedelta(days=6)

def read_df(path_or_dfname: Path, required=True, desc=""):
    if isinstance(path_or_dfname, Path):
        if path_or_dfname.exists():
            logging.info("Reading %s from %s", desc or path_or_dfname.name, path_or_dfname)
            return pd.read_csv(path_or_dfname)
        else:
            msg = f"Required file not found: {path_or_dfname}"
            if required:
                logging.error(msg)
                raise FileNotFoundError(msg)
            else:
                logging.warning(msg)
                return pd.DataFrame()
    else:
        raise ValueError("Provide a Path to a CSV file for this simple version.")

def write_excel(df: pd.DataFrame, path: Path, sheet_name="Sheet1"):
    logging.info("Writing Excel: %s (sheet=%s, rows=%d)", path, sheet_name, len(df))
    df.to_excel(path, index=False, sheet_name=sheet_name)

def simple_teradata_query(sql_text: str) -> pd.DataFrame:
    """Optional: run a Teradata query and return a DataFrame.
       Keep it simple; uncomment teradatasql import to use."""
    logging.info("Running TD query (first 200 chars): %s", sql_text[:200].replace("\n", " "))
    # with teradatasql.connect(host=TERADATA_CFG["host"], user=TERADATA_CFG["user"], password=TERADATA_CFG["password"]) as con:
    #     return pd.read_sql(sql_text, con)
    # For now, raise to make it explicit.
    raise NotImplementedError("Teradata connection is not configured. Provide creds and uncomment code.")

# ----------------------------
# First-run detection
# ----------------------------
ini_run = "N"
if not AC_MASTER_FILE.exists():
    ini_run = "Y"
logging.info("First run? %s", ini_run)

# ----------------------------
# Week windows (follow SAS intent, but kept simple)
# - If first run: wk_start = LAUNCH_DT, wk_start_mini4 = LAUNCH_DT_MINI4
# - Else: both start = this week's WEEK4 start (Thursday-anchored)
# - wk_end = this week's WEEK4 end minus 5 days (mirrors SAS)
# ----------------------------
tday = TODAY
week4_start = beginning_of_week(WEEK4_START_WEEKDAY, tday)
week4_end = end_of_week(WEEK4_START_WEEKDAY, tday)

if ini_run == "Y":
    wk_start = LAUNCH_DT
    wk_start_mini4 = LAUNCH_DT_MINI4
else:
    wk_start = week4_start
    wk_start_mini4 = week4_start

wk_end = (week4_end - timedelta(days=5))

logging.info("Week window - wk_start: %s, wk_start_mini4: %s, wk_end: %s",
             wk_start.isoformat(), wk_start_mini4.isoformat(), wk_end.isoformat())
logging.info("runday: %s", RUNDAY)

# ----------------------------
# Formats (as simple dicts)
# ----------------------------
stagefmt = {
    "Démarche exploratoire/comprendre le besoin": "11.Démarche exploratoire/comprendre le besoin",
    "Discovery/Understand Needs": "12.Discovery/Understand Needs",
    "Review Options": "21.Review Options",
    "Present/Gain Commitment": "31.Present/Gain Commitment",
    "Intégration commencée": "41.Intégration commencée",
    "Onboarding Started": "42.Onboarding Started",
    "Opportunity Lost": "51.Opportunity Lost",
    "Opportunity Won": "61.Opportunity Won",
}

cs_cmt = {
    "COM1": "Test population (less samples)",
    "COM2": "Match population",
    "COM3": "Mismatch population (less samples)",
    "COM4": "Non Anomaly Population",
    "COM5": "Anomaly Population",
    "COM6": "Number of Deposit Sessions",
    "COM7": "Number of Accounts",
    "COM8": "Number of Transactions",
    "COM9": "Non Blank Population",
    "COM10": "Blank Population",
    "COM11": "Unable to Assess",
    "COM12": "Number of Failed Data Elements",
    "COM13": "Population Distribution",
    "COM14": "Reconciled Population",
    "COM15": "Not Reconciled Population",
    "COM16": "Pass",
    "COM17": "Fail",
    "COM18": "Not Applicable",
    "COM19": "Potential Fail",
}

# ----------------------------
# Upstream dataset guards + reads
# ----------------------------
c360_detail_pre = read_df(INPUTS["c360_detail_pre"], required=True, desc="c360_detail_pre")
c360_detail_more_in_pre = read_df(INPUTS["c360_detail_more_in_pre"], required=True, desc="c360_detail_more_in_pre")
c360_detail = read_df(INPUTS["c360_detail"], required=True, desc="c360_detail")

# ----------------------------
# Tracking (Teradata) – replace with live queries if you have access
# ----------------------------
# Equivalent SAS filter:
#   from ddw01.evnt_prod_track_log
#   where advc_salt_typ = 'Advice Tool'
#     and evnt_dt > date '&wk_start' - 90
#
# Here we show the SQL; uncomment to run if TD creds are set.
tracking_sql = f"""
select *
from ddw01.evnt_prod_track_log
where advc_salt_typ = 'Advice Tool'
  and evnt_dt > DATE '{(wk_start - timedelta(days=90)).isoformat()}'
"""
# tracking_all = simple_teradata_query(tracking_sql)

# For an offline demo, you can comment the line above and instead read a CSV:
tracking_all_path = AC_DIR / "tracking_all.csv"
if tracking_all_path.exists():
    logging.info("Reading offline tracking_all from %s", tracking_all_path)
    tracking_all = pd.read_csv(tracking_all_path)
else:
    logging.warning("No tracking_all data available (TD query or CSV). Continuing with empty frame.")
    tracking_all = pd.DataFrame(columns=["OPPOR_ID", "ADVC_TOOL_NM", "EVNT_DT"])

# Distinct tool uses
if not tracking_all.empty:
    tracking_tool_use_distinct = (
        tracking_all
        .dropna(subset=["OPPOR_ID", "ADVC_TOOL_NM"])
        .assign(ADVC_TOOL_NM=lambda d: d["ADVC_TOOL_NM"].str.upper())
        [["OPPOR_ID", "ADVC_TOOL_NM"]]
        .drop_duplicates()
    )
    tracking_count_tool_use_pre2 = (
        tracking_all
        .dropna(subset=["OPPOR_ID", "ADVC_TOOL_NM"])
        .assign(ADVC_TOOL_NM=lambda d: d["ADVC_TOOL_NM"].str.upper())
        .groupby("OPPOR_ID")["ADVC_TOOL_NM"].nunique()
        .reset_index(name="count_unique_tool_used")
        .sort_values("count_unique_tool_used", ascending=False)
    )
else:
    tracking_tool_use_distinct = pd.DataFrame(columns=["OPPOR_ID", "ADVC_TOOL_NM"])
    tracking_count_tool_use_pre2 = pd.DataFrame(columns=["OPPOR_ID", "count_unique_tool_used"])

# ----------------------------
# AOT (Teradata)
# ----------------------------
aot_sql = f"""
select oppor_id, count(*) as count_aot
from ddw01.evnt_prod_aot
where ess_src_evnt_dt between DATE '{wk_start_mini4.isoformat()}' and DATE '{wk_end.isoformat()}'
  and oppor_id is not null
group by 1
"""
# aot_all_oppor = simple_teradata_query(aot_sql)

# Offline fallback:
aot_all_oppor_path = AC_DIR / "aot_all_oppor.csv"
if aot_all_oppor_path.exists():
    aot_all_oppor = pd.read_csv(aot_all_oppor_path)
else:
    aot_all_oppor = pd.DataFrame(columns=["oppor_id", "count_aot"])

aot_all_oppor_unique = aot_all_oppor[["oppor_id"]].drop_duplicates()

# ----------------------------
# Link AOT to Client360
# ----------------------------
c360_detail_link_aot = (
    c360_detail
    .merge(aot_all_oppor_unique.rename(columns={"oppor_id": "aot_oppor_id"}),
           left_on="oppor_id", right_on="aot_oppor_id", how="left")
)
# Condition from SAS (only sets a flag for 'Personal Accounts'):
c360_detail_link_aot["C360_POA_LINK_AOT"] = np.where(
    (c360_detail_link_aot.get("PROD_CATG_NM") == "Personal Accounts") &
    (c360_detail_link_aot["aot_oppor_id"].notna()), 1, 0
)

# ----------------------------
# Filtered views (mirror SAS DATA step)
# ----------------------------
c360_detail_more = c360_detail_more_in_pre.copy()
c360_detail_more["oppor_stage_nm_f"] = c360_detail_more["oppor_stage_nm"].map(stagefmt).fillna(c360_detail_more["oppor_stage_nm"])

c360_detail_filtered = c360_detail_more[
    (c360_detail_more.get("asct_prod_fmly_nm") == "Risk Protection") &
    (c360_detail_more.get("lob") == "Retail") &
    (c360_detail_link_aot.set_index(c360_detail_link_aot.index).reindex(c360_detail_more.index)["C360_POA_LINK_AOT"].fillna(0).eq(0)) &
    (c360_detail_more["oppor_stage_nm"].isin(["Opportunity Won", "Opportunity Lost"]))
].copy()

# ----------------------------
# Rationale validation
# ----------------------------
# We only assess where IS_PROD_APRP_FOR_CLNT == 'Not Appropriate - Rationale'
pa_subset = c360_detail_more_in_pre.loc[
    c360_detail_more_in_pre["IS_PROD_APRP_FOR_CLNT"] == "Not Appropriate - Rationale",
    ["evnt_id", "IS_PROD_APRP_FOR_CLNT", "CLNT_RTNL_TXT"]
].copy()

def rationale_validity(txt: str) -> dict:
    """Mirror the simple SAS checks in python:
       - length > 5
       - not all the same char
       - at least 2 alnum characters
    """
    out = {"xfail_chars_gt5": 1, "xfail_rep_char": 1, "xfail_ge_2_alnum": 1}
    if pd.isna(txt):
        return out
    x = re.sub(r"\s+", " ", str(txt).strip()).upper()
    # length > 5 ?
    out["xfail_chars_gt5"] = 0 if len(x) > 5 else 1
    # not all same char?
    if x:
        first = x[0]
        only_first_removed = re.sub(re.escape(first), "", x)
        out["xfail_rep_char"] = 0 if len(only_first_removed) > 0 else 1
    # at least 2 alnum?
    alnum = re.sub(r"[^A-Z0-9]", "", x)
    out["xfail_ge_2_alnum"] = 0 if len(alnum) >= 2 else 1
    return out

if not pa_subset.empty:
    checks = pa_subset["CLNT_RTNL_TXT"].apply(rationale_validity).apply(pd.Series)
    pa_subset = pd.concat([pa_subset, checks], axis=1)
    pa_subset["prod_not_aprp_rtnl_txt_cat"] = np.where(
        (pa_subset[["xfail_chars_gt5", "xfail_rep_char", "xfail_ge_2_alnum"]].sum(axis=1) == 0),
        "Valid",
        "Invalid"
    )
else:
    pa_subset["prod_not_aprp_rtnl_txt_cat"] = pd.Series(dtype=str)

# Merge back to build c360_detail_more_in
c360_detail_more_in = c360_detail_more_in_pre.merge(
    pa_subset[["evnt_id", "prod_not_aprp_rtnl_txt_cat"]],
    on="evnt_id",
    how="left"
)

def map_pa_category(row):
    x = row.get("IS_PROD_APRP_FOR_CLNT")
    if pd.isna(x) or x == "":
        return "Not Available"
    if x == "Not Appropriate - Rationale":
        return row.get("prod_not_aprp_rtnl_txt_cat", "Invalid")
    return x

c360_detail_more_in["prod_not_aprp_rtnl_txt_cat"] = c360_detail_more_in.apply(map_pa_category, axis=1)

# ----------------------------
# Tool usage flag on c360_detail_pre
# ----------------------------
if not tracking_count_tool_use_pre2.empty:
    tracking_tool_use = tracking_count_tool_use_pre2[["OPPOR_ID"]].copy()
    tracking_tool_use["tool_used"] = "Tool Used"
else:
    tracking_tool_use = pd.DataFrame(columns=["OPPOR_ID", "tool_used"])

c360_detail_flagged = c360_detail_pre.merge(
    tracking_tool_use, on="OPPOR_ID", how="left"
)
c360_detail_flagged["TOOL_USED"] = np.where(
    c360_detail_flagged["tool_used"].isna(), "Tool Not Used", "Tool Used"
)
c360_detail_flagged.drop(columns=["tool_used"], inplace=True)

# ----------------------------
# Prep & de-dup by opportunity
# ----------------------------
c360_sorted = c360_detail_flagged.sort_values(["OPPOR_ID"])
# level_oppor: first row of each OPPOR_ID = 1, etc. (SAS starts from 1 after increment)
c360_sorted["level_oppor"] = c360_sorted.groupby("OPPOR_ID").cumcount() + 1

# Attach tool names
tmp_pa_c360_4ac_pre = c360_sorted.merge(
    tracking_tool_use_distinct, on="OPPOR_ID", how="left"
)
tmp_pa_c360_4ac = tmp_pa_c360_4ac_pre[tmp_pa_c360_4ac_pre["level_oppor"] == 1].copy()

# Build AC rows (constant columns kept simple)
tmp = tmp_pa_c360_4ac
tmp["RegulatoryName"] = "CB6"
tmp["LOB"] = "Retail"
tmp["ReportName"] = "CB6 Client360 Product Appropriateness"
tmp["ControlRisk"] = "Completeness"
tmp["TestType"] = "Anomaly"
tmp["TestPeriod"] = "Origination"
tmp["ProductType"] = tmp.get("PROD_CATG_NM", pd.Series(dtype=str))
tmp["segment"] = "Account Open"
tmp["segment2"] = tmp.get("ASCT_PROD_FMLY_NM", pd.Series(dtype=str))
tmp["segment3"] = tmp.get("PROD_SRVC_NM", pd.Series(dtype=str))
tmp["segment6"] = tmp.get("oppor_stage_nm", pd.Series(dtype=str))
tmp["segment7"] = tmp.get("TOOL_USED", pd.Series(dtype=str))
tmp["segment8"] = tmp.get("ADVC_TOOL_NM", pd.Series(dtype=str))
tmp["segment10"] = pd.to_datetime(tmp.get("evnt_dt")).dt.strftime("%Y%m")
tmp["CommentCode"] = "COM13"
tmp["Comments"] = tmp["CommentCode"].map(cs_cmt).fillna("Population Distribution")
# SnapDate = end of week (Sunday-anchored)
snap_end = end_of_week(WEEK7_END_WEEKDAY, TODAY)
tmp["SnapDate"] = snap_end
tmp["DateCompleted"] = TODAY

cols_for_ac = [
    "RegulatoryName","LOB","ReportName","ControlRisk","TestType","TestPeriod",
    "ProductType","segment","segment2","segment3","segment6","segment7","segment8",
    "segment10","CommentCode","Comments","SnapDate","DateCompleted",
    "IS_PROD_APRP_FOR_CLNT","prod_not_aprp_rtnl_txt_cat"
]
# Fill missing keys safely
for c in cols_for_ac:
    if c not in tmp.columns:
        tmp[c] = np.nan

work_tmp_ac = tmp[cols_for_ac + ["evnt_dt"]].copy()

# Build segment4 (pa_result bucket) and segment5 (rationale validity)
def pa_bucket(x):
    if x == "Product Appropriateness assessed outside Client 360":
        return "Product Appropriateness assessed outside Client 360"
    if x == "Not Appropriate - Rationale":
        return "Product Not Appropriate"
    if x == "Client declined product appropriateness assessment":
        return "Client declined product appropriateness assessment"
    if x == "Product Appropriate":
        return "Product Appropriate"
    return "Missing"

work_tmp_ac["segment4"] = work_tmp_ac["IS_PROD_APRP_FOR_CLNT"].apply(pa_bucket)
work_tmp_ac["segment5"] = work_tmp_ac["prod_not_aprp_rtnl_txt_cat"]

# Aggregate AC (group and count)
group_cols = [
    "RegulatoryName","LOB","ReportName","ControlRisk","TestType","TestPeriod",
    "ProductType",
    # RDE constant from SAS:
]
work_tmp_ac["RDE"] = "PA002_Client360_Completeness_RDE"
group_cols += ["RDE","segment","segment2","segment3","segment4","segment5",
               "segment6","segment7","segment8","segment10","CommentCode","Comments",
               "DateCompleted","SnapDate"]

tmp_pa_c360_ac_assessment = (
    work_tmp_ac
    .groupby(group_cols, dropna=False)
    .size()
    .reset_index(name="Volume")
)
tmp_pa_c360_ac_assessment["Amount"] = np.nan

# ----------------------------
# Append to "ac.pa_client360_autocomplete" (CSV as a simple store)
# ----------------------------
if AC_MASTER_FILE.exists():
    ac_master = pd.read_csv(AC_MASTER_FILE)
else:
    ac_master = pd.DataFrame(columns=tmp_pa_c360_ac_assessment.columns)

ac_master = pd.concat([ac_master, tmp_pa_c360_ac_assessment], ignore_index=True)

# Sort and dedupe by keys similar to SAS
sort_keys = [
    "RegulatoryName","LOB","ReportName","ControlRisk","TestType","TestPeriod",
    "ProductType","RDE","segment","segment2","segment3","segment4","segment5",
    "segment6","segment7","segment8","segment10","DateCompleted"
]
ac_master.sort_values(sort_keys, inplace=True)

# In SAS: remove records where (first.DateCompleted and last.DateCompleted)
# This keeps duplicates except the single unique? The SAS code:
#   if not (first.DateCompleted and last.DateCompleted);
# means drop solitary rows in a BY group keyed on DateCompleted. We’ll mimic simply:
def drop_singletons(df, by_cols):
    # mark group sizes by those columns, then drop rows where group size == 1
    gsize = df.groupby(by_cols).size().rename("gsize")
    tmp = df.merge(gsize, left_on=by_cols, right_index=True, how="left")
    return tmp[tmp["gsize"] > 1].drop(columns=["gsize"])

ac_final = drop_singletons(ac_master, sort_keys)

# Save master + final export
ac_master.to_csv(AC_MASTER_FILE, index=False)
write_excel(ac_final, BASE_OUT_DIR / "pa_client360_autocomplete.xlsx", sheet_name="autocomplete")

# ----------------------------
# Detail extract (subset and export)
# ----------------------------
t1 = tmp_pa_c360_4ac_pre.copy()
# pa_result buckets (same as earlier)
t1["pa_result"] = t1["IS_PROD_APRP_FOR_CLNT"].apply(pa_bucket)

detail_cols = {
    "segment10": "evnt_month",
    "DateCompleted": "reporting_date",
    "EVNT_DT": "event_date",
    "EVNT_THSTMP": "event_timestamp",
    "OPPOR_ID": "opportunity_id",
    "OPPOR_REC_TYP": "opportunity_type",
    "PROD_CD": "product_code",
    "PROD_CATG_NM": "product_category_name",
    "ASCT_PROD_FMLY_NM": "product_family_name",
    "PROD_SRVC_NM": "product_name",
    "oppor_stage_nm": "oppor_stage_nm",
    "TOOL_USED": "tool_used",
    "ADVC_TOOL_NM": "tool_nm",
    "CLNT_RTNL_TXT": "pa_rationale",
    "prod_not_aprp_rtnl_txt_cat": "pa_rationale_validity",
    "RBC_OPPOR_OWN_ID": "employee_id",
    "OCCPT_JOB_CD": "job_code",
    "HR_POSN_TITL_EN": "position_title",
    "ORG_UNT_NO": "employee_transit",
    "POSN_STRT_DT": "position_start_date",
}

keep_cols = list(detail_cols.keys()) + ["pa_result", "evnt_dt"]
for c in keep_cols:
    if c not in t1.columns:
        t1[c] = np.nan

detail = t1[
    t1["pa_result"].isin([
        "Product Not Appropriate",
        "Missing",
        "Product Appropriateness assessed outside Client 360"
    ])
].copy()

detail["week_week_ending"] = pd.to_datetime(detail["evnt_dt"]).dt.date.apply(
    lambda d: end_of_week(WEEK7_END_WEEKDAY, d) if pd.notna(d) else pd.NaT
)

# Final selection/renaming
detail_out = pd.DataFrame({
    "evnt_month": detail["segment10"],
    "reporting_date": pd.to_datetime(detail["DateCompleted"]).dt.date,
    "week_week_ending": detail["week_week_ending"],
    "event_date": pd.to_datetime(detail["EVNT_DT"]).dt.date,
    "event_timestamp": pd.to_datetime(detail["EVNT_THSTMP"], errors="coerce"),
    "opportunity_id": detail["OPPOR_ID"],
    "opportunity_type": detail["OPPOR_REC_TYP"],
    "product_code": detail["PROD_CD"],
    "product_category_name": detail["PROD_CATG_NM"],
    "product_family_name": detail["ASCT_PROD_FMLY_NM"],
    "product_name": detail["PROD_SRVC_NM"],
    "oppor_stage_nm": detail["oppor_stage_nm"],
    "tool_used": detail["TOOL_USED"],
    "tool_nm": detail["ADVC_TOOL_NM"],
    "pa_result": detail["pa_result"],
    "pa_rationale": detail["CLNT_RTNL_TXT"],
    "pa_rationale_validity": detail["prod_not_aprp_rtnl_txt_cat"],
    "employee_id": detail["RBC_OPPOR_OWN_ID"],
    "job_code": detail["OCCPT_JOB_CD"],
    "position_title": detail["HR_POSN_TITL_EN"],
    "employee_transit": detail["ORG_UNT_NO"],
    "position_start_date": pd.to_datetime(detail["POSN_STRT_DT"], errors="coerce").dt.date,
})

detail_path = OUT_DIR_TODAY / f"pa_client360_detail_{RUNDAY}.xlsx"
write_excel(detail_out, detail_path, sheet_name="detail")

# Pivot export (same as autocomplete final per spec)
pivot_path = BASE_OUT_DIR / "pa_client360_pivot.xlsx"
write_excel(ac_final, pivot_path, sheet_name="Autocomplete")

logging.info("All done. Log: %s", logfile)
logging.info("Outputs:")
logging.info("  - %s", BASE_OUT_DIR / "pa_client360_autocomplete.xlsx")
logging.info("  - %s", pivot_path)
logging.info("  - %s", detail_path)
