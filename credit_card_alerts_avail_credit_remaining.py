#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CREDIT CARD ALERTS – AVAIL_CREDIT_REMAINING (Weekly Controls)
Python port of the provided SAS program.

Owner      : <your-team-or-name>
Environment: ENV=DEV/PROD
Purpose    : Extract from Hive, join prefs + deliveries, compute
             timeliness/accuracy/completeness, export detail & AC table.
"""

import os
import sys
import math
import json
import shutil
import logging
import subprocess
from pathlib import Path
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd

# ---------- Config / Environment ----------
ENV      = os.getenv("ENV", "DEV")  # mirrors &env defaulting to DEV
REGPATH  = Path(os.getenv("REGPATH", Path.home()))  # mirrors &regpath
LOGPATH  = Path(os.getenv("LOGPATH", REGPATH))
OUTPATH  = Path(os.getenv("OUTPATH", REGPATH))
USER     = os.getenv("USER") or os.getenv("USERNAME") or "unknown"

# Standard run-date label (YYYYMMDD) used for foldering/exports
REPORT_DT = date.today()
LABEL     = REPORT_DT.strftime("%Y%m%d")

LOGFILE   = LOGPATH / f"cbg_pa_cards_{LABEL}.log"
LSTFILE   = LOGPATH / f"cbg_pa_cards_{LABEL}.lst"  # informational only
OUTDIR    = OUTPATH / LABEL
OUTDIR.mkdir(parents=True, exist_ok=True)

# For parity with SAS libnames
DATAOUT = OUTDIR
AC      = OUTDIR

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGFILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("cards_avail_credit_remaining")

def banner():
    log.info(">>>>>>>>>>> Program         : credit_card_alerts_avail_credit_remaining.py <<<<<<<<<<<")
    log.info(">>>>>>>>>>> Start Time      : %s <<<<<<<<<<<", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info(">>>>>>>>>>> User ID         : %s <<<<<<<<<<<", USER)
    log.info(">>>>>>>>>>> Host            : %s <<<<<<<<<<<", os.uname().nodename if hasattr(os, "uname") else "windows")
    log.info(">>>>>>>>>>> Env             : %s <<<<<<<<<<<", ENV)
banner()

# ---------- Kerberos (adjust to your infra) ----------
def kinit():
    """
    Mirrors: x 'cd; kinit -f PRYUBSRWIN@MAPLE.FG.RBC.COM -t PRYUBSRWIN_PROD.kt';
    If your session already has a TGT, this is a no-op.
    """
    princ = os.getenv("KERB_PRINCIPAL", "PRYUBSRWIN@MAPLE.FG.RBC.COM")
    keytab = os.getenv("KERB_KEYTAB",   "PRYUBSRWIN_PROD.kt")
    try:
        subprocess.run(
            ["kinit", "-f", princ, "-t", keytab],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        log.info("Kerberos kinit OK for %s using %s", princ, keytab)
    except FileNotFoundError:
        log.warning("kinit not found; assuming existing Kerberos context")
    except subprocess.CalledProcessError as e:
        log.error("kinit failed: %s", e.stderr.decode("utf-8", errors="ignore"))

# Uncomment if you actually need to renew within the script
# kinit()

# ---------- Date Window Calculation ----------
# SAS:
# start_dt_ini   = '30JUN2022'd
# week_end_dt_ini= '25JUL2022'd
START_DT_INI    = date(2022, 6, 30)
WEEK_END_DT_INI = date(2022, 7, 25)

def sas_week4_end(d: date) -> date:
    """
    SAS intnx('week.4', d, 0) aligns to a week ending on Thursday.
    We return the week-ending Thursday for the week containing d.
    """
    # Python weekday(): Monday=0 ... Sunday=6
    # We want Thursday=3 as the "end of week"
    weekday = d.weekday()
    delta_to_thu = (3 - weekday) % 7  # days forward to Thursday
    return d + timedelta(days=delta_to_thu)

# Current report’s weekend = week.4 end minus 2 days
week4_end = sas_week4_end(REPORT_DT)
END_DT    = week4_end - timedelta(days=2)

START_DT  = START_DT_INI if END_DT <= WEEK_END_DT_INI else (END_DT - timedelta(days=6))

def d2str(d: date) -> str:
    return d.strftime("%Y-%m-%d")

WEEK_START_DT = d2str(START_DT)        # 'yyyy-mm-dd'
WEEK_END_DT   = d2str(END_DT)
PARDT         = d2str(START_DT - timedelta(days=7))
WEEK_END_DT_P1= d2str(END_DT + timedelta(days=1))

log.info("Window: %s -> %s (p+1=%s) prevStart=%s", WEEK_START_DT, WEEK_END_DT, WEEK_END_DT_P1, PARDT)
log.info("report_dt=%s", REPORT_DT.strftime("%Y-%m-%d"))

# ---------- Hive Connection ----------
# Choose one connector approach. PyHive example shown below.
# Requires: pip install pyhive[hive] thrift_sasl sasl==0.2.1 thrift==0.16.0 pandas numpy openpyxl
from pyhive import hive

HIVE_KERBEROS_SERVICE_NAME = os.getenv("HIVE_KRB_SERVICE", "hive")

# If you rely on ZK service discovery, use your HS2 load balancer or host list.
# Example placeholders (adjust to your environment):
HIVE_HOST   = os.getenv("HIVE_HOST", "strplpaed12007.fg.rbc.com")
HIVE_PORT   = int(os.getenv("HIVE_PORT", "10000"))
HIVE_SCHEMA = os.getenv("HIVE_SCHEMA", "prod_brt0_ess")

def hive_conn():
    """
    Kerberized HS2 connection. If you need ZooKeeper discovery,
    route via your org's HS2 front-door or use JDBC via JayDeBeApi.
    """
    return hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        username=USER,               # Kerberos principal must map
        auth="KERBEROS",
        kerberos_service_name=HIVE_KERBEROS_SERVICE_NAME,
        database=HIVE_SCHEMA,
    )

def hive_query(sql: str) -> pd.DataFrame:
    log.info("Running Hive query (%d chars)...", len(sql))
    with hive_conn() as conn:
        with conn.cursor() as cur:
            # Session settings to mirror SAS pass-through
            cur.execute("SET tez.queue.name=PRYUB")
            cur.execute("SET hive.execution.engine=tez")
            cur.execute("SET hive.compute.query.using.stats=true")
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    log.info(" -> %d rows, %d cols", len(df), df.shape[1] if not df.empty else 0)
    return df

# ---------- 3a. Alert Decision Cards ----------
SQL_XB80_CARDS = f"""
select *
from (
  select
    event_activity_type,
    source_event_id,
    partition_date,
    cast(regexp_replace(eventattributes['ess_process_timestamp'],'T|Z',' ') as timestamp)     as ess_process_timestamp,
    cast(regexp_replace(eventattributes['ess_src_event_timestamp'],'T|Z',' ') as timestamp)   as ess_src_event_timestamp,
    get_json_object(eventattributes['sourceEventHeader'],'$.eventId')                         as eventId,
    cast(regexp_replace(get_json_object(eventattributes['sourceEventHeader'],'$.eventTimestamp'),'T|Z',' ') as timestamp) as eventTimestamp,
    get_json_object(eventattributes['eventPayload'],'$.accountId')                            as accountId,
    get_json_object(eventattributes['eventPayload'],'$.alertType')                            as alertType,
    cast(get_json_object(eventattributes['eventPayload'],'$.thresholdAmount') as decimal(10,2)) as thresholdAmount,
    get_json_object(eventattributes['eventPayload'],'$.customerId')                           as customerId,
    get_json_object(eventattributes['eventPayload'],'$.accountCurrency')                      as accountCurrency,
    cast(get_json_object(eventattributes['eventPayload'],'$.creditLimit') as decimal(10,2))   as creditLimit,
    get_json_object(eventattributes['eventPayload'],'$.maskedAccount')                        as maskedAccount,
    get_json_object(eventattributes['eventPayload'],'$.decisionId')                           as decisionId,
    cast(get_json_object(eventattributes['eventPayload'],'$.alertAmount') as decimal(10,2))   as alertAmount
  from prod_brt0_ess.xb80__credit_card_system_interface
  where partition_date > '{PARDT}'
    and event_activity_type = 'Alert Decision Cards'
    and get_json_object(eventattributes['eventPayload'],'$.alertType') = 'AVAIL_CREDIT_REMAINING'
    and event_timestamp between '{WEEK_START_DT}' and '{WEEK_END_DT_P1}'
) d
"""

# ---------- 3b. Client Alert Preferences ----------
SQL_PREFS = f"""
select * from
(
  -- initial load
  select
    event_timestamp                                              as event_timestamp_p,
    event_channel_type                                           as event_channel_type_p,
    event_activity_type                                          as event_activity_type_p,
    partition_date                                               as partition_date_p,
    cast(regexp_replace(eventattributes['ess_process_timestamp'],'T|Z',' ') as timestamp)     as ess_process_timestamp_p,
    cast(regexp_replace(eventattributes['ess_src_event_timestamp'],'T|Z',' ') as timestamp)   as ess_src_event_timestamp_p,
    get_json_object(eventattributes['sourceEventHeader'],'$.eventId')                         as eventId_p,
    cast(regexp_replace(get_json_object(eventattributes['sourceEventHeader'],'$.eventTimestamp'),'T|Z',' ') as timestamp) as eventtimestamp_p,
    get_json_object(eventattributes['sourceEventHeader'],'$.eventActivityName')               as eventActivityName_p,
    get_json_object(eventattributes['eventPayload'],'$.preferenceType')                       as preferenceType_p,
    get_json_object(eventattributes['eventPayload'],'$.clientId')                             as clientId_p,
    get_json_object(eventattributes['eventPayload'],'$.isBusiness')                           as isBusiness_p,
    get_json_object(eventattributes['eventPayload'],'$.sendAlertEligible')                    as sendAlertEligible_p,
    get_json_object(eventattributes['eventPayload'],'$.active')                               as active_p,
    get_json_object(eventattributes['eventPayload'],'$.threshold')                            as threshold_p,
    get_json_object(eventattributes['eventPayload'],'$.custId')                               as custId_p,
    get_json_object(eventattributes['eventPayload'],'$.account')                              as account_p,
    get_json_object(eventattributes['eventPayload'],'$.maskedAccountNo')                      as maskedAccountNo_p,
    get_json_object(eventattributes['eventPayload'],'$.externalAccount')                      as externalAccount_p,
    get_json_object(eventattributes['eventPayload'],'$.productType')                          as productType_p
  from prod_brt0_ess.fflq__client_alert_preferences_dep_initial_load
  where event_activity_type in ('Create Account Preference','Update Account Preference')
    and get_json_object(eventattributes['eventPayload'],'$.preferenceType') = 'AVAIL_CREDIT_REMAINING'
    and get_json_object(eventattributes['eventPayload'],'$.productType')    = 'CREDIT_CARD'
    and partition_date = '20220324'

  union all

  -- incremental
  select
    event_timestamp, event_channel_type, event_activity_type, partition_date,
    cast(regexp_replace(eventattributes['ess_process_timestamp'],'T|Z',' ') as timestamp),
    cast(regexp_replace(eventattributes['ess_src_event_timestamp'],'T|Z',' ') as timestamp),
    get_json_object(eventattributes['sourceEventHeader'],'$.eventId'),
    cast(regexp_replace(get_json_object(eventattributes['sourceEventHeader'],'$.eventTimestamp'),'T|Z',' ') as timestamp),
    get_json_object(eventattributes['sourceEventHeader'],'$.eventActivityName'),
    get_json_object(eventattributes['eventPayload'],'$.preferenceType'),
    get_json_object(eventattributes['eventPayload'],'$.clientId'),
    get_json_object(eventattributes['eventPayload'],'$.isBusiness'),
    get_json_object(eventattributes['eventPayload'],'$.sendAlertEligible'),
    get_json_object(eventattributes['eventPayload'],'$.active'),
    get_json_object(eventattributes['eventPayload'],'$.threshold'),
    get_json_object(eventattributes['eventPayload'],'$.custId'),
    get_json_object(eventattributes['eventPayload'],'$.account'),
    get_json_object(eventattributes['eventPayload'],'$.maskedAccountNo'),
    get_json_object(eventattributes['eventPayload'],'$.externalAccount'),
    get_json_object(eventattributes['eventPayload'],'$.productType')
  from prod_brt0_ess.fflq__client_alert_preferences_dep
  where event_timestamp < '{WEEK_END_DT_P1}'
    and event_activity_type in ('Create Account Preference','Update Account Preference')
    and get_json_object(eventattributes['eventPayload'],'$.preferenceType') = 'AVAIL_CREDIT_REMAINING'
    and get_json_object(eventattributes['eventPayload'],'$.productType')    = 'CREDIT_CARD'
) p
"""

# ---------- 3c. Alert Delivery Audit ----------
SQL_INBOX = f"""
select
  cast(regexp_replace(eventattributes['ess_process_timestamp'],'T|Z',' ') as timestamp)          as ess_process_timestamp_a,
  cast(regexp_replace(eventattributes['ess_src_event_timestamp'],'T|Z',' ') as timestamp)        as ess_src_event_timestamp_a,
  event_activity_type                                                                                     as event_activity_type_a,
  source_event_id                                                                                         as source_event_id_a,
  partition_date                                                                                          as partition_date_a,
  event_timestamp                                                                                         as event_timestamp_a,
  cast(regexp_replace(get_json_object(eventattributes['sourceEventHeader'],'$.eventTimestamp'),'T|Z',' ') as timestamp) as eventtimestamp_a,
  get_json_object(eventattributes['sourceEventHeader'],'$.eventId')                                       as eventId_a,
  get_json_object(eventattributes['sourceEventHeader'],'$.eventActivityName')                             as eventActivityName_a,
  get_json_object(eventattributes['eventPayload'],'$.alertSent')                                          as alertSent_a,
  get_json_object(eventattributes['eventPayload'],'$.sendInbox')                                          as sendInbox_a,
  get_json_object(eventattributes['eventPayload'],'$.alertType')                                          as alertType_a,
  cast(get_json_object(eventattributes['eventPayload'],'$.thresholdAmount') as decimal(12,2))             as thresholdAmount_a,
  get_json_object(eventattributes['eventPayload'],'$.sendSMS')                                            as sendSMS_a,
  get_json_object(eventattributes['eventPayload'],'$.sendPush')                                           as sendPush_a,
  get_json_object(eventattributes['eventPayload'],'$.maskedAccount')                                      as maskedAccount_a,
  get_json_object(eventattributes['eventPayload'],'$.reasonCode')                                         as reasonCode_a,
  get_json_object(eventattributes['eventPayload'],'$.decisionId')                                         as decisionId_a,
  cast(get_json_object(eventattributes['eventPayload'],'$.alertAmount') as decimal(12,2))                 as alertAmount_a,
  get_json_object(eventattributes['eventPayload'],'$.accountId')                                          as accountId_a,
  get_json_object(eventattributes['eventPayload'],'$.accountProduct')                                     as accountProduct_a,
  get_json_object(eventattributes['eventPayload'],'$.sendEmail')                                          as sendEmail_a
from prod_brt0_ess.fft0__alert_inbox_dep
where event_activity_type = 'Alert Delivery Audit'
  and get_json_object(eventattributes['eventPayload'],'$.alertType')='AVAIL_CREDIT_REMAINING'
  and event_timestamp >= '{WEEK_START_DT}'
"""

# --- Extracts ---
log.info("Extracting from Hive...")
xb80_cards     = hive_query(SQL_XB80_CARDS)
cards_dec_pref = hive_query(SQL_PREFS)
fft0_inbox     = hive_query(SQL_INBOX)

# --- 4. Dedupes, Final Join & Basic Profiling ---
# Join decisions to preferences
log.info("Joining decisions with preferences...")
merged = xb80_cards.merge(
    cards_dec_pref,
    left_on=["accountId","customerId"],
    right_on=["externalAccount_p","custId_p"],
    how="left",
    suffixes=("","_pdup")
)

# dec_tm_ge_pref_tm: eventtimestamp > eventtimestamp_p
for col in ["eventTimestamp","eventtimestamp_p"]:
    if col in merged.columns:
        merged[col] = pd.to_datetime(merged[col], errors="coerce")
merged["dec_tm_ge_pref_tm"] = np.where(
    (merged["eventTimestamp"].notna()) & (merged["eventtimestamp_p"].notna()) & (merged["eventTimestamp"] > merged["eventtimestamp_p"]),
    "Y","N"
)

# Keep one decision per decisionId – prefer where decision is newer than pref, then latest eventtimestamp_p
log.info("Deduping decisions by decisionId with preference to newer-than-pref...")
merged_sorted = merged.sort_values(
    by=["decisionId","dec_tm_ge_pref_tm","eventtimestamp_p"],
    ascending=[True, False, False],
    kind="mergesort"
)
# First occurrence per decisionId
xb80_cards_dec_pref2 = merged_sorted.drop_duplicates(subset=["decisionId"], keep="first").copy()

# Dedup deliveries to one per decisionId_a (latest by eventtimestamp_a)
if not fft0_inbox.empty:
    for col in ["eventtimestamp_a"]:
        fft0_inbox[col] = pd.to_datetime(fft0_inbox[col], errors="coerce")
    fft0_inbox2 = (
        fft0_inbox.sort_values(by=["decisionId_a","eventtimestamp_a"], ascending=[True, True])
                  .drop_duplicates(subset=["decisionId_a"], keep="last")
                  .copy()
    )
else:
    fft0_inbox2 = fft0_inbox.copy()

# Join decisions to deliveries
log.info("Joining decisions with deliveries...")
cards_dec_pref_inbox = xb80_cards_dec_pref2.merge(
    fft0_inbox2, left_on="decisionId", right_on="decisionId_a", how="left", suffixes=("","")
)

# Simple profiling freq (optional logs)
log.info("Profiling dec_tm_ge_pref_tm vs isBusiness_p")
if "isbusiness_p" in cards_dec_pref_inbox.columns:
    log.info("Counts:\n%s", cards_dec_pref_inbox[["dec_tm_ge_pref_tm","isbusiness_p"]].value_counts(dropna=False).to_string())

# --- 5. Enrichment & Latency Bucketing ---
cdf = cards_dec_pref_inbox.copy()

# Exclude business clients where decision is after pref (as per original)
if "isbusiness_p" in cdf.columns:
    cdf = cdf[~((cdf["isbusiness_p"] == "true") & (cdf["dec_tm_ge_pref_tm"] == "Y"))].copy()

# Dates
def to_dt(s, fmt=None):
    return pd.to_datetime(s, errors="coerce", format=fmt)

cdf["ess_src_event_timestamp"] = to_dt(cdf.get("ess_src_event_timestamp"))
cdf["ess_process_timestamp"]   = to_dt(cdf.get("ess_process_timestamp"))
cdf["event_timestamp"]         = to_dt(cdf.get("event_timestamp"))
cdf["eventTimestamp"]          = to_dt(cdf.get("eventTimestamp"))
cdf["eventtimestamp_a"]        = to_dt(cdf.get("eventtimestamp_a"))

cdf["src_event_date"] = cdf["ess_src_event_timestamp"].dt.date
cdf["process_date"]   = cdf["ess_process_timestamp"].dt.date
cdf["event_date"]     = cdf["event_timestamp"].dt.date
cdf["decision_date"]  = cdf["eventTimestamp"].dt.date

# threshold_limit_check = alertamount <= thresholdamount
cdf["thresholdAmount"] = pd.to_numeric(cdf.get("thresholdAmount"), errors="coerce")
cdf["alertAmount"]     = pd.to_numeric(cdf.get("alertAmount"), errors="coerce")
cdf["threshold_limit_check"] = np.where(
    (cdf["alertAmount"].notna()) & (cdf["thresholdAmount"].notna()) & (cdf["alertAmount"] <= cdf["thresholdAmount"]),
    "Y","N"
)

# Missing flags and SLA
cdf["Found_Missing"] = np.where(cdf["eventtimestamp_a"].isna() | cdf["eventTimestamp"].isna(), "Y", "")
cdf["Time_Diff"] = (cdf["eventtimestamp_a"] - cdf["eventTimestamp"]).dt.total_seconds()
cdf["SLA_Ind"]   = np.where(cdf["Time_Diff"].notna() & (cdf["Time_Diff"] <= 1800), "Y","N")

def alert_time_bucket(sec):
    if pd.isna(sec):    return "00 - Timestamp is missing"
    if sec < 0:         return "00 - Less than 0 seconds"
    # Then successive thresholds
    bounds = [
        (1800,   "01 - <= 30 minutes"),
        (3600,   "02 - >30 & <=60 mins"),
        (7200,   "03 - >1 & <=2 hours"),
        (10800,  "04 - >2 & <=3 hours"),
        (14400,  "05 - >3 & <=4 hours"),
        (18000,  "06 - >4 & <=5 hours"),
        (21600,  "07 - >5 & <=6 hours"),
        (25200,  "08 - >6 & <=7 hours"),
        (28800,  "09 - >7 & <=8 hours"),
        (32400,  "10 - >8 & <=9 hours"),
        (36000,  "11 - >9 & <=10 hours"),
        (86400,  "12 - >10 & <=24 hours"),
        (172800, "13 - >1 & <=2 days"),
        (259200, "14 - >2 & <=3 days"),
    ]
    for hi, lab in bounds:
        if sec <= hi:
            return lab
    return "15 - >3 days"

cdf["Alert_Time"] = cdf["Time_Diff"].apply(alert_time_bucket)

# Persist main final table
cards_alert_final = cdf.copy()
cards_alert_final_path = DATAOUT / "cards_alert_final.parquet"
cards_alert_final.to_parquet(cards_alert_final_path, index=False)
log.info("Wrote %s", cards_alert_final_path)

# --- 6. Reporting / Samples / Summary ---

# ac_Cards_Alert_Time_Count
ac_Cards_Alert_Time_Count = (
    cards_alert_final.groupby("Alert_Time", dropna=False)
    .size()
    .reset_index(name="Decision_Count")
    .sort_values("Alert_Time")
)
ac_time_count_path = DATAOUT / "ac_Cards_Alert_Time_Count.csv"
ac_Cards_Alert_Time_Count.to_csv(ac_time_count_path, index=False)

# Quick counts (log)
log.info("Total_Records: %d; Total_Distinct_DecisionId: %d",
         len(cards_alert_final),
         cards_alert_final["decisionId"].nunique(dropna=True))

# Stratified SRS sample per decision_date (10 per stratum)
def stratified_sample(df, n=10):
    if len(df) <= n:
        return df
    return df.sample(n=n, random_state=42)

cards_alert_final["decision_date"] = pd.to_datetime(cards_alert_final["decision_date"], errors="coerce")
cards_alert_final_srt = cards_alert_final.sort_values(by=["decision_date","decisionId"])
alert_card_base_samples = (
    cards_alert_final_srt.groupby("decision_date", group_keys=False)
                         .apply(stratified_sample, n=10)
                         .copy()
)
samples_path = DATAOUT / "alert_card_base_samples.parquet"
alert_card_base_samples.to_parquet(samples_path, index=False)

# Accuracy (sample)
def yymmdd6(d):
    # SAS put(date, yymmdd6.) e.g., 202408 -> yymmdd without separators, 6 chars
    return pd.to_datetime(d, errors="coerce").strftime("%y%m%d")

Accuracy = (
    alert_card_base_samples.assign(
        ControlRisk="Accuracy",
        TestType="Sample",
        RDE="Alert010_Accuracy_Available_Credit",
        CommentCode=np.where(alert_card_base_samples["threshold_limit_check"]=="Y","COM16","COM19"),
        Segment10=alert_card_base_samples["decision_date"].apply(yymmdd6),
        DateCompleted=pd.to_datetime(REPORT_DT),
        SnapDate=pd.to_datetime(alert_card_base_samples["decision_date"])\
                   .dt.to_period("W-THU")\
                   .apply(lambda p: p.end_time.date()),  # week.3/e approximated to week end on Thu
    ).groupby(["ControlRisk","TestType","RDE","CommentCode","Segment10","DateCompleted","SnapDate"], dropna=False)
     .agg(Volume=("decisionId","count"),
          Bal=("alertAmount","sum"),
          Amount=("thresholdAmount","sum"))
     .reset_index()
)

# Timeliness (population)
Timeliness = (
    cards_alert_final.assign(
        ControlRisk="Timeliness",
        TestType="Anomaly",
        RDE="Alert011_Timeliness_SLA",
        CommentCode=np.where(cards_alert_final["SLA_Ind"]=="Y","COM16","COM19"),
        Segment10=cards_alert_final["decision_date"].apply(yymmdd6),
        DateCompleted=pd.to_datetime(REPORT_DT),
        SnapDate=pd.to_datetime(cards_alert_final["decision_date"])\
                   .dt.to_period("W-THU")\
                   .apply(lambda p: p.end_time.date()),
    ).groupby(["ControlRisk","TestType","RDE","CommentCode","Segment10","DateCompleted","SnapDate"], dropna=False)
     .agg(Volume=("decisionId","count"),
          Bal=("alertAmount","sum"),
          Amount=("thresholdAmount","sum"))
     .reset_index()
)

# Completeness (population)
Completeness_Recon = (
    cards_alert_final.assign(
        ControlRisk="Completeness",
        TestType="Reconciliation",
        RDE="Alert012_Completeness_All_Clients",
        CommentCode=np.where(cards_alert_final["decisionId_a"].fillna("").astype(str).str.strip() != "","COM16","COM19"),
        Segment10=cards_alert_final["decision_date"].apply(yymmdd6),
        DateCompleted=pd.to_datetime(REPORT_DT),
        SnapDate=pd.to_datetime(cards_alert_final["decision_date"])\
                   .dt.to_period("W-THU")\
                   .apply(lambda p: p.end_time.date()),
    ).groupby(["ControlRisk","TestType","RDE","CommentCode","Segment10","DateCompleted","SnapDate"], dropna=False)
     .agg(Volume=("decisionId","count"),
          Bal=("alertAmount","sum"),
          Amount=("thresholdAmount","sum"))
     .reset_index()
)

# Union (Autocomplete_Table). If you have a $cmt. format, map here; otherwise leave as-is.
Autocomplete_Table = pd.concat([Accuracy, Timeliness, Completeness_Recon], ignore_index=True)
autocomplete_path = DATAOUT / "Autocomplete_Table.csv"
Autocomplete_Table.to_csv(autocomplete_path, index=False)

# --- 7. Failure Detail Exports ---
def to_excel(df, path, sheet_name):
    df = df.copy()
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as xw:
        df.to_excel(xw, index=False, sheet_name=sheet_name)

# Completeness failures
Completeness_Fail = (
    cards_alert_final[cards_alert_final["decisionId_a"].fillna("").str.strip() == ""]
    .assign(
        event_month = cards_alert_final["decision_date"].apply(yymmdd6),
        reporting_date = pd.to_datetime(REPORT_DT),
        event_week_ending = pd.to_datetime(cards_alert_final["decision_date"])\
                                .dt.to_period("W-THU")\
                                .apply(lambda p: p.end_time.date()),
        LOB="Credit Cards",
        Product="Credit Cards",
        account_number=lambda d: d["accountId"],
        decision="AlertDecision",
        event_date=lambda d: d["decision_date"],
        custid_mask=lambda d: "******" + d["customerId"].astype(str).str[-3:].fillna("")
    )[["event_month","reporting_date","event_week_ending","LOB","Product","account_number",
       "decision","thresholdAmount","alertAmount","decisionId","event_date","custid_mask"]]
)
to_excel(Completeness_Fail, OUTDIR / "Alert_Cards_Completeness_Detail.xlsx", "Alert_Cards_Completeness_Detail")

# Timeliness failures (SLA_Ind ne 'Y')
Timeliness_Fail = (
    cards_alert_final[cards_alert_final["SLA_Ind"] != "Y"]
    .assign(
        event_month = cards_alert_final["decision_date"].apply(yymmdd6),
        reporting_date = pd.to_datetime(REPORT_DT),
        event_week_ending = pd.to_datetime(cards_alert_final["decision_date"])\
                                .dt.to_period("W-THU")\
                                .apply(lambda p: p.end_time.date()),
        LOB="Credit Cards",
        Product="Credit Cards",
        account_number=lambda d: d["accountId"],
        decision="AlertDecision",
        event_date=lambda d: d["decision_date"],
        decisionTimestamp=lambda d: d["eventTimestamp"],
        sent_timestamp=lambda d: d["eventtimestamp_a"],
        total_minutes=lambda d: np.ceil(d["Time_Diff"]/60.0),
        custid_mask=lambda d: "******" + d["customerId"].astype(str).str[-3:].fillna("")
    )[["event_month","reporting_date","event_week_ending","LOB","Product","account_number",
       "decision","thresholdAmount","alertAmount","decisionId","event_date",
       "decisionTimestamp","sent_timestamp","total_minutes","custid_mask"]]
)
to_excel(Timeliness_Fail, OUTDIR / "Alert_Cards_Timeliness_Detail.xlsx", "Alert_Cards_Timeliness_Detail")

# Accuracy failures: from sample where threshold_limit_check != 'Y'
Accuracy_Fail = (
    alert_card_base_samples[alert_card_base_samples["threshold_limit_check"] != "Y"]
    .assign(
        event_month = alert_card_base_samples["decision_date"].apply(yymmdd6),
        reporting_date = pd.to_datetime(REPORT_DT),
        event_week_ending = pd.to_datetime(alert_card_base_samples["decision_date"])\
                                .dt.to_period("W-THU")\
                                .apply(lambda p: p.end_time.date()),
        LOB="Credit Cards",
        Product="Credit Cards",
        account_number=lambda d: d["accountId"],
        decision="AlertDecision",
        event_date=lambda d: d["decision_date"],
        custid_mask=lambda d: "******" + d["customerId"].astype(str).str[-3:].fillna("")
    )[["event_month","reporting_date","event_week_ending","LOB","Product","account_number",
       "decision","thresholdAmount","alertAmount","decisionId","event_date","custid_mask"]]
)
to_excel(Accuracy_Fail, OUTDIR / "Alert_Cards_Accuracy_Detail.xlsx", "Alert_Cards_Accuracy_Detail")

# --- 8. Historical AC Table (append/refresh) ---
# Skeleton rowset to guarantee structure
Alert_Cards_AC_week = pd.DataFrame({
    "RegulatoryName": ["C86"],
    "LOB": ["Credit Cards"],
    "ReportName": ["C86 Alerts"],
    "ControlRisk": [np.nan],
    "TestPeriod": ["Portfolio"],
    "ProductType": ["Credit Cards"],
    "RDE": [np.nan],
    "SubDE": [" "],
    "Segment": [np.nan],
    "Segment2": [np.nan],
    "Segment3": [np.nan],
    "Segment4": [np.nan],
    "Segment5": [np.nan],
    "Segment6": [np.nan],
    "Segment7": [np.nan],
    "Segment8": [np.nan],
    "Segment9": [np.nan],
    "Segment10": [np.nan],
    "HoldoutFlag": ["N"],
    "CommentCode": [np.nan],
    "SnapDate": [pd.to_datetime(date.today())]
}).iloc[0:0]  # empty with schema

ac_table_path = AC / "alert_cards_ac.parquet"

# Ensure base exists, then union-in new period rows (if any) – here we just ensure file exists
if not ac_table_path.exists():
    Alert_Cards_AC_week.to_parquet(ac_table_path, index=False)

# Read existing + merge unique by SnapDate
ac_existing = pd.read_parquet(ac_table_path)
alert_cards_ac = pd.concat([Alert_Cards_AC_week, ac_existing], ignore_index=True)
alert_cards_ac = alert_cards_ac.sort_values(by=["SnapDate"])
alert_cards_ac.to_parquet(ac_table_path, index=False)
log.info("AC table updated at %s", ac_table_path)

# --- 9. Closeout & Permissions ---
# Set permissive bits on recent outputs created by current user (last 12h)
try:
    for p in OUTDIR.glob("*"):
        try:
            os.chmod(p, 0o777)
        except Exception:
            pass
except Exception:
    pass

log.info(">>>>>>>>>>> End Time        : %s <<<<<<<<<<<", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
