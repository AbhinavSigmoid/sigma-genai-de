import streamlit as st
import boto3
import pandas as pd
import json
from io import StringIO

BUCKET = "sigma-datatech-676382"

s3 = boto3.client("s3")
cw = boto3.client("cloudwatch")

st.set_page_config(
    page_title="Sigma Command Center",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 Sigma Command Center")

# --------------------------------------------------
# Load Latest Incident Report
# --------------------------------------------------

incident_data = None

try:
    resp = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix="reports/"
    )

    files = sorted(
        resp.get("Contents", []),
        key=lambda x: x["LastModified"],
        reverse=True
    )

    if files:
        latest_report = files[0]["Key"]

        report_content = s3.get_object(
            Bucket=BUCKET,
            Key=latest_report
        )["Body"].read().decode()

        incident_data = json.loads(report_content)

except Exception as e:
    st.error(f"Unable to load incident report: {e}")

# --------------------------------------------------
# KPI CARDS
# --------------------------------------------------

st.header("📊 Incident KPIs")

if incident_data:
    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Recovered Records",
        incident_data["recovery"]["rows_loaded"]
    )

    col2.metric(
        "Quarantined Records",
        incident_data["recovery"]["quarantined_count"]
    )

    col3.metric(
        "Recovery Time",
        f"{incident_data['total_duration_sec']} sec"
    )

# --------------------------------------------------
# ROOT CAUSE
# --------------------------------------------------

if incident_data:
    st.header("🔥 Root Cause")

    st.error(
        incident_data["forensics"]["root_cause_hypothesis"]
    )

# --------------------------------------------------
# BUSINESS IMPACT
# --------------------------------------------------

if incident_data:
    st.header("💰 Business Impact")

    col1, col2 = st.columns(2)

    col1.metric(
        "Missing Records",
        incident_data["impact"]["records_missing"]
    )

    col2.metric(
        "Merchants Affected",
        incident_data["impact"]["merchants_affected"]
    )

# --------------------------------------------------
# RECOVERY SUMMARY
# --------------------------------------------------

if incident_data:
    st.header("✅ Recovery Summary")

    st.success(
        f"Recovered {incident_data['recovery']['rows_loaded']} records"
    )

    st.write(
        f"Quarantined Records: {incident_data['recovery']['quarantined_count']}"
    )

    st.write(
        f"Reason: {incident_data['recovery']['quarantine_reason']}"
    )

# --------------------------------------------------
# QUARANTINE FILE
# --------------------------------------------------

st.header("⚠️ Quarantine Files")

try:
    resp = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix="quarantine/"
    )

    files = sorted(
        resp.get("Contents", []),
        key=lambda x: x["LastModified"],
        reverse=True
    )

    if files:
        latest_csv = files[0]["Key"]

        csv_data = s3.get_object(
            Bucket=BUCKET,
            Key=latest_csv
        )["Body"].read().decode()

        df = pd.read_csv(StringIO(csv_data))

        st.dataframe(
            df,
            use_container_width=True
        )

except Exception as e:
    st.error(f"Unable to load quarantine file: {e}")

# --------------------------------------------------
# CLOUDWATCH ALARMS
# --------------------------------------------------

st.header("🔔 CloudWatch Alarms")

alarms = [
    "sigma-snowflake-zero-load",
    "sigma-lambda-version-change",
    "sigma-pipeline-row-divergence",
]

for alarm in alarms:
    try:
        resp = cw.describe_alarms(
            AlarmNames=[alarm]
        )

        if resp["MetricAlarms"]:
            state = resp["MetricAlarms"][0]["StateValue"]

            if state == "OK":
                st.success(f"{alarm}: {state}")

            elif state == "ALARM":
                st.error(f"{alarm}: {state}")

            else:
                st.warning(f"{alarm}: {state}")

    except Exception as e:
        st.error(f"{alarm}: {e}")

# --------------------------------------------------
# INCIDENT TIMELINE
# --------------------------------------------------

if incident_data:
    st.header("🕒 Incident Timeline")

    timeline = incident_data.get("timeline", [])

    for item in timeline:
        st.write(
            f"**{item['ts']}** — {item['event']}"
        )

# --------------------------------------------------
# AGENT PERFORMANCE
# --------------------------------------------------

if incident_data:
    st.header("🤖 Agent Performance")

    agent_df = pd.DataFrame(
        incident_data["agent_performance"]
    )

    st.dataframe(
        agent_df,
        use_container_width=True
    )

# --------------------------------------------------
# FULL INCIDENT REPORT
# --------------------------------------------------

if incident_data:
    with st.expander("📄 Full Incident Report"):
        st.json(incident_data)