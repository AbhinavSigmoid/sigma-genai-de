import streamlit as st
import time

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------

st.set_page_config(
    page_title="Runbook Guardian",
    page_icon="🛡️",
    layout="wide"
)

# ---------------------------------------------------
# SESSION STATE INIT
# ---------------------------------------------------

if "input_tokens" not in st.session_state:
    st.session_state["input_tokens"] = 0

if "output_tokens" not in st.session_state:
    st.session_state["output_tokens"] = 0

if "cost" not in st.session_state:
    st.session_state["cost"] = 0.0

# ---------------------------------------------------
# TOKEN CALCULATOR
# ---------------------------------------------------

def calculate_tokens(text):

    words = len(text.split())

    estimated_tokens = int(words * 1.3)

    return estimated_tokens

def update_cost(input_text, output_text):

    input_tokens = calculate_tokens(input_text)
    output_tokens = calculate_tokens(output_text)

    total_tokens = input_tokens + output_tokens

    estimated_cost = total_tokens * 0.000002

    st.session_state["input_tokens"] += input_tokens
    st.session_state["output_tokens"] += output_tokens
    st.session_state["cost"] += estimated_cost

# ---------------------------------------------------
# AI MOCK FUNCTIONS
# ---------------------------------------------------

def call_nova_pro(prompt):

    response = """
# 🛡️ Silver Pipeline Operational Runbook

---

# 1. Purpose

This pipeline reads Bronze layer customer transaction data,
applies cleansing + transformations,
and writes curated records into the Silver layer.

Pipeline Schedule:
- Daily at 2:00 AM

Criticality:
- HIGH

Downstream Dependencies:
- Gold analytics pipeline
- Executive dashboards
- ML feature pipelines

---

# 2. Prerequisites

Before running pipeline:

✅ Python virtual environment activated

✅ Database connectivity verified

✅ Source Bronze table available

✅ Sufficient disk space available

✅ AWS credentials configured

---

# 3. Pipeline Execution

Run command:

python silver_pipeline.py

Expected Runtime:
- 8 to 12 minutes

Expected Output:
- Silver table updated successfully
- Validation metrics generated

---

# 4. Validation Checks

After execution verify:

1. Row count increase matches expected ingestion

2. Null values below threshold

3. No duplicate transaction IDs

4. Partition count matches current date

5. Data freshness timestamp updated

---

# 5. Failure Handling

If pipeline fails:

Step 1:
- Check pipeline logs

Step 2:
- Verify database connectivity

Step 3:
- Validate source Bronze data availability

Step 4:
- Retry pipeline once

Step 5:
- Escalate repeated failures

---

# 6. Known Risks

⚠️ Current pipeline does NOT support rollback.

⚠️ Partial writes may affect downstream systems.

⚠️ Retry may duplicate records if overwrite fails midway.

---

# 7. Escalation Matrix

Primary Contact:
- Data Engineering Lead

Secondary Contact:
- Platform Reliability Engineer

Escalate Immediately If:
- Retry fails twice
- Data corruption detected
- Partition mismatch occurs

---

# 8. Recommended Engineering Improvements

- Add transactional writes
- Add rollback checkpoints
- Add failure alerting
- Add partition recovery support
- Add automated validation framework

---
"""

    update_cost(prompt, response)

    return response

def call_nova_lite(prompt):

    response = [
        {
            "question": "Where are the pipeline logs stored?",
            "answer": """
Logs are stored in:
/logs/silver_pipeline/

You should check:
- latest_error.log
- execution.log
"""
        },

        {
            "question": "How do I rollback partial writes?",
            "answer": """
Currently rollback is NOT supported.

This is a production engineering gap.
The pipeline lacks transactional recovery.
"""
        },

        {
            "question": "What if retry fails again?",
            "answer": """
Escalate incident immediately and stop downstream jobs.
"""
        },

        {
            "question": "How do I validate correctness?",
            "answer": """
Check row counts, duplicates, timestamps, and partitions.
"""
        },

        {
            "question": "Who is backup contact?",
            "answer": """
Escalate to Platform Reliability Engineer.
"""
        }
    ]

    update_cost(prompt, str(response))

    return response

# ---------------------------------------------------
# CUSTOM CSS
# ---------------------------------------------------

st.markdown("""
<style>

[data-testid="stAppViewContainer"] {
    background: linear-gradient(to right, #0f172a, #111827);
    color: white;
}

[data-testid="stSidebar"] {
    background-color: #020617;
}

.block-container {
    max-width: 95%;
    padding-top: 1rem;
}

.card {
    background-color: #1e293b;
    padding: 20px;
    border-radius: 18px;
    margin-bottom: 20px;
}

.question-card {
    background-color: #1d4ed8;
    padding: 18px;
    border-radius: 14px;
    margin-bottom: 15px;
}

.answer-card {
    background-color: #14532d;
    padding: 15px;
    border-radius: 12px;
    margin-bottom: 25px;
}

.danger-card {
    background-color: #991b1b;
    padding: 25px;
    border-radius: 18px;
}

</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------
# SIDEBAR
# ---------------------------------------------------

st.sidebar.title("🛡️ Runbook Guardian")

page = st.sidebar.radio(
    "Navigation",
    [
        "Dashboard",
        "Pipeline Analyzer",
        "Runbook Generator",
        "Junior Engineer",
        "Gap Analysis",
        "Hidden Trap",
        "Interactive Q&A"
    ]
)

st.sidebar.divider()

st.sidebar.subheader("📊 AI Usage Metrics")

st.sidebar.metric(
    "Input Tokens",
    st.session_state["input_tokens"]
)

st.sidebar.metric(
    "Output Tokens",
    st.session_state["output_tokens"]
)

st.sidebar.metric(
    "Estimated Nova Cost",
    f"${st.session_state['cost']:.6f}"
)

st.sidebar.divider()

pipeline_status = st.sidebar.selectbox(
    "Pipeline Status",
    [
        "Healthy",
        "Warning",
        "Critical"
    ]
)

monitoring = st.sidebar.toggle(
    "Monitoring Enabled",
    value=True
)

incident_mode = st.sidebar.toggle(
    "3 AM Incident Simulation"
)

# ---------------------------------------------------
# HEADER
# ---------------------------------------------------

st.title("🛡️ Runbook Guardian")
st.caption("AI-Powered Production Reliability Platform")

# ---------------------------------------------------
# STATUS
# ---------------------------------------------------

if pipeline_status == "Healthy":
    st.success("✅ Pipeline Operating Normally")

elif pipeline_status == "Warning":
    st.warning("⚠️ Operational Risks Detected")

else:
    st.error("🚨 Critical Pipeline Failure")

# ---------------------------------------------------
# DASHBOARD
# ---------------------------------------------------

if page == "Dashboard":

    st.subheader("📊 AI Ops Overview")

    st.markdown("""
    <div class="card">

    This platform validates whether a data pipeline
    is truly production-ready.

    It evaluates:
    - operational documentation
    - recovery readiness
    - escalation clarity
    - incident handling capability
    - hidden engineering risks

    </div>
    """, unsafe_allow_html=True)

    if monitoring:
        st.info("📡 Monitoring Active")

    if incident_mode:
        st.warning("🚨 3 AM Incident Simulation Enabled")

# ---------------------------------------------------
# PIPELINE ANALYZER
# ---------------------------------------------------

elif page == "Pipeline Analyzer":

    st.subheader("🧠 Pipeline Analyzer")

    st.markdown("""
    Paste pipeline code below.

    Nova Pro will analyze the implementation
    and generate operational documentation.
    """)

    default_pipeline = """
def load_to_silver():

    data = read_bronze()

    transformed = transform(data)

    write_to_silver(transformed)

    print("Pipeline completed")
"""

    pipeline_code = st.text_area(
        "Paste Pipeline Code",
        value=default_pipeline,
        height=300
    )

    if st.button("Save Pipeline Code"):

        st.session_state["pipeline_code"] = pipeline_code

        st.success("Pipeline code saved successfully.")

    if "pipeline_code" in st.session_state:

        st.code(
            st.session_state["pipeline_code"],
            language="python"
        )

# ---------------------------------------------------
# RUNBOOK GENERATOR
# ---------------------------------------------------

elif page == "Runbook Generator":

    st.subheader("🤖 AI Runbook Generator")

    if "pipeline_code" not in st.session_state:

        st.warning("Please save pipeline code first.")

    else:

        if st.button("Generate Runbook"):

            with st.spinner("Nova Pro generating runbook..."):
                time.sleep(2)

                runbook = call_nova_pro(
                    st.session_state["pipeline_code"]
                )

                st.session_state["runbook"] = runbook

            st.success("Runbook Generated Successfully")

        if "runbook" in st.session_state:

            st.markdown(f"""
            <div class="card">
            <pre>{st.session_state["runbook"]}</pre>
            </div>
            """, unsafe_allow_html=True)

# ---------------------------------------------------
# JUNIOR ENGINEER
# ---------------------------------------------------

elif page == "Junior Engineer":

    st.subheader("👨‍💻 Junior Engineer Simulator")

    if "runbook" not in st.session_state:

        st.warning("Generate runbook first.")

    else:

        if st.button("Simulate Questions"):

            with st.spinner("Nova Lite reviewing runbook..."):
                time.sleep(2)

                questions = call_nova_lite(
                    st.session_state["runbook"]
                )

                st.session_state["questions"] = questions

        if "questions" in st.session_state:

            for item in st.session_state["questions"]:

                st.markdown(
                    f"""
                    <div class="question-card">
                    <b>❓ Question:</b><br><br>
                    {item["question"]}
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                st.markdown(
                    f"""
                    <div class="answer-card">
                    <b>✅ AI Guidance:</b><br><br>
                    {item["answer"]}
                    </div>
                    """,
                    unsafe_allow_html=True
                )

# ---------------------------------------------------
# GAP ANALYSIS
# ---------------------------------------------------

elif page == "Gap Analysis":

    st.subheader("🚨 Production Gap Analysis")

    st.table({
        "Question": [
            "Where are logs stored?",
            "Rollback partial writes?",
            "Retry fails again?",
            "Validate correctness?",
            "Backup contact?"
        ],

        "Classification": [
            "RUNBOOK GAP",
            "GOOD QUESTION",
            "RUNBOOK GAP",
            "UNNECESSARY",
            "RUNBOOK GAP"
        ]
    })

    st.markdown("""
    <div class="danger-card">

    <h2>🚨 Critical Engineering Gap</h2>

    The pipeline has NO rollback implementation.

    The runbook assumed rollback existed,
    but the actual system lacks recovery support.

    This is a REAL production engineering risk.

    </div>
    """, unsafe_allow_html=True)

    st.subheader("🛠️ Updated Runbook After Gap Fixes")

    updated_runbook = """
# UPDATED SILVER PIPELINE RUNBOOK

---

# Added Recovery Procedures

1. Before retrying pipeline:
   - Validate partial writes
   - Check duplicate transaction IDs
   - Verify latest successful checkpoint

2. Incident Containment Steps:
   - Pause downstream Gold pipelines
   - Disable scheduled retries temporarily
   - Preserve failed execution logs

3. Manual Recovery Guidance:
   - Restore latest validated backup snapshot
   - Reprocess affected partition only
   - Validate row count consistency

4. Escalation Improvements:
   - Trigger Sev-2 incident if duplicate propagation detected
   - Notify Platform Reliability Engineer immediately

5. Engineering Recommendations:
   - Add transactional database writes
   - Add rollback checkpoints
   - Add idempotent retry handling
   - Add automated corruption detection

---

# UPDATED VALIDATION CHECKS

- Validate duplicate transaction IDs
- Compare partition-level row counts
- Verify no partial overwrite occurred
- Confirm downstream freshness timestamps

---

# UPDATED KNOWN RISKS

⚠️ Rollback is currently manual.

⚠️ Partial recovery remains high-risk until transactional safeguards are implemented.

---
"""

    st.markdown(
        f"""
        <div class="card">
        <pre>{updated_runbook}</pre>
        </div>
        """,
        unsafe_allow_html=True
    )

# ---------------------------------------------------
# HIDDEN TRAP
# ---------------------------------------------------

elif page == "Hidden Trap":

    st.subheader("🕳️ Hidden Trap Discovery")

    st.markdown("""
    <div class="danger-card">

    <h2>⚠️ Hidden Production Trap</h2>

    Junior engineer simulation exposed:

    <b>"How do I rollback partial writes?"</b>

    The runbook assumed rollback existed.

    But actual pipeline implementation
    has NO rollback support.

    This means:
    - partial writes may corrupt downstream data
    - duplicate records may occur
    - manual recovery is required

    <br><br>

    <h3>🚨 Key Insight</h3>

    Documentation alone cannot make
    a pipeline production-ready.

    </div>
    """, unsafe_allow_html=True)

# ---------------------------------------------------
# INTERACTIVE Q&A
# ---------------------------------------------------

elif page == "Interactive Q&A":

    st.subheader("💬 Interactive AI Operations Assistant")

    st.markdown("""
    Ask operational or incident-related questions.
    """)

    user_question = st.text_input(
        "Ask Your Question"
    )

    if st.button("Get AI Guidance"):

        if user_question.strip() == "":
            st.warning("Please enter a question.")

        else:

            with st.spinner("Nova Pro analyzing issue..."):
                time.sleep(2)

                ai_response = f"""
Operational Guidance:

Question:
{user_question}

Recommended Steps:
1. Check logs
2. Validate dependencies
3. Verify database connectivity
4. Check data consistency
5. Escalate if issue persists

Risk Level:
Medium
"""

                update_cost(user_question, ai_response)

            st.markdown(
                f"""
                <div class="answer-card">
                <b>🤖 AI Guidance:</b><br><br>
                {ai_response}
                </div>
                """,
                unsafe_allow_html=True
            )

# ---------------------------------------------------
# FOOTER
# ---------------------------------------------------

st.divider()

st.caption("""
Sigma DataTech AI Reliability Challenge

Focus:
Production Reliability • AI Ops • Incident Readiness
""")