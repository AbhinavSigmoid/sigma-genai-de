# Chaos Log — Team Name: Team 9

## Day 12 | Wednesday 4 June 2026

---

## Pre-Exercise Answer (fill before Phase 1)

**Question:** Should the 9 tool functions be one Lambda or separate Lambdas? What breaks if they are one?

**Your answer:**

The 9 tools should be separate Lambda functions. If they are combined into one Lambda, every agent gets unnecessary permissions, deployments become risky, debugging becomes harder, and a failure in one tool can impact all other tools. Separate Lambdas provide isolation, better security, and easier maintenance.

---

## Phase 2 — Manual Investigation

*You have 60 minutes. Find the root cause before the agents do.*

**Records in Kinesis (02:00–02:20 UTC):** 847 records sent

**Records in S3 (02:00–02:20 UTC):** 1 file, 4.7 MB total

**Records in Snowflake (02:00–02:20):** 0 rows loaded

---

**Failure timestamp:** 02:11 UTC

**What changed at that timestamp:**

Lambda `sigma-kinesis-producer` was deployed from v1 to v2.

**Root cause (your hypothesis):**

Lambda v2 changed `merchant_name` to `merchant_nm` and changed the date format, causing Snowflake COPY INTO to reject records.

**Why no alert fired:**

No CloudWatch alarm existed for zero-row Snowflake loads.

**Time taken to find this:** 45 minutes

---

**Signals you connected:**

Kinesis record counts, S3 file delivery, Snowflake row counts, Lambda version history.

**Signal you missed (fill this in Phase 3 after seeing the agent output):**

The exact Lambda version deployment event and schema mismatch correlation.

---

## Phase 3 — Comparison

**What I found (Phase 2 manual):**

* Time taken: 45 minutes
* Root cause found? Partial
* SLA breach identified? No
* Prevention created? No

**What the agent found (Phase 3):**

* Time taken: 26 seconds
* Root cause found? Yes
* SLA breach identified? Yes
* Prevention created? Yes (3 live alarms)

**What I missed that the agent caught:**

The agent correlated Lambda deployment history, schema changes, Snowflake failures, and SLA impact automatically.

**Why the agent caught it:**

The agent had access to multiple tools and correlated data across services in seconds.

---

## Judgment Questions

**Forensics Agent:**
*The agent found the root cause by correlating Lambda version history with Snowflake query history. What is the one CloudWatch alarm that would have caught this at 02:12 instead of 09:03? Write it as a metric alarm definition.*

Your answer:

Alarm Name: sigma-snowflake-zero-load

Metric: Snowflake rows loaded

Condition: Trigger if rows loaded = 0 for 2 consecutive pipeline runs.

---

**Recovery Agent:**
*The recovery used transaction_id as the idempotency key. What happens if a legitimate duplicate transaction_id exists in the source data? How would you change the deduplication logic?*

Your answer:

A legitimate duplicate transaction could be incorrectly removed. I would use a composite key such as transaction_id + merchant_id + timestamp or maintain source-specific unique identifiers.

---

**Hardening Agent:**
*The sigma-lambda-version-change alarm fires on any Lambda error spike after a version change. Your team deploys 20 Lambda functions per day in prod. Would you keep this alarm? If yes, how do you stop it from spamming? If no, what replaces it?*

Your answer:

Yes. I would keep it but add thresholds, deployment windows, and correlation with error metrics so it only fires when a deployment causes abnormal failures.

---

## Your Honest Reflection

**Which part of the manual investigation took longest and why:**

Correlating evidence across CloudWatch, Kinesis, S3, and Snowflake took the longest because the failure was silent and no alert was triggered.

**What would have happened if this hit prod at 2 AM with no agents:**

The issue would likely remain undetected until business users reported incorrect dashboard numbers, causing revenue reporting errors and SLA violations.

**One thing you would add to this platform that none of the 6 agents currently do:**

Automatic canary deployment validation that compares row counts and schema compatibility before promoting a Lambda version to production.

---

*Push this file to your team fork before the Phase 2 checkpoint.*
*Incomplete answers are flagged by validate_day12.py*
