# 05 — Lessons Learned
## ZenClarity UrbanFlow V2 — Snowflake vs Redshift Benchmark Series

---

## Overview
This document captures what broke during the build, design decisions made
under constraint, what we would tune next, and what we would do differently
on a second pass. Honest engineering reflection — not a highlight reel.

---

## What Broke

### 1. Redshift Spectrum — Views Not Supported
**What happened:** First attempt to run staging models on Redshift failed
immediately. Redshift Spectrum does not support creating views over external
tables — a fundamental limitation not obvious until hitting it.

**Fix:** Pushed incremental materialization down to the staging layer on
Redshift. Added `target.type` Jinja conditionals to staging config blocks.

**Lesson:** Always validate external table capabilities before designing
the staging layer. Spectrum is powerful for reads but has real constraints
on how downstream models can reference it.

---

### 2. YAML Block Scalar Whitespace
**What happened:** `sources.yml` used `>` block scalar for the Jinja
database/schema conditionals. Redshift received `"\n    nyc_taxi_db\n  "`
instead of `"nyc_taxi_db"` — causing `database does not exist` errors.

**Fix:** Switched from multi-line `>` block scalar to single-line inline
Jinja strings. Eliminated whitespace issue entirely.

**Lesson:** YAML block scalars and Jinja conditionals do not mix cleanly.
Always use inline Jinja for database/schema references in sources.yml.

---

### 3. Schema Case Sensitivity
**What happened:** dbt_project.yml defined schemas as uppercase
(`STG_NYC_TAXI`). Redshift created them as lowercase (`stg_nyc_taxi`).
On subsequent runs dbt found both versions and refused to proceed —
`approximate match` compilation error.

**Fix:** Lowercased all schema names in dbt_project.yml. Snowflake is
case-insensitive so `stg_nyc_taxi` resolves to `STG_NYC_TAXI` correctly.
Redshift lowercases consistently — no more ambiguity.

**Lesson:** Always use lowercase schema names in dbt_project.yml when
targeting both Snowflake and Redshift. Snowflake handles it gracefully.
Redshift does not.

---

### 4. Ghost Tables from Failed Runs
**What happened:** Early failed runs created tables in the wrong database
(`dev` instead of `nyc_taxi_db`). dbt found multiple approximate matches
and blocked execution on subsequent runs.

**Fix:** Manually dropped ghost schemas and tables in Redshift Query
Editor v2 before retrying.

**Lesson:** When migrating a dbt target to a new database, drop all
artifacts in the old location before running against the new one.
Redshift's approximate match protection is strict — it will not guess.

---

### 5. Snowflake-Specific Syntax
**What happened:** Six models used Snowflake-only functions —
`IFF()`, `HOUR()`, `LATERAL FLATTEN`, `array_construct_compact()`,
`TABLE(GENERATOR())`, and a CTE named `snapshot` (reserved in Redshift).

**Fix:** Added `target.type` Jinja conditionals inline for each case.
No models were split. All fixes are documented in `02_materialization_decisions.md`.

**Lesson:** When building a portable dbt codebase, audit every model
for warehouse-specific functions before assuming portability. Snowflake
has a richer function library than Redshift — most gaps have workarounds
but require explicit handling.

---

### 6. dbt-redshift Adapter Not Installed
**What happened:** First `dbt debug` against Redshift failed with
`No module named dbt.adapters.redshift`. The adapter was not installed
in the virtual environment.

**Fix:** `pip install dbt-redshift` inside the active venv.

**Lesson:** Each dbt target requires its own adapter package.
Add all required adapters to `requirements.txt` so the environment
is reproducible.

---

## What We Would Tune Next

### Redshift — fact_trip DISTKEY + SORTKEY
Q1 cold scan took 26 seconds — almost entirely cold start + full scan overhead.
Adding distribution and sort keys to fact_trip would reduce scan time on
repeated cold queries:

```sql
{{ config(
    dist  = 'cab_type',
    sort  = ['pickup_date_key', 'cab_type']
) }}
```

Expected improvement: 40-60% reduction on Q1 cold after first build.

### Snowflake — Materialize int_trip_data_quarantine
Q4 cold took 2.586s — slowest Snowflake cold query. The DQ summary view
resolves through multiple view layers before hitting data. Materializing
`int_trip_data_quarantine` as a table would cut this significantly.

### Both Engines — Baseline Compute Pre-warm
Redshift cold start penalty (~25s) is an operational concern for BI
dashboards. In production, configuring a baseline RPU on Redshift
Serverless would keep compute warm during business hours and eliminate
the cold start entirely.

Snowflake equivalent: set warehouse `AUTO_SUSPEND` to a higher value
during peak BI hours.

### Redshift — dq_trip_issue_summary UNION ALL Scalability
The current SPLIT_PART + UNION ALL pattern supports exactly 4 DQ failure
reasons. Adding a 5th check requires a manual new UNION ALL block.
A more scalable pattern would use a cross-join on a hardcoded reason list —
but for 4 reasons the current approach is readable and maintainable.

---

## What We Would Do Differently

### 1. Validate Spectrum Capabilities First
Before designing the staging layer, run a simple view creation test
against a Spectrum external table. One SQL statement would have
surfaced the limitation immediately and saved multiple failed runs.

### 2. Use Lowercase Schema Names From Day One
Defining schema names in lowercase in dbt_project.yml from the start
eliminates the case sensitivity issue entirely. Both Snowflake and
Redshift handle lowercase gracefully — uppercase causes problems only
on Redshift.

### 3. Set Up Redshift Permissions Before Running dbt
The `dbt_dev_viper` service account needed multiple permission grants
before dbt could create schemas and tables. A proper IAM + RBAC setup
script run before the first `dbt debug` would eliminate this friction.

### 4. Add requirements.txt for dbt Adapters
A `requirements.txt` with all dbt adapter packages pinned to specific
versions would make the environment fully reproducible:
### 5. Run dbt compile Before dbt run
Running `dbt compile --target redshift_dev` before the first `dbt run`
would surface all Snowflake-specific syntax errors at compile time —
faster feedback loop than waiting for a full run to fail mid-execution.

---

## Summary

| Issue | Root Cause | Fix | Prevention |
|-------|-----------|-----|------------|
| Views over Spectrum | Spectrum limitation | Incremental staging | Test before designing |
| YAML whitespace | Block scalar + Jinja | Inline Jinja | Use inline always |
| Schema case mismatch | Uppercase in dbt_project.yml | Lowercase everywhere | Lowercase from day one |
| Ghost tables | Failed runs in wrong DB | Manual drop + retry | Clean slate before retry |
| Snowflake-only syntax | No portability audit | Jinja conditionals | Audit before building |
| Missing adapter | Not in venv | pip install | Add to requirements.txt |
