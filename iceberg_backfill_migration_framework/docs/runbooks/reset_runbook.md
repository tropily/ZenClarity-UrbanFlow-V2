# UrbanFlow V2 — Reset & Recovery Runbook
**Author:** Nguyen Le | ZenClarity Consulting  
**Location:** `docs/runbooks/reset_runbook.md`  
**Last Updated:** March 18, 2026  
**Scope:** DynamoDB audit table `UrbanFlow_Migration_Audit`

---

## Purpose

This runbook covers operational reset and gap detection procedures for the
UrbanFlow V2 migration pipeline. Use these utilities when:

- A slice failed mid-run and needs to be re-queued
- A day was incorrectly marked LANDED and needs to be replayed
- You need to identify which slices never completed

These utilities operate on the DynamoDB audit table only.
They do not modify Iceberg partitions or S3 data.

---

## Audit Table Reference

| Field      | Format                              | Example                              |
|------------|-------------------------------------|--------------------------------------|
| `slice_id` | `cab_type#yyyy_mm_dd`               | `green#2024_01_15`                   |
| `batch_id` | `engine#cab_type#yyyy_mm_ref#uuid8` | `emr#green#2024_01_ALL#abc123`       |
| `status`   | `LANDED` or `FAILED`                | `LANDED`                             |
| `engine`   | `emr` or `glue`                     | `emr`                                |
| `landed_at`| ISO 8601 timestamp                  | `2024-01-15T08:32:01`                |

---

## Commands

### 1. `reset_slice()` — Reset a single day to PENDING

**File:** `reset_utilities.py`

**When to use:**
- A specific day was incorrectly marked LANDED
- A day failed mid-write and needs to be replayed
- You are testing idempotency behavior on a known slice

**Signature:**
```python
reset_slice(slice_id, db_hook, new_status='PENDING')
```

**Parameters:**

| Parameter    | Required | Default     | Description                        |
|--------------|----------|-------------|------------------------------------|
| `slice_id`   | Yes      | —           | Day key: `cab_type#yyyy_mm_dd`     |
| `db_hook`    | Yes      | —           | Airflow DynamoDBHook instance      |
| `new_status` | No       | `'PENDING'` | Target status after reset          |

**Example calls:**
```python
# Reset a single green taxi day
reset_slice("green#2024_01_15", db_hook)

# Reset to a custom status
reset_slice("yellow#2024_03_01", db_hook, new_status="FAILED")
```

**Expected output:**
```
Reset green#2024_01_15 → PENDING
```

**What happens next:**
The DAG's idempotency gate (`check_pending_keys`) will no longer find this
slice as LANDED. The next DAG run covering this slice will re-trigger the engine.

---

### 2. `list_failed_slices()` — Scan for all FAILED slices

**File:** `list_failed_slice.py`

**When to use:**
- After a partial pipeline run to identify gaps
- Before a replay run to confirm which slices need recovery
- During ops review to check pipeline health

**Signature:**
```python
list_failed_slices(db_hook)
```

**Parameters:**

| Parameter | Required | Description                   |
|-----------|----------|-------------------------------|
| `db_hook` | Yes      | Airflow DynamoDBHook instance |

**Example call:**
```python
failed = list_failed_slices(db_hook)
print(failed)
```

**Expected output:**
```python
[
  {'slice_id': 'green#2024_03_15', 'status': 'FAILED', 'engine': 'emr', ...},
  {'slice_id': 'yellow#2024_06_02', 'status': 'FAILED', 'engine': 'glue', ...}
]
```

**What happens next:**
Use the returned `slice_id` list to drive individual `reset_slice()` calls
or pass directly into a replay DAG run.

---

## Common Recovery Patterns

### Pattern 1 — Single day replay
```python
# Step 1: Reset the slice
reset_slice("green#2024_01_15", db_hook)

# Step 2: Trigger DAG manually via Airflow UI
# cab_type=green | year=2024 | month=01 | day=15
```

### Pattern 2 — Bulk recovery after partial month run
```python
# Step 1: Find all failed slices
failed = list_failed_slices(db_hook)

# Step 2: Reset each one
for item in failed:
    reset_slice(item['slice_id'], db_hook)

# Step 3: Trigger DAG for the full month
# cab_type=green | year=2024 | month=01
# DAG will skip already-LANDED days automatically
```

---

## Safety Notes

- `reset_slice()` only updates the DynamoDB audit record. It does **not** delete
  Iceberg partitions. If the Iceberg write succeeded before the audit write failed,
  the replay will overwrite the partition via `overwritePartitions()` — this is safe.

- `list_failed_slices()` uses a full table `scan()`. For large audit tables,
  add a `FilterExpression` on `cab_type` or date range to reduce read costs.

- Never reset a LANDED slice unless you intend to replay it.
  The idempotency gate exists to prevent double-writes — resetting bypasses it.

---

## Related Files

| File                              | Purpose                        |
|-----------------------------------|--------------------------------|
| `reset_utilities.py`              | `reset_slice()` implementation |
| `list_failed_slice.py`            | `list_failed_slices()` implementation |
| `engine_volumetric_router.py`     | DAG — idempotency gate + audit writes |
| `iceberg_migration_utils.py`      | Execution wrapper + timing     |
