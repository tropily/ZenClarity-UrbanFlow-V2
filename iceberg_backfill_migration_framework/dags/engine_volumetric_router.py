from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import datetime, timedelta
import calendar
import uuid
import logging

# ══════════════════════════════════════════════════════
# 0.0 Control Tower: Configuration
# ══════════════════════════════════════════════════════
S3_BUCKET     = "teo-nyc-taxi"
CLUSTER_ID = "j-1CS1GS9JW1D45"
GLUE_DATABASE = "teo-nyc-taxi-db"
GLUE_TABLE    = "trip_data"
AUDIT_TABLE   = "UrbanFlow_Migration_Audit"


# ══════════════════════════════════════════════════════
# HELPER 1 — Expand any slice input to day-level keys
# Format: {cab_type}#{yyyy}_{mm}_{dd}
# Example: green#2024_01_01
# ══════════════════════════════════════════════════════
def expand_slice_to_day_keys(cab_type, year, month, day):
    """
    Resolves any slice input to a list of day-level slice_id keys.
    Day slice   (y/m/d)  → 1 key
    Month slice (y/m)    → 28-31 keys
    Year slice  (y)      → 365-366 keys
    """
    prefix = cab_type.lower()

    if month and str(month).lower() != "none":
        m = int(month)
        if day and str(day).lower() != "none":
            d = int(day)
            return [f"{prefix}#{year}_{str(m).zfill(2)}_{str(d).zfill(2)}"]
        else:
            last_day = calendar.monthrange(int(year), m)[1]
            return [
                f"{prefix}#{year}_{str(m).zfill(2)}_{str(d).zfill(2)}"
                for d in range(1, last_day + 1)
            ]
    else:
        keys = []
        for m in range(1, 13):
            last_day = calendar.monthrange(int(year), m)[1]
            for d in range(1, last_day + 1):
                keys.append(
                    f"{prefix}#{year}_{str(m).zfill(2)}_{str(d).zfill(2)}"
                )
        return keys


# ══════════════════════════════════════════════════════
# HELPER 2 — Batch read DynamoDB (max 100 keys per call)
# Layer 1 only — retries=3 on write_audit handles
# transient audit write failures
# ══════════════════════════════════════════════════════
def check_pending_keys(day_keys, db_hook):
    """
    Batch reads DynamoDB to find which day keys are NOT yet LANDED.
    Returns list of pending keys that still need processing.
    """
    dynamodb = db_hook.get_conn()
    pending  = []

    chunks = [day_keys[i:i+100] for i in range(0, len(day_keys), 100)]

    for chunk in chunks:
        response = dynamodb.batch_get_item(
            RequestItems={
                AUDIT_TABLE: {
                    'Keys': [{'slice_id': key} for key in chunk],
                    'ProjectionExpression':     'slice_id, #s',
                    'ExpressionAttributeNames': {'#s': 'status'}
                }
            }
        )

        landed = {
            item['slice_id']
            for item in response['Responses'].get(AUDIT_TABLE, [])
            if item.get('status') == 'LANDED'
        }

        for key in chunk:
            if key not in landed:
                pending.append(key)

    return pending


# ══════════════════════════════════════════════════════
# MAIN — Pre-flight: Audit + Volume Check + Router
# ══════════════════════════════════════════════════════
def run_preflight_checks(**kwargs):
    """
    Gate 1 — Idempotency: DynamoDB batch check.
    Gate 2 — Volumetric: S3 scan → route to EMR or Glue.
    """
    params  = kwargs.get('params', {})
    logging.info(f"🛰️  UI Form Received: {params}")

    cab_type = params.get('cab_type')
    year     = params.get('year')
    month    = params.get('month')
    day      = params.get('day')

    if not cab_type or not year:
        logging.error("❌ CRITICAL: cab_type and year are required.")
        return 'stop_migration'

    # ── Gate 1: DynamoDB idempotency check ──
    day_keys = expand_slice_to_day_keys(cab_type, year, month, day)
    logging.info(f"🔍 Checking {len(day_keys)} day key(s) against audit table")

    db_hook      = DynamoDBHook(aws_conn_id='aws_default', region_name='us-east-1')
    pending_keys = check_pending_keys(day_keys, db_hook)

    logging.info(
        f"🔑 pending_keys count={len(pending_keys)} "
        f"first={pending_keys[0] if pending_keys else 'none'} "
        f"last={pending_keys[-1] if pending_keys else 'none'}"
    )

    if not pending_keys:
        logging.info(
            f"🛑 ALL {len(day_keys)} day(s) already LANDED — "
            f"no engine triggered."
        )
        return 'stop_migration'

    logging.info(
        f"✅ {len(day_keys) - len(pending_keys)} day(s) already LANDED | "
        f"{len(pending_keys)} pending → proceeding to engine"
    )

    # ── Gate 2: S3 volumetric scan (source) ──
    s3_prefix = f"processed/trip_data/cab_type={cab_type}/year={year}/"
    if month and str(month).lower() != "none":
        s3_prefix += f"month={int(month)}/"
        if day and str(day).lower() != "none":
            s3_prefix += f"day={int(day)}/"

    s3_hook          = S3Hook(aws_conn_id='aws_default')
    keys             = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=s3_prefix)
    total_size_bytes = 0

    if keys:
        for key in keys:
            total_size_bytes += s3_hook.get_key(key, S3_BUCKET).content_length

    size_gb = total_size_bytes / (1024**3)
    logging.info(f"🚀 Scout Result: {size_gb:.4f} GB across {len(keys or [])} files")

    # ── Build batch_id ──
    run_short  = str(uuid.uuid4())[:8]
    month_part = str(month).zfill(2) if month and str(month).lower() != "none" else "ALL"
    day_part   = str(day).zfill(2)   if day   and str(day).lower()   != "none" else "ALL"
    slice_ref  = f"{cab_type}#{year}_{month_part}_{day_part}"

    # ── XCom handshake ──
    ti = kwargs['ti']
    ti.xcom_push(key='pending_keys', value=pending_keys)
    ti.xcom_push(key='cab_type',     value=cab_type)
    ti.xcom_push(key='y',            value=year)
    ti.xcom_push(key='m',            value=month or "ALL")
    ti.xcom_push(key='d',            value=day   or "ALL")

    # ── Routing ──
    if size_gb > 0.05:
        batch_id = f"emr#{slice_ref}#{run_short}"
        ti.xcom_push(key='batch_id', value=batch_id)
        ti.xcom_push(key='engine',   value='emr')
        logging.info(f"🔥 High Density {size_gb:.4f} GB → EMR HEAVY SERVE | {batch_id}")
        return 'trigger_emr_heavy_serve'

    elif size_gb > 0:
        batch_id = f"glue#{slice_ref}#{run_short}"
        ti.xcom_push(key='batch_id', value=batch_id)
        ti.xcom_push(key='engine',   value='glue')
        logging.info(f"☁️  Low Density {size_gb:.4f} GB → GLUE NET PLAY | {batch_id}")
        return 'trigger_glue_net_play'

    else:
        logging.warning("⚠️  Empty slice — no source data found. Stopping.")
        return 'stop_migration'


# ══════════════════════════════════════════════════════
# POST-JOB — Write LANDED audit records after success
# ══════════════════════════════════════════════════════
def write_audit_landed(**kwargs):
    """
    Batch writes one LANDED record per pending day key.
    retries=3 on the task handles transient write failures.
    """
    ti           = kwargs['ti']
    pending_keys = ti.xcom_pull(key='pending_keys')
    engine       = ti.xcom_pull(key='engine')
    batch_id     = ti.xcom_pull(key='batch_id')

    if isinstance(pending_keys, str):
        logging.warning("⚠️  pending_keys came back as string — wrapping in list")
        pending_keys = [pending_keys]

    if not pending_keys:
        logging.warning("⚠️  No pending keys in XCom — nothing to write.")
        return

    logging.info(f"📝 Writing {len(pending_keys)} LANDED record(s)")

    db_hook  = DynamoDBHook(aws_conn_id='aws_default', region_name='us-east-1')
    table    = db_hook.get_conn().Table(AUDIT_TABLE)
    ttl_time = int((datetime.utcnow() + timedelta(days=90)).timestamp())

    with table.batch_writer() as batch:
        for key in pending_keys:
            batch.put_item(Item={
                'slice_id':  key,
                'batch_id':  batch_id,
                'status':    'LANDED',
                'engine':    engine,
                'landed_at': datetime.utcnow().isoformat(),
                'ttl':       ttl_time
            })

    logging.info(
        f"✅ Wrote {len(pending_keys)} LANDED record(s) | "
        f"engine={engine} | batch={batch_id}"
    )


# ══════════════════════════════════════════════════════
# THE STADIUM
# ══════════════════════════════════════════════════════
with DAG(
    dag_id='engine_volumetric_router',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['Router', 'Bi_Modal', 'Viper_V2'],
    params={
        "cab_type": Param("yellow", type="string",
                          enum=["yellow", "green", "fhv", "high_volume_fhv"]),
        "year":     Param("2024", type="string",
                          minLength=4, maxLength=4),
        "month":    Param(None, type=["null", "string"],
                          pattern="^(0[1-9]|1[0-2])$"),
        "day":      Param(None, type=["null", "string"],
                          pattern="^(0[1-9]|[12][0-9]|3[01])$"),
    }
) as dag:

    gatekeeper = BranchPythonOperator(
        task_id='preflight_audit_and_volume_check',
        python_callable=run_preflight_checks
    )

    emr_step = EmrAddStepsOperator(
        task_id='trigger_emr_heavy_serve',
        job_flow_id=CLUSTER_ID,
        aws_conn_id='aws_default',
        steps=[{
            'Name': "Viper-EMR-{{ ti.xcom_pull(key='batch_id') }}",
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--py-files',
                    f's3://{S3_BUCKET}/iceberg_backfill_migration_framework'
                    f'/scripts/iceberg_migration_utils.py',
                    f's3://{S3_BUCKET}/iceberg_backfill_migration_framework'
                    f'/scripts/emr_iceberg_backfill_migration.py',
                    '--cab-type',       "{{ ti.xcom_pull(key='cab_type') }}",
                    '--backfill-year',  "{{ ti.xcom_pull(key='y') }}",
                    '--backfill-month', "{{ ti.xcom_pull(key='m') }}",
                    '--backfill-day',   "{{ ti.xcom_pull(key='d') }}",
                    '--database',       GLUE_DATABASE,
                    '--table',          GLUE_TABLE,
                    '--batch-id',       "{{ ti.xcom_pull(key='batch_id') }}"
                ]
            }
        }]
    )

    glue_step = GlueJobOperator(
        task_id='trigger_glue_net_play',
        job_name='glue_iceberg_backfill_migration',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        script_args={
            "--cab_type":       "{{ ti.xcom_pull(key='cab_type') }}",
            "--backfill_year":  "{{ ti.xcom_pull(key='y') }}",
            "--backfill_month": "{{ ti.xcom_pull(key='m') }}",
            "--backfill_day":   "{{ ti.xcom_pull(key='d') }}",
            "--database":       GLUE_DATABASE,
            "--table":          GLUE_TABLE,
            "--batch_id":       "{{ ti.xcom_pull(key='batch_id') }}"
        }
    )

    stop_migration = PythonOperator(
        task_id='stop_migration',
        python_callable=lambda: logging.info("🛑 Router Tripped: Safe exit.")
    )

    write_audit = PythonOperator(
        task_id='write_audit_landed',
        python_callable=write_audit_landed,
        trigger_rule='one_success',
        retries=3,
        retry_delay=timedelta(seconds=30),
    )

    gatekeeper >> [emr_step, glue_step, stop_migration]
    [emr_step, glue_step] >> write_audit