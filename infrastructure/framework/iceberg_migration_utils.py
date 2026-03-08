import boto3
import time
import functools
from datetime import datetime, UTC
from decimal import Decimal

# === THE DECORATOR (The Gatekeeper) ===
def migration_control_tower(engine_type="GLUE_4.0"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(session, table_name, year, month, batch_id, *args, **kwargs):
            db = session.resource('dynamodb', region_name='us-east-1')
            audit_table = db.Table('UrbanFlow_Migration_Audit')
            
            # Using month/year for the unique slice ID
            slice_id = f"{table_name}#{year}-{month:02d}"
            
            # --- PHASE 1: IDEMPOTENCY CHECK ---
            try:
                audit_table.put_item(
                    Item={
                        'slice_id': slice_id,
                        'batch_id': batch_id,
                        'status': 'STARTED',
                        'engine': engine_type,
                        'start_time': datetime.now(UTC).isoformat(),
                        'pilot': 'Pete_Viper'
                    },
                    # Only allow start if it doesn't exist OR previously FAILED
                    ConditionExpression="attribute_not_exists(slice_id) OR #s = :f",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":f": "FAILED"}
                )
                print(f"Tower: {slice_id} cleared for takeoff. Launching {engine_type}...")
            except db.meta.client.exceptions.ConditionalCheckFailedException:
                print(f"Tower: [SKIP] {slice_id} is already LANDED. No double-faults.")
                return None

            # --- PHASE 2: EXECUTION ---
            try:
                start_exec = datetime.now(UTC)
                # Calls trigger_glue_migration with all arguments
                result = func(session, table_name, year, month, batch_id, *args, **kwargs)
                
                end_exec = datetime.now(UTC)
                duration = (end_exec - start_exec).total_seconds()

                # --- PHASE 3: THE LANDING (Update Scoreboard) ---
                audit_table.update_item(
                    Key={'slice_id': slice_id},
                    UpdateExpression="SET #s = :l, end_time = :et, duration_sec = :d, records = :r",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":l": "LANDED",
                        ":et": end_exec.isoformat(),
                        ":d": Decimal(str(round(duration, 2))),
                        ":r": Decimal(str(result.get('record_count', 0)))
                    }
                )
                print(f"Tower: ✅ {slice_id} successfully LANDED. (Duration: {duration:.2f}s)")
                return result

            except Exception as e:
                audit_table.update_item(
                    Key={'slice_id': slice_id},
                    UpdateExpression="SET #s = :f, error_msg = :err",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":f": "FAILED", ":err": str(e)}
                )
                print(f"Tower: ❌ {slice_id} CRASHED. Error recorded in Audit.")
                raise e
        return wrapper
    return decorator

# === THE TRIGGER (The Pilot) ===
@migration_control_tower(engine_type="GLUE_4.0")
def trigger_glue_migration(session, table_name, year, month, batch_id, day=None, row_limit=None):
    glue = session.client('glue', region_name='us-east-1')
    
    # Ensure name matches exactly what is in Glue Console
    job_name = 'glue_iceberg_backfill_migration'
    
    # Handle optional day and row_limit strings for Glue parameters
    day_str = str(day) if day else "None"
    limit_str = str(row_limit) if row_limit else "None"
    
    response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            '--BACKFILL_YEAR': str(year),
            '--BACKFILL_MONTH': str(month),
            '--BACKFILL_DAY': day_str,     # Now explicitly included
            '--ROW_LIMIT': limit_str,       # Now explicitly included
            '--BATCH_ID': batch_id
        }
    )
    
    run_id = response['JobRunId']
    print(f"Engine: {job_name} launched (RunId: {run_id}). Monitoring flight...")
    
    # Polling Loop
    while True:
        status_res = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = status_res['JobRun']['JobRunState']
        
        if state == 'SUCCEEDED':
            return {"record_count": 0, "run_id": run_id}
        elif state in ['FAILED', 'STOPPED', 'TIMEOUT']:
            error_msg = status_res['JobRun'].get('ErrorMessage', 'Check Glue CloudWatch logs')
            raise Exception(f"Engine Stalled: {state} - {error_msg}")
            
        time.sleep(30) # Wait 30 seconds before checking again

# === THE FIRST SERVE (Execution) ===
def start_recovery(session):
    # Unique batch name for today's run
    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M')
    current_batch = f"URBANFLOW_RECOVERY_JAN_{timestamp}"
    
    print(f"--- 🎾 TOURNAMENT START: {current_batch} ---")
    
    # Launching for Jan 2024. Set day=None for the whole month.
    trigger_glue_migration(
        session, 
        table_name="yellow_taxi", 
        year=2024, 
        month=1, 
        batch_id=current_batch, 
        day=2, 
        row_limit=None
    )

# --- THE ACTUAL TRIGGER ---
# 1. Initialize the session 
active_session = boto3.Session() 

# 2. Call the function
start_recovery(active_session)