import functools
import time
from datetime import datetime, timezone

# ==============================
# Migration Control Tower
# Responsibility: execution wrapper, timing, structured logging
# NOT responsible for: DynamoDB audit writes (DAG owns that)
# ==============================
def migration_control_tower(engine_type="VOLUMETRIC"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(session, batch_id, *args, **kwargs):

            print(f"🛰️  Tower: [{engine_type}] {batch_id} — cleared for takeoff")
            start = datetime.now(timezone.utc)

            try:
                # Execute the Spark job
                result = func(session, batch_id, *args, **kwargs)

                duration = (datetime.now(timezone.utc) - start).total_seconds()
                print(
                    f"✅ Tower: [{engine_type}] {batch_id} — LANDED | "
                    f"{result.get('record_count', 0):,} records | "
                    f"{duration:.2f}s"
                )
                return result

            except Exception as e:
                duration = (datetime.now(timezone.utc) - start).total_seconds()
                print(
                    f"❌ Tower: [{engine_type}] {batch_id} — CRASHED | "
                    f"{duration:.2f}s | error={str(e)}"
                )
                raise  # DAG on_failure_callback handles audit FAILED write

        return wrapper
    return decorator