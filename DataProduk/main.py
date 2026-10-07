import sys
from datetime import datetime

print(">>> main.py started")

from src.data_produk.pipelines.run_daily_etl import run_daily_etl
from src.data_produk.pipelines.config import _failure_ctx, BQ_TARGETS
from src.data_produk.utils.log import (
    setup_event_logging,
    emit,
)
from src.data_produk.utils.notify import (
    send_alert_email,
    build_pipeline_failure_email,
    close_smtp,
)

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
    if dry_run:
        print(">>> dry-run mode enabled")

    run_key = datetime.now().strftime("%Y%m%d%H%M")

    # This project does not capture a run transcript; the failure email still
    # needs a log_path value (empty in dry-run and in production here).
    log_path = ""

    # Structured JSON events: written for BOTH production and dry-run
    _, stop_event_logging = setup_event_logging(run_key, dry_run)

    try:
        print(f">>> run_key={run_key}")
        emit("PIPELINE", "main", f"ETL start (run_key={run_key})")
        print(">>> calling run_daily_etl()")
        run_daily_etl(dry_run=dry_run)
        emit("PIPELINE", "main", "ETL Pipeline finished")
        print(">>> main.py finished")
    except Exception as e:
        emit(
            "PIPELINE", "main",
            f"Unhandled exception: {type(e).__name__} {e}",
            level="ERROR",
        )
        ctx = _failure_ctx or {}
        subject, body_html = build_pipeline_failure_email(
            "data_produk",
            e,
            run_key=run_key,
            log_path=log_path,
            stage=ctx.get("stage", ""),
            bq_updates=BQ_TARGETS,
            minio_files=ctx.get("minio_files") or [],
            rollback_hint=ctx.get("rollback_hint", ""),
            enable_explanation=not dry_run,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        raise
    finally:
        close_smtp()
        stop_event_logging()
