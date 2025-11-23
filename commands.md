# Operational Commands

## Initial Backfill (November 18, 2025)

```bash
docker-compose exec airflow-scheduler airflow dags backfill -s 2025-11-18 -e 2025-11-18  incremental_EL_dag
```

## Future Backfill (Remaining dates)
```bash
docker-compose exec airflow-scheduler airflow dags backfill --start-date 2025-11-14 --end-date 2026-01-08 extract_and_filter_orders
```