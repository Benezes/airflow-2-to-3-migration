# Anbima yield curves

## What broke

The Airflow 2 DAG passed `timetable=` into `DAG()`, used `HttpSensor` from `airflow.sensors.http_sensor`, `PythonSensor` from `airflow.contrib.sensors.python_sensor`, `PostgresOperator` from `airflow.operators.postgres_operator`, contrib `KubernetesPodOperator`, and `DummyOperator`. `timetable` as a distinct DAG argument is removed in Airflow 3 (`schedule` is the only scheduling argument).

## Why

Scheduling collapsed onto `schedule`, which accepts a cron string, timedelta, or a timetable instance. Sensors and the Postgres operator moved to providers. Contrib sensors no longer exist.

## What changed

- Custom `DualSessionTimetable` → `MultipleCronTriggerTimetable("0 8 * * 1-5", "0 18 * * 1-5", timezone="America/Sao_Paulo")` passed as `schedule=`
- `FileSensor` + `PythonSensor` + `HttpSensor` from current providers
- `PostgresOperator` → `SQLExecuteQueryOperator`
- Kubernetes validation pod uses `airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`
- PRE/IPCA vertices still upsert with the same grain `(ref_date, curva, vertice_dias)`
