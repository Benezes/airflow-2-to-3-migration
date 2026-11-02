import logging

LOGGER = logging.getLogger("legacy_etl.notify")


class MarketDataFailureNotifier:
    def __init__(self, channel="market-data-alerts"):
        self.channel = channel

    def __call__(self, context):
        dag = context.get("dag")
        ti = context.get("task_instance") or context.get("ti")
        dag_id = getattr(dag, "dag_id", "unknown")
        task_id = getattr(ti, "task_id", "unknown")
        LOGGER.error(
            "alert channel=%s dag_id=%s task_id=%s execution_date=%s",
            self.channel,
            dag_id,
            task_id,
            context.get("execution_date"),
        )
