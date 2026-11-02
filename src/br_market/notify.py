from __future__ import annotations

import logging

from airflow.sdk import BaseNotifier

LOGGER = logging.getLogger("br_market.notify")


class MarketDataFailureNotifier(BaseNotifier):
    def __init__(self, channel: str = "market-data-alerts") -> None:
        super().__init__()
        self.channel = channel

    def notify(self, context) -> None:
        dag = context.get("dag")
        ti = context.get("task_instance") or context.get("ti")
        dag_id = getattr(dag, "dag_id", "unknown")
        task_id = getattr(ti, "task_id", "unknown")
        LOGGER.error(
            "alert channel=%s dag_id=%s task_id=%s logical_date=%s",
            self.channel,
            dag_id,
            task_id,
            context.get("logical_date"),
        )
