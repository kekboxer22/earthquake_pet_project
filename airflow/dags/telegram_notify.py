"""
Shared module for sending Telegram notifications from Airflow DAGs.
Bot token and chat_id are read from environment variables (set in docker-compose.yaml).
"""

import os
import logging
import requests

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


def _send_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram TOKEN/CHAT_ID not set — notification not sent")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to send Telegram notification: {e}")


def on_failure_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    exception = context.get("exception")

    text = (
        f"❌ <b>DAG failed</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Task: <code>{task_id}</code>\n"
        f"Run: <code>{run_id}</code>\n"
        f"Error: {exception}"
    )
    _send_message(text)


def on_success_callback(context):
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    text = (
        f"✅ <b>DAG succeeded</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Run: <code>{run_id}</code>"
    )
    _send_message(text)


def send_custom_message(text: str):
    """Public wrapper for sending arbitrary text (e.g. with computed metrics)."""
    _send_message(text)
