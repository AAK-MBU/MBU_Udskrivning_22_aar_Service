"""Helper functions"""

import os
import logging
import requests

from automation_server_client import AutomationServer
from automation_server_client._models import Workqueue

logger = logging.getLogger(__name__)

ATS_TOKEN = os.getenv("ATS_TOKEN")
ATS_URL = os.getenv("ATS_URL")


def fetch_workqueue(workqueue_name: str):
    """
    Helper function to fetch the desired workqueue based on a provided workqueue_name
    """

    headers = {"Authorization": f"Bearer {ATS_TOKEN}"}

    full_url = f"{ATS_URL}/workqueues/by_name/tan.udskrivning22.{workqueue_name}"

    response_json = requests.get(full_url, headers=headers, timeout=60).json()
    workqueue_id = response_json.get("id")

    os.environ["ATS_WORKQUEUE_OVERRIDE"] = str(workqueue_id)  # override it
    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    return workqueue


def fetch_workqueue_workitems(workqueue: Workqueue):
    """
    Helper function to fetch all workitems for a given workqueue (with pagination).
    """

    ats_headers = {"Authorization": f"Bearer {ATS_TOKEN}"}

    all_items = []

    page = 1

    size = 200  # use max allowed per page to minimize requests

    while True:
        full_url = f"{ATS_URL}/workqueues/{workqueue.id}/items?page={page}&size={size}"
        response = requests.get(full_url, headers=ats_headers, timeout=60)
        response.raise_for_status()

        response_json = response.json()
        items = response_json.get("items", [])
        all_items.extend(items)

        total_pages = response_json.get("total_pages", 1)

        if page >= total_pages:
            break

        page += 1

    return all_items
