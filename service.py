"""
Windows Service for continuously fetching and processing workitems from ATS.

This script defines a Windows service that:
- Starts automatically (or manually) on a Windows machine.
- Runs your process logic continuously in the background.
- Stops cleanly when Windows sends a stop signal.

The service uses pywin32's `ServiceFramework` to integrate with the
Windows Service Control Manager (SCM), which handles start/stop events.
"""

import sys

import time
import win32serviceutil
import win32service
import win32event
import servicemanager

from helpers import helper_functions, faglig_vurdering_udfoert, get_forms, add_to_final_queue


# ╔══════════════════════════════════════════════╗
# ║ 🔥 REMOVE BEFORE DEPLOYMENT (TEMP OVERRIDES) 🔥 ║
# ╚══════════════════════════════════════════════╝
### This block disables SSL verification and overrides env vars ###
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
_old_request = requests.Session.request
def unsafe_request(self, *args, **kwargs):
    kwargs['verify'] = False
    return _old_request(self, *args, **kwargs)
requests.Session.request = unsafe_request
# ╔══════════════════════════════════════════════╗
# ║ 🔥 REMOVE BEFORE DEPLOYMENT (TEMP OVERRIDES) 🔥 ║
# ╚══════════════════════════════════════════════╝


class WorkqueueService(win32serviceutil.ServiceFramework):
    """
    Defines the Windows service and its lifecycle behavior.

    This class connects the Python logic to Windows' service management layer.

    When the service is installed and started using:
        python service.py install
        python service.py start

    Windows will:
      1. Instantiate this class.
      2. Call `SvcDoRun()` to start it.
      3. Call `SvcStop()` when stopping it.
    """

    _svc_name_ = "WorkqueueService"
    _svc_display_name_ = "MBU Udskrivning 22 år - Workqueue Processing Service"
    _svc_description_ = "Fetches and processes workitems from ATS continuously"

    def __init__(self, args=None, mock_run=False):
        """
        Called once when Windows starts the service.

        Sets up:
          - An event (`hWaitStop`) to signal when the service should stop.
          - A `running` flag that controls the main loop.
        """

        if not mock_run:
            super().__init__(args)

        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

        self.running = True

    def SvcStop(self):
        """
        Called when Windows sends a "Stop Service" signal.

        This method:
          - Notifies Windows that the service is stopping.
          - Logs a message to the Windows Event Log.
          - Sets `self.running = False` so the main loop can exit gracefully.
        """

        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)

        servicemanager.LogInfoMsg("Workqueue service stopping...")

        self.running = False

        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        """
        Called when Windows starts the service.

        Basically the "main entry point" inside the Windows context.
        It logs a message and starts the actual service logic in `main()`.
        """

        servicemanager.LogInfoMsg("Workqueue service starting...")

        self.main()

    def main(self):
        """
        Core logic of the service — runs continuously until stopped.

        This loop:
          1. Iterates over a list of predefined workqueues.
          2. Fetches workitems from ATS for each queue.
          3. Passes them to the appropriate handler function.
          4. Waits 5 minutes before repeating.

        If an exception occurs, it logs the error and waits 60 seconds
        before trying again (this prevents the service from crashing).
        """

        while self.running:
            try:
                # Find workitems with pending status and reevaluate them - if completed, update status to new so workitem reruns
                print("Step 1 -> Fetching workitems for 'faglig_vurdering_udfoert' workqueue...")
                workqueue_name = "tan.udskrivning22.faglig_vurdering_udfoert"

                workqueue = helper_functions.fetch_workqueue(workqueue_name)
                workitems = helper_functions.fetch_workqueue_workitems(workqueue)

                faglig_vurdering_udfoert.main(workitems)

                # Step 2 -> Get formular submissions for the 2 udskrivning formulars and add workitems to journalization queue
                print("Step 2 -> Get formular submissions for the 2 udskrivning formulars and add workitems to journalization queue")
                form_results = get_forms.get_forms()
                print(f"found {len(form_results)} <-- form_Results")
                for res in form_results:
                    workqueue_name = "jou.solteqtand.main"
                    workqueue = helper_functions.fetch_workqueue(workqueue_name)
                    existing_refs = {str(r) for r in helper_functions.get_workqueue_item_references(workqueue)}

                    form_id = res.get("form_id")
                    if form_id != "3d55417d-ffe1-4485-b529-592ac20f767c":
                        continue

                    if form_id in existing_refs:
                        print(f"Workitem for form_id {form_id} already exists in journalizing queue — skipping creation.")

                    else:
                        workqueue.add_item(data={"item": {"reference": form_id, "data": res}}, reference=form_id)

                        print(f"Created new workitem for form_id {form_id} in journalizing queue.")

                # Fetch process dashboard, check if pending citizens are ready to be completed, and create workitems in the final workqueue
                print("Step 3 -> Finding ready process runs and adding workitems to final queue...")
                add_to_final_queue.main()

                # Sleep for 5 minutes before next run
                print("Sleeping for 5 minutes...\n")
                time.sleep(300)

            except Exception as e:
                servicemanager.LogErrorMsg(f"Error in service loop: {e}")
                time.sleep(60)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        # e.g. "install", "start", "stop" → real service command
        win32serviceutil.HandleCommandLine(WorkqueueService)

    else:
        # Local mock mode
        service = WorkqueueService(mock_run=True)
        service.main()
