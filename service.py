"""
Windows Service for continuously fetching and processing workitems from ATS.

This script defines a Windows service that:
- Starts automatically (or manually) on a Windows machine.
- Runs your Python process logic continuously in the background.
- Stops cleanly when Windows sends a stop signal.

The service uses pywin32's `ServiceFramework` to integrate with the
Windows Service Control Manager (SCM), which handles start/stop events.
"""

import time
import win32serviceutil
import win32service
import win32event
import servicemanager

from helpers import faglig_vurdering_udfoert, formular_indsendt, helper_functions


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

    def __init__(self, args):
        """
        Called once when Windows starts the service.

        Sets up:
          - An event (`hWaitStop`) to signal when the service should stop.
          - A `running` flag that controls the main loop.
        """

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
                for workqueue_name in ["faglig_vurdering_udfoert", "formular_indsendt"]:
                    workqueue = helper_functions.fetch_workqueue(workqueue_name)
                    workitems = helper_functions.fetch_workqueue_workitems(workqueue)

                    if workqueue_name == "faglig_vurdering_udfoert":
                        faglig_vurdering_udfoert.main(workitems)

                    if workqueue_name == "formular_indsendt":
                        formular_indsendt.main(workitems)

                # Sleep for 10 seconds before next run
                time.sleep(10)

            except Exception as e:
                servicemanager.LogErrorMsg(f"Error in service loop: {e}")
                time.sleep(60)


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(WorkqueueService)
