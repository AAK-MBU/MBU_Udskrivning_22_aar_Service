"""Module for fetching patients that have turned 22 as of today's date"""

import os

from automation_server_client._models import WorkItem

from mbu_dev_shared_components.solteqtand.database.db_handler import SolteqTandDatabase

import helper_functions

SOLTEQ_TAND_DB_CONN_STRING = os.getenv("DBCONNECTIONSTRINGSOLTEQTAND")


def main(workitems):
    """Main function to execute the script."""

    for item_dict in workitems:
        if item_dict.get("status") != "pending user action":
            continue

        item = WorkItem(**item_dict)

        citizen_cpr = item.reference

        db_handler = SolteqTandDatabase(conn_str=SOLTEQ_TAND_DB_CONN_STRING)

        citizen_bookings = check_if_faglig_vurdering_udfoert(db_handler=db_handler, cpr=citizen_cpr)

        if citizen_bookings:
            for i, book in enumerate(citizen_bookings):
                print(f"i: {i}")
                print("printing book:")
                print(book)

            if len(citizen_bookings) > 1:
                item.fail(message="Borgeren har mere end 1 aftale med aftaletype 'Z - 22 år - Borger fyldt 22 år'!")

            else:
                if citizen_bookings[0].get("Status") in ("632", "634"):
                    item.update_status(status="new", message="Status opdateret af service")


def check_if_faglig_vurdering_udfoert(db_handler: SolteqTandDatabase, cpr: str):
    """
    Check if a citizen has a booking with the specified aftaletype and -status
    """

    query = """
        SELECT
            b.BookingID,
            b.CreatedDateTime,
            bt.Description,
            b.Status
        FROM
            [tmtdata_prod].[dbo].[BOOKING] b
        JOIN
            PATIENT p ON p.patientId = b.patientId
        JOIN
            BOOKINGTYPE bt ON bt.BookingTypeID = b.BookingTypeID
        WHERE
            cpr = ?
            AND Description = 'Z - 22 år - Borger fyldt 22 år'
        ORDER BY
            CreatedDateTime DESC
    """

    # pylint: disable=protected-access
    return db_handler._execute_query(query, params=(cpr,))


if __name__ == "__main__":
    workqueue_name = "faglig_vurdering_udfoert"
    workqueue = helper_functions.fetch_workqueue(workqueue_name)
    test_workitems = helper_functions.fetch_workqueue_workitems(workqueue)

    if workqueue_name == "faglig_vurdering_udfoert":
        main(test_workitems)
