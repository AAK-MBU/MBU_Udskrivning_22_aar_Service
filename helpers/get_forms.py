"""Module to get forms from the SQL database."""

import os
import urllib.parse

from sqlalchemy import Column, MetaData, Table, create_engine, select, text


def get_forms():
    """
    Fetches the next available form from a specified table in the SQL database
    using SQLAlchemy.

    Returns:
        list[dict]: List of dictionaries with form data.

    Raises:
        Exception: For any unexpected errors.
    """

    try:
        connection_string = os.environ.get("DBCONNECTIONSTRINGPROD")
        if not connection_string:
            raise ValueError("DBCONNECTIONSTRINGPROD environment variable not set.")

        # If the connection string is a long ODBC string, use odbc_connect
        if not connection_string.lower().startswith("mssql+pyodbc:///?odbc_connect="):
            connection_string = (
                "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(connection_string)
            )

        engine = create_engine(connection_string)

        metadata = MetaData()
        journalizing = Table(
            "view_Journalizing",
            metadata,
            Column("form_id"),
            Column("form_type"),
            Column("form_data"),
            Column("status"),
            Column("destination_system"),
            schema="journalizing",
        )
        metadata_tbl = Table(
            "Metadata",
            metadata,
            Column("os2formWebformId"),
            Column("isActive"),
            schema="journalizing",
        )

        # Compose the SQL
        borger_cpr_nummer_manuelt = text(
            "JSON_VALUE([view_Journalizing].form_data, '$.data.borger_cpr_nummer_manuelt') AS borger_cpr_nummer_manuelt"
        )
        vaelg_tandlaege_api = text(
            "JSON_VALUE([view_Journalizing].form_data, '$.data.vaelg_tandlaege_api') AS vaelg_tandlaege_api"
        )
        tandlaege_navn_manuelt = text(
            "JSON_VALUE([view_Journalizing].form_data, '$.data.tandlaege_navn_manuelt') AS tandlaege_navn_manuelt"
        )
        tandlaege_adresse_manuelt = text(
            "JSON_VALUE([view_Journalizing].form_data, '$.data.tandlaege_adresse__dawa') AS tandlaege_adresse_manuelt"
        )
        tandlaege_ydernummer_manuelt = text(
            "JSON_VALUE([view_Journalizing].form_data, '$.data.tandlaege_ydernummer_manuelt') AS tandlaege_ydernummer_manuelt"
        )
        tandlaege_telefonnummer_manuelt = text(
            "JSON_VALUE([view_Journalizing].form_data, '$.data.tandlaege_telefonnummer_manuelt') AS tandlaege_telefonnummer_manuelt"
        )
        samtykke_valg = text(
            "JSON_VALUE([view_Journalizing].form_data, '$.data.samtykke_valg') AS samtykke_valg"
        )
        url = text(
            "(SELECT TOP 1 JSON_VALUE(a.value, '$.url') FROM OPENJSON(JSON_QUERY([view_Journalizing].form_data, '$.data.attachments')) a) AS url"
        )

        stmt = (
            select(
                journalizing.c.form_id,
                journalizing.c.form_type,
                borger_cpr_nummer_manuelt,
                vaelg_tandlaege_api,
                tandlaege_navn_manuelt,
                tandlaege_adresse_manuelt,
                tandlaege_ydernummer_manuelt,
                tandlaege_telefonnummer_manuelt,
                samtykke_valg,
                url,
                journalizing.c.form_data,
            )
            .select_from(
                journalizing.join(
                    metadata_tbl,
                    metadata_tbl.c.os2formWebformId == journalizing.c.form_type,
                )
            )
            .where(
                journalizing.c.status == "New",
                journalizing.c.form_type.in_(["udskrivning_22_aar_privat_tandkl", "udskrivning_22_aar_tandpleje_for"]),
                metadata_tbl.c.isActive == 1,
            )
        )

        with engine.connect() as conn:
            result_proxy = conn.execute(stmt)

            result = []

            for row in result_proxy:
                try:
                    row_as_dict = row._asdict()

                    if "purged" in row_as_dict.get("form_data"):
                        continue

                    if row_as_dict.get("vaelg_tandlaege_api"):
                        parts = [p.strip() for p in row_as_dict["vaelg_tandlaege_api"].split("||")]
                        klinik_navn, klinik_adresse, klinik_ydernummer = parts

                    else:
                        klinik_navn = row_as_dict.get("tandlaege_navn_manuelt")
                        klinik_adresse = row_as_dict.get("tandlaege_adresse_manuelt")
                        klinik_ydernummer = row_as_dict.get("tandlaege_ydernummer_manuelt", None)

                    samtykke_valg_bool = row_as_dict.get("samtykke_valg") == "ja"

                    data = {
                        "cpr": row_as_dict.get("borger_cpr_nummer_manuelt"),
                        "klinik_navn": klinik_navn,
                        "klinik_adresse": klinik_adresse,
                        "klinik_ydernummer": klinik_ydernummer or None,
                        "klinik_telefonnummer": row_as_dict.get("tandlaege_telefonnummer_manuelt"),
                        "samtykke_valg": samtykke_valg_bool,
                        "form_id": row_as_dict.get("form_id"),
                        "form_type": row_as_dict.get("form_type"),
                        "form_data": row_as_dict.get("form_data"),
                        "url": row_as_dict.get("url"),
                    }

                    result.append(data)

                except AttributeError:
                    result.append(dict(row))

        return result

    except Exception as e:
        print(f"Error occurred while getting form data: {e}")

        raise
