"""Basic I/O operations on database-like systems.

Each function takes a connection_info argument which is a dict identifying the system to
connect to. It should always have a "system_type" member identifying one of the following
supported system:
    - "oracle" for oracledb.connect
    - "postgres" for psycopg2.connect
    - "odbc" for odbc_connect (see below), a helper function that turns keyword args into an odbc
        connection string
    - "mysql" for mysql.connector.connect

The remaining KVs of connection_info should map directly to the keyword arguments used in calling
the constructor indicated in the list above, with some exceptions:
    - For oracle, if your system uses an SID instead of a name, you cannot pass the easy connection
        string as the "dsn" argument, so instead pass "host", "port", and "sid" individually.
"""

# pylint:disable=broad-except

import oracledb
import psycopg2
import pyodbc
from mysql import connector as mysql_connector
import pandas as pd

from .util import sizeof_fmt, reveal_secrets, SB_LOGGER


def sql_extract(
    sql_query: str,
    connection_info: dict,
    query_params=None,
    lob_columns: list = None,
    chunks_prefix: str = None,
    chunksize: int = 1000,
) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. Column names are accepted and returned in all lowercase.

    :param sql_query: The SELECT statement to get data with
    :param connection_info: Target system info; see this module's docstring
    :param query_params: List or dict specifying the values of bind variables for the query
    :param lob_columns: Names of columns containing LOB-type data that must be read as an
        additional step
    :param chunks_prefix: If given, saves the query results in chunks as pickled files
        with the given prefix (including path) to local disk. Files are named "{prefix}_0",
        etc. and can be read with pandas.read_pickle. Intended for queries too large to load into
        memory.
    :param chunksize: How many rows to load into memory at a time. If chunks_prefix is
        given, this also determines rows per Parquet file.
    :return: Either a DataFrame result or a list of pickle filenames
    """
    # pylint:disable=too-many-arguments

    info = reveal_secrets(connection_info)
    function = _switch(
        info,
        oracle=_oracle_sql_extract,
        postgres=_get_sql_extract("Postgres", psycopg2.connect),
        odbc=_get_sql_extract("ODBC", _odbc_connect),
        mysql=_get_sql_extract("MySQL", mysql_connector.connect),
    )
    dataframe = function(
        sql_query, info, query_params, lob_columns, chunks_prefix, chunksize
    )
    return dataframe


def insert(
    dataframe: pd.DataFrame,
    table_identifier: str,
    connection_info: dict,
    pre_insert_statements: list[str] = None,
    pre_insert_params: list = None,
    max_error_proportion: float = 0.05,
) -> pd.DataFrame:
    """Takes a dataframe and table identifier and appends the data into that table.
    Dataframe columns must match table column names (case insensitive, order irrelevant).

    :param dataframe: The data to insert
    :param table_identifier: The table to insert into (`schema.table`)
    :param connection_info: The database connection info dict (see module docstring)
    :param pre_insert_statements: An optional list of sql statements to execute before inserting;
        for example, to delete rows
    :param pre_insert_params: An optional list of query parameters (lists or dicts) to go along with
        each pre-insert statement (aka bind variables)
    :param max_error_proportion: If the proportion of failed insert rows is greater than this, the
        entire transaction is rolled back (including pre-insert statements)
    """
    # pylint:disable=too-many-arguments

    info = reveal_secrets(connection_info)
    function = _switch(
        info,
        oracle=_oracle_insert,
        postgres=_get_insert("Postgres", psycopg2.connect),
        odbc=_get_insert("ODBC", _odbc_connect),
        mysql=_get_insert("MySQL", mysql_connector.connect),
    )
    return function(
        dataframe,
        table_identifier,
        info,
        pre_insert_statements,
        pre_insert_params,
        max_error_proportion,
    )


def update(
    dataframe: pd.DataFrame,
    table_identifier: str,
    match_on: list,
    connection_info: dict,
    pre_update_statements: list[str] = None,
    pre_update_params: list = None,
    max_error_proportion: float = 0.05,
) -> pd.DataFrame:
    """Takes a dataframe and table identifier and updates the data into that table.

    :param dataframe: The data to insert
    :param table_identifier: The table to insert into (`schema.table`)
    :param match on: A list of columns for matching. Every other column of the dataframe is used
        to update the matching row of the database table. Case insensitive.
    :param connection_info: The database connection info dict (see module docstring)
    :param pre_update_statements: An optional list of sql statements to execute before updating;
        for example, to delete rows
    :param pre_update_params: An optional list of query parameters (lists or dicts) to go along with
        each pre-update statement (aka bind variables)
    :param max_error_proportion: If the proportion of failed insert rows is greater than this, the
        entire transaction is rolled back (including pre-insert statements)
    """
    # pylint:disable=too-many-arguments

    info = reveal_secrets(connection_info)
    function = _switch(
        info,
        oracle=_oracle_update,
        postgres=_get_update("Postgres", psycopg2.connect),
        odbc=_get_update("ODBC", _odbc_connect),
        mysql=_get_update("MySQL", mysql_connector.connect),
    )
    return function(
        dataframe,
        table_identifier,
        match_on,
        info,
        pre_update_statements,
        pre_update_params,
        max_error_proportion,
    )


def execute_sql(sql_statement: str, connection_info: dict, query_params=None):
    """Executes the given SQL statement, with an optional list or dict specifying the values of
    bind variables for the query."""

    info = reveal_secrets(connection_info)
    function = _switch(
        info,
        oracle=_oracle_execute_sql,
        postgres=_get_execute_sql("Postgres", psycopg2.connect),
        odbc=_get_execute_sql("ODBC", _odbc_connect),
        mysql=_get_execute_sql("MySQL", mysql_connector.connect),
    )
    return function(sql_statement, info, query_params)


def _switch(connection_info, **kwargs):
    for key, value in kwargs.items():
        if connection_info["system_type"] == key:
            del connection_info["system_type"]
            return value
    raise ValueError(f'System type "{connection_info["system_type"]}" is not supported')


##########
# Oracle functions
##########


def _log_oracle_error(error, sql_query):
    try:
        offset = error.args[0].offset
    except (IndexError, AttributeError):
        pass
    else:
        line_no = len(sql_query[:offset].split("\n"))
        line = (
            sql_query[:offset].split("\n")[-1] + "█" + sql_query[offset:].split("\n")[0]
        )
        message = f"Line {line_no}: {line[:100]}"
        SB_LOGGER.error("Oracle: Database error - %s\n%s", error, message)


def _prepare_oracle_connection(connection_info):
    # Enable "thick mode" for oracle db, which is required for CIW
    # See: https://github.com/oracle/python-oracledb/discussions/170
    oracledb.init_oracle_client()
    # If an sid is used, a dsn needs to be constructed to support this
    if "sid" in connection_info:
        if "port" in connection_info:
            port = connection_info["port"]
            del connection_info["port"]
        else:
            port = 1521
        dsn = oracledb.makedsn(connection_info["host"], port, connection_info["sid"])
        connection_info["dsn"] = dsn
        del connection_info["host"]
        del connection_info["sid"]


def _oracle_host(dsn_string):
    try:
        # First try it with Net Connect Descriptor String
        return dsn_string.split("HOST")[1].split(")")[0].replace("=", "").strip()
    except IndexError:
        # Now try it using Easy Connect syntax
        return dsn_string.split("/")[0].split(":")[0].strip()


def _oracle_sql_extract(
    sql_query: str,
    connection_info: dict,
    query_params=None,
    lob_columns: list = None,
    chunks_prefix: str = None,
    chunksize: int = 1000,
) -> pd.DataFrame:
    """Oracle-specific implementation of the sql_extract function"""
    # pylint:disable=too-many-statements
    # pylint:disable=too-many-branches
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-arguments

    if lob_columns is None:
        lob_columns = []
    else:
        lob_columns = [i.lower() for i in lob_columns]
    _prepare_oracle_connection(connection_info)
    if "encoding" not in connection_info:
        connection_info["encoding"] = "UTF-8"
    with oracledb.connect(**connection_info) as conn:
        host = _oracle_host(conn.dsn)
        sql_snip = " ".join(sql_query.split())[:200] + " ..."
        log_str = f"Oracle: Reading from {host}: {sql_snip}"
        if query_params:
            log_str += f"\nwith injected params: {query_params}"
        SB_LOGGER.info(log_str)

        cursor = conn.cursor()
        cursor.arraysize = chunksize
        try:
            if query_params:
                cursor.execute(sql_query, query_params)
            else:
                cursor.execute(sql_query)
            columns = [i[0].lower() for i in cursor.description]

            if chunks_prefix:
                count = 0
                size = 0
                filenames = []
                while True:
                    rows = cursor.fetchmany()
                    if not rows:
                        break
                    data = pd.DataFrame(rows, columns=columns)
                    count += len(data.index)
                    for column in lob_columns:
                        data[column] = data[column].map(
                            lambda x: x.read() if x else None
                        )
                    size += sum(data.memory_usage())
                    filename = f"{chunks_prefix}_{len(filenames)}"
                    filenames.append(filename)
                    data.to_pickle(filename)
            else:
                rows = cursor.fetchall()
                data = pd.DataFrame(rows, columns=columns)
                count = len(data.index)
                for column in lob_columns:
                    SB_LOGGER.info("Reading data from LOB column %s", column)
                    data[column] = data[column].map(lambda x: x.read() if x else None)
                size = sum(data.memory_usage())

            SB_LOGGER.info("Oracle: Read %s rows, %s", count, sizeof_fmt(size))
            if chunks_prefix:
                return filenames
            return data

        except oracledb.DatabaseError as exc:
            _log_oracle_error(exc, sql_query)
            raise


def _oracle_insert(
    dataframe: pd.DataFrame,
    table_identifier: str,
    connection_info: dict,
    pre_insert_statements: list[str] = None,
    pre_insert_params: list = None,
    max_error_proportion: float = 0.05,
) -> pd.DataFrame:
    """Oracle-specific implementation of the insert function"""
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-arguments

    batch_size = 500
    errors = 0
    insert_sql = (
        f'INSERT INTO {table_identifier} ({",".join(list(dataframe.columns))}) '
        + f'VALUES ({",".join(":" + i for i in dataframe.columns)})'
    )
    _prepare_oracle_connection(connection_info)
    if "encoding" not in connection_info:
        connection_info["encoding"] = "UTF-8"
    if pre_insert_statements is None:
        pre_insert_statements = []

    # Replace NA values with None and turn to list of dicts
    records = [
        {k: None if pd.isnull(v) else v for k, v in i.items()}
        for i in dataframe.to_dict("records")
    ]

    with oracledb.connect(**connection_info) as conn:
        host = _oracle_host(conn.dsn)
        cursor = conn.cursor()

        _oracle_execute_statements(
            conn, cursor, host, pre_insert_statements, pre_insert_params
        )

        try:
            SB_LOGGER.info("Oracle: Inserting into %s on %s", table_identifier, host)
            # Insert records in batches
            for start in range(0, len(records), batch_size):
                to_insert = records[start : start + batch_size]
                cursor.executemany(insert_sql, to_insert, batcherrors=True)
                batch_errors = cursor.getbatcherrors()
                for error in batch_errors[: 10 - errors]:
                    SB_LOGGER.error(
                        "Oracle: Database error %s while inserting data %s",
                        error.message,
                        to_insert[error.offset],
                    )
                errors += len(batch_errors)
            if records:
                error_proportion = float(errors) / float(len(records))
                if error_proportion > max_error_proportion:
                    SB_LOGGER.error(
                        "%s of insert actions failed, exceeding the set maximum (%s); rolling back "
                        "transaction.",
                        f"{error_proportion:.1%}",
                        f"{max_error_proportion:.1%}",
                    )
                    conn.rollback()
                else:
                    conn.commit()
        except Exception:
            conn.rollback()
            raise

    # Logging
    if errors > 10:
        SB_LOGGER.error(
            "Oracle: %s more database errors while inserting not shown", errors - 10
        )
    SB_LOGGER.info(
        "Oracle: Inserted %s rows, %s",
        len(records) - errors,
        sizeof_fmt(sum(dataframe.memory_usage())),
    )
    if errors:
        raise RuntimeError(f"Failed to insert {errors} records")


def _oracle_update(
    dataframe: pd.DataFrame,
    table_identifier: str,
    match_on: list,
    connection_info: dict,
    pre_update_statements: list[str] = None,
    pre_update_params: list = None,
    max_error_proportion: float = 0.05,
) -> pd.DataFrame:
    """Oracle-specific implementation of the update function"""
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-arguments

    batch_size = 500
    errors = 0
    set_columns = [i for i in dataframe.columns if i not in match_on]
    set_list = [f"{i} = :{i}" for i in set_columns]
    match_list = [f"{i} = :{i}" for i in match_on]
    update_sql = (
        f'UPDATE {table_identifier} SET {", ".join(set_list)} '
        + f'WHERE {" AND ".join(match_list)}'
    )
    SB_LOGGER.info(update_sql)
    _prepare_oracle_connection(connection_info)
    if "encoding" not in connection_info:
        connection_info["encoding"] = "UTF-8"
    if pre_update_statements is None:
        pre_update_statements = []

    # Replace NA values with None and turn to list of dicts
    records = [
        {k: None if pd.isnull(v) else v for k, v in i.items()}
        for i in dataframe.to_dict("records")
    ]

    with oracledb.connect(**connection_info) as conn:
        host = _oracle_host(conn.dsn)
        cursor = conn.cursor()

        _oracle_execute_statements(
            conn, cursor, host, pre_update_statements, pre_update_params
        )

        try:
            SB_LOGGER.info("Oracle: Updating data in %s on %s", table_identifier, host)
            # Update records in batches
            for start in range(0, len(records), batch_size):
                to_update = records[start : start + batch_size]
                cursor.executemany(update_sql, to_update, batcherrors=True)
                batch_errors = cursor.getbatcherrors()
                for error in batch_errors[: 10 - errors]:
                    SB_LOGGER.error(
                        "Oracle: Database error %s while updating data %s",
                        error.message,
                        to_update[error.offset],
                    )
                errors += len(batch_errors)
            if records:
                error_proportion = float(errors) / float(len(records))
                if error_proportion > max_error_proportion:
                    SB_LOGGER.error(
                        "%s of update actions failed, exceeding the set maximum (%s); rolling back "
                        "transaction.",
                        f"{error_proportion:.1%}",
                        f"{max_error_proportion:.1%}",
                    )
                    conn.rollback()
                else:
                    conn.commit()
        except Exception:
            conn.rollback()
            raise

    # Logging
    if errors > 10:
        SB_LOGGER.error(
            "Oracle: %s more database errors while updating not shown", errors - 10
        )
    SB_LOGGER.info(
        "Oracle: Updated %s rows, %s",
        len(records) - errors,
        sizeof_fmt(sum(dataframe.memory_usage())),
    )
    if errors:
        raise RuntimeError(f"Failed to update {errors} records")


def _oracle_execute_sql(sql_statement, connection_info: dict, query_params=None):
    """Oracle-specific implementation of the execute_sql function"""

    _prepare_oracle_connection(connection_info)
    if "encoding" not in connection_info:
        connection_info["encoding"] = "UTF-8"
    if isinstance(sql_statement, str):
        sql_statement = [sql_statement]
        query_params = [query_params]

    with oracledb.connect(**connection_info) as conn:
        host = _oracle_host(conn.dsn)
        cursor = conn.cursor()
        _oracle_execute_statements(conn, cursor, host, sql_statement, query_params)
        conn.commit()


def _oracle_execute_statements(conn, cursor, host, statements, query_params=None):
    if query_params is None:
        query_params = [None] * len(statements)
    try:
        for sql, params in zip(statements, query_params):
            sql_snip = " ".join(sql.split())[:200] + " ..."
            log_str = f"Oracle: Executing on {host}: {sql_snip}"
            if params:
                log_str += f"\nwith injected params: {params}"
            SB_LOGGER.info(log_str)
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
    # pylint:disable=broad-except
    except Exception as err:
        if isinstance(err, oracledb.DatabaseError):
            _log_oracle_error(err, sql)
        conn.rollback()
        raise


##########
# Helper function for ODBC connections
##########


def _odbc_connect(**kwargs):
    """Connects to an ODBC database, taking keyword arguments as elements of the connection string.
    E.g. passing uid="myself" adds the element "UID=myself;" to the connection string.
    """

    # Passwords with special characters must be escaped
    # See https://github.com/mkleehammer/pyodbc/issues/569#issuecomment-496234942
    kwargs["pwd"] = "{" + kwargs["pwd"].replace("}", "}}") + "}"
    connection_string = ";".join([f"{k.upper()}={v}" for k, v in kwargs.items()])
    return pyodbc.connect(connection_string, timeout=60)


##########
# Functions for all other supported database types
##########


def _get_sql_extract(system_type, connection_func):
    """Returns a sql_extract function implementation specific to the identified database system
    type"""

    def do_sql_extract(
        sql_query: str,
        connection_info: dict,
        query_params=None,
        lob_columns: list = None,
        chunks_prefix: str = None,
        chunksize: int = 1000,
    ) -> pd.DataFrame:
        """System-specific implementation of the sql_extract function"""
        # pylint:disable=too-many-statements
        # pylint:disable=too-many-branches
        # pylint:disable=too-many-locals
        # pylint:disable=too-many-arguments

        if lob_columns:
            raise ValueError(
                f"The lob_columns parameter is not supported for {system_type} databases."
            )
        with connection_func(**connection_info) as conn:
            if system_type == "ODBC":
                host = connection_info["server"]
            else:
                host = connection_info["host"]
            sql_snip = " ".join(sql_query.split())[:200] + " ..."
            log_str = f"{system_type}: Reading from {host}: {sql_snip}"
            if query_params:
                log_str += f"\nwith injected params: {query_params}"
            SB_LOGGER.info(log_str)

            cursor = conn.cursor()
            cursor.arraysize = chunksize
            if query_params:
                cursor.execute(sql_query, query_params)
            else:
                cursor.execute(sql_query)
            columns = [i[0].lower() for i in cursor.description]

            if chunks_prefix:
                count = 0
                size = 0
                filenames = []
                while True:
                    rows = cursor.fetchmany()
                    if not rows:
                        break
                    data = pd.DataFrame([list(i) for i in rows], columns=columns)
                    count += len(data.index)
                    size += sum(data.memory_usage())
                    filename = f"{chunks_prefix}_{len(filenames)}"
                    filenames.append(filename)
                    data.to_pickle(filename)
            else:
                rows = cursor.fetchall()
                data = pd.DataFrame([list(i) for i in rows], columns=columns)
                count = len(data.index)
                size = sum(data.memory_usage())

            SB_LOGGER.info("%s: Read %s rows, %s", system_type, count, sizeof_fmt(size))
            if chunks_prefix:
                return filenames
            return data

    return do_sql_extract


def _get_insert(system_type, connection_func):
    """Returns an insert function implementation specific to the identified database system type"""

    def do_insert(
        dataframe: pd.DataFrame,
        table_identifier: str,
        connection_info: dict,
        pre_insert_statements: list[str] = None,
        pre_insert_params: list = None,
        max_error_proportion: float = 0.05,
    ) -> pd.DataFrame:
        """System-specific implementation of the insert function"""
        # pylint:disable=too-many-locals
        # pylint:disable=too-many-arguments
        # pylint:disable=too-many-branches

        if system_type == "ODBC":
            insert_sql = (
                f'INSERT INTO {table_identifier} ({",".join(list(dataframe.columns))}) '
                + f'VALUES ({",".join(["?"] * len(dataframe.columns))})'
            )
            # Replace NA values with None and turn to list of lists
            records = [
                [None if pd.isnull(j) else j for j in i]
                for i in dataframe.values.tolist()
            ]
        else:
            param_list = [f"%({i})s" for i in dataframe.columns]
            insert_sql = (
                f'INSERT INTO {table_identifier} ({",".join(list(dataframe.columns))}) '
                + f'VALUES ({",".join(param_list)})'
            )
            # Replace NA values with None and turn to list of dicts
            records = [
                {k: None if pd.isnull(v) else v for k, v in i.items()}
                for i in dataframe.to_dict("records")
            ]

        errors = 0
        if pre_insert_statements is None:
            pre_insert_statements = []

        with connection_func(**connection_info) as conn:
            if system_type == "ODBC":
                host = connection_info["server"]
            else:
                host = connection_info["host"]
            cursor = conn.cursor()

            _execute_statements(
                system_type,
                conn,
                cursor,
                host,
                pre_insert_statements,
                pre_insert_params,
            )

            try:
                SB_LOGGER.info(
                    "%s: Inserting into %s on %s", system_type, table_identifier, host
                )
                # Insert records individually
                for to_insert in records:
                    try:
                        cursor.execute(insert_sql, to_insert)
                    # pylint:disable=broad-except
                    except Exception as err:
                        if errors < 10:
                            SB_LOGGER.error(
                                "%s: Error while inserting data %s:\n%s",
                                system_type,
                                to_insert,
                                err,
                            )
                        errors += 1
                if records:
                    error_proportion = float(errors) / float(len(records))
                    if error_proportion > max_error_proportion:
                        SB_LOGGER.error(
                            "%s: %s of insert actions failed, exceeding the set maximum "
                            "(%s); rolling back transaction.",
                            system_type,
                            f"{error_proportion:.1%}",
                            f"{max_error_proportion:.1%}",
                        )
                        conn.rollback()
                    else:
                        conn.commit()
            except Exception:
                conn.rollback()
                raise

        # Logging
        if errors > 10:
            SB_LOGGER.error(
                "%s: %s more database errors while inserting not shown",
                system_type,
                errors - 10,
            )
        SB_LOGGER.info(
            "%s: Inserted %s rows (%s)",
            system_type,
            len(records) - errors,
            sizeof_fmt(sum(dataframe.memory_usage())),
        )
        if errors:
            raise RuntimeError(f"Failed to insert {errors} records")

    return do_insert


def _get_update(system_type, connection_func):
    """Returns an update function implementation spe3cific to the identified database system type"""

    def do_update(
        dataframe: pd.DataFrame,
        table_identifier: str,
        match_on: list,
        connection_info: dict,
        pre_update_statements: list[str] = None,
        pre_update_params: list = None,
        max_error_proportion: float = 0.05,
    ) -> pd.DataFrame:
        """System-specific implementation of the update function"""
        # pylint:disable=too-many-locals
        # pylint:disable=too-many-arguments
        # pylint:disable=too-many-branches

        set_columns = [i for i in dataframe.columns if i not in match_on]
        if system_type == "ODBC":
            set_list = [f"{i} = ?" for i in set_columns]
            match_list = [f"{i} = ?" for i in match_on]
            # Replace NA values with None and turn to list of lists
            set_values = [
                [None if pd.isnull(j) else j for j in i]
                for i in dataframe[set_columns].values.tolist()
            ]
            match_values = [
                [None if pd.isnull(j) else j for j in i]
                for i in dataframe[match_on].values.tolist()
            ]
            # Combine lists from each group so they will insert into the update statement
            records = [l + r for l, r in zip(set_values, match_values)]
        else:
            set_list = [f"{i} = %({i})s" for i in set_columns]
            match_list = [f"{i} = %({i})s" for i in match_on]
            # Replace NA values with None and turn to list of dicts
            records = [
                {k: None if pd.isnull(v) else v for k, v in i.items()}
                for i in dataframe.to_dict("records")
            ]
        update_sql = (
            f'UPDATE {table_identifier} SET {", ".join(set_list)} '
            + f'WHERE {" AND ".join(match_list)}'
        )

        errors = 0
        if pre_update_statements is None:
            pre_update_statements = []

        with connection_func(**connection_info) as conn:
            if system_type == "ODBC":
                host = connection_info["server"]
            else:
                host = connection_info["host"]
            cursor = conn.cursor()

            _execute_statements(
                system_type,
                conn,
                cursor,
                host,
                pre_update_statements,
                pre_update_params,
            )

            try:
                SB_LOGGER.info(
                    "%s: Updating data in %s on %s", system_type, table_identifier, host
                )
                # Update records individually
                for to_update in records:
                    try:
                        cursor.execute(update_sql, to_update)
                    # pylint:disable=broad-except
                    except Exception as err:
                        if errors < 10:
                            SB_LOGGER.error(
                                "%s: Error while updating data %s:\n%s",
                                system_type,
                                to_update,
                                err,
                            )
                        errors += 1
                if records:
                    error_proportion = float(errors) / float(len(records))
                    if error_proportion > max_error_proportion:
                        SB_LOGGER.error(
                            "%s of update actions failed, exceeding the set maximum "
                            "(%s); rolling back transaction.",
                            f"{error_proportion:.1%}",
                            f"{max_error_proportion:.1%}",
                        )
                        conn.rollback()
                    else:
                        conn.commit()
            except Exception:
                conn.rollback()
                raise

        # Logging
        if errors > 10:
            SB_LOGGER.error(
                "%s: %s more database errors while updating not shown",
                system_type,
                errors - 10,
            )
        SB_LOGGER.info(
            "%s: Updated %s rows (%s)",
            system_type,
            len(records) - errors,
            sizeof_fmt(sum(dataframe.memory_usage())),
        )
        if errors:
            raise RuntimeError(f"{system_type}: Failed to update {errors} records")

    return do_update


def _get_execute_sql(system_type, connection_func):
    """Returns an execute_sql function implmementation specific to the identified database system
    type"""

    def do_execute_sql(sql_statement, connection_info: dict, query_params=None):
        """System-specific implementation of the execute_sql function"""

        if isinstance(sql_statement, str):
            sql_statement = [sql_statement]
            query_params = [query_params]

        with connection_func(**connection_info) as conn:
            if system_type == "ODBC":
                host = connection_info["server"]
            else:
                host = connection_info["host"]
            cursor = conn.cursor()
            _execute_statements(
                system_type, conn, cursor, host, sql_statement, query_params
            )
            conn.commit()

    return do_execute_sql


def _execute_statements(system_type, conn, cursor, host, statements, query_params=None):
    # pylint:disable=too-many-arguments
    if query_params is None:
        query_params = [None] * len(statements)
    try:
        for sql, params in zip(statements, query_params):
            sql_snip = " ".join(sql.split())[:200] + " ..."
            log_str = f"{system_type}: Executing on {host}: {sql_snip}"
            if params:
                log_str += f"\nwith injected params: {params}"
            SB_LOGGER.info(log_str)
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
    except Exception:
        conn.rollback()
        raise
