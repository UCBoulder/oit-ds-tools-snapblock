"""Functions for connecting to GraphQL APIs.

Each function takes a connection_info argument which is a dict identifying the endpoint to
connect to. It should always have an "endpoint" argument identifying the URL to POST to. The
remaining KVs of connection_info should correspond to the HTTP headers to send (e.g. for
authentication). The "content-type" header is automatically set to "application/json".
"""

from pprint import pformat
from typing import Callable

import requests

from .util import sizeof_fmt, reveal_secrets, SB_LOGGER


class GraphQLError(Exception):
    """Exception for when GraphQL response lists errors"""

    def __init__(self, message, errors):
        super().__init__(message)
        self.errors = errors


def _graphql_query(request):
    response = requests.post(**request)
    response.raise_for_status()
    result = response.json()
    received_size = len(response.content)
    sent_size = len(response.request.body)
    if "errors" in result:
        errors = result["errors"]
        message = f"Response listed {len(errors)} errors:\n"
        message += "\n".join(pformat(i) for i in errors[:5])
        if len(errors) > 5:
            message += f"\n(Plus {len(errors) - 5} more)"
        message += f'\n\nRequest JSON:\n{pformat(request["json"])[:2000]}'
        raise GraphQLError(message, errors)
    return result["data"], sent_size, received_size


def query(
    query_str: str,
    connection_info: dict,
    variables: dict = None,
    operation_name: str = None,
    next_variables_getter: Callable = None,
):
    """POSTs a GraphQL query or mutation and returns the "data" entry of the response.

    If next_variables_getter is given, this is a function which will take the "data" entry of the
    response. If it returns a dict, then the query will be POSTED again using this dict as the
    new variables param (i.e. to get the next page of data). If it returns None or {}, the function
    ends. With this option, a list of "data" response entries is returned instead of a singular.
    """

    # pylint:disable=too-many-locals
    # pylint:disable=too-many-arguments
    connection_info = reveal_secrets(connection_info)
    if next_variables_getter:
        current_vars = variables
        next_vars = next_variables_getter
    else:
        current_vars = variables

        def next_vars(_):
            return

    message = f'GraphQL: Querying {connection_info["endpoint"]}: {query_str[:200]} ...'
    if operation_name:
        message += f"\nusing operation {operation_name}"
    SB_LOGGER.info(message)

    request = {"url": connection_info["endpoint"]}
    request["headers"] = {k: v for k, v in connection_info.items() if k != "endpoint"}
    request["json"] = {"query": query_str}
    if operation_name:
        request["json"]["operationName"] = operation_name

    result_data = []
    total_sent = 0
    total_received = 0
    while current_vars or not result_data:
        if current_vars:
            request["json"]["variables"] = current_vars
        data, sent_size, received_size = _graphql_query(request)
        result_data.append(data)
        total_sent += sent_size
        total_received += received_size
        current_vars = next_vars(data)

    message = (
        f"GraphQL: Sent {sizeof_fmt(total_sent)} and received {sizeof_fmt(total_received)}"
        " of data"
    )
    if len(result_data) > 1:
        message += " with {len(result_data)} requests"
    else:
        result_data = result_data[0]
    SB_LOGGER.info(message)
    return result_data
