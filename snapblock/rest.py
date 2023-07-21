"""Functions for connecting to REST APIs.

Each function takes a connection_info argument which is a dict identifying the endpoint to
connect to. It should always have a "domain" key identifying the domain to send the request to.
For example, to send a request to "https://canvas.colorado.edu/api/v1/users/self", set
`connection_info["domain"] = "https://canvas.colorado.edu"` and pass "/api/v1/users/self" for the
`endpoint` param.

The remaining KVs of connection_info should correspond to the HTTP headers to send (e.g. for
authentication), and/or you can include an "auth" key set to a tuple or list of length 2 to use
Basic Authentication. These values are simply passed to the requests module's function as args.

Each function attempts to return a Python object from JSON results, but if results can't be
JSONified, then the raw bytes content is returned instead.
"""

from typing import Callable, Literal
from json.decoder import JSONDecodeError
from multiprocessing.pool import ThreadPool

import requests
from requests.exceptions import HTTPError
import pandas as pd

from .util import sizeof_fmt, reveal_secrets, SB_LOGGER


def get(
    endpoint: str,
    connection_info: dict,
    params: dict = None,
    next_page_getter: Callable = None,
    to_dataframe: bool = False,
) -> list:
    """Sends a GET request to the specified endpoint, along with any params, and returns a list
    of JSON results.

    For paginated data, supply a function to next_page_getter that takes a requests.response object
    and returns either the str endpoint of the next page of data or a dict of params to update the
    initial params with. The next_page_getter should return None when where are no more pages.
    Paginated data is returned as a list of JSON results: if each result is a list, these are
    concatenated together to form a single list.

    If to_dataframe is true, the JSON results will be used to initialize a pandas.Dataframe to be
    returned instead.
    """
    # pylint:disable=too-many-arguments

    return _get(endpoint, connection_info, params, next_page_getter, to_dataframe)


def post(
    endpoint: str, connection_info: dict, params=None, data=None, json=None, files=None
):
    """Sends a POST request along with any data and returns the JSON response. See requests.post
    for more details."""
    # pylint:disable=too-many-arguments

    return _send_modify_request(
        requests.post, endpoint, connection_info, params, data, json, files
    )


def put(
    endpoint: str, connection_info: dict, params=None, data=None, json=None, files=None
):
    """Sends a PUT request along with any data and returns the JSON response. See requests.put
    for more details."""
    # pylint:disable=too-many-arguments

    return _send_modify_request(
        requests.put, endpoint, connection_info, params, data, json, files
    )


def patch(
    endpoint: str, connection_info: dict, params=None, data=None, json=None, files=None
):
    """Sends a PATCH request along with any data and returns the JSON response. See requests.patch
    for more details."""
    # pylint:disable=too-many-arguments

    return _send_modify_request(
        requests.patch, endpoint, connection_info, params, data, json, files
    )


def delete(
    endpoint: str, connection_info: dict, params=None, data=None, json=None, files=None
):
    """Sends a DELETE request along with any data and returns the JSON response. See requests.delete
    for more details."""
    # pylint:disable=too-many-arguments

    return _send_modify_request(
        requests.delete, endpoint, connection_info, params, data, json, files
    )


def get_many(
    endpoints: list[str],
    connection_info: dict,
    params_list: list[dict] = None,
    next_page_getter: Callable = None,
    to_dataframe: bool = False,
    on_error: Literal["raise", "catch"] = "raise",
    num_workers: int = 1,
) -> list:
    """Sends many GET requests defined by a list of endpoints and a corresponding list of params
    dicts. Other params are applied to every GET request. Returns a list of lists (or a list of
    dataframes) comprising the result of each individual GET request. See rest.get task for more
    info.

    If on_error is "catch", any HTTPErrors raised by requests.get will be returned inline within
    the results list, preserving order. For example if you want to ignore 404 responses but raise
    anything else:
        ```
        results = get_many(..., on_error="catch")
        non_404_errors = [
            i
            for i in results
            if isinstance(i, requests.exceptions.HTTPError)
            and i.response.status_code != requests.codes["not found"]
        ]
        if non_404_errors:
            raise non_404_errors[0]
        ok_results = [i for i in results if not isinstance(i, requests.exceptions.HTTPError)]
        ```

    If num_workers > 1, will use a ThreadPool with this many workers to send the requests. The
    order of results will be preserved. Note that this is superior to using get.map due to
    cleaner logs (Prefect Cloud only supports up to 10,000 log entries per flow).
    """
    # pylint:disable=too-many-arguments
    # pylint:disable=too-many-locals

    connection_info = reveal_secrets(connection_info)
    SB_LOGGER.info(
        "REST: Sending %s GET requests to %s ...",
        len(endpoints),
        connection_info["domain"],
    )
    if params_list is None:
        params_list = [None] * len(endpoints)
    if len(endpoints) != len(params_list):
        raise ValueError(
            f"Got {len(endpoints)} endpoints but {len(params_list)} params dicts"
        )

    # single-threaded
    if num_workers == 1:
        results = [
            _soft_catch(_get)(
                endpoint,
                connection_info,
                params,
                next_page_getter,
                to_dataframe,
                log=False,
            )
            for endpoint, params in zip(endpoints, params_list)
        ]

    # multi-threaded
    else:
        kwargs_list = [
            {
                "endpoint": endpoint,
                "connection_info": connection_info,
                "params": params,
                "next_page_getter": next_page_getter,
                "to_dataframe": to_dataframe,
                "log": False,
            }
            for endpoint, params in zip(endpoints, params_list)
        ]
        results = []
        count = 0
        with ThreadPool(num_workers) as pool:
            results_iter = pool.imap(_soft_catch(_get_mappable), kwargs_list)
            while True:
                try:
                    results.append(next(results_iter))
                    count += 1
                    if count % 1000 == 0:
                        SB_LOGGER.info(
                            "Received %s results out of %s so far...",
                            count,
                            len(endpoints),
                        )
                except StopIteration:
                    break

    # Filter and log results
    final_results = []
    successes = 0
    total_size = 0
    caught = 0
    uncaught = []
    for result in results:
        if not isinstance(result, BaseException):
            data, size = result
            final_results.append(data)
            successes += 1
            total_size += size
        elif on_error == "catch" and isinstance(result, HTTPError):
            final_results.append(result)
            caught += 1
        else:
            uncaught.append(result)
    SB_LOGGER.info(
        "REST: Received %s success responses (%s) and caught %s HTTP response errors",
        successes,
        sizeof_fmt(total_size),
        caught,
    )
    # If uncaught exceptions, raise them now
    if uncaught:
        SB_LOGGER.error(
            "REST: Encountered %s uncaught exceptions while sending requests. Only the first "
            "will be raised.",
            len(uncaught),
        )
        raise uncaught[0]
    return final_results


def post_many(
    endpoints: list[str],
    connection_info: dict,
    params_list: list = None,
    data_list: list = None,
    json_list: list = None,
    files_list: list = None,
    num_workers: int = 1,
) -> list:
    """Sends many POST requests along with any data and returns a list of JSON responses. See
    requests.post for more details.

    If num_workers > 1, will use a threadpool with this many workers to send the requests. the
    order of results will be preserved, but not the order in which requests are sent. note that
    this is superior to using post.map due to cleaner logs."""
    # pylint:disable=too-many-arguments

    return _send_modify_requests(
        requests.post,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers,
    )


def put_many(
    endpoints: list[str],
    connection_info: dict,
    params_list: list = None,
    data_list: list = None,
    json_list: list = None,
    files_list: list = None,
    num_workers: int = 1,
) -> list:
    """Sends many PUT requests along with any data and returns a list of JSON responses. See
    requests.put for more details.

    If num_workers > 1, will use a threadpool with this many workers to send the requests. the
    order of results will be preserved, but not the order in which requests are sent. note that
    this is superior to using put.map due to cleaner logs."""
    # pylint:disable=too-many-arguments

    return _send_modify_requests(
        requests.put,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers,
    )


def patch_many(
    endpoints: list[str],
    connection_info: dict,
    params_list: list = None,
    data_list: list = None,
    json_list: list = None,
    files_list: list = None,
    num_workers: int = 1,
) -> list:
    """Sends many PATCH requests along with any data and returns a list of JSON responses. See
    requests.patch for more details.

    If num_workers > 1, will use a threadpool with this many workers to send the requests. the
    order of results will be preserved, but not the order in which requests are sent. note that
    this is superior to using patch.map due to cleaner logs."""
    # pylint:disable=too-many-arguments

    return _send_modify_requests(
        requests.patch,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers,
    )


def delete_many(
    endpoints: list[str],
    connection_info: dict,
    params_list: list = None,
    data_list: list = None,
    json_list: list = None,
    files_list: list = None,
    num_workers: int = 1,
) -> list:
    """Sends many DELETE requests along with any data and returns a list of JSON responses. See
    requests.delete for more details.

    If num_workers > 1, will use a threadpool with this many workers to send the requests. the
    order of results will be preserved, but not the order in which requests are sent. note that
    this is superior to using delete.map due to cleaner logs."""
    # pylint:disable=too-many-arguments

    return _send_modify_requests(
        requests.delete,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers,
    )


#####
# Helper functions
#####


def _get(
    endpoint,
    connection_info,
    params,
    next_page_getter,
    to_dataframe,
    log=True,
):
    # pylint:disable=too-many-arguments
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-branches
    # pylint:disable=unnecessary-lambda-assignment

    if not next_page_getter:
        next_page_getter = lambda _: None
    info = reveal_secrets(connection_info)
    domain = info.pop("domain")
    url = domain + endpoint
    if log:
        SB_LOGGER.info("REST: Sending GET to %s ...", url)
    auth = info.pop("auth", None)
    kwargs = {"headers": info, "timeout": 60}
    if auth:
        if isinstance(auth, list):
            kwargs["auth"] = tuple(auth)
        else:
            kwargs["auth"] = auth
    if params:
        kwargs["params"] = params
    size = 0
    data = []
    while True:
        response = requests.get(url, **kwargs)
        response.raise_for_status()
        size += len(response.content)
        try:
            result = response.json()
        except JSONDecodeError:
            result = response.content
        if isinstance(result, list):
            data += result
        else:
            data.append(result)
        next_page_info = next_page_getter(response)
        if not next_page_info:
            break
        if isinstance(next_page_info, str):
            url = next_page_info
        elif isinstance(next_page_info, dict):
            try:
                kwargs["params"].update(next_page_info)
            except KeyError:
                kwargs["params"] = next_page_info
        else:
            raise TypeError(
                f"Param next_page_getter must return a str or dict, not {type(next_page_info)}"
            )

    if log:
        SB_LOGGER.info(
            "REST: Received %s objects (%s total)", len(data), sizeof_fmt(size)
        )
        if to_dataframe:
            return pd.DataFrame(data)
        return data
    # If this function isn't logging data directly, return the domain so it can be logged
    if to_dataframe:
        return pd.DataFrame(data), size
    return data, size


def _get_mappable(kwargs):
    return _get(**kwargs)


def _send_modify_request(
    method, endpoint, connection_info, params, data, json, files, log=True
):
    # pylint:disable=too-many-arguments
    # pylint:disable=too-many-branches
    info = reveal_secrets(connection_info)
    domain = info.pop("domain")
    url = domain + endpoint
    if log:
        SB_LOGGER.info("REST: Sending %s to %s ...", method.__name__.upper(), url)
    auth = info.pop("auth", None)
    kwargs = {"headers": info}
    if auth:
        if isinstance(auth, list):
            kwargs["auth"] = tuple(auth)
        else:
            kwargs["auth"] = auth
    if params:
        kwargs["params"] = params
    if data:
        kwargs["data"] = data
    if json:
        kwargs["json"] = json
    if files:
        kwargs["files"] = files
    response = method(url, **kwargs)
    response.raise_for_status()
    if response.request.body:
        size = len(response.request.body)
    else:
        size = 0
    if log:
        SB_LOGGER.info("REST: Sent %s bytes", sizeof_fmt(size))
        try:
            return response.json()
        except JSONDecodeError:
            return response.content
    # If this function isn't logging data directly, return the domain so it can be logged
    try:
        return response.json(), size
    except JSONDecodeError:
        return response.content, size


def _send_modify_requests(
    method,
    endpoints,
    connection_info,
    params_list,
    data_list,
    json_list,
    files_list,
    num_workers,
):
    # pylint:disable=too-many-arguments
    # pylint:disable=too-many-branches
    # pylint:disable=too-many-locals

    connection_info = reveal_secrets(connection_info)
    method_name = method.__name__.upper()
    SB_LOGGER.info(
        "REST: Sending %s %s requests to %s ...",
        len(endpoints),
        method_name,
        connection_info["domain"],
    )
    if params_list is None:
        params_list = [None] * len(endpoints)
    if data_list is None:
        data_list = [None] * len(endpoints)
    if json_list is None:
        json_list = [None] * len(endpoints)
    if files_list is None:
        files_list = [None] * len(endpoints)

    if len(endpoints) != len(params_list):
        raise ValueError(
            f"Got {len(endpoints)} endpoints but {len(params_list)} params objects"
        )
    if len(endpoints) != len(data_list):
        raise ValueError(
            f"Got {len(endpoints)} endpoints but {len(data_list)} data objects"
        )
    if len(endpoints) != len(json_list):
        raise ValueError(
            f"Got {len(endpoints)} endpoints but {len(json_list)} json objects"
        )
    if len(endpoints) != len(files_list):
        raise ValueError(
            f"Got {len(endpoints)} endpoints but {len(files_list)} files objects"
        )

    # single-threaded
    if num_workers == 1:
        results = [
            _soft_catch(_send_modify_request)(
                method, endpoint, connection_info, params, data, json, files, log=False
            )
            for endpoint, params, data, json, files in zip(
                endpoints, params_list, data_list, json_list, files_list
            )
        ]

    # multi-threaded
    else:
        kwargs_list = [
            {
                "method": method,
                "endpoint": endpoint,
                "connection_info": connection_info,
                "params": params,
                "data": data,
                "json": json,
                "files": files,
                "log": False,
            }
            for endpoint, params, data, json, files in zip(
                endpoints, params_list, data_list, json_list, files_list
            )
        ]
        results = []
        count = 0
        with ThreadPool(num_workers) as pool:
            results_iter = pool.imap(_soft_catch(_send_mappable), kwargs_list)
            while True:
                try:
                    results.append(next(results_iter))
                    count += 1
                    if count % 1000 == 0:
                        SB_LOGGER.info(
                            "Received %s results out of %s so far...",
                            count,
                            len(endpoints),
                        )
                except StopIteration:
                    break

    # return results
    total_size = sum(
        0 if isinstance(result, BaseException) else result[1] for result in results
    )
    SB_LOGGER.info(
        "REST: Received %s responses (%s total)", len(results), sizeof_fmt(total_size)
    )
    failures = [i for i in results if isinstance(i, BaseException)]
    if failures:
        SB_LOGGER.error(
            "REST: Encountered %s exceptions while sending requests. Only the first "
            "will be raised.",
            len(failures),
        )
        raise failures[0]
    return [response for response, size in results]


def _send_mappable(kwargs):
    return _send_modify_request(**kwargs)


def _soft_catch(function):
    # pylint:disable=broad-except
    # Modify the function to return exceptions instead of raising them
    def modified(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as err:
            return err

    return modified
