# Import modules
import logging

# Import third-party modules
import requests


# Module-level logger
logger = logging.getLogger(__name__)


_SUPPORTED_METHODS: dict[str, callable] = {
    "get": requests.get,
    "post": requests.post,
    "put": requests.put,
    "delete": requests.delete,
    "patch": requests.patch,
    "head": requests.head,
    "options": requests.options,
}


def API_request(
    url: str,
    headers: dict | None = None,
    data=None,
    params: dict | None = None,
    json: dict | None = None,
    files=None,
    request_type: str = "GET",
    timeout: float | None = None,
    **kwargs,
):
    """
    Make an HTTP request to an API and return the JSON-decoded response.

    Parameters:
        url (str): URL of the API.
        headers (dict | None): Headers to send.
        data: Body payload for ``GET``/``POST``/etc.
        params (dict | None): Query string parameters.
        json (dict | None): JSON body (used for non-GET methods).
        files: Files to attach.
        request_type (str): HTTP verb. Defaults to ``"GET"``. Case insensitive.
        timeout (float | None): Request timeout in seconds.
        **kwargs: Additional arguments forwarded to ``requests``.

    Returns:
        Any: JSON-decoded response body.

    Raises:
        ValueError: If ``request_type`` is not supported.
        requests.HTTPError: If the server returned a non-2xx status.
        requests.RequestException: For network-level failures.
    """
    method = request_type.lower()
    request_fn = _SUPPORTED_METHODS.get(method)
    if request_fn is None:
        raise ValueError(
            f"Invalid request type: {request_type}. "
            f"Supported: {sorted(_SUPPORTED_METHODS)}"
        )

    # GET sends ``data`` rather than ``json`` to preserve previous behaviour
    common_kwargs = dict(
        url=url,
        headers=headers,
        params=params,
        files=files,
        timeout=timeout,
        **kwargs,
    )
    if method == "get":
        request_kwargs = {**common_kwargs, "data": data}
    else:
        request_kwargs = {**common_kwargs, "data": data, "json": json}

    try:
        response = request_fn(**request_kwargs)
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error(
            f"API request {method.upper()} {url} failed -> "
            f"HTTP {e.response.status_code}: {e.response.text[:500]}"
        )
        raise
    except requests.RequestException as e:
        logger.error(
            f"API request {method.upper()} {url} failed -> "
            f"{type(e).__name__}: {e}"
        )
        raise

    try:
        return response.json()
    except ValueError:
        # Response was not JSON; surface raw text to the caller rather than
        # silently raising deep inside ``requests``.
        logger.warning(
            f"API response for {method.upper()} {url} is not JSON; "
            "returning raw text."
        )
        return response.text
