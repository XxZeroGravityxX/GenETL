# Import modules
import os
import requests


def API_request(
    url,
    headers=None,
    data=None,
    params=None,
    json=None,
    files=None,
    request_type="GET",
    timeout=None,
    **kwargs,
):
    """
    Make a request to an API.

    Parameters:
        url: str. URL of the API.
        headers: dict (Optional). Headers to send to the API.
        data: dict (Optional). Data to send to the API.
        params: dict (Optional). Parameters to send to the API.
        json: dict (Optional). JSON data to send to the API.
        files: list (Optional). Files to send to the API.
        request_type: str (Optional). Type of request. The default is 'GET'.
        timeout: Optional. Timeout for the request.
        **kwargs: Additional arguments to pass to requests.

    Returns:
        response: dict. Response from the API (JSON).
    """

    ## Make request to the API
    if request_type.lower() == "get":
        ### A GET request to the API
        response = requests.get(
            url=url,
            data=data,
            headers=headers,
            params=params,
            files=files,
            timeout=timeout,
            **kwargs,
        ).json()
    elif request_type.lower() == "post":
        ### A POST request to the API
        response = requests.post(
            url=url,
            data=data,
            headers=headers,
            json=json,
            files=files,
            timeout=timeout,
            **kwargs,
        ).json()
    elif request_type.lower() == "put":
        ### A PUT request to the API
        response = requests.put(
            url=url,
            data=data,
            headers=headers,
            json=json,
            files=files,
            timeout=timeout,
            **kwargs,
        ).json()
    elif request_type.lower() == "delete":
        ### A DELETE request to the API
        response = requests.delete(
            url=url,
            data=data,
            headers=headers,
            json=json,
            files=files,
            timeout=timeout,
            **kwargs,
        ).json()
    elif request_type.lower() == "patch":
        ### A PATCH request to the API
        response = requests.patch(
            url=url,
            data=data,
            headers=headers,
            json=json,
            files=files,
            timeout=timeout,
            **kwargs,
        ).json()
    elif request_type.lower() == "head":
        ### A HEAD request to the API
        response = requests.head(
            url=url,
            data=data,
            headers=headers,
            json=json,
            files=files,
            timeout=timeout,
            **kwargs,
        ).json()
    elif request_type.lower() == "options":
        ### A OPTIONS request to the API
        response = requests.options(
            url=url,
            data=data,
            headers=headers,
            json=json,
            files=files,
            timeout=timeout,
            **kwargs,
        ).json()
    else:
        raise ValueError(f"Invalid request type: {request_type}")

    return response
