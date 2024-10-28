from settings.credentials import (
    API_SERVICE_URL,
    API_ADMIN_PASSWORD,
    API_USERNAME,
)
import requests
import logging

logger = logging.getLogger(__name__)


def api_authenticate():
    """
    Function to authenticate with the IBF API. Returns a bearer token to be used when posting data

    Returns:
        bearer_token (str): token to be used as authentication for api requests
    """
    API_LOGIN_URL = API_SERVICE_URL + "user/login"
    login_response = requests.post(
        API_LOGIN_URL,
        data=[("email", API_USERNAME), ("password", API_ADMIN_PASSWORD)],
    )
    return login_response.json()["user"]["token"]


def api_post_request(path, body=None, files=None):
    """
    Post function which will authenticate with the IBF API first and then makes a requests

    Args:
        path (str): api enpoint path relative to base url (e.g., admin-area-dynamic-data/exposure)
        body (Dict): api post body (dictionary)
        files (bitestring): string of bytes to transfer a binary file to the portal
    """
    TOKEN = api_authenticate()

    if body is not None:
        headers = {
            "Authorization": "Bearer " + TOKEN,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    elif files is not None:
        headers = {"Authorization": "Bearer " + TOKEN}

    r = requests.post(API_SERVICE_URL + path, json=body, files=files, headers=headers)
    if r.status_code >= 400:
        logger.info(r.text)
        logger.error("PIPELINE ERROR")
        raise ValueError()
