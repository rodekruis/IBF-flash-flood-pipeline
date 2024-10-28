from settings.credentials import (
    API_SERVICE_URL,
    API_ADMIN_PASSWORD,
    API_USERNAME,
)
import requests
import logging

logger = logging.getLogger(__name__)


def api_authenticate():
    API_LOGIN_URL = API_SERVICE_URL + "user/login"
    login_response = requests.post(
        API_LOGIN_URL,
        data=[("email", API_USERNAME), ("password", API_ADMIN_PASSWORD)],
    )
    return login_response.json()["user"]["token"]


def api_post_request(path, body=None, files=None):
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
