import datetime
import requests

from utils import (
    JOUVE_API_DOMAIN,
    JOUVE_API_USERNAME,
    JOUVE_API_PASSWORD,
    JOUVE_API_VAULT_GUID,
)


class ApiJouveException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__()


mfile_token = {}


def get_authentication_token():
    if not mfile_token:
        mfile_token["value"] = _get_authentication_token()
    return mfile_token["value"]


def _get_authentication_token() -> str:
    expiration = datetime.datetime.now() + datetime.timedelta(hours=1)
    uri = "/REST/server/authenticationtokens"
    response = requests.post(
        f"{JOUVE_API_DOMAIN}{uri}",
        headers={"Content-Type": "application/json"},
        json={
            "Username": JOUVE_API_USERNAME,
            "Password": JOUVE_API_PASSWORD,
            "VaultGuid": JOUVE_API_VAULT_GUID,
            "Expiration": expiration.isoformat(),
        },
    )
    if response.status_code != 200:
        raise ApiJouveException("Error getting API Jouve authentication token")
    response_json = response.json()
    return response_json["Value"]


def get_user(application_id: str) -> dict:
    token = get_authentication_token()

    uri = "/REST/vault/extensionmethod/VEM_GetJeuneByID"
    response = requests.post(
        f"{JOUVE_API_DOMAIN}{uri}",
        headers={
            "X-Authentication": token,
        },
        data=str(application_id),
    )
    try:
        assert response.status_code == 200
        return response.json()
    except:
        raise ApiJouveException("Error getting API jouve GetJeuneByID")


def get_user_list():
    token = get_authentication_token()
    uri = "/REST/vault/extensionmethod/VEM_GetJeunesBetweenDates"
    response = requests.post(
        f"{JOUVE_API_DOMAIN}{uri}",
        headers={
            "X-Authentication": token,
        },
        data="02/05/2021-08#02/05/2021-09",
    )
    return response.json()


def get_image(objectId, fileId):
    if not objectId or not fileId:
        raise ApiJouveException("Empty image reference")
    token = get_authentication_token()
    uri = f"/REST/objects/0/{objectId}/latest/files/{fileId}/content"
    response = requests.get(
        f"{JOUVE_API_DOMAIN}{uri}",
        headers={
            "X-Authentication": token,
        },
    )
    return response


def get_application_image(id):
    application = get_user(id)
    application_id = application["id"]

    image = get_image(application["docObjectID"], application["docFileID"])
    return (
        image.content,
        application_id,
        {
            "objectId": application["docObjectID"],
            "fileId": application["docFileID"],
            "applicationId": application_id,
        },
    )
