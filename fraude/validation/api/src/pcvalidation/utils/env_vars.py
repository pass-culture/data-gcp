import os

GCS_BUCKET = os.environ.get("GCS_BUCKET", "data-bucket-dev")
API_KEYS = ["9d207bf0"]

fake_users_db = {
    "testuser": {
        "username": "testuser",
        # Here the hash pswd is hashed from 'secret'
        "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW",
        "disabled": False,
    }
}
