import os

GCS_BUCKET = os.environ.get("GCS_BUCKET", "data-bucket-dev")
API_KEYS = ["9d207bf0"]

fake_users_db = {
    "testuser": {
        "username": "testuser",
        # "full_name": "John Doe",
        # "email": "johndoe@example.com",
        "hashed_password": "fakehashed-secret",
        "disabled": False,
    }
}
