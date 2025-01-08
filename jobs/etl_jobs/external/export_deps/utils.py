import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

ENV_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")
GCP_PROJECT_ID = os.environ.get("PROJECT_NAME")


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


# Generate a 32-byte AES key (256 bits)
def genreate_aes_key():
    # generate random symetric key to save in secret manager
    # key = os.urandom(32)

    # key in clear for testing
    key = b"\xa1J\x04\xd9S\x99_,-\xa3\x0ck%\x9f\xba\xf8\xe5p\xf1\xc9\xde\x82\xf8\x91|\xa2\\\xe9\x98\xa8K\xc1"
    save_key(key)
    return key


def save_key():
    pass


# Encrypt the Parquet file using AES-256
def encrypt_file(input_file, output_file, key):
    # Read the file content
    with open(input_file, "rb") as f:
        plaintext = f.read()

    # Generate a random IV (Initialization Vector)
    iv = os.urandom(16)

    # Create an AES-256 cipher object
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    # Pad the plaintext to make it compatible with AES block size
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_plaintext = padder.update(plaintext) + padder.finalize()

    # Encrypt the data
    ciphertext = encryptor.update(padded_plaintext) + encryptor.finalize()

    # Write the IV and ciphertext to the output file
    with open(output_file, "wb") as f:
        f.write(iv + ciphertext)


def decrypt_file(input_file, output_file, key):
    # Read the encrypted file content
    with open(input_file, "rb") as f:
        iv = f.read(16)  # First 16 bytes are the IV
        ciphertext = f.read()

    # Create an AES-256 cipher object
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()

    # Decrypt the ciphertext
    padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

    # Remove padding from plaintext
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

    # Write the decrypted data to the output file
    with open(output_file, "wb") as f:
        f.write(plaintext)
