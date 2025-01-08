import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


# Generate a 32-byte AES key (256 bits)
def generate_aes_key():
    # generate random symetric key to save in secret manager
    # key = os.urandom(32)

    # key in clear for testing
    key = b"\xa1J\x04\xd9S\x99_,-\xa3\x0ck%\x9f\xba\xf8\xe5p\xf1\xc9\xde\x82\xf8\x91|\xa2\\\xe9\x98\xa8K\xc1"
    save_key(key)
    return key


def save_key(key):
    pass


# Encrypt the Parquet file using AES-256
def encrypt_file(input_file, output_file, key):
    try:
        # Ensure the key is 32 bytes (AES-256 requires this)
        if len(key) != 32:
            raise ValueError("Key must be 32 bytes long for AES-256 encryption.")

        with open(input_file, "rb") as f:
            plaintext = f.read()

        # Generate a random IV (16 bytes for AES)
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

        print(f"File encrypted successfully: {output_file}")
    except Exception as e:
        print(f"Error encrypting file: {e}")
        raise


def decrypt_file(input_file, output_file, key):
    try:
        # Ensure the key is 32 bytes (AES-256 requires this)
        if len(key) != 32:
            raise ValueError("Key must be 32 bytes long for AES-256 decryption.")

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

        print(f"File decrypted successfully: {output_file}")
    except Exception as e:
        print(f"Error decrypting file: {e}")
        raise


def obfuscate_data(input_parquet_file, **kwargs):
    obfuscated_parquet_file = input_parquet_file
    # TODO: privately Salt and watermark data in SQL for speed with DuckDB

    # # Load the Parquet file
    # conn = duckdb.connect()
    # conn.execute("INSTALL httpfs;")
    # conn.execute("LOAD httpfs;")
    # conn.execute("SET s3_region='europe-west1';")

    # query = f"""
    # SELECT * FROM parquet_scan('{input_parquet_file}');
    # """

    def obfuscation_query(select_query, obfuscation_kwargs):
        obf_functions = obfuscation_kwargs.get("functions")
        obf_functions_kwargs = obfuscation_kwargs.get("functions_kwargs")
        obf_fields = obfuscation_kwargs.get("obf_fields")

        obf_query = f"""WITH original_data as ({select_query}), SELECT {[func(field,**kwargs) for (func,field,kwargs) in zip(obf_functions,obf_fields,obf_functions_kwargs)].join(',')} FROM original_data"""
        return obf_query

    # TO DO: how to store secretly and maintain the obfuscation functions and their parameters

    # result = conn.execute(obfuscation_query).fetchdf()
    # # Save the data back into a Parquet file
    # obfuscated_parquet_file = 'unencrypted_output.parquet'
    # df.to_parquet(obfuscated_parquet_file)

    return obfuscated_parquet_file
