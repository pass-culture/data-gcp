import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
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


def encrypt_file_aes_gcm(input_file, output_file, key):
    try:
        # Read the plaintext from the input file
        with open(input_file, "rb") as f:
            plaintext = f.read()

        # Generate a random nonce (12 bytes recommended for AES-GCM)
        nonce = os.urandom(12)

        # Create an AES-GCM cipher object
        aesgcm = AESGCM(key)

        # Encrypt the plaintext
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)

        # Write the nonce and ciphertext to the output file
        with open(output_file, "wb") as f:
            f.write(nonce + ciphertext)

        print(f"File encrypted successfully: {output_file}")
    except Exception as e:
        print(f"Error encrypting file: {e}")
        raise


def decrypt_file_aes_gcm(input_file, output_file, key):
    try:
        # Read the nonce and ciphertext from the input file
        with open(input_file, "rb") as f:
            data = f.read()
        nonce = data[:12]  # Extract the first 12 bytes as the nonce
        ciphertext = data[12:]  # The rest is the ciphertext

        # Create an AES-GCM cipher object
        aesgcm = AESGCM(key)

        # Decrypt the ciphertext
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)

        # Write the plaintext to the output file
        with open(output_file, "wb") as f:
            f.write(plaintext)

        print(f"File decrypted successfully: {output_file}")
    except Exception as e:
        print(f"Error decrypting file: {e}")
        raise


def make_obfuscation_query(select_query, obfuscation_kwargs):
    # TO DO: how to store secretly and maintain the obfuscation functions and their parameters
    obf_functions = obfuscation_kwargs.get("functions")
    obf_functions_kwargs = obfuscation_kwargs.get("functions_kwargs")
    obf_fields = obfuscation_kwargs.get("obf_fields")
    obf_query = f"""WITH original_data as ({select_query}), SELECT {[func(field,**kwargs) for (func,field,kwargs) in zip(obf_functions,obf_fields,obf_functions_kwargs)].join(',')} FROM original_data"""
    return obf_query


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

    # obfuscation_query = make_obfuscation_query(query, obfuscation_kwargs)
    # result = conn.execute(obfuscation_query).fetchdf()
    # # Save the data back into a Parquet file
    # obfuscated_parquet_file = 'unencrypted_output.parquet'
    # df.to_parquet(obfuscated_parquet_file)

    return obfuscated_parquet_file
