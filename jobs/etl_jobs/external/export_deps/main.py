import filecmp
import os

import requests

from utils import decrypt_file, encrypt_file, generate_aes_key, obfuscate_data

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
GCP_PROJECT_ID = os.environ.get("PROJECT_NAME")


gcs_url = "https://storage.cloud.google.com/data-bucket-prod/sandbox_prod/vbusson/20250108/publicly_available_data_000000000000.parquet"
input_file = "unencrypted_input.parquet"

# Download parquet files
with open(input_file, "wb") as f:
    f.write(requests.get(gcs_url).content)

private_obfuscation_kwargs = {}
obfuscated_file = obfuscate_data(input_file, **private_obfuscation_kwargs)

# Generate an AES key
key = generate_aes_key()
print(f"Generated aes key: {key}")

# Encrypt the Parquet file
encrypted_parquet_file = "encrypted_output.parquet.aes"
encrypt_file(obfuscated_file, encrypted_parquet_file, key)

print(f"Encrypted file saved as: {encrypted_parquet_file}")

# Decrypt the file
decrypted_parquet_file = "decrypted_output.parquet"
decrypt_file(encrypted_parquet_file, decrypted_parquet_file, key)

# assert files are identical

if filecmp.cmp(obfuscated_file, decrypted_parquet_file, shallow=False):
    print("Decryption successful: Files match.")
else:
    print("Decryption failed: Files do not match.")
