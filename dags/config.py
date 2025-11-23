from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Read the private key file
KEY_PATH = "/opt/airflow/keys/snowflake_key.p8"
with open(KEY_PATH, 'rb') as key_file:
    private_key_pem = key_file.read()

# Convert to the format Snowflake needs
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,  # Since you said no passphrase
    backend=default_backend()
)

# Serialize to DER format
my_snowflake_private_key = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

snowflake_config = {
    "user": "luketrmai",
    "account": "jfspetv-wgb43135",
    "private_key": my_snowflake_private_key,
    "warehouse": "compute_wh",
    "database": "raw",
    "schema": "instacart_raw",
}
