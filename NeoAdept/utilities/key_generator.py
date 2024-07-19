from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from ..utilities.db_utility import DB_Utility,Mongo_DB_Manager
from ..utilities.utility import Utility
import base64


def key_generator(db) :

    # Generate server private key
    server_private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    # Generate server public key
    server_public_key = server_private_key.public_key()
    
     # Serialize private key to PEM format
    private_key_pem = server_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    # Serialize public key to PEM format
    public_key_pem = server_public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    # Convert keys to base64 encoding for storage in MongoDB
    private_key_b64 = base64.b64encode(private_key_pem).decode('utf-8')
    public_key_b64 = base64.b64encode(public_key_pem).decode('utf-8')

    key_document = {
        "server_private_key": private_key_b64,
        "server_public_key": public_key_b64,
        "decryption_apis" :[]
    }

    # Insert the document into the MongoDB collection
    Mongo_DB_Manager.create_document(db["CONFIG_KEYS"],key_document)

    print("Keys stored in MongoDB")






 