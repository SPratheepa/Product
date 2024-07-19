import json,traceback,base64
from functools import wraps
from flask import request,g,jsonify,current_app,session
from NeoAdept.gbo.bo import Base_Response
from flask_jwt_extended import get_jwt, get_jwt_identity


from NeoAdept.config import Config
from NeoAdept.gbo.common import Custom_Error
from NeoAdept.utilities.constants import CONSTANTS

from ..utilities.utility import Utility
from ..utilities.db_utility import Collection_Manager, DB_Utility

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_private_key

def check_blacklisted_token(view_func):
    @wraps(view_func)
    def decorated_function(*args, **kwargs):
        try:
            jti = get_jwt()["jti"]
            response = Utility().handle_blacklisted_jti(jti)  # Use the function here
            if response is not None:
                return response
            return view_func(*args, **kwargs)
        except Exception as e:
            return Utility.generate_exception_response(e)
    return decorated_function

def check_jwt_token(view_func,db,config:Config,session):
    @wraps(view_func)
    def decorated_function(*args, **kwargs):
        try:
            auth_header = request.headers.get('Authorization')
            
            if not auth_header:
                return Utility.generate_error_response("Authorization header is missing",201)
            
            token = auth_header.split(" ")[1]  # Assuming the format is "Bearer <token>"
            
            response = DB_Utility.check_token(token,get_jwt_identity()["email"],db["USER_DETAILS"])
            
            if response is not None:
                return response
            
            g.db = db
            
            if config.CLIENT_ENV != CONSTANTS.CLIENT:
                db_name = get_jwt_identity()["client_db_name"]
                if db_name:
                    g.db = Collection_Manager(config).configure_client(config.db_url,config.max_pool_size)[db_name]
            
            api_name = view_func.__name__
            if api_name.startswith("get_doc"):
                api_name = "get_doc"
            if api_name not in CONSTANTS.IGNORE_PERMISSION_API_LIST:
                
                if session.permissions is None:
                    return Base_Response(status=CONSTANTS.FAILED, status_code=403, message="Session expired.Please log in again").__dict__
                    #return Utility.generate_error_response("Session expired.Please log in again")
                permissions = session.permissions

                module_details_map = current_app.module_details_map
                response = DB_Utility.check_permissions(permissions,api_name,module_details_map)
            
                if response is not None:
                    return response
            
            return view_func(*args, **kwargs)
        except Exception as e:
            return Utility.generate_exception_response(e)
    return decorated_function

def decrypt_request_data(encrypted_data,server_private_key): # using server private key
        try :          
            encrypted_data = base64.b64decode(encrypted_data)           
            decrypted_data = server_private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            data = decrypted_data.decode('utf-8')            
            return data
        except ValueError as e:            
            raise Custom_Error(e)
        except Exception as e:           
            raise Custom_Error(e)

def decrypt_request(view_func,server_private_key,decryption_apis):
    @wraps(view_func)
    def decorated_function(*args, **kwargs):
        try:
            api_name = request.endpoint  # Alternatively, use request.path for full path            
            if api_name in decryption_apis:
                request_data = request.json
                if 'data' not in request_data:
                    raise ValueError("No 'data' field in request JSON")
                
                encrypted_data = request_data['data']                
                decrypted_data = decrypt_request_data(encrypted_data, server_private_key)
                if decrypted_data is not None:                
                    kwargs['request_data'] =json.loads(decrypted_data)  # Pass the decrypted data as an argument to the route handler
            else:
                    kwargs['request_data'] = request.json
        except Exception as e:
            print(f"Decryption failed for API '{request.endpoint}': {e}")
            traceback.print_exc()

        # Proceed with the original function
        return view_func(*args, **kwargs)
    return decorated_function
