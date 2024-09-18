import random,string,bcrypt

from datetime import datetime
from flask import Config, json,current_app,session
from flask_jwt_extended import create_access_token,get_jwt
from pymongo import MongoClient

from ..utilities.collection_names import COLLECTIONS
from ..gbo.bo import Common_Fields
from ..gbo.common import Custom_Error
from ..pojo.user_details import USER_DETAILS
from ..pojo.access_token import ACCESS_TOKEN
from ..pojo.client_details import CLIENT_DETAILS,SUBSCRIPTION_DETAILS
from ..requests.login_request import forgot_password_request, login_request,create_product_admin_request, verify_otp_request,change_password_request
from ..services.prod_ctrl_service import PROD_Ctrl_Service
from .prod_ctrl_service_temp import PROD_Ctrl_Service_temp
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility

class Login_Service:
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,config:Config,logger,db,keyset_map,session):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.logger = logger
            self.ui_template_service=PROD_Ctrl_Service(logger,db,keyset_map)
            self.ui_template_service_tmp=PROD_Ctrl_Service_temp(logger,db,keyset_map)
            self.config = config
            self.db = db
            self.mongo_client = MongoClient(self.config.db_url,maxPoolSize=self.config.max_pool_size)
            self.neo_db =  self.mongo_client[self.config.neo_db]
            self.keyset_map = keyset_map
            #self.common_service = Common_Service(logger,db,keyset_map)
            
            
        
    def create_product_admin(self,request_data):
        create_prod_admin_request = create_product_admin_request(request_data) 
        create_prod_admin_request.parse_request()
        create_prod_admin_request.validate_request()
        
        login_details_obj = create_prod_admin_request.login_details_obj
        query = {"email": login_details_obj.email, **Utility.get_active_and_not_deleted_query()}
        
        current_user = Mongo_DB_Manager.read_one_document(self.neo_db[COLLECTIONS.MASTER_USER_DETAILS], query)
        if current_user:
            raise Custom_Error(CONSTANTS.USER_ALREADY_EXISTS)
        
        password = login_details_obj.password
        hashed_new_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        neo_cv_client = Mongo_DB_Manager.read_one_document(self.neo_db[COLLECTIONS.MASTER_CLIENT_DETAILS], {"client_name": CONSTANTS.NEO_CV}) 
        
        if not neo_cv_client:
            subscription_details = []
            subscription_detail = SUBSCRIPTION_DETAILS(start_date=Utility.get_current_date(), end_date="2050-01-01")
            subscription_details.append(subscription_detail.__dict__)
            neo_cv_client_data = CLIENT_DETAILS(client_name=CONSTANTS.NEO_CV,api_url='13.201.157.90:82',domain='neoadepts.com',db_name=self.config.db_name,status="active",subscription_details=subscription_details,is_deleted=False,created_on=Utility.get_current_time())
            neo_cv_client_data.__dict__.pop('_id', None)
            neo_cv_client_id = Mongo_DB_Manager.create_document(self.neo_db[COLLECTIONS.MASTER_CLIENT_DETAILS],neo_cv_client_data.__dict__)
        else:
            neo_cv_client_id = neo_cv_client["_id"] 
            
        request_data.update({"password":hashed_new_password,"role":'product-admin',"client_id": str(neo_cv_client_id),**Utility.get_active_and_not_deleted_query()})
        common_fields = Common_Fields(created_on=Utility.get_current_time())
        request_data.update(common_fields.__dict__)
        
        user_id = Mongo_DB_Manager.create_document(self.neo_db[COLLECTIONS.MASTER_USER_DETAILS],request_data)
        if user_id: 
            self.common_service.add_user_permission_for_user(DB_Utility.obj_id_to_str(user_id),"product_admin",self.neo_db)
            return None
        raise Custom_Error('Could not insert product_admin') 
        
    def get_db_name(self, origin):
        domain = Utility.get_origin(origin)
        query = {"domain": domain, **Utility.get_active_and_not_deleted_query()}
        client_info = Mongo_DB_Manager.read_one_document(self.neo_db[COLLECTIONS.MASTER_CLIENT_DETAILS],query)
        if client_info and 'db_name' in client_info:
            return client_info['db_name']
        return None
        
    def login(self,login_data,origin):
        db_name = self.get_db_name(origin)
        if db_name is None:
            raise Custom_Error("Domain is not mapped")   
                    
        login_data_request = login_request(login_data) 
        login_data_request.parse_request()
        login_data_request.validate_request()
        
        login_details_obj = USER_DETAILS(**login_data)
        user_collection = self.mongo_client[db_name][COLLECTIONS.MASTER_USER_DETAILS]
        current_user_obj = self.get_current_user_obj(login_details_obj.email,user_collection)       
        if not (self.password_check(current_user_obj,login_details_obj.password)):
            raise Custom_Error(CONSTANTS.INVALID_PWD)
        db_name = self.mongo_client[db_name]
        return self.frame_login_response(current_user_obj,user_collection,db_name,True,None,login_details_obj)
        
    def frame_login_response(self,current_user_obj,user_collection,db,is_login=True,domain = None,login_details_obj= None):
        client_obj = None
        client_collection = self.neo_db[COLLECTIONS.MASTER_CLIENT_DETAILS]

        if self.config.CLIENT_ENV == CONSTANTS.CLIENT:
            current_date = Utility.get_current_date()
            
            query = {"db_name": self.config.db_name,  
                    "subscription_details": {
                        "$elemMatch": {
                            "start_date": {"$lte": current_date},
                            "end_date": {"$gte": current_date},
                        }
                    },     
                    **Utility.get_active_data_query()}
                        
            client = Mongo_DB_Manager.read_one_document(client_collection, query)
            if not client:
                raise Custom_Error(CONSTANTS.NO_ACTIVE_SUBSCRIPTION_ERR_MSG)
        else:
            query = {"_id":DB_Utility.str_to_obj_id(current_user_obj.client_id)}
            client = Mongo_DB_Manager.read_one_document(client_collection, query)
            
        client_obj=CLIENT_DETAILS(**client)          
        
        if is_login:
            return self.handle_login(current_user_obj, user_collection, db, domain, login_details_obj, client_obj)
        
        return self.handle_non_login(current_user_obj, db,domain, client_obj)
    
    def handle_login(self, current_user_obj, user_collection, db, domain, login_details_obj, client_obj):
        client_db_name = None
        combined_permissions = self.get_combined_permissions(current_user_obj.role, db, current_user_obj.permissions)

        if hasattr(login_details_obj, 'client_domain') and login_details_obj.client_domain:
            client_domain = login_details_obj.client_domain
            result = self.get_db_by_domain(client_domain)
            client_db_name = result['client_db_name']
            domain = {'client_id': result['client_id'], 'client_name': result['client_name']}

        combined_widget = self.get_combined_widget(current_user_obj, db)
        user_data = self.prepare_user_data(current_user_obj, combined_permissions, client_obj, client_db_name, combined_widget)
        access_token = create_access_token(identity=user_data, expires_delta=False)
        if access_token:
            self.update_user_token_in_collection(current_user_obj.email, access_token, Utility.get_current_time(), user_collection)
            enabled_column_list = self.get_column_visibility(current_user_obj._id, db)
            del current_user_obj.visibility
            json_current_user_modified = self.modify_current_user(current_user_obj, client_obj.client_name, combined_permissions, db, domain, enabled_column_list)
            return json_current_user_modified, access_token

    def handle_non_login(self, current_user_obj, db, domain,client_obj):
        id = current_user_obj.portal_view_id if current_user_obj.portal_view_id else current_user_obj._id
        role = current_user_obj.role
        
        if current_user_obj.portal_view_id is None:
            portal_first_user = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_USER_DETAILS], {})
            if portal_first_user:
                current_user_obj.portal_view_id = portal_first_user["_id"]
                current_user_obj.portal_view_role = portal_first_user["role"]
        
        enabled_column_list = self.get_column_visibility(id, db)
        del current_user_obj.visibility
        combined_permissions = self.get_combined_permissions(role, db, current_user_obj.permissions)
        json_current_user_modified = self.modify_current_user(current_user_obj, client_obj.client_name, combined_permissions, db, domain, enabled_column_list, True)
        return json_current_user_modified
    
    def update_user_token_in_collection(self, email, access_token, updated_on,user_collection):
        result = Mongo_DB_Manager.update_document(user_collection, {"email": email}, {"token": access_token, "updated_on": updated_on})
        if result == 0:
            raise Custom_Error('Token not updated in collection')
    
    def store_user_data(self,user_id, permissions,combined_widget):
        redis_client = current_app.config['SESSION_REDIS']
        redis_client.set(f"user:{user_id}", json.dumps({"permissions":permissions,"widget_enable_for_db":combined_widget}))

    def prepare_user_data(self, current_user_obj,permissions=None,client_obj=None,client_db_name = None,combined_widget=None):
        db_name = client_obj.db_name 
        session["permissions"] = permissions
        session["widget_enable_for_db"] = combined_widget
        self.store_user_data(current_user_obj.email,permissions,combined_widget)
        access_token_data = ACCESS_TOKEN(
                                email=current_user_obj.email,
                                phone=current_user_obj.phone,
                                client_name=client_obj.client_name if client_obj else None,
                                client_id=DB_Utility.obj_id_to_str(client_obj._id) if client_obj else None,
                                role=current_user_obj.role,
                                db_name=db_name,
                                client_db_name = client_db_name
                                #permissions = permissions,
                                #widget_enable_for_db = combined_widget
                            )
        print("sesion--------------------------------------------",session)
        return access_token_data.__dict__
    
    def get_db_by_domain(self, domain):
        query = {"domain": domain, **Utility.get_active_and_not_deleted_query()}
        client_collection = self.neo_db[COLLECTIONS.MASTER_CLIENT_DETAILS]
        client_info = Mongo_DB_Manager.read_one_document(client_collection,query)
        
        if not client_info:
            raise Custom_Error('domain not mapped')
        
        if 'db_name' not in client_info or client_info['db_name']==None:
            raise Custom_Error('db information not found for ',domain)
        
        return {
            'client_db_name': client_info['db_name'],
            'client_id': DB_Utility.obj_id_to_str(client_info['_id']),
            'client_name': client_info['client_name']
        }
    
    def get_api_url(self, domain):
        query = {"domain": domain, **Utility.get_active_and_not_deleted_query()}
        client_info = Mongo_DB_Manager.read_one_document(self.neo_db[COLLECTIONS.MASTER_CLIENT_DETAILS], query)
        if not client_info:
            raise Custom_Error("Domain is not mapped")
        return {'api_url':client_info['api_url'],'ats_url':client_info['ats_url'],'version':self.config.version}

    def password_check(self, current_user_obj, password):
        input_pwd_bytes = password.encode('utf-8')
        
        if isinstance(current_user_obj.password, bytes):
            hashed_db_pwd_bytes = current_user_obj.password
        else:
            hashed_db_pwd_bytes = current_user_obj.password.encode('utf-8')
        if(bcrypt.checkpw(input_pwd_bytes, hashed_db_pwd_bytes)):
            return True
        return False
        
    def modify_current_user(self,current_user_obj,client_name,permissions,db,domain = None,enabled_column_list = None,switch_portal_view = False):
        role_info = []
        current_user_obj._id = str(current_user_obj._id)    
            
        if switch_portal_view:
            role_info,count = self.ui_template_service_tmp.load_roles(db,role_name = current_user_obj.portal_view_role)
        else:
            if hasattr(current_user_obj, 'role'):              
                role_info,count = self.ui_template_service_tmp.load_roles(db,role_name = current_user_obj.role)
        
        if role_info:
            current_user_obj.role_info = role_info
        
        modified_user_data = json.loads(json.dumps(current_user_obj.__dict__, default=DB_Utility.custom_encoder, indent = 4))
        fields_to_remove = ['otp', 'otp_timestamp', 'password', 'token', 'current_password', 'db_name', 'new_password', 'notes', 'is_deleted','client_name','entity_id','user_id','client_domain']
        json_current_user_modified = {key: value for key, value in modified_user_data.items() if key not in fields_to_remove}
        json_current_user_modified['client_name'] = client_name
        json_current_user_modified['permissions'] = permissions
        json_current_user_modified['table_settings'] = enabled_column_list
        return json_current_user_modified
                
    def reset_password(self,identity_data,db):
        
        identity_data_obj=ACCESS_TOKEN(**identity_data)
        user_collection = db[COLLECTIONS.MASTER_USER_DETAILS]
        new_password = Utility.generate_random_password()  # Implement password generation logic
        hashed_new_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        print("new_password ",new_password)
        
        query = {"email": identity_data_obj.email}
        update = {"password": hashed_new_password}
        update_result = Mongo_DB_Manager.update_document(user_collection, query, update)
        if update_result > 0:
            jti = get_jwt()["jti"]
            current_app.blacklist.add(jti)
            return None
        
        raise Custom_Error(CONSTANTS.PWD_RESET_FAIL)
    
    def change_portal_view(self,identity_data,request_data):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        user_collection = self.neo_db[COLLECTIONS.MASTER_USER_DETAILS]
        
        query = {"email": identity_data_obj.email}
        update = {"portal_view_id": request_data["_id"],"portal_view_role": request_data["role"]}
        update_result = Mongo_DB_Manager.update_document(user_collection, query, update)
        if update_result == 0:
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
    
    def forgot_password(self,login_data,origin):
        
        db_name = self.get_db_name(origin)
        if db_name is None:
            raise Custom_Error("Domain is not mapped") 

        forgot_pswd_login_request = forgot_password_request(login_data) 
        forgot_pswd_login_request.parse_request()
        forgot_pswd_login_request.validate_request()
        
        login_details_obj = forgot_pswd_login_request.login_details_obj
        #user_collection = self.user_collection.get(db_name, self.client_user_details_collection)
        user_collection = self.mongo_client[db_name][COLLECTIONS.MASTER_USER_DETAILS]
        
        otp = ''.join(random.choices(string.digits, k=6))
        query = {"email": login_details_obj.email, **Utility.get_active_data_query()}
        update = {"otp": otp, "otp_timestamp": Utility.get_current_timestamp()}
        update_result = Mongo_DB_Manager.update_document(user_collection,query,update)
        if update_result == 0:
            raise Custom_Error('User not found or otp not generated')
        result = {'otp':otp}
        return result  
    
    def get_current_user_obj(self,email,user_collection):
        query = {"email": email, **Utility.get_active_data_query()} 
        current_user = Mongo_DB_Manager.read_one_document(user_collection, query)
        if not current_user:
            raise Custom_Error(CONSTANTS.USER_FAIL)
        current_user_obj = USER_DETAILS(**current_user)
        return current_user_obj
    
    def verify_otp(self, login_data,origin):
        
        db_name = self.get_db_name(origin)
        if db_name is None:
            raise Custom_Error("Domain is not mapped") 

        verify_otp_login_request = verify_otp_request(login_data) 
        verify_otp_login_request.parse_request()
        verify_otp_login_request.validate_request()
        
        login_details_obj=verify_otp_login_request.login_details_obj
        #user_collection = self.user_collection.get(db_name, self.client_user_details_collection)
        user_collection = self.mongo_client[db_name][COLLECTIONS.MASTER_USER_DETAILS]
                
        current_user_obj = self.get_current_user_obj(login_details_obj.email,user_collection)

        otp = current_user_obj.otp
        otp_timestamp = current_user_obj.otp_timestamp
        if otp_timestamp is None:
            raise Custom_Error(CONSTANTS.INVALID_OTP)
        
        time_difference = self.compare_time(otp_timestamp)
        
        if (time_difference <= int(self.config.otp_expiration_seconds)):
                if login_details_obj.otp == otp:
                    
                    new_password = login_details_obj.new_password
                    hashed_new_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
                    
                    query = {"email": login_details_obj.email}
                    update = {"otp": None, "otp_timestamp": None,"password":hashed_new_password}
                    update_result = Mongo_DB_Manager.update_document(user_collection,query,update)
                    if update_result > 0:
                        return None
                    
                raise Custom_Error(CONSTANTS.INVALID_OTP)
        raise Custom_Error(CONSTANTS.OTP_EXP)

    def compare_time(self,otp_timestamp):
                
        current_timestamp = datetime.strptime(Utility.get_current_timestamp(), "%Y-%m-%d %H:%M:%S.%f")
        otp_timestamp = datetime.strptime(otp_timestamp, "%Y-%m-%d %H:%M:%S.%f")
        time_difference = (current_timestamp - otp_timestamp).total_seconds()
        return time_difference  
    
    def change_password(self,identity_data,login_data,db):
        
        change_pswd_login_request = change_password_request(login_data) 
        change_pswd_login_request.parse_request()
        change_pswd_login_request.validate_request()
        
        identity_data_obj=ACCESS_TOKEN(**identity_data)
        login_data_obj=USER_DETAILS(**login_data)     
    
        #user_collection=self.client_db["USER_DETAILS"] if identity_data_obj.db_name else self.db["USER_DETAILS"]
        user_collection = db[COLLECTIONS.MASTER_USER_DETAILS]
        current_user_obj = self.get_current_user_obj(identity_data_obj.email,user_collection)
        
        if self.password_check(current_user_obj,login_data_obj.current_password):                
                new_password = login_data_obj.new_password 
                hashed_new_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
                
                query = {"email": identity_data_obj.email}
                update = {"password": hashed_new_password}
                update_result = Mongo_DB_Manager.update_document(user_collection,query,update)  
                
                if update_result > 0:
                    jti = get_jwt()["jti"]
                    current_app.blacklist.add(jti)
                    return None
                
                raise Custom_Error(CONSTANTS.PWD_RESET_FAIL)
        raise Custom_Error(CONSTANTS.INVALID_PWD)

    def auth_login(self,identity_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
            
        user_collection = db[COLLECTIONS.MASTER_USER_DETAILS]
        domain = None
        if identity_data_obj.client_db_name is not None:
            domain = self.get_domain_by_db(identity_data_obj.client_db_name)
            user_collection = self.neo_db[COLLECTIONS.MASTER_USER_DETAILS]
                    
        current_user_obj = self.get_current_user_obj(identity_data_obj.email,user_collection)    
        return self.frame_login_response(current_user_obj,user_collection,db,is_login=False,domain=domain)
    
    def get_domain_by_db(self, client_db_name):
        query = {"db_name": client_db_name, **Utility.get_active_and_not_deleted_query()}
        client_collection = self.neo_db[COLLECTIONS.MASTER_CLIENT_DETAILS]
        client_info = Mongo_DB_Manager.read_one_document(client_collection,query)
        if not client_info:
            raise Custom_Error('db not available')
        
        return {
            'client_id': DB_Utility.obj_id_to_str(client_info['_id']),
            'client_name': client_info['client_name']
        }

    def get_user_permissions(self, user_id, db):
        query = {'user_id': user_id}
        user_doc = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_USER_DETAILS], query)
        if user_doc:
            return user_doc.get("permissions",[])        
        return {}

    def get_role_permissions(self, role_name, db):
        query = {'role_name': role_name}
        role_permissions_doc = Mongo_DB_Manager.read_one_document(db["MASTER_ROLE_PERMISSION"], query)
        if role_permissions_doc:
            return role_permissions_doc.get('permissions', [])
        return {}

    def get_combined_permissions1(self,role_name, db_name,permission):
        user_permissions = permission
        role_permissions = self.get_role_permissions(role_name, db_name)       
        if not user_permissions:
            return role_permissions

        combined_permissions = role_permissions.copy()
        
        # Merge user permissions with role permissions
        for module, user_perm in user_permissions.items():
            if module not in combined_permissions:
                combined_permissions[module] = user_perm.copy()
            else:
                combined_perm = combined_permissions[module]
                for key, value in user_perm.items():
                    if value != "default":
                        combined_perm[key] = value
                    elif key in role_permissions.get(module, {}):
                        combined_perm[key] = role_permissions[module][key]
                    
        for module, combined_perm in combined_permissions.items():
            user_perm = user_permissions.get(module, {})  # Get user permissions for this module (if any)
            for key in list(combined_perm.keys()):  # Iterate over a copy to avoid modifying while iterating
                if key not in user_perm and key not in role_permissions.get(module, {}):
                    del combined_perm[key]
                    
        for module, combined_perm in combined_permissions.items():
            for key in set(combined_perm.keys()) & set(['add', 'update', 'delete', 'view', 'upload']):
                # Check if key is present and not "default" in combined_perm
                if combined_perm.get(key, None) == "default":
                    # If not present or "default", inherit from role (if available)
                    if key in role_permissions.get(module, {}):
                        combined_perm[key] = role_permissions[module][key]
                    else:
                        del combined_perm[key]  # Remove key if not in user or role permissions


        return combined_permissions
    
    def get_combined_permissions(self, role_name, db_name, user_permissions):
        
        # Fetch role permissions
        role_permissions = self.get_role_permissions(role_name, db_name)
        # If there are no user permissions, return role permissions as combined permissions
        if not user_permissions:
            return role_permissions
        
        combined_permissions = role_permissions.copy()

        # Merge user permissions with role permissions
        for module, user_module_perms in user_permissions.items():
            if module not in combined_permissions:
                combined_permissions[module] = user_module_perms.copy()
            else:
                combined_module_perms = combined_permissions[module]
                for permission, user_perm_value in user_module_perms.items():
                    if user_perm_value == "default":
                        # Use role permission value if user permission is "default"
                        combined_module_perms[permission] = role_permissions[module].get(permission, False)
                    else:
                        # Otherwise, use the user permission value
                        combined_module_perms[permission] = user_perm_value
                        
        # Ensure combined_permissions include all permissions from role_permissions where user_permissions are missing
        for module, role_module_perms in role_permissions.items():
            if module not in combined_permissions:
                combined_permissions[module] = role_module_perms.copy()
            else:
                combined_module_perms = combined_permissions[module]
                for permission, role_perm_value in role_module_perms.items():
                    if permission not in combined_module_perms:
                        combined_module_perms[permission] = role_perm_value
        return combined_permissions
  
    def get_column_visibility(self, user_id,db): 
        document = db["MASTER_COLUMN_VISIBILITY"].find_one()      
        if document:
            document.pop("_id")       
            doc_keys = document.keys()                 
        user_document = Mongo_DB_Manager.read_one_document(db["MASTER_USER_DETAILS"],{'_id': DB_Utility.str_to_obj_id(user_id)})     
        if user_document:            
            user_document.pop("_id")  
            if "visibility" in user_document and user_document["visibility"]:  
                visibility_doc = user_document["visibility"]  
                for key, value in visibility_doc.items():
                    if key in doc_keys:
                        if "columns" in value and value["columns"]:
                            for update_item in value["columns"]:
                                db_column = update_item["db_column"]
                                # Find the corresponding item in CLIENT_DETAILS and update enable value
                                for detail in document[key]:
                                    if detail["db_column"] == db_column:
                                        detail["enable"] = update_item["enable"]
                                        detail["default_enable"] = update_item["default_enable"]
                                        detail["widget_enable"] = update_item["widget_enable"]
                                        detail["ui_column"] = update_item["ui_column"]
                                        if "order" in update_item and update_item["order"]:
                                            detail["order"] = update_item["order"]
                                        break
        return document
    
    def get_combined_widget(self,current_user_obj, db):
        combined_widget_response = {}

        # Retrieve generic widget_enable settings from the sample collection
        sample_collection_docs = {doc["key"]: doc for doc in db.sample.find({})}
        # Retrieve generic column visibility settings
        column_visibility_docs = {}
        column_visibility_cursor = db['MASTER_COLUMN_VISIBILITY'].find({})
        for column_visibility_doc in column_visibility_cursor:
            for collection_name, columns in column_visibility_doc.items():
                if collection_name != "_id":
                    if collection_name not in column_visibility_docs:
                        column_visibility_docs[collection_name] = {}
                    for column in columns:
                        column_name = column["db_column"]
                        column_visibility_docs[collection_name][column_name] = column

        # Check if USER_DETAILS.visibility is None
        if current_user_obj.visibility:
            # Process collections in USER_DETAILS.visibility
            for collection_name, collection_info in current_user_obj.visibility.items():
                combined_widget_response[collection_name] = {}

                # Check if collection widget_enable is specified in USER_DETAILS.visibility
                if "widget_enable" in collection_info:
                    combined_widget_response[collection_name]["widget_enable"] = collection_info["widget_enable"]
                else:
                    # Fallback to generic widget_enable from sample collection if not specified in USER_DETAILS.visibility
                    sample_collection_doc = sample_collection_docs.get(collection_name)
                    if sample_collection_doc and "widget_enable" in sample_collection_doc:
                        combined_widget_response[collection_name]["widget_enable"] = sample_collection_doc["widget_enable"]

                # Initialize list to store columns
                columns = []

                # Process columns for the current collection from USER_DETAILS.visibility
                if "columns" in collection_info:
                    for column_info in collection_info["columns"]:
                        column_entry = {
                            "db_column": column_info["db_column"],
                            "widget_enable": column_info.get("widget_enable", True)  # Default to True if not specified
                        }
                        columns.append(column_entry)

                # Fallback to generic widget_enable from column_visibility collection for columns not specified
                generic_columns = column_visibility_docs.get(collection_name, {})
                for column_name, column_info in generic_columns.items():
                    if not any(column["db_column"] == column_name for column in columns):
                        column_entry = {
                            "db_column": column_name,
                            "widget_enable": column_info.get("widget_enable", True)  # Default to True if not specified
                        }
                        columns.append(column_entry)

                # Add columns list to the response
                combined_widget_response[collection_name]["columns"] = columns
    
        # Process collections in sample collection which are not in USER_DETAILS.visibility
        for collection_name, sample_collection_doc in sample_collection_docs.items():
            if collection_name not in combined_widget_response:
                combined_widget_response[collection_name] = {
                    "widget_enable": sample_collection_doc.get("widget_enable", True),
                    "columns": []
                }
                generic_columns = column_visibility_docs.get(collection_name, {})
                for column_name, column_info in generic_columns.items():
                    column_entry = {
                        "db_column": column_name,
                        "widget_enable": column_info.get("widget_enable", True)  # Default to True if not specified
                    }
                    combined_widget_response[collection_name]["columns"].append(column_entry)

            else:
                # Ensure all columns from column_visibility are included
                generic_columns = column_visibility_docs.get(collection_name, {})
                for column_name, column_info in generic_columns.items():
                    if not any(column["db_column"] == column_name for column in combined_widget_response[collection_name]["columns"]):
                        column_entry = {
                            "db_column": column_name,
                            "widget_enable": column_info.get("widget_enable", True)  # Default to True if not specified
                        }
                        combined_widget_response[collection_name]["columns"].append(column_entry)
        for collection_name, collection_info in combined_widget_response.items():
            if not any(column["db_column"] == "_id" for column in collection_info["columns"]):
                collection_info["columns"].append({"db_column": "_id", "widget_enable": True})

        return combined_widget_response