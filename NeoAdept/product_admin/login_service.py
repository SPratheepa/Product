import bcrypt,random,string,requests

from flask import json,current_app
from datetime import datetime
from flask_jwt_extended import create_access_token,get_jwt

from ..gbo.bo import Common_Fields
from ..pojo.user.user_details import USER_DETAILS
from ..pojo.user.access_token import ACCESS_TOKEN
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..pojo.client.client_details import CLIENT_DETAILS,SUBSCRIPTION_DETAILS
#from..services.UI_template_service import UI_Template_Service
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..requests.login_request import forgot_password_request, login_request,create_product_admin_request,verify_otp_request,change_password_request
from ..gbo.common import Custom_Error

class Login_Service:
    
    def __init__(self,config,logger,db,client_db):
        
        self.logger = logger
        #self.ui_template_service=UI_Template_Service(logger)
        self.config = config
        self.db = db
        self.client_db = client_db
        self.user_details_collection = self.db.get_collection("USER_DETAILS")
        self.client_user_details_collection = self.client_db.get_collection("USER_DETAILS")
        self.client_details_collection = self.db.get_collection("CLIENT_DETAILS")
        self.user_collection = {'NeoAdept': self.user_details_collection}
        
    def create_product_admin(self,request_data):
        
        create_prod_admin_request = create_product_admin_request(request_data) 
        create_prod_admin_request.parse_request()
        create_prod_admin_request.validate_request()
        
        login_details_obj = create_prod_admin_request.login_details_obj
        query = {**{"email": login_details_obj.email}, **Utility.get_active_and_not_deleted_query()}
        current_user = Mongo_DB_Manager.read_one_document(self.user_details_collection, query)
        
        if not current_user:
            password = login_details_obj.password
            hashed_new_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
            
            query={"client_name": "NeoCV"}
            neo_cv_client = Mongo_DB_Manager.read_one_document(self.client_details_collection, query) 
            
            if not neo_cv_client:
                
                subscription_details = []
                subscription_detail = SUBSCRIPTION_DETAILS(start_date=Utility.get_current_date(), end_date="2050-01-01")
                subscription_details.append(subscription_detail.__dict__)
                neo_cv_client_data = CLIENT_DETAILS(client_name="NeoCV",api_url='13.201.157.90:82',domain='neoadepts.com',db_name='NeoAdept',status="active",subscription_details=subscription_details,is_deleted=False,created_on=Utility.get_current_timestamp())
                neo_cv_client_data.__dict__.pop('_id', None)
                neo_cv_client_id = Mongo_DB_Manager.create_document(self.client_details_collection,neo_cv_client_data.__dict__)
                
            else:
                
                neo_cv_client_id = neo_cv_client["_id"] 
                
            request_data.update({"password":hashed_new_password,"role":'product-admin',"client_id": str(neo_cv_client_id),**Utility.get_active_and_not_deleted_query()})
            common_fields = Common_Fields(created_on=Utility.get_current_timestamp())
            request_data.update(common_fields.__dict__)
            
            user_id = Mongo_DB_Manager.create_document(self.user_details_collection,request_data)
            if user_id is not None: 
                return None
            else:
                raise Custom_Error('Could not insert product_admin')  
            
        raise Custom_Error(CONSTANTS.USER_ALREADY_EXISTS)
        
    def get_db_name(self, origin):
        
        domain = Utility.get_origin(origin)
        
        query = {"domain": domain, **Utility.get_active_and_not_deleted_query()}
        client_info = Mongo_DB_Manager.read_one_document(self.client_details_collection,query)
        if not client_info:
            return None
        
        result = {'db_name':client_info['db_name']}
        return result['db_name'] if result['db_name'] else None
        
    def login(self,login_data,origin):
        
        db_name = self.get_db_name(origin)
        if db_name is None:
            raise Custom_Error("Domain is not mapped")   
        
        login_data_request = login_request(login_data) 
        login_data_request.parse_request()
        login_data_request.validate_request()
        
        login_details_obj = login_data_request.login_details_obj
                     
        user_collection = self.user_collection.get(db_name, self.client_user_details_collection)
        current_user_obj = self.get_current_user_obj(login_details_obj.email,user_collection)
            
        if not (self.password_check(current_user_obj,login_details_obj.password)):
            raise Custom_Error(CONSTANTS.INVALID_PWD)
        
        return self.frame_login_response(current_user_obj,db_name,user_collection)
        
    def frame_login_response(self,current_user_obj,db_name,user_collection,is_auth_login=True):
        client_obj=None
        if db_name != 'NeoAdept' :  
            current_date = Utility.get_current_date()

            query = {"db_name": db_name, 
                    "subscription_details": {
                        "$elemMatch": {
                            "start_date": {"$lte": current_date},
                            "end_date": {"$gte": current_date},
                        }
                    },     
                    **Utility.get_active_data_query}
            client = Mongo_DB_Manager.read_one_document(self.client_details_collection, query)
            if not client:
                raise Custom_Error(CONSTANTS.NO_ACTIVE_SUBSCRIPTION_ERR_MSG)
            client_obj=CLIENT_DETAILS(**client)
                  
        if is_auth_login:
            
            user_data = self.prepare_user_data(current_user_obj, client_obj)
            
            access_token = create_access_token(identity = user_data,expires_delta = False)
            if access_token:
                self.update_user_token_in_collection(current_user_obj.email, access_token, Utility.get_current_timestamp(), user_collection)
                json_current_user_modified = self.modify_current_user(current_user_obj)
            return json_current_user_modified,access_token
        
        json_current_user_modified = self.modify_current_user(current_user_obj)
        return json_current_user_modified
    
    def update_user_token_in_collection(self, email, access_token, updated_on,user_collection):
        
        query = {"email": email}
        update = {"token": access_token, "updated_on": updated_on}
        result = Mongo_DB_Manager.update_document(user_collection, query, update)
        if result == 0:
            raise Custom_Error('Token not updated in collection')
    
    def prepare_user_data(self, current_user_obj, client_obj=None):
        access_token_data = ACCESS_TOKEN(
                                email=current_user_obj.email,
                                phone=current_user_obj.phone,
                                _id=DB_Utility.obj_id_to_str(current_user_obj._id),
                                client_name=client_obj.client_name if client_obj else None,
                                client_id=DB_Utility.obj_id_to_str(client_obj._id) if client_obj else None,
                                role=current_user_obj.role,
                                db_name=client_obj.db_name if client_obj else None
                            )

        if current_user_obj.entity_id is not None:
            access_token_data.entity_id = current_user_obj.entity_id
    
        user_data = access_token_data.__dict__
    
        return user_data
                       
    def get_api_url(self, domain):
        
        query = {"domain": domain, **Utility.get_active_and_not_deleted_query()}
        client_info = Mongo_DB_Manager.read_one_document(self.client_details_collection, query)
        if not client_info:
            raise Custom_Error("Domain is not mapped")
        result = {'api_url':client_info['api_url']}
        return result

    def password_check(self, current_user_obj, password):
        
        input_pwd_bytes = password.encode('utf-8')
        
        if isinstance(current_user_obj.password, bytes):
            hashed_db_pwd_bytes = current_user_obj.password
        else:
            hashed_db_pwd_bytes = current_user_obj.password.encode('utf-8')
        if(bcrypt.checkpw(input_pwd_bytes, hashed_db_pwd_bytes)):
            return True
        return False
           
    def modify_current_user(self,current_user_obj):
        
        current_user_obj._id = str(current_user_obj._id)
        fields_to_remove = ['otp', 'otp_timestamp', 'password', 'token', 'current_password', 'db_name', 'new_password', 'notes']

        # Remove specified fields if they exist
        for field in fields_to_remove:
            if hasattr(current_user_obj, field):
                delattr(current_user_obj, field)
               
        '''
        if hasattr(current_user_obj, 'role'):              
                role_info = self.ui_template_service.load_for_role(current_user_obj.role)
                #print("role_info info ::",role_info)
                if role_info:
                    current_user_obj.role_info = role_info
                else:
                    current_user_obj.role_info = []
        '''
        modified_user_data = json.loads(json.dumps(current_user_obj.__dict__, default=DB_Utility.custom_encoder, indent = 4))
        json_current_user_modified=self.modify_user(modified_user_data)
        return json_current_user_modified
    
    def modify_user(self,current_user_json):
        
        fields_to_remove = ['otp', 'otp_timestamp', 'password', 'token', 'current_password', 'db_name', 'new_password', 'notes', 'is_deleted']
        modified_user_data={key: value for key, value in current_user_json.items() if key not in fields_to_remove}
        return modified_user_data
                
    def reset_password(self,identity_data):
        
        identity_data_obj=USER_DETAILS(**identity_data)
        user_collection=self.client_db["USER_DETAILS"] if identity_data_obj.db_name else self.db["USER_DETAILS"]
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
    
    def forgot_password(self,login_data,origin):
        
        db_name = self.get_db_name(origin)
        if db_name is None:
            raise Custom_Error("Domain is not mapped") 
        
        forgot_pswd_login_request = forgot_password_request(login_data) 
        forgot_pswd_login_request.parse_request()
        forgot_pswd_login_request.validate_request()
        
        login_details_obj = forgot_pswd_login_request.login_details_obj
        user_collection = self.user_collection.get(db_name, self.client_user_details_collection)
        
        
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
        user_collection = self.user_collection.get(db_name, self.client_user_details_collection)
                
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
    
    def change_password(self,identity_data,login_data):
        
        change_pswd_login_request = change_password_request(login_data) 
        change_pswd_login_request.parse_request()
        change_pswd_login_request.validate_request()
        
        identity_data_obj=USER_DETAILS(**identity_data)
        login_data_obj=USER_DETAILS(**login_data)     
    
        user_collection=self.client_db["USER_DETAILS"] if identity_data_obj.db_name else self.db["USER_DETAILS"]
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
   
    def auth_login(self,identity_data):
        
        identity_data_obj = USER_DETAILS(**identity_data)
              
        user_collection = self.user_collection['NeoAdept'] if identity_data_obj.db_name is None else self.client_user_details_collection
        current_user_obj = self.get_current_user_obj(identity_data_obj.email,user_collection)          
        
        db_name = 'NeoAdept' if identity_data_obj.db_name is None else identity_data_obj.db_name
        return self.frame_login_response(current_user_obj,db_name,user_collection,False)
