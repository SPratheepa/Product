from flask import Blueprint, request, current_app,g,session
from flask_jwt_extended import jwt_required,  get_jwt_identity, get_jwt

from ..gbo.bo import Base_Response
from ..gbo.common import Custom_Error
from ..services.login_service import Login_Service
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..utilities.decorator import check_blacklisted_token,check_jwt_token,decrypt_request

import json

class Login_Route(Blueprint):
    def __init__(self, name, import_name,config,logger,db,keyset_map,server_private_key,decryption_apis):
        super(Login_Route, self).__init__(name, import_name)
        self.server_private_key = server_private_key
        self.decryption_apis = decryption_apis
        self.logger = logger
        self.config = config
        self.db = db
        
        self.login_service = Login_Service(config,logger,db,keyset_map,session)
                
        self.route('/get_instance',methods = ['GET']) (self.get_instance)
        self.route('/login', methods=['POST'])(decrypt_request((self.login),self.server_private_key,self.decryption_apis))
        self.route('/reset_password', methods=['POST']) ( self.secure_route(self.reset_password))
        self.route('/change_portal_view', methods=['POST']) ( self.secure_route(self.change_portal_view))
        self.route('/forgot_password', methods=['POST']) (self.forgot_password)
        self.route('/verify_otp', methods=['POST']) (self.verify_otp)
        self.route('/create_product_admin_for_development', methods=['POST']) (self.create_product_admin_for_development)
        #self.route('/create_template_for_role', methods=['POST']) (self.login_controller.create_template_for_role)
        self.route('/change_password', methods=['POST']) ( self.secure_decrypt_route(self.change_password))   
        self.route('/logout', methods=['POST']) ( self.secure_route(self.logout))
        self.route('/auth_login', methods=['POST']) ( self.secure_decrypt_route(self.auth_login))
    
    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, session)))

    def secure_decrypt_route(self, view_func):
        return jwt_required()(check_blacklisted_token(decrypt_request(check_jwt_token(view_func, self.db, self.config, session), self.server_private_key, self.decryption_apis)))
    
    def get_instance(self): 
        try:
            origin = request.headers.get('Origin') 
            domain = Utility.get_origin(origin)
            result = self.login_service.get_api_url(domain)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=result,count=1)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def login(self,request_data = None):
        try:
            origin = request.headers.get('Origin') 
            json_current_user_modified,access_token = self.login_service.login(request_data,origin)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.DRS,data=json_current_user_modified)
            response.token = access_token
            json_string = json.dumps(response.__dict__)
            data = json.loads(json_string)
            return response.__dict__
        except Custom_Error as e:
            print(str(e))
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            print(str(e))
            self.logger.error(e)
            return Utility.generate_exception_response(e)   
    
    def reset_password(self):
        try:
            identity_data = get_jwt_identity()
            self.login_service.reset_password(identity_data,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.PRS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def forgot_password(self):
        try:
            origin = request.headers.get('Origin')
            result = self.login_service.forgot_password(request.json,origin)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.OTP,data=result)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def verify_otp(self):
        try:
            origin = request.headers.get('Origin')
            self.login_service.verify_otp(request.json,origin)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.PCS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def create_product_admin_for_development(self):
        try:          
            request_data = request.json
            self.login_service.create_product_admin(request_data)
            return Utility.generate_success_response(is_table=False,message="product_admin user added successfully")
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def change_password(self,request_data = None):
        try:
            identity_data = get_jwt_identity()
            self.login_service.change_password(identity_data,request_data,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.PCS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''
    def create_template_for_role(self):
        try:            
            roles = request.json.get("roles")            
            response = self.ui_template_service.add_template(roles)           
            return response
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''
        
    def logout(self):
        try:
            jti = get_jwt()["jti"]
            current_app.blacklist.add(jti)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.LOGOUT)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)

    def auth_login(self,request_data = None):
        try:
            identity_data = get_jwt_identity()            
            json_current_user_modified=self.login_service.auth_login(identity_data,g.db)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=json_current_user_modified,count=1)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def change_portal_view(self):
        try:
            identity_data = get_jwt_identity()
            if identity_data["role"] in (CONSTANTS.PRODUCT_ADMIN,CONSTANTS.PRODUCT_USER,CONSTANTS.ADMIN):
                self.login_service.change_portal_view(identity_data,request.json)
                return Utility.generate_success_response(is_table=False,message="Portal is going to switch")
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.UNAUTHORIZED_ERR_MSG)

        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)