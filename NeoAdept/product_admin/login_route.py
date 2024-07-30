from flask import Blueprint, request, current_app
from flask_jwt_extended import jwt_required,  get_jwt_identity, get_jwt

from ..gbo.bo import Base_Response
from ..gbo.common import Custom_Error
from ..product_admin.login_service import Login_Service
#from ..services.UI_template_service import PROD_Ctrl_Service
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..utilities.decorator import check_blacklisted_token,check_jwt_token

class LoginController:
    def __init__(self,config,logger,db,client_db):
        self.login_service = Login_Service(config,logger,db,client_db)
        #self.ui_template_service = PROD_Ctrl_Service(logger)        
        self.logger = logger
               
    def get_instance(self): 
        try:
            
            origin = request.headers.get('Origin') 
            domain = Utility.get_origin(origin)
            result = self.login_service.get_api_url(domain)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.DRS,data=result)
            return response.__dict__
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def login(self):
        try:
            
            origin = request.headers.get('Origin') 
            json_current_user_modified,access_token = self.login_service.login(request.json,origin)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.DRS,data=json_current_user_modified)
            response.token=access_token
            return response.__dict__
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)   
    
    def reset_password(self):
        try:
            identity_data = get_jwt_identity()
            result = self.login_service.reset_password(identity_data)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.PRS)
            return response.__dict__
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
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.OTP,data=result)
            return response.__dict__
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def verify_otp(self):
        try:
            origin = request.headers.get('Origin')
            result = self.login_service.verify_otp(request.json,origin)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.PCS)
            return response.__dict__
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def create_product_admin_for_development(self):
        try:          
            request_data = request.json
            result = self.login_service.create_product_admin(request_data)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message='product_admin user added successfully')
            return response.__dict__
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def change_password(self):
        try:
            identity_data = get_jwt_identity()
            result = self.login_service.change_password(identity_data,request.json)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.PCS)
            return response.__dict__
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
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.LOGOUT)
            return response.__dict__
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)

    def auth_login(self):
        try:
            identity_data = get_jwt_identity()            
            json_current_user_modified=self.login_service.auth_login(identity_data)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.DRS,data=json_current_user_modified)
            return response.__dict__             
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
      
class LoginRoute(Blueprint):
    def __init__(self, name, import_name,config,logger,db,client_db):
        super(LoginRoute, self).__init__(name, import_name)
        self.login_controller = LoginController(config,logger,db,client_db)
        self._register_routes(db,client_db)

    def _register_routes(self,db,client_db):
        self.route('/get_instance',methods = ['GET']) (self.login_controller.get_instance)
        self.route('/login', methods=['POST']) (self.login_controller.login)
        self.route('/reset_password', methods=['POST']) (jwt_required()(check_blacklisted_token(check_jwt_token(self.login_controller.reset_password,db,client_db))))
        self.route('/forgot_password', methods=['POST']) (self.login_controller.forgot_password)
        self.route('/verify_otp', methods=['POST']) (self.login_controller.verify_otp)
        self.route('/create_product_admin_for_development', methods=['POST']) (self.login_controller.create_product_admin_for_development)
        #self.route('/create_template_for_role', methods=['POST']) (self.login_controller.create_template_for_role)
        self.route('/change_password', methods=['POST']) (jwt_required()(check_blacklisted_token(check_jwt_token(self.login_controller.change_password,db,client_db))))       
        self.route('/logout', methods=['POST']) (jwt_required()(check_blacklisted_token(check_jwt_token(self.login_controller.logout,db,client_db))))
        self.route('/auth_login', methods=['POST']) (jwt_required()(check_blacklisted_token(check_jwt_token(self.login_controller.auth_login,db,client_db))))
        
