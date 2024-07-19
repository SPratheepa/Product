from flask import Blueprint,request
from flask_jwt_extended import jwt_required, get_jwt_identity

from ..product_admin.user_service import User_Service
from ..gbo.bo import Base_Response
from ..gbo.common import Custom_Error
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..utilities.decorator import check_blacklisted_token,check_jwt_token

class UserController:
    def __init__(self,config,logger,db,client_db,keyset_map):
        self.user_service = User_Service(config,logger,db,client_db,keyset_map)
        self.logger = logger
              
    def add_new_user(self):
        try:
            identity_data = get_jwt_identity()
            result = self.user_service.add_new_user(identity_data,request.json)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = CONSTANTS.ADD_USER_SUCCESS)
            return response.__dict__
                   
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    
    def update_user_details(self):
        try:
            identity_data = get_jwt_identity()
            result = self.user_service.update_user_details(identity_data,request.json)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = CONSTANTS.UPDATE_USER_SUCCESS)
            return response.__dict__
                   
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
      
    def delete_user(self):
        try:
            identity_data = get_jwt_identity()
            result = self.user_service.delete_user(identity_data,request.json)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = CONSTANTS.DELETE_USER_SUCCESS)
            return response.__dict__
                       
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_user_list(self):
        try:
            identity_data = get_jwt_identity()
            docs,count = self.user_service.get_user_list(identity_data, request.json)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.DRS,data=docs,count=count)
            return response.__dict__                       
            
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''    
    def upload_user_from_excel(self):
        try:
            identity_data = get_jwt_identity()
            json_documents,column_names=read_excel(request)
            if column_names:
                missing_keys = [key for key in CONSTANTS.USER_KEYS if key not in column_names]
                if missing_keys:
                    return  Utility.generate_error_response(CONSTANTS.EXCEL_NOT_VALID,201)
            email_from_token,role_from_token = Utility.get_data_from_identity(identity_data)   
            if role_from_token == CONSTANTS.PRODUCT_ADMIN or CONSTANTS.ADMIN :   
               return self.user_service.upload_users(json_documents,identity_data)                     
            else:
                return Utility.generate_error_response(CONSTANTS.ADD_USER_ERR2,201)                        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''
class UserRoute(Blueprint):

    def __init__(self, name, import_name,config,logger,db,client_db,keyset_map):
        super(UserRoute, self).__init__(name, import_name)
        self.user_controller = UserController(config,logger,db,client_db,keyset_map)
        self._register_routes(db,client_db)
    
    def _register_routes(self,db,client_db):
        self.route('/add_new_user',methods = ['POST']) (jwt_required()(check_blacklisted_token(check_jwt_token(self.user_controller.add_new_user,db,client_db))))
        self.route('/update_user_details', methods=['PUT'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.user_controller.update_user_details,db,client_db))))
        self.route('/delete_user', methods=['POST'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.user_controller.delete_user,db,client_db))))
        self.route('/get_user_list', methods=['POST'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.user_controller.get_user_list,db,client_db))))
        #self.route('/upload_user_from_excel', methods=['POST'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.user_controller.upload_user_from_excel,db,client_db))))

        