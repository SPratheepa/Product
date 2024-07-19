from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity

from ..product_admin.client_service import Client_Service
from ..gbo.bo import Base_Response
from ..gbo.common import Custom_Error
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..utilities.decorator import check_blacklisted_token,check_jwt_token

class ClientController:
    def __init__(self,config,logger,db,client_db,keyset_map):
        self.client_service = Client_Service(config,logger,db,client_db,keyset_map)
        self.logger = logger
              
    def add_client_details(self):
        try:
            
            identity_data = get_jwt_identity()
            result = self.client_service.add_client_details(identity_data,request.json)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = CONSTANTS.ADD_CLIENT_SUCCESS_MSG)
            return response.__dict__

        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def add_subscriptions(self):
        try:
            identity_data = get_jwt_identity()
            result = self.client_service.add_subscriptions(identity_data,request.json)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = CONSTANTS.ADD_SUBSCRIPTION_SUCCESS_MSG)
            return response.__dict__

        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def update_client_details(self):
        try:
            identity_data = get_jwt_identity()
            result = self.client_service.update_client_details(identity_data,request.json)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = CONSTANTS.UPDATE_CLIENT_SUCCESS_MSG)
            return response.__dict__
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def delete_client_details(self):
        try:
            identity_data = get_jwt_identity()
            result = self.client_service.delete_client_details(identity_data,request.json)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = CONSTANTS.DELETE_CLIENT_SUCCESS_MSG)
            return response.__dict__
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def get_client_details(self):
        try:
            identity_data = get_jwt_identity()
            docs,count = self.client_service.get_client_details(identity_data, request.json)
            response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=CONSTANTS.DRS,data=docs,count=count)
            return response.__dict__
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def upload_client_details_from_excel(self):
        try:
            identity_data = get_jwt_identity()
            result = self.client_service.upload_client_data(request,identity_data)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = "Upload function executed")
            response.response = result
            return response.__dict__
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    '''
    def update_client_settings(self):
        try:
            identity_data = get_jwt_identity()
            result = self.client_service.update_client_settings(request.json,identity_data)
            response = Base_Response(status = CONSTANTS.SUCCESS, status_code = CONSTANTS.SUCCESS_STATUS_CODE, message = CONSTANTS.UPDATE_CLIENT_SUCCESS_MSG)
            return response.__dict__
                    
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''
    
class ClientRoute(Blueprint):

    def __init__(self, name, import_name,config,logger,db,client_db,keyset_map):
        super(ClientRoute, self).__init__(name, import_name)
        self.client_controller = ClientController(config,logger,db,client_db,keyset_map)
        self._register_routes(db,client_db)
    
    def _register_routes(self,db,client_db):
        self.route('/add_client_details',methods = ['POST']) (jwt_required()(check_blacklisted_token(check_jwt_token(self.client_controller.add_client_details,db,client_db))))
        self.route('/add_subscriptions',methods = ['POST']) (jwt_required()(check_blacklisted_token(check_jwt_token(self.client_controller.add_subscriptions,db,client_db))))
        self.route('/update_client_details', methods=['PUT'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.client_controller.update_client_details,db,client_db))))
        self.route('/delete_client_details', methods=['POST'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.client_controller.delete_client_details,db,client_db))))
        self.route('/get_client_details', methods=['POST'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.client_controller.get_client_details,db,client_db))))
        self.route('/upload_client_details_from_excel', methods=['POST'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.client_controller.upload_client_details_from_excel,db,client_db))))
        #self.route('/update_client_settings', methods=['POST'])(jwt_required()(check_blacklisted_token(check_jwt_token(self.client_controller.update_client_settings,db,client_db))))
