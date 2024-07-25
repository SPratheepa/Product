from functools import partial
from flask import Blueprint, json, request,g
from flask_jwt_extended import jwt_required, get_jwt_identity

from ..gbo.bo import Base_Response
from ..gbo.common import Custom_Error
from ..services.client_service import Client_Service
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..utilities.decorator import check_blacklisted_token,check_jwt_token
from ..utilities.db_utility import DB_Utility

class Client_Route(Blueprint):

    def __init__(self, name, import_name, config, logger, db, keyset_map, session):
        super(Client_Route, self).__init__(name, import_name)
        self.client_service = Client_Service(config,logger,db,keyset_map)
        self.logger = logger
        self.db = db
        self.config = config
        self.session = session

        api_list = {
            '/save_client_details': self.save_client_details,
            '/add_subscriptions': self.add_subscriptions,
            '/delete_client_details': self.delete_client_details,
            '/get_client_details': self.get_client_details,
        }

        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, self.session)))

    def save_client_details(self):
        try:
            identity_data = get_jwt_identity()
            if '_id' in request.json:
                self.client_service.update_client_details(identity_data,request.json,g.db)
            else:
                self.client_service.add_client_details(identity_data,request.json,g.db)
            return Utility.generate_success_response(is_table=False,message='Client details saved successfully')
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def add_subscriptions(self):
        try:
            identity_data = get_jwt_identity()
            self.client_service.add_subscriptions(identity_data,request.json,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.ADD_SUBSCRIPTION_SUCCESS_MSG)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    '''
    def update_client_details(self):
        try:
            identity_data = get_jwt_identity()
            self.client_service.update_client_details(identity_data,request.json)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.UPDATE_CLIENT_SUCCESS_MSG)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''    
    
    def delete_client_details(self):
        try:
            identity_data = get_jwt_identity()
            self.client_service.delete_client_details(identity_data,request.json,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DELETE_CLIENT_SUCCESS_MSG)        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def get_client_details(self):
        try:
            identity_data = get_jwt_identity()
            docs,count = self.client_service.get_client_details(identity_data, request.json,g.db)
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,CONSTANTS.CLIENT_DETAILS)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''    
    def upload_client_details_from_excel(self):
        try:
            identity_data = get_jwt_identity()
            self.client_service.upload_client_data(request,identity_data)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DSS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    
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
