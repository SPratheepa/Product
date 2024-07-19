from functools import partial
from flask import Blueprint, g,request
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..gbo.common import Custom_Error
from ..services.user_service import User_Service
from ..utilities.db_utility import DB_Utility
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..utilities.decorator import check_blacklisted_token,check_jwt_token

class User_Route(Blueprint):

    def __init__(self, name, import_name, config, logger, db, keyset_map, session):
        super(User_Route, self).__init__(name, import_name)
        self.db = db
        self.config = config
        self.session = session
        self.logger = logger
        self.user_service = User_Service(config,logger,db,keyset_map)
        
        api_list = {
            '/save_user': self.save_user,
            '/delete_user': self.delete_user,
            '/get_user_list': self.get_user_list,
            '/upload_user_from_excel': self.upload_user_from_excel
        }
        
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, self.session)))

    def save_user(self):
        try:
            identity_data = get_jwt_identity()
            if '_id' in request.json:
                self.user_service.update_user_details(identity_data, request.json, g.db)
            else:
                self.user_service.add_new_user(identity_data,request.json, g.db)
            return Utility.generate_success_response(is_table=False,message='Saved user successfully')    
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    '''
    def update_user_details(self):
        try:
            
            identity_data = get_jwt_identity()
            self.user_service.update_user_details(identity_data,request.json)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.UPDATE_USER_SUCCESS) 
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''    
    def delete_user(self):
        try:
            identity_data = get_jwt_identity()
            self.user_service.delete_user(identity_data,request.json, g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DELETE_USER_SUCCESS)  
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_user_list(self):
        try:
            identity_data = get_jwt_identity()
            docs,count = self.user_service.get_user_list(identity_data, request.json,g.db)
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"user_details")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def upload_user_from_excel(self):
        try:
            identity_data = get_jwt_identity()
            response_data = self.user_service.upload_user_data(request,identity_data,g.db)
            return Utility.generate_success_response_for_crud(success_message="Upload function executed",result_field="response", results=response_data)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)