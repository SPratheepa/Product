from functools import partial
from flask import Blueprint, request, current_app,g,session
from flask_jwt_extended import jwt_required,  get_jwt_identity, get_jwt

from ..gbo.bo import Base_Response
from ..gbo.common import Custom_Error
from ..services.register_service import Register_Service
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..utilities.decorator import check_blacklisted_token,check_jwt_token
from ..utilities.db_utility import DB_Utility

class Register_Route(Blueprint):
    def __init__(self, name, import_name, config, logger, db, keyset_map):
        super(Register_Route, self).__init__(name, import_name)
        self.db = db
        self.config = config
        
        self.register_service = Register_Service(config,logger,db,keyset_map)
        self.logger = logger
        
        api_list = {
            '/client_registration': self.client_registration,
            '/get_registration_details': self.get_registration_details,
            '/update_registration_status': self.update_registration_status
        }
        
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, session)))

    def client_registration(self):
        try:
            self.register_service.register_client(request.json)           
            return Utility.generate_success_response(is_table=False,message='Email sent and registration data stored successfully.')      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_registration_details(self):
        try:
            identity_data = get_jwt_identity()
            docs,count = self.register_service.get_registration_details(identity_data, request.json,g.db)           
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"registration_details")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def update_registration_status(self):
        try:
            identity_data = get_jwt_identity()
            result = self.register_service.update_registration_status(identity_data,request.json,g.db)            
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DAS,data=result)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)