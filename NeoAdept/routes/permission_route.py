from flask import Blueprint, request,g,session
from NeoAdept.utilities.db_utility import DB_Utility
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..services.permission_service import Permission_Service
from NeoAdept.gbo.common import Custom_Error
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..pojo.access_token import ACCESS_TOKEN
from ..utilities.decorator import check_blacklisted_token,check_jwt_token
from functools import partial

class Permission_Route(Blueprint):
    def __init__(self, name, import_name,config, logger, db, keyset_map):
        super(Permission_Route, self).__init__(name, import_name)
        self.db = db  # Store the db instance
        self.config = config  # Store the config instance
        
        self.permission_service = Permission_Service(logger,db,keyset_map)
        self.logger = logger
        
        api_list = {
            '/save_role_permission': self.save_role_permission,
            '/save_user_permission': self.save_user_permission,
            '/get_role_permission': self.get_role_permission,
            '/get_user_permission': self.get_user_permission
        }
        
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, session)))

    def save_role_permission(self):
        try:
            identity_data = get_jwt_identity()
            self.permission_service.save_role_permission(identity_data,request.json,g.db)           
            return Utility.generate_success_response(is_table=False,message='Role permission data saved successfully.')      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)

    def save_user_permission(self):
        try:
            identity_data = get_jwt_identity()
            self.permission_service.save_user_permission(identity_data,request.json,g.db)           
            return Utility.generate_success_response(is_table=False,message='User permission data saved successfully.')      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
            
    def get_role_permission(self):
        try:
            
            docs,count = self.permission_service.get_role_permission( request.json,g.db)           
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"role_permission")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)

    def get_user_permission(self):
        try:
            result = self.permission_service.get_permissions(request.json,g.db)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=result,count = 0)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)