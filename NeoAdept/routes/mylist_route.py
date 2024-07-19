from functools import partial
from flask import Blueprint, g, request
from NeoAdept.utilities.db_utility import DB_Utility
from flask_jwt_extended import get_jwt_identity, jwt_required

from ..utilities.decorator import check_blacklisted_token, check_jwt_token
from ..gbo.common import Custom_Error
from ..services.mylist_service import My_List_Service
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility

class My_List_Route(Blueprint):
    def __init__(self, name, import_name, config, logger, db, keyset_map, session):
        super(My_List_Route, self).__init__(name, import_name)
        self.list_service = My_List_Service(config,logger,db,keyset_map)
        self.db = db
        self.config = config
        self.logger = logger
        self.session = session
        
        api_list = {
            '/add_group_list': self.add_group_list,
            '/rename_group_list': self.rename_group_list,
            '/delete_group_list': self.delete_group_list,
            '/get_group_list': self.get_group_list,
            '/add_cv_list_to_group': self.add_cv_list_to_group,
            '/remove_cv_list_from_group': self.remove_cv_list_from_group,
            '/get_cv_group_list': self.get_cv_group_list,
            '/move_cv_to_list': self.move_cv_to_list
        }
                
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, self.session)))

    def add_group_list(self):
        try:
            identity_data = get_jwt_identity()
            self.list_service.add_new_group(request.json,identity_data,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DAS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def rename_group_list(self):
        try:
            identity_data = get_jwt_identity()
            self.list_service.rename_group_list(request.json,identity_data,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DUS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def delete_group_list(self):
        try:
            identity_data = get_jwt_identity()
            self.list_service.delete_list(request.json,identity_data,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DDS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_group_list(self):
        try:  
            identity_data = get_jwt_identity()
            docs,count = self.list_service.get_group_list(identity_data, request.json,g.db)           
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"group_list")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def add_cv_list_to_group(self):
        try:
            identity_data = get_jwt_identity()
            self.list_service.add_cv_list_to_group(request.json,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DAS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def remove_cv_list_from_group(self):
        try:
            identity_data = get_jwt_identity()
            self.list_service.remove_cv_from_group(request.json,g.db)
            return Utility.generate_success_response(is_table=False,message='Removed from group successfully.')
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_cv_group_list(self):
        try:
            
            identity_data = get_jwt_identity()
            docs,count = self.list_service.get_file_email_list(identity_data, request.json,g.db)           
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"group_view")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)

    def move_cv_to_list(self):
        try:
            identity_data = get_jwt_identity()
            self.list_service.move_cv_to_list(request.json,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DAS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
