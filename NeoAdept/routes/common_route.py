from functools import partial
from flask import Blueprint, request,g
from NeoAdept.utilities.db_utility import DB_Utility
from NeoAdept.utilities.module_permission import Module_Permission
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..services.common_service import Common_Service
from NeoAdept.gbo.common import Custom_Error
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS
from ..pojo.user.access_token import ACCESS_TOKEN
from ..utilities.decorator import check_blacklisted_token,check_jwt_token

class Common_Route(Blueprint):

    def __init__(self, name, import_name,config, logger, db, keyset_map, session):
        super(Common_Route, self).__init__(name, import_name)
        self.common_service = Common_Service(logger,db,keyset_map)
        self.logger = logger
        self.db = db
        self.config = config
        self.session = session

        api_list = {
            '/upload_attachments': self.upload_attachments,
            '/get_doc/attachment/<module_type>/<file>': self.get_doc,
            '/get_log_details': self.get_log_details,
            '/global_search_details': self.global_search_details,
            '/save_search_details': self.save_search_details,
            '/recent_search_details': self.recent_search_details,
            '/add_module_details': self.add_module_details,
            '/get_module_details': self.get_module_details,
            '/get_document/<folder>/<file>': self.get_document,
            '/update_column_settings': self.update_column_settings,
            '/enable_user_widget': self.enable_user_widget,
            '/get_history': self.get_history,
            '/get_db_details': self.get_db_details,
        }
    
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, self.session)))

    def upload_attachments(self):
        try:            
            identity_data = ACCESS_TOKEN(**get_jwt_identity())
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data)
            '''if role_from_token not in [CONSTANTS.ADMIN, CONSTANTS.PRODUCT_ADMIN, CONSTANTS.PRODUCT_USER]:
                raise Custom_Error(CONSTANTS.AUTHORIZATION_ERR)'''
            attachment_info = self.common_service.upload_attachments(request,g.db)
            return Utility.generate_success_response(is_table=False,message="Attachments uploaded successfully.",data=attachment_info)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)

    def get_doc(self,module_type, file):
        try:
            module_type = request.view_args['module_type']
            file_name = request.view_args['file']
            if not module_type or not file_name:
                raise Custom_Error("Both 'module_type' and 'file_name' must be provided")    
            return self.common_service.get_doc(module_type,file_name)  
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_log_details(self):
        try:
            identity_data = get_jwt_identity()
            docs,count = self.common_service.get_log_details(identity_data, request.json,g.db)           
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"log_details")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)

    def global_search_details(self):
        try:
            identity_data = get_jwt_identity()
            docs = self.common_service.global_search_details(identity_data, request.json,g.db)           
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DRS,data=docs)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)    
        
    def save_search_details(self):
        try:
            identity_data = get_jwt_identity()
            docs = self.common_service.save_search_details(identity_data, request.json,g.db)           
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DAS,data=docs)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)  
        
    def recent_search_details(self):
        try:
            identity_data = get_jwt_identity()
            docs = self.common_service.recent_search_details(identity_data, request.json,g.db)           
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DRS,data=docs)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)   
        
    def add_module_details(self):
        try:
            self.common_service.add_module_details(request.json,g.db)           
            return Utility.generate_success_response(is_table=False,message='Module data saved successfully.')      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_module_details(self):
        try:            
            identity_data = get_jwt_identity()
            docs,count = self.common_service.get_module_details(identity_data, request.json,g.db)           
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"module_details")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def load_role_permission(self):
        try:
            Module_Permission(self.config.role_permission_file).load_role_permission(g.db)
            return Utility.generate_success_response(is_table=False, message='Role permissions loaded successfully.')
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_document(self, folder, file):
        try:
            folder = request.view_args['folder']
            file_name = request.view_args['file']
            if not folder or not file_name:
                raise Custom_Error("Both 'folder' and 'file_name' must be provided")
            return self.common_service.get_document(folder, file_name)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def update_column_settings(self):
        try:
            identity_data = get_jwt_identity()
            docs = self.common_service.update_column_settings(identity_data, request.json,self.db)           
            return Utility.generate_success_response(is_table=False,message="Updated column settings",data=docs)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def enable_user_widget(self):
        try:
            identity_data = get_jwt_identity()
            docs = self.common_service.enable_user_widget(identity_data, request.json,g.db)           
            return Utility.generate_success_response(is_table=False,message="Enabled/disabled widget for user",data=docs)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_history(self):
        try:            
            identity_data = get_jwt_identity()
            docs = self.common_service.get_history(identity_data, request.json,g.db)          
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_db_details(self):
        try:            
            identity_data = get_jwt_identity()
            docs,count = self.common_service.get_db_details(identity_data, request.json,g.db)          
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)