from functools import partial
from flask import Blueprint, request,g,session
from flask_jwt_extended import jwt_required, get_jwt_identity

from NeoAdept.utilities.db_utility import DB_Utility

from ..pojo.access_token import ACCESS_TOKEN
from ..gbo.common import Custom_Error
from ..services.prod_ctrl_service import PROD_Ctrl_Service
from ..services.prod_ctrl_service_temp import PROD_Ctrl_Service_temp
from ..utilities.decorator import check_blacklisted_token,check_jwt_token
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS

class Prod_Ctrl_Route(Blueprint):

    def __init__(self, name, import_name, config, logger, db,keyset_map):
        super(Prod_Ctrl_Route, self).__init__(name, import_name)
        self.db = db
        self.config = config
        
        self.ui_template_service = PROD_Ctrl_Service(logger,db,keyset_map)
        self.ui_template_service_tmp = PROD_Ctrl_Service_temp(logger,db,keyset_map)
        self.logger = logger
        self.config = config
        
        api_list = {
            '/upsert_widget': self.upsert_widget,
            '/upsert_page': self.upsert_page,
            '/upsert_sub_menu': self.upsert_sub_menu,
            '/upsert_menu': self.upsert_menu,
            '/upsert_role': self.upsert_role,
            '/delete_role': self.delete_role,
            '/delete_menu': self.delete_menu,
            '/delete_sub_menu': self.delete_sub_menu,
            '/delete_page': self.delete_page,
            '/delete_widget': self.delete_widget,
            '/delete_ui_template': self.delete_ui_template,
            '/get_widgets': self.get_widgets,
            '/get_pages': self.get_pages,
            '/get_menus': self.get_menus,
            '/get_sub_menus': self.get_sub_menus,
            '/get_roles': self.get_roles,
            '/upload_excel_data': self.upload_excel_data
        }
                
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, session)))

    def upsert_widget(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)           
            
            self.ui_template_service.upsert_widget(request.json,email_from_token,g.db)    
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.ADD_WIDGET)    
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def upsert_page(self):       
        try:            
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)          
            
            self.ui_template_service.upsert_page(request.json,email_from_token,g.db)           
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.ADD_PAGE)    
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def upsert_sub_menu(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)         
            self.ui_template_service.upsert_sub_menu(request.json,email_from_token,g.db)                           
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.ADD_SUB_MENU)    
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def upsert_menu(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)           
            self.ui_template_service.upsert_menu(request.json,email_from_token,g.db)                         
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.ADD_MENU)            
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def upsert_role(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)           
            self.ui_template_service.upsert_role(request.json,email_from_token,g.db)    
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.ADD_ROLE)    
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def delete_role(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)          
            _id = request.json.get("_id") 
            self.ui_template_service.delete_obj("role",_id,email_from_token,g.db)  
            self.ui_template_service.delete_role_permissions_for_role(_id, g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DEL_ROLE)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def delete_sub_menu(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)        
            _id = request.json.get("_id") 
            self.ui_template_service.delete_obj("sub_menu",_id,email_from_token,g.db)                          
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DEL_SUB_MENU)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def delete_page(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)        
            _id = request.json.get("_id") 
            self.ui_template_service.delete_obj("page",_id,email_from_token,g.db)                          
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DEL_PAGE)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def delete_widget(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)       
            _id = request.json.get("_id") 
            self.ui_template_service.delete_obj("widget",_id,email_from_token,g.db)                          
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DEL_WIDGET)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def delete_menu(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)          
            _id = request.json.get("_id") 
            self.ui_template_service.delete_obj("menu",_id,email_from_token,g.db)                   
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DEL_MENU)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def delete_ui_template(self):       
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)          
            _id =  request.json.get("_id")
            template_type = request.json.get("template_type")
            self.ui_template_service.delete_obj(template_type,_id,email_from_token,g.db)             
            return Utility.generate_success_response(is_table=False,message=f'{template_type} deleted successfully')
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_widgets(self):
        try:                  
            docs,count = self.ui_template_service_tmp.load_widgets(g.db,request.json) 
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"widget")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_pages(self):
        try:                
            docs,count = self.ui_template_service_tmp.load_pages(g.db,request.json)
            #docs,count = self.ui_template_service.load_pages(g.db,request.json)
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"page")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def get_sub_menus(self):
        try:                     
            docs,count = self.ui_template_service_tmp.load_sub_menus(g.db,request.json)
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"sub_menu")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_menus(self):
        try:                      
            docs,count = self.ui_template_service_tmp.load_menus(g.db,request.json)
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"menu")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def get_roles(self):
        try:                      
            docs,count = self.ui_template_service_tmp.load_roles(g.db,request_data = request.json)
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"role")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def upload_excel_data(self):
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data)
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
            response_data = self.ui_template_service.upload_excel_data(request,email_from_token,g.db)           
            return Utility.generate_success_response_for_crud(success_message="Upload function executed",result_field="response", results=response_data)
        except Exception as e:
            self.logger.error(e) 
            return Utility.generate_exception_response(e)
