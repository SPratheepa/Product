from functools import partial
from flask import Blueprint, g, request
from NeoAdept.utilities.db_utility import DB_Utility
from flask_jwt_extended import get_jwt_identity, jwt_required
from NeoAdept.gbo.common import Custom_Error
from NeoAdept.pojo.user.access_token import ACCESS_TOKEN
from NeoAdept.services.dynamic_widget_service import Dynamic_widget_Service
from NeoAdept.utilities.constants import CONSTANTS
from NeoAdept.utilities.decorator import check_blacklisted_token, check_jwt_token
from NeoAdept.utilities.utility import Utility

class Dynamic_widget_Route(Blueprint):

    def __init__(self, name, import_name,config, logger, db,keyset_map,session, keyset_map_dt, sql_db):
        super(Dynamic_widget_Route, self).__init__(name, import_name)
        self.dynamic_widget_service = Dynamic_widget_Service(logger,db,keyset_map,keyset_map_dt,session,sql_db)
        self.logger = logger
        self.db = db
        self.config = config
        self.session = session
        
        api_list = {
            '/save_dynamic_widget': self.save_dynamic_widget,
            '/get_dynamic_widget': self.get_dynamic_widget,
            '/generate_dynamic_widget_query': self.generate_dynamic_widget_query,
            '/preview_query_result': self.preview_query_result,
            '/delete_dynamic_widget': self.delete_dynamic_widget
        }
                
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, self.session)))

    def get_dynamic_widget(self):
        try:
            identity_data = get_jwt_identity()
            request_data = request.json
            docs,count = self.dynamic_widget_service.get_dynamic_widget(identity_data, request_data,g.db)           
            is_download = request_data.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"dynamic_widget")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def save_dynamic_widget(self):
        try:
            identity_data = get_jwt_identity()
            identity_data_obj = ACCESS_TOKEN(**identity_data) 
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)  
            request_data = request.json
            if '_id' in request_data:
                result = self.dynamic_widget_service.update_dynamic_widget(request_data,g.db,email_from_token)
            else:
                result = self.dynamic_widget_service.save_dynamic_widget(request_data,g.db,email_from_token)
            #return Utility.generate_success_response(is_table=False,message=CONSTANTS.ADD_WIDGET)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DAS,data=result)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def delete_dynamic_widget(self):
        try:
            
            identity_data = get_jwt_identity()
            result = self.dynamic_widget_service.delete_dynamic_widget(identity_data,request.json,g.db)
            return Utility.generate_success_response(is_table=False,message='Dynamic widget deleted successfully') 
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def generate_dynamic_widget_query(self):
        try:
            identity_data = get_jwt_identity()
            id = request.args.get('_id') 
            if not id:
                raise Custom_Error("Please provide widget _id")
            request_data = request.json
            docs,count = self.dynamic_widget_service.generate_dynamic_widget_query1(identity_data,id,request_data,g.db)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
            
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def preview_query_result(self):
        try:
            identity_data = get_jwt_identity()
            request_data = request.json
            if "db_type" in request_data and request_data["db_type"]=="SQL":
                docs,count = self.dynamic_widget_service.preview_query_result_for_sql(request_data,identity_data)
            else:
                docs,count = self.dynamic_widget_service.preview_query_result(request_data,g.db,identity_data)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)