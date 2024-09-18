from functools import partial
import os,datetime

from flask import Blueprint, json, request,g,session
from NeoAdept.gbo.common import Custom_Error
from NeoAdept.utilities.db_utility import DB_Utility
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..utilities.decorator import check_blacklisted_token,check_jwt_token
from ..services.email_service import Email_Service
from ..utilities.utility import Utility
from ..utilities.constants import CONSTANTS

class Email_Route(Blueprint):
    def __init__(self, name, import_name,config, logger, db, keyset_map):
        super(Email_Route, self).__init__(name, import_name)
        self.email_service = Email_Service(config,keyset_map,logger,db)
        self.logger = logger
        self.db = db
        self.config = config
        
        
        api_list = {
            '/send_email': self.send_email,
            '/view_mail_history': self.view_mail_history
        }
                
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, session)))

    def _cache_view(self, view):
        # Apply caching to the view
        return self.cache.cached(timeout=180)(view)

    def send_email(self):
        try:
            
            result = self.email_service.send_email(request,g.db)
            return Utility.generate_success_response(is_table=False,message='Email sent and mail data stored successfully.',data=result) 
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)  
    
    def view_mail_history(self):
        try:
            identity_data = get_jwt_identity()
            docs,count = self.email_service.view_mail_history(identity_data, request.json,g.db)           
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"email_details")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        