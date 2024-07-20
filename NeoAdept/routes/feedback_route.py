from functools import partial
from flask import Blueprint, g, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from ..pojo.access_token import ACCESS_TOKEN
from ..gbo.common import Custom_Error
from ..services.feedback_service import Feedback_Service
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..utilities.decorator import check_blacklisted_token, check_jwt_token

class Feedback_Route(Blueprint):

    def __init__(self, name, import_name, logger, db, config, keyset_map, session):
        super(Feedback_Route, self).__init__(name, import_name)
        self.feedback_service = Feedback_Service(logger,db,config,keyset_map)
        self.logger = logger
        self.db = db
        self.config = config
        self.session = session

        api_list = {
            '/create_feedback': self.create_feedback,
            '/upload_attachment': self.upload_attachment,
            '/get_feedback_list': self.get_feedback_list
        }
        
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, self.session)))

    def create_feedback(self):
        try:
            identity_data = get_jwt_identity()
            self.feedback_service.save_feedback_details(request.json, identity_data,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DAS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def upload_attachment(self):
        try:    
            print(request.files)
            result_list = self.feedback_service.upload_attachment(request.files) 
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=result_list,count=len(result_list))        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_feedback_list(self):
        try:
            identity_data = get_jwt_identity()
            docs,count = self.feedback_service.get_feedback_list(request.json,identity_data,g.db)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
