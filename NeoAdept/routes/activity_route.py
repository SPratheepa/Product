from functools import partial
from flask import Blueprint, g, jsonify, request
from NeoAdept.utilities.db_utility import DB_Utility
from flask_jwt_extended import get_jwt_identity, jwt_required

from ..pojo.access_token import ACCESS_TOKEN
from ..utilities.decorator import check_blacklisted_token, check_jwt_token
from ..gbo.common import Custom_Error
from ..services.activity_service import Activity_Service
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility

class Activity_Route(Blueprint):

    def __init__(self, name, import_name,config, logger, db, keyset_map, session):
        super(Activity_Route, self).__init__(name, import_name)
        self.activity_service = Activity_Service(keyset_map,logger,db)
        self.logger = logger
        self.db = db
        self.config = config
        self.previous_request_data = {}
        self.session = session

        api_list = {
            '/save_activity': self.save_activity,
            '/delete_activity': self.delete_activity,
            '/get_all_activity': self.get_all_activity,
        }

        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view):
        # Wrap the view with the necessary decorators
        return jwt_required()(check_blacklisted_token(partial(check_jwt_token, db=self.db, config=self.config, session=self.session)(view)))

    def save_activity(self):
        try:
            identity_data = get_jwt_identity()
            result = self.activity_service.save_activity(request.json,identity_data,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DAS,data=result)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def delete_activity(self):
        try:
            identity_data = get_jwt_identity()
            self.activity_service.delete_activity(request.json,identity_data,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DDS)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''
    def get_all_activity(self):
        try:
            docs,count = self.activity_service.get_all_activity(request.json,g.db)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    '''    

    def get_all_activity(self):
        try:
            
            identity_data = get_jwt_identity()
            docs,count = self.activity_service.get_all_activity(identity_data, request.json,g.db)           
            is_download = request.json.get('is_download',False)
            if is_download == True:
                return DB_Utility.get_data_in_excel(docs,"activity_details")
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)      
        
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)