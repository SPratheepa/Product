from functools import partial
from flask import Blueprint, g, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from NeoAdept.gbo.common import Custom_Error
from NeoAdept.services.dynamic_db_service import Dynamic_DB_Service
from NeoAdept.utilities.constants import CONSTANTS
from NeoAdept.utilities.decorator import check_blacklisted_token, check_jwt_token
from NeoAdept.utilities.utility import Utility

class Dynamic_DB_Route(Blueprint):

    def __init__(self, name, import_name, config,logger, db, keyset_map,session,keyset_map_dt, sql_db,sql_table_list):
        super(Dynamic_DB_Route, self).__init__(name, import_name)
        self.dynamic_db_service = Dynamic_DB_Service(logger,keyset_map_dt,sql_table_list,session)
        self.logger = logger
        self.db = db
        self.config = config
        self.session = session

        api_list = {
            '/get_collection_list': self.get_collection_list
        }
        
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, self.session)))
            
    def get_collection_list(self):
        try:
            identity_data = get_jwt_identity()
            request_data = request.json 
            if "db_type" in request_data and request_data["db_type"]=="SQL":
                collections,count = self.dynamic_db_service.get_sql_table_list(identity_data, request_data) 
            else:
                collections, count = self.dynamic_db_service.get_collection_list(request_data, g.db)
            return Utility.generate_success_response(is_table=True, message=CONSTANTS.DRS, data=collections, count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)