from functools import partial
from flask import Blueprint, g, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from NeoAdept.pojo.access_token import ACCESS_TOKEN

from ..gbo.common import Custom_Error
from ..services.dropdown_service import Dropdown_Service
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..utilities.decorator import check_blacklisted_token, check_jwt_token

class Dropdown_Route(Blueprint):

    def __init__(self, name, import_name, config,logger,db, keyset_map,session, filters):
        super(Dropdown_Route, self).__init__(name, import_name)
        self.dropdown_service = Dropdown_Service(logger,db,keyset_map,filters)
        self.logger = logger
        self.db = db
        self.config = config
        self.session = session
        
        api_list = {
            '/dropdown_list': self.dropdown_list,
            '/get_filters_for_collection': self.get_filters_for_collection
        }
        
        for api, method in api_list.items():
            self.add_url_rule(api, view_func=self.secure_route(method), methods=['POST'])

    def secure_route(self, view_func):
        return jwt_required()(check_blacklisted_token(check_jwt_token(view_func, self.db, self.config, self.session)))

    def dropdown_list(self):
        try:
            key = request.json.get('key')
            if not key:
                docs,count = self.dropdown_service.drop_down_list,self.dropdown_service.dd_count
                #docs,count = self.dropdown_service.get_full_dropdown_list(g.db)
            else:
                docs,count = self.dropdown_service.drop_down_list["key"],len(docs)
                #docs,count = self.dropdown_service.get_dropdown_list(request.json,g.db)
            return Utility.generate_success_response(is_table=True,message=CONSTANTS.DRS,data=docs,count=count)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def get_filters_for_collection(self):
        try:
            identity_data = ACCESS_TOKEN(**get_jwt_identity())
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data)
            #if role_from_token == CONSTANTS.USER or role_from_token == CONSTANTS.ADMIN or role_from_token == CONSTANTS.PRODUCT_ADMIN:
            if "collection_name" not in request.json:
                raise Custom_Error("collection_name is must in the request")
            docs = self.dropdown_service.get_filters_for_collection(request.json, email_from_token,g.db)
            return Utility.generate_success_response(is_table=False,message=CONSTANTS.DRS,data=docs)
            #raise Custom_Error(CONSTANTS.AUTHORIZATION_ERR)
        except Custom_Error as e:
            self.logger.error(e)
            return Utility.generate_error_response(str(e))
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)