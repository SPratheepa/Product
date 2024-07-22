import json
import logging,os
import base64
import copy

from datetime import datetime
from flask import Flask,send_from_directory
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_swagger_ui import get_swaggerui_blueprint
from flask_caching import Cache

from logging.handlers import RotatingFileHandler

from pathlib import Path

from NeoAdept.config import Config
from NeoAdept.pojo.common.directory import DIRECTORY
from NeoAdept.utilities.menu_widget import Menu_Widget
from NeoAdept.utilities.module_permission import Module_Permission

from NeoAdept.utilities.key_generator import key_generator
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from NeoAdept.routes.dropdown_route import Dropdown_Route
from NeoAdept.routes.dynamic_db_route import Dynamic_DB_Route
from NeoAdept.routes.dynamic_widget_route import Dynamic_widget_Route
from NeoAdept.routes.feedback_route import Feedback_Route
from NeoAdept.utilities.utility import Utility
from NeoAdept.utilities.db_utility import Collection_Manager,DB_Utility,Mongo_DB_Manager
from NeoAdept.utilities.constants import CONSTANTS
from NeoAdept.routes.candidate_route import Candidate_Route
from NeoAdept.routes.ui_template_route import UI_Template_Route
from NeoAdept.routes.job_route import Job_Route
from NeoAdept.routes.login_route import Login_Route
from NeoAdept.routes.client_route import Client_Route
from NeoAdept.routes.user_route import User_Route
from NeoAdept.routes.mylist_route import My_List_Route
from NeoAdept.routes.activity_route import Activity_Route
from NeoAdept.routes.company_route import Company_Route
from NeoAdept.routes.common_route import Common_Route
from NeoAdept.routes.email_route import Email_Route
from NeoAdept.routes.register_route import Register_Route
from NeoAdept.routes.permission_route import Permission_Route

class Init:
    def __init__(self):
        self.config = Config()
        connection_manager = Collection_Manager(self.config)
        self.db = connection_manager.connect_db(self.config.db_name)
        if Mongo_DB_Manager.is_collection_empty(self.db['MODULE_DETAILS']):
            Module_Permission(self.config.role_permission_file).load_module_details(self.db)

        if self.config.CLIENT_ENV == CONSTANTS.CLIENT and Mongo_DB_Manager.is_collection_empty(self.db['WIDGET']):
            Menu_Widget(self.config.ui_template_file).load_widget_menu(self.db)
            key_generator(self.db)

        if Mongo_DB_Manager.is_collection_empty(self.db['COLUMN_VISIBILITY']):
            self.enable_collections_columns(self.keyset_map,self.db)
    
    def enable_collections_columns(self,key_set_map,db) : 
        key_set_map_copy = copy.deepcopy(key_set_map)
        for main_key in key_set_map_copy:            
            sub_key_list = key_set_map_copy[main_key]             
            sub_key_list.pop("key")
            sub_key_list = self.pop_keys(sub_key_list,main_key)           
            if not isinstance(sub_key_list, list):
                sub_key_list = list(sub_key_list.values())
            sub_key_list = sub_key_list[1:]
            new_sub_key_list = []            
            for order, sub_key in enumerate(sub_key_list, start=1):
                sub_key_dict = {
                    "db_column": sub_key,
                    "ui_column": sub_key,
                    "order": order,
                    "enable": order <= 4,
                    "default_enable" : order <= 7,
                    "widget_enable" :True
                }
                new_sub_key_list.append(sub_key_dict)
            
            key_set_map_copy[main_key] = new_sub_key_list
            
        Mongo_DB_Manager.create_document(db["COLUMN_VISIBILITY"],key_set_map_copy)
    
    def pop_keys(self,sub_key_list,main_key):
        keys_to_remove_map = {
        "USER_DETAILS": CONSTANTS.USER_DETAILS_KTR,
        "PAGE": CONSTANTS.PAGE_KTR,
        "MENU": CONSTANTS.MENU_KTR,
        "SUB_MENU": CONSTANTS.SUB_MENU_KTR,
        "ROLE" : CONSTANTS.ROLE_KTR
        
    }
        keys_to_remove = keys_to_remove_map.get(main_key, [])
        filtered_sub_key_list = {k: v for k, v in sub_key_list.items() if k not in keys_to_remove}

        return filtered_sub_key_list
    
    def load_module_details(self):
        module_details_map = {}
        module_details_collection = self.db["MODULE_DETAILS"]
        modules = Mongo_DB_Manager.read_documents(module_details_collection,{})
        for module in modules:
            module_details_map[module["module"]] = module
        return module_details_map    
    
    def load_loc_patterns(db):
        documents = db["view_country_state_city"] .find()
        transformed_data = []
        for doc in documents:
            country = doc.get("country")
            for state in doc.get("states", []):
                state_name = state.get("state_name")
                cities = state.get("cities", [])                    
                transformed_data.extend([country, state_name] + cities)
        print(transformed_data)
        patterns = [{"label": "LOCATION", "pattern": city.lower()} for city in cities]
        with open('patterns.jsonl', 'w') as f:
            for city in transformed_data:
                pattern = {
                    "label": "LOCATION",
                    "pattern": [{"LOWER": city}]
                }
                f.write(json.dumps(pattern) + '\n')
                
                
    def create_view(db):
        contact_details_view_pipeline = [
                                                {"$unwind": "$contact_list"},
                                                {
                                                    "$project": {
                                                        "_id": "$_id",
                                                        "company_name": "$company_name",
                                                        "comments": "$contact_list.comments",
                                                        "contact_id": "$contact_list.contact_id",
                                                        "email": "$contact_list.email",
                                                        "name": "$contact_list.name",
                                                        "phone": "$contact_list.phone"
                                                    }
                                                }
                                            ]               
        recent_search_pipeline = [
                                        {
                                            "$group": {
                                            "_id": {
                                                "user": "$user",
                                                "module": "$module"
                                            },
                                            "search_id": { "$push": "$search_id" }
                                            }
                                        },
                                        {
                                            "$project": {
                                            "_id": 0,
                                            "user": "$_id.user",
                                            "module": "$_id.module",
                                            "search_id": 1
                                            }
                                        }
                                    ]
        Collection_Manager.create_view(db,"ATS_COMPANY_DETAILS","CONTACT_DETAILS_VIEW",contact_details_view_pipeline)
        Collection_Manager.create_view(db,"ATS_SEARCH_DETAILS","SEARCH_DETAILS_VIEW",recent_search_pipeline)    
        
if __name__ == "__main__":
    Init()
    
    


