from NeoAdept.gbo.bo import Pagination
from NeoAdept.pojo.registration_details import REGISTRATION_DETAILS
from NeoAdept.requests.register_request import register_client_request,update_client_status_request
from NeoAdept.services.email_service import Email_Service
from NeoAdept import config
from NeoAdept.services.common_service import Common_Service
from datetime import datetime
from flask import Config, json,current_app
from flask_jwt_extended import create_access_token,get_jwt

from ..gbo.common import Custom_Error

from ..pojo.access_token import ACCESS_TOKEN
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from pymongo import MongoClient
from flask_mail import Message,Mail


class Register_Service:
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,config:Config,logger,db,keyset_map):
        if not hasattr(self, 'initialized'):
            self.logger = logger
            self.config = config
            self.db = db
            self.mongo_client = MongoClient(self.config.db_url,maxPoolSize=self.config.max_pool_size)
            self.neo_db =  self.mongo_client[self.config.neo_db]
            self.email_service = Email_Service(config,keyset_map,logger,db)
            self.registration_collection = "REGISTRATION_DETAILS"  
            self.html_collection = "HTML_TAGS"
            self.SENDPULSE_API_KEY = self.config.sendpulse_api_key
            self.SENDPULSE_API_SECRET = self.config.sendpulse_api_secret
            self.SENDPULSE_TOKEN_URL = self.config.sendpulse_token_url
            self.SENDPULSE_EMAIL_URL = self.config.sendpulse_email_url
            self.key_nested_key_map = keyset_map
            if "REGISTRATION_DETAILS" in keyset_map:
                self.key_map = self.key_nested_key_map["REGISTRATION_DETAILS"]
            self.common_service = Common_Service(logger,db,keyset_map)
            
        
    def register_client(self,data): 
        
        data_request = register_client_request(data) 
        data_request.parse_request()
        data_request.validate_request()
        
        data_obj = REGISTRATION_DETAILS(**data)
        
        data_obj.created_on = Utility.get_current_time()
        name = data_obj.name
        email = data_obj.email
        query = {
            '$or': [
                {'name': name, 'status': 'Pending'},
                {'email': email, 'status': 'Pending'},
                {'phone': data_obj.phone, 'status': 'Pending'},
                {'company': data_obj.company, 'status': 'Pending'}
            ]
        }
        existing_clients = Mongo_DB_Manager.read_documents(self.neo_db[self.registration_collection],query)
        for existing_client in existing_clients:
            if existing_client:
                raise Custom_Error('A client with the same name, email, phone, or company already exists in the collection with status Pending')    
        
        email_template = self.get_html_tag("registration")
        text = "Thanks for connecting with us. Our team will reach out to you."    
        subject = "Welcome to NeoCV"        
        from_email = "admin@neoadept.com"
         
        #data = Utility.frame_email(from_email,subject,email,name,email_template,text)
        #response = Utility.third_party_email_function(self.SENDPULSE_API_KEY,self.SENDPULSE_API_SECRET,self.SENDPULSE_TOKEN_URL,self.SENDPULSE_EMAIL_URL,data)
        #if response.status_code != 200:
        #    raise Custom_Error(response.json())

        html_template = email_template.replace('{name}', name)
        msg = Message(subject=subject,sender=from_email, recipients=[email])
        msg.body = text
        msg.html = html_template
        mail = Mail(current_app)
        mail.send(msg)
           
        data_obj.status = 'Pending'
        data_obj.comments = 'Email is sent to company'
        attributes_to_delete = ["updated_by","updated_on","_id"]
        data_obj = DB_Utility.delete_attributes_from_obj(data_obj,attributes_to_delete)
        result = Mongo_DB_Manager.create_document(self.neo_db[self.registration_collection],data_obj.__dict__)
             
    def get_html_tag(self,key):
        query = {"key": key}
        result = Mongo_DB_Manager.read_one_document(self.db[self.html_collection],query)
        if not result:
            return None
        return result["html_tag"] if result["html_tag"] else None
            
    def get_registration_details(self,identity_data,request_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        role_from_token = identity_data_obj.role
        
        if role_from_token not in [CONSTANTS.PRODUCT_ADMIN, CONSTANTS.PRODUCT_USER]:
           raise Custom_Error('Cannot view registration details')
        
        pagination = Pagination(**request_data) 
        
        self.common_service.create_log_details(identity_data_obj.email,request_data,"get_registration_details",db)
               
        query = DB_Utility.frame_get_query(pagination,self.key_map)
           
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[self.registration_collection],query,pagination) 

        if docs and len(docs)>0:
            #count = Mongo_DB_Manager.count_documents(db[self.registration_collection],query)
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,REGISTRATION_DETAILS),count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
    
    def update_registration_status(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        if role_from_token not in [CONSTANTS.PRODUCT_ADMIN, CONSTANTS.PRODUCT_USER]:
            raise Custom_Error('Cannot update client registration status')
        
        data_request = update_client_status_request(request_data) 
        data_request.parse_request()
        data_request.validate_request()
        data_obj = REGISTRATION_DETAILS(**request_data)
        _id = DB_Utility.str_to_obj_id(data_obj._id)  
        query = {"_id": _id}
        cursor = Mongo_DB_Manager.read_one_document(db[self.registration_collection],query)
        if not cursor:
            raise Custom_Error('client registration info not found for this _id')
        DB_Utility.remove_extra_attributes(data_obj.__dict__,request_data)
        del data_obj._id        
        
        data_obj.updated_on = Utility.get_current_time()
        data_obj.updated_by = email_from_token
        
        result = Mongo_DB_Manager.update_document(db[self.registration_collection], query, data_obj.__dict__)
        if result == 0:
            raise Custom_Error('Could not update client registration info')  

        data = Mongo_DB_Manager.read_one_document(db[self.registration_collection],query)
        return DB_Utility.convert_obj_id_to_str_id(data)