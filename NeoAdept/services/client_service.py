from datetime import datetime
from threading import Thread
from flask import json, Config
from NeoAdept.services.common_service import Common_Service
from pymongo import MongoClient

from ..utilities.collection_names import COLLECTIONS
from ..gbo.bo import Pagination
from ..gbo.common import Custom_Error
from ..pojo.access_token import ACCESS_TOKEN
from ..pojo.client_details import CLIENT_DETAILS
from ..requests.client_request import create_client_request,create_client_subscription_request,update_client_request,delete_client_request,upload_client_request
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..utilities.db_utility import Collection_Manager, DB_Utility, Mongo_DB_Manager

class Client_Service:  
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
            self.mongo_client = Collection_Manager().configure_client(config.db_url,config.max_pool_size)
            self.key_map = keyset_map[COLLECTIONS.MASTER_CLIENT_DETAILS]
            ##self.common_service = Common_Service(logger,db,keyset_map)
    
    def add_client_details(self,identity_data,client_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        client_data_request = create_client_request(client_data) 
        client_data_request.parse_request()
        client_data_request.validate_request()
        client_data_obj = CLIENT_DETAILS(**client_data)  
        
        if client_data_obj.client_name == CONSTANTS.NEO_CV:
            raise Custom_Error('Cannot save product client')
                
        client_collection = db[COLLECTIONS.MASTER_CLIENT_DETAILS]
        
        fields_to_check = ["client_name", "api_url", "domain"]
        query = DB_Utility.fields_to_check(client_data_obj,fields_to_check)
        existing_client = Mongo_DB_Manager.read_one_document(client_collection, query)

        if existing_client:
            field = next((field for field in fields_to_check if getattr(client_data_obj, field) == existing_client[field]), None)
            if field:
                raise Custom_Error(f"{field} already exists")  
        
        client_data_obj.created_on = Utility.get_current_time()
        client_data_obj.created_by = email_from_token
        attributes_to_delete = ["updated_by","updated_on","_id","db_name"]
        client_data_obj = DB_Utility.delete_attributes_from_obj(client_data_obj,attributes_to_delete)
        
        client_id = Mongo_DB_Manager.create_document(client_collection,client_data_obj.__dict__)
        client_data_obj.db_name = DB_Utility.obj_id_to_str(client_id)

        # Update the db_name in the client_details_collection
        update_values = {"db_name": client_data_obj.db_name}
        update_db = Mongo_DB_Manager.update_document(client_collection, {"_id": client_id}, update_values)
    
        Thread(target=self.create_client_database, args=(client_data_obj.db_name, db)).start()
        
        if not client_id:
            raise Custom_Error(CONSTANTS.ADD_CLIENT_ERR_MSG2)
        
    def create_client_database(self, db_name, main_db):
        client_db = self.mongo_client[db_name]
        if db_name not in self.mongo_client.list_database_names():
            doc = Mongo_DB_Manager.read_one_document(main_db[COLLECTIONS.CONFIG_KEYS],{})
            if doc:
                self.client_default_collections = doc.get("client_default_collections")   
                self.add_client_collection(client_db)
                self.copy_collection_list_dropdown(main_db, client_db)
        return client_db
        
    def add_client_collection(self,db):
        if self.client_default_collections:
            for collection_name in self.client_default_collections:
                if collection_name not in db.list_collection_names():
                        db.create_collection(collection_name)
        else:
            print("no collections available... pls configure client_default_collections CONFIG_KEYS")
            
    def copy_collection_list_dropdown(self, main_db, client_db):
        original_collection = main_db[COLLECTIONS.CONFIG_COLLECTION_LIST_DROPDOWN]
        dropdown_documents = list(original_collection.find())
        
        new_collection = client_db[COLLECTIONS.CONFIG_COLLECTION_LIST_DROPDOWN]
        if dropdown_documents:
            new_collection.insert_many(dropdown_documents)
            
        for doc in dropdown_documents:
            collection_name = doc['value']
            self.copy_collection(main_db, client_db, collection_name)
            
    def copy_collection(self, main_db, client_db, collection_name):
        original_collection = main_db[collection_name]
        new_collection = client_db[collection_name]
        
        documents = list(original_collection.find())
        if documents:
            new_collection.insert_many(documents)
            
    def add_subscriptions(self, identity_data, client_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        role_from_token = identity_data_obj.role
        
        client_data_request = create_client_subscription_request(client_data) 
        client_data_request.parse_request()
        client_data_request.validate_request()
        
        client_data_obj = CLIENT_DETAILS(**client_data)
        
        client_collection = db[COLLECTIONS.MASTER_CLIENT_DETAILS]    
        
        query = {"_id": DB_Utility.str_to_obj_id(client_data_obj._id), **Utility.get_is_deleted_false_query()}
        existing_client = Mongo_DB_Manager.read_one_document(client_collection,query)
        
        if not existing_client:
            raise Custom_Error(CONSTANTS.CLIENT_NOT_FOUND)
            
        existing_client_obj = CLIENT_DETAILS(**existing_client)
        existing_subscriptions = existing_client_obj.subscription_details

        for subscription in client_data_obj.subscription_details:
            start_date = subscription['start_date']
            end_date = subscription['end_date']
            for existing_subscription in existing_subscriptions:
                existing_start_date = existing_subscription['start_date']
                existing_end_date = existing_subscription['end_date']
                if existing_start_date < end_date and existing_end_date > start_date:
                    raise Custom_Error(CONSTANTS.OVERLAPPING_SUBSCRIPTION_ERR_MSG)
                    
        query = {"_id": DB_Utility.str_to_obj_id(client_data_obj._id), **Utility.get_is_deleted_false_query()} 
        update = {"$push": {"subscription_details": {"$each": client_data_obj.subscription_details}}}
        result = client_collection.update_one(query, update)
                        
        if result.modified_count == 0:   
            raise Custom_Error(CONSTANTS.ADD_SUBSCRIPTION_ERR_MSG)  

    def update_client_details(self,identity_data,client_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
            
        client_data_request = update_client_request(client_data) 
        client_data_request.parse_request()
        client_data_request.validate_request()
        client_data_obj = CLIENT_DETAILS(**client_data)

        _id = DB_Utility.str_to_obj_id(client_data_obj._id)
        client_collection = db[COLLECTIONS.MASTER_CLIENT_DETAILS]
        
        client_info = Mongo_DB_Manager.read_one_document(client_collection, {"_id": _id})
        if client_info and client_info["client_name"] == CONSTANTS.NEO_CV:
            raise Custom_Error("Client update not allowed for NeoCV")              

        list_of_keys = ["api_url","domain","client_name"]
        query = DB_Utility.update_keys_check(client_data_obj,list_of_keys,_id)
                
        cursor = Mongo_DB_Manager.read_documents(client_collection,query)
        existing_clients = list(cursor)

        if _id not in [client['_id'] for client in existing_clients]:
            raise Custom_Error(CONSTANTS.CLIENT_NOT_FOUND)
        
        for existing_client in list(existing_clients):
            if existing_client["_id"] != _id:
                raise Custom_Error('client_name or api_url or domain already exists for other documents')
            
        client_data_obj.updated_on, client_data_obj.updated_by = Utility.settings_for_data_operation(email_from_token, CONSTANTS.UPDATE)
        attributes_to_delete = ["created_by","created_on","_id","subscription_details","db_name"]
        client_data_obj = DB_Utility.delete_attributes_from_obj(client_data_obj,attributes_to_delete)
            
        result = Mongo_DB_Manager.update_document(client_collection,  {"_id": _id}, client_data_obj.__dict__)
        if result == 0:
            raise Custom_Error(CONSTANTS.UPDATE_CLIENT_ERR_MSG1)
        
    def rename_database(self, old_db_name, new_db_name):
        new_db = self.mongo_client[new_db_name]
        old_db = self.mongo_client[old_db_name]
        
        if new_db_name not in self.mongo_client.list_database_names():
            for collection_name in old_db.list_collection_names():
                new_db.create_collection(collection_name)
                old_collection = old_db[collection_name]
                
                if old_collection.count_documents({}) > 0:
                    new_collection = new_db[collection_name]
                    new_collection.insert_many(old_collection.find())
                old_collection.drop()

    def get_user_collection_by_client(self,client_obj):
        db_name = (self.config.neo_db if client_obj.client_name == CONSTANTS.NEO_CV 
            else client_obj.db_name)
        return self.mongo_client[db_name][COLLECTIONS.MASTER_USER_DETAILS]
            
    def delete_client_details(self,identity_data, client_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        client_data_request = delete_client_request(client_data) 
        client_data_request.parse_request()
        client_data_request.validate_request()
            
        client_data_obj = CLIENT_DETAILS(**client_data)
        _id =  client_data_obj._id 
        query = {"_id": DB_Utility.str_to_obj_id(_id)}
        
        client_collection = db[COLLECTIONS.MASTER_CLIENT_DETAILS]
        existing_client = Mongo_DB_Manager.read_one_document(client_collection,query)
            
        if not existing_client:
            raise Custom_Error(CONSTANTS.CLIENT_NOT_FOUND)

        if existing_client.get(CONSTANTS.IS_DELETED) == CONSTANTS.TRUE:
            raise Custom_Error(CONSTANTS.CLIENT_ALREADY_DELETED)
                
        client_data_obj.updated_on,client_data_obj.updated_by,client_data_obj.is_deleted = Utility.settings_for_data_operation(email_from_token, CONSTANTS.DELETE,is_delete=True)
        attributes_to_delete = ["client_name","api_url","domain","status","client_address","contact_person","phone","email","created_by","created_on","_id","db_name","subscription_details"]
        client_data_obj = DB_Utility.delete_attributes_from_obj(client_data_obj,attributes_to_delete)
            
        result = Mongo_DB_Manager.update_document(client_collection, query, client_data_obj.__dict__)
        if result != 1:
            raise Custom_Error(CONSTANTS.COULD_NOT_DELETE_CLIENT)
        
        query = {"client_id": _id, **Utility.get_delete_false_query()}
        client_user = Mongo_DB_Manager.update_documents(db[COLLECTIONS.MASTER_USER_DETAILS],query,client_data_obj.__dict__)
        
    def get_client_details(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        role_from_token = identity_data_obj.role 
        #self.common_service.create_log_details(identity_data_obj.email,request_data,"get_client_details",db)
        
        client_id = request_data.get('_id')
        if client_id:
            filter_by = {"_id": [DB_Utility.str_to_obj_id(client_id)]} 
            if 'filter_by' not in request_data:            
                request_data = {"filter_by":[filter_by]}
            else:
                request_data["filter_by"].append(filter_by)
        request_data.pop('_id', None)
        
        pagination = Pagination(**request_data)        
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[COLLECTIONS.MASTER_CLIENT_DETAILS],query,pagination)
        
        if pagination.is_download==True:
                return docs,count
            
        new_docs=[]
        for doc in docs:
            if 'created_on' in doc and isinstance(doc['created_on'], datetime):
                doc['created_on'] = doc['created_on'].strftime("%Y-%m-%d %H:%M:%S.%f")
            if 'updated_on' in doc and isinstance(doc['updated_on'], datetime):
                doc['updated_on'] = doc['updated_on'].strftime("%Y-%m-%d %H:%M:%S.%f")
            docs_obj = CLIENT_DETAILS(**doc)
            docs_obj._id = DB_Utility.obj_id_to_str(docs_obj._id)           
            new_docs.append(docs_obj.__dict__)
                
        if new_docs and len(new_docs)>0:
            return new_docs,count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)  