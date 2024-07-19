from datetime import datetime
from threading import Thread
from flask import json, Config
from NeoAdept.services.common_service import Common_Service
from pymongo import MongoClient

from ..gbo.bo import Pagination
from ..gbo.common import Custom_Error
from ..pojo.user.access_token import ACCESS_TOKEN
from ..pojo.client.client_details import CLIENT_DETAILS
from ..pojo.user.user_details import USER_DETAILS
from ..requests.client_request import create_client_request,create_client_subscription_request,update_client_request,delete_client_request,upload_client_request
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager

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
            self.user_details_collection = 'USER_DETAILS'
            self.client_details_collection = 'CLIENT_DETAILS'
            self.mongo_client = MongoClient(self.config.db_url,maxPoolSize=self.config.max_pool_size)
            self.key_nested_key_map = keyset_map    
            if "CLIENT_DETAILS" in keyset_map:
                self.key_map = self.key_nested_key_map["CLIENT_DETAILS"]
            self.common_service = Common_Service(logger,db,keyset_map)
    
    def add_client_details(self,identity_data,client_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        '''if role_from_token != CONSTANTS.PRODUCT_ADMIN:
            raise Custom_Error(CONSTANTS.ADD_CLIENT_ERR_MSG1)'''
        
        client_data_request = create_client_request(client_data) 
        client_data_request.parse_request()
        client_data_request.validate_request()
        client_data_obj = CLIENT_DETAILS(**client_data)  
        
        if client_data_obj.client_name == CONSTANTS.NEO_CV:
            raise Custom_Error('Cannot save product client')
        
        #client_data_obj.db_name = client_data_obj.client_name.replace(' ', '_')
                
        client_collection = db[self.client_details_collection]
        
        #fields_to_check = ["client_name", "api_url", "domain", "db_name"]
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
        query = {"_id": client_id}
        update_values = {"db_name": client_data_obj.db_name}
        update_db = Mongo_DB_Manager.update_document(client_collection, query, update_values)
           
        Thread(target=self.create_client_database, args=(client_data_obj.db_name, db)).start()
        
        if not client_id:
            raise Custom_Error(CONSTANTS.ADD_CLIENT_ERR_MSG2)
               
    def create_client_database(self, db_name, main_db):
        client_db = self.mongo_client[db_name]
        if db_name not in self.mongo_client.list_database_names():
            self.add_client_collection(client_db)
            self.copy_collection_list_dropdown(main_db, client_db)
        return client_db
               
    def add_client_collection(self,db):
        if self.config.client_collection_names:
            names = self.config.client_collection_names.split(',')
            for collection_name in names:
                if collection_name not in db.list_collection_names():
                        db.create_collection(collection_name)
        else:
            print("no collections available... pls configure in .env")
            
    def copy_collection_list_dropdown(self, main_db, client_db):
        original_collection = main_db['COLLECTION_LIST_DROPDOWN']
        dropdown_documents = list(original_collection.find())
        
        new_collection = client_db['COLLECTION_LIST_DROPDOWN']
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
        
        '''if role_from_token != CONSTANTS.PRODUCT_ADMIN:
            raise Custom_Error(CONSTANTS.ADD_CLIENT_ERR_MSG3)'''
        
        client_data_request = create_client_subscription_request(client_data) 
        client_data_request.parse_request()
        client_data_request.validate_request()
        
        client_data_obj = CLIENT_DETAILS(**client_data)
        
        client_collection = db[self.client_details_collection]    
        
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
            
        '''if role_from_token != CONSTANTS.PRODUCT_ADMIN:
            raise Custom_Error(CONSTANTS.UPDATE_CLIENT_ERR_MSG2)'''
                       
        client_data_request = update_client_request(client_data) 
        client_data_request.parse_request()
        client_data_request.validate_request()
        client_data_obj = CLIENT_DETAILS(**client_data)

        _id = DB_Utility.str_to_obj_id(client_data_obj._id)
        client_collection = db[self.client_details_collection]
        query = {"_id": _id}
        
        client_info = Mongo_DB_Manager.read_one_document(client_collection,query)
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
            
        query = {"_id": _id}
        result = Mongo_DB_Manager.update_document(client_collection, query, client_data_obj.__dict__)
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
                    
        if client_obj.client_name == CONSTANTS.NEO_CV:
            user_collection = self.mongo_client[self.config.neo_db][self.user_details_collection]
        else:
            db_name = client_obj.db_name
            user_collection = self.mongo_client[db_name][self.user_details_collection]
        return user_collection
            
    def delete_client_details(self,identity_data, client_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        '''if role_from_token != CONSTANTS.PRODUCT_ADMIN:
            raise Custom_Error(CONSTANTS.DELETE_CLIENT_ERR_MSG)'''
        
        client_data_request = delete_client_request(client_data) 
        client_data_request.parse_request()
        client_data_request.validate_request()
            
        client_data_obj = CLIENT_DETAILS(**client_data)
        _id =  client_data_obj._id 
        query = {"_id": DB_Utility.str_to_obj_id(client_data_obj._id)}
        
        client_collection = db[self.client_details_collection]
        existing_client = Mongo_DB_Manager.read_one_document(client_collection,query)
            
        if not existing_client:
            raise Custom_Error(CONSTANTS.CLIENT_NOT_FOUND)

        if existing_client.get(CONSTANTS.IS_DELETED) == CONSTANTS.TRUE:
            raise Custom_Error(CONSTANTS.CLIENT_ALREADY_DELETED)
                       
        client_data_obj.updated_on,client_data_obj.updated_by,client_data_obj.is_deleted = Utility.settings_for_data_operation(email_from_token, CONSTANTS.DELETE,is_delete=True)
        #attributes_to_delete = ["client_name","api_url","domain","status","client_address","contact_person","phone","email","created_by","created_on","_id","db_name","subscription_details","client_id"]
        attributes_to_delete = ["client_name","api_url","domain","status","client_address","contact_person","phone","email","created_by","created_on","_id","db_name","subscription_details"]

        client_data_obj = DB_Utility.delete_attributes_from_obj(client_data_obj,attributes_to_delete)
            
        result = Mongo_DB_Manager.update_document(client_collection, query, client_data_obj.__dict__)
        if result != 1:
            raise Custom_Error(CONSTANTS.COULD_NOT_DELETE_CLIENT)
        
        query = {"client_id": _id, **Utility.get_delete_false_query()}
        
        existing_client_obj = CLIENT_DETAILS(**existing_client)
        
        user_collection = db[self.user_details_collection]
        
        client_user = Mongo_DB_Manager.update_documents(user_collection,query,client_data_obj.__dict__)
        
              
    def get_client_details(self,identity_data,request_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        role_from_token = identity_data_obj.role 
            
        '''if role_from_token != CONSTANTS.PRODUCT_ADMIN:
            raise Custom_Error(CONSTANTS.GET_CLIENT_ERR_MSG)'''
        
        
        self.common_service.create_log_details(identity_data_obj.email,request_data,"get_client_details",db)
        
        client_id = request_data.get('_id')
        if client_id:
            filter_by = {"_id": [DB_Utility.str_to_obj_id(client_id)]} 
            if 'filter_by' not in request_data:            
                request_data = {"filter_by" :[filter_by]}
            else:
                request_data["filter_by"].append(filter_by)
        request_data.pop('_id', None)
        
        pagination = Pagination(**request_data)        
         
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[self.client_details_collection],query,pagination)
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
            #count = Mongo_DB_Manager.count_documents(db[self.client_details_collection],query)
            if pagination.is_download==True:
                return docs,count
            return new_docs,count
                       
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)  
    
    '''
    def upload_client_data(self,request, identity_data):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        if role_from_token != CONSTANTS.PRODUCT_ADMIN:
            raise Custom_Error(CONSTANTS.ADD_CLIENT_ERR_MSG1)
        
        client_data_request = upload_client_request(request) 
        client_data_request.parse_request()
        client_data_request.validate_request()
        
        documents,column_names = DB_Utility.read_excel(request)   
        if column_names:
            missing_keys = [key for key in CONSTANTS.CLIENT_KEYS if key not in column_names]
            if missing_keys:
                raise Custom_Error(CONSTANTS.EXCEL_NOT_VALID)
      
        result_dict = {}
        for index, doc in enumerate(documents):
                              
            if not isinstance(doc, CLIENT_DETAILS):
                result_dict[index] = "Skipping non-CLIENT_DETAILS entry" 
                continue
                
            doc = CLIENT_DETAILS(**doc) 
            
            if DB_Utility.check_null_value_or_invalid_status_from_excel(doc, index, result_dict,CONSTANTS.CLIENT_KEYS):
                continue
            
            if doc.email is not None:
                if DB_Utility.is_valid_email(doc.email,doc,result_dict,index):
                    continue  
                
            if doc.phone is not None:        
                if DB_Utility.is_valid_mobile(doc.phone,doc,result_dict,index):
                    continue
                                                                 
            if self.check_validation_in_client_collection(doc,index, result_dict,self.client_details_collection):
                continue
                                
            doc_id = doc._id
            if not doc_id:
                doc.status = "active"
                add_attributes_to_delete = ["_id","updated_on","updated_by"]
                attributes_to_delete_update=None
                
            else:
                attributes_to_delete_update = ["created_by","created_on","_id","db_name"]
                if doc.subscription_details is None:
                    del doc.subscription_details
                add_attributes_to_delete = None
                
            doc.is_deleted = False
            result_dict[index] = Mongo_DB_Manager.insert_or_update_obj(self.client_details_collection,doc, email_from_token,attributes_to_delete_update,add_attributes_to_delete)
        
        json_result_dict = json.loads(json.dumps(result_dict, default=DB_Utility.custom_encoder, indent=4)) 
        response_data = {"data": json_result_dict, "count": len(result_dict)}
        return response_data
          
    def convert_subcription_details(self,doc):
        subscription_details = doc.subscription_details
        if isinstance(subscription_details, str):
            subscription_details_json = subscription_details.replace("'", '"')
            subscription_details_array = json.loads(subscription_details_json)
            doc.subscription_details = subscription_details_array  
            return doc
         
    def check_validation_in_client_collection(self,doc,index, result_dict,collection):
        
        doc_dict = {key: getattr(doc, key) for key in doc.__dict__ if key not in CONSTANTS.KEYS_TO_REMOVE}

        if doc.subscription_details is not None:
            doc = self.convert_subcription_details(doc)    

        if doc._id is not None: 
            _id = DB_Utility.str_to_obj_id(doc._id)    
            query = {
                        "$or": [
                            {"_id": _id},
                            {"$and": [
                                {"$or": [{"api_url": doc.api_url},{"domain": doc.domain},{"client_name": doc.client_name}]
                                },
                                {"_id": {"$ne": _id}}
                            ]}                          
                        ]
                    }
            
            cursor = Mongo_DB_Manager.read_documents(self.client_details_collection,query)
            existing_clients = list(cursor)
            
            if _id not in [client['_id'] for client in existing_clients]:
                status = f'The id {doc._id} does not exist'
                DB_Utility.update_status(doc, result_dict, index,status)                            
                return True
            for existing_client in list(existing_clients):
                existing_client_obj = CLIENT_DETAILS(**existing_client)    
                if existing_client_obj._id != _id:
                    status = f'client_name or api_url or domain already exists for the _id {existing_client_obj._id}'
                    DB_Utility.update_status(doc, result_dict, index,status)                            
                    return True
                subscription_details = doc.subscription_details
                subscription_details_db = []
                if subscription_details is not None:
                    #doc = self.convert_subcription_details(subscription_details,doc)    
                    #check for overlapping subscriptions    
                    existing_subscriptions = existing_client_obj.subscription_details
                    if existing_subscriptions is not None: 
                        for subscription in doc.subscription_details:
                            start_date=subscription['start_date']
                            end_date=subscription['end_date']
                            for existing_subscription in existing_subscriptions:
                                existing_start_date = existing_subscription['start_date']
                                existing_end_date = existing_subscription['end_date']
                                if existing_start_date < end_date and existing_end_date > start_date:
                                    status = f'{CONSTANTS.OVERLAPPING_SUBSCRIPTION_ERR_MSG} for the index {index}'
                                    DB_Utility.update_status(doc, result_dict, index,status)
                                    return True
                            subscription_details_db=existing_subscriptions                 
                            subscription_details_db.append(subscription)                       
                            doc.subscription_details=subscription_details_db
                        
            result_dict = {key: value for key, value in result_dict.items() if key not in CONSTANTS.KEYS_TO_REMOVE}
                                          
            if result_dict == doc_dict:
                status = f'No update required for the {doc._id}'
                DB_Utility.update_status(doc,result_dict, index,status)
                return True                           
        else:
            fields_to_check = ["client_name", "api_url", "domain", "db_name"]
            query = DB_Utility.fields_to_check(doc,fields_to_check)
            existing_client = Mongo_DB_Manager.read_one_document(self.client_details_collection, query)
            if existing_client:
                field = next((field for field in fields_to_check if getattr(doc, field) == existing_client[field]), None)
                if field:
                    status = f'{field} already exists in the db for _id {existing_client["_id"]},incase of  update,the id {existing_client["_id"]} is missing in the request '
                    DB_Utility.update_status(doc, result_dict, index,status)
                return True   
        return False
    
        
    def update_client_settings(self,settings,identity_data):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        client_id = identity_data_obj.client_id
        if role_from_token != CONSTANTS.PRODUCT_ADMIN:
            raise Custom_Error(CONSTANTS.UPDATE_CLIENT_ERR_MSG2)
        
        
        client_data_request = update_settings_request(settings) 
        client_data_request.parse_request()
        client_data_request.validate_request()
        
        settings_obj = CLIENT_DETAILS(**settings)
        
        query = {"_id": DB_Utility.str_to_obj_id(client_id)}
        existing_client = Mongo_DB_Manager.read_one_document(self.client_details_collection,query)
        if not existing_client:
            raise Custom_Error(CONSTANTS.CLIENT_NOT_FOUND)
        
        settings_obj.updated_on, settings_obj.updated_by = Utility.settings_for_data_operation(email_from_token)
   
        attributes_to_delete = ["client_name","api_url","domain","db_name","status","is_deleted","client_address","contact_person","phone","email","created_by","created_on","subscription_details","_id"]
        [delattr(settings_obj, attr) for attr in attributes_to_delete]
        
        result = Mongo_DB_Manager.update_document(self.client_details_collection,query,settings_obj.__dict__)
        if result == 0:
            raise Custom_Error(CONSTANTS.UPDATE_CLIENT_ERR_MSG1)
        
        #settings_info = {
        #        'page': settings.get('page',None),
        #        'widget': settings.get('widget',None),
        #        'menu': settings.get('menu',None)
        #}
    '''               
        