from datetime import datetime
from NeoAdept.pojo.common.directory import DIRECTORY
from NeoAdept.services.common_service import Common_Service
import bcrypt

from pymongo import MongoClient
from flask import json, Config
from ..gbo.bo import Pagination
from ..gbo.common import Custom_Error
from ..pojo.user.user_details import USER_DETAILS
from ..pojo.client.client_details import CLIENT_DETAILS
from ..pojo.user.access_token import ACCESS_TOKEN
from ..requests.user_request import create_user_request,update_user_request,delete_user_request,upload_user_request
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager

class User_Service:  
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
            self.user_details_collection = "USER_DETAILS"
            self.attachment_details = "ATTACHMENT_DETAILS"
            self.mongo_client = MongoClient(self.config.db_url,maxPoolSize=self.config.max_pool_size)
            self.client_collection = self.mongo_client[self.config.neo_db]["CLIENT_DETAILS"]
            self.key_nested_key_map = keyset_map
            if "USER_DETAILS" in keyset_map:
                self.key_map = self.key_nested_key_map["USER_DETAILS"]
            self.directory = DIRECTORY()
            self.common_service = Common_Service(logger,db,keyset_map)
        
    def get_user_collection_by_client_id(self,client_id):
        query =  {"_id": DB_Utility.str_to_obj_id(client_id)}
        
        user_client = Mongo_DB_Manager.read_one_document(self.client_collection,query)
        user_client_obj = CLIENT_DETAILS(**user_client)
           
        if user_client_obj.client_name == CONSTANTS.NEO_CV:
            user_collection = self.mongo_client[self.config.neo_db][self.user_details_collection]
        else:
            db_name = user_client_obj.db_name
            user_collection = self.mongo_client[db_name][self.user_details_collection]
        return user_collection
    
    def get_client_name_by_id(self, client_id):
        
        query =  {"_id": DB_Utility.str_to_obj_id(client_id)}
        client = Mongo_DB_Manager.read_one_document(self.client_collection,query)
        client_obj = CLIENT_DETAILS(**client)
        return client_obj.client_name if client_obj.client_name else None
        
    def add_new_user(self,identity_data,user_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        user_data_obj = USER_DETAILS(**user_data)           
        
        user_data_request = create_user_request(user_data) 
        user_data_request.parse_request()
        user_data_request.validate_request()
        
        if role_from_token == CONSTANTS.USER:
            raise Custom_Error(CONSTANTS.ADD_USER_ERR2)
        
        client_db_name = identity_data_obj.client_db_name
        
        if role_from_token == CONSTANTS.PRODUCT_USER:
            if user_data_obj.role != CONSTANTS.USER or client_db_name is None or client_db_name == self.config.neo_db:
                raise Custom_Error('product-user can add client-users only')
            
        if role_from_token == CONSTANTS.ADMIN:
            
            if user_data_obj.role != CONSTANTS.USER:
                raise Custom_Error(CONSTANTS.ADD_USER_ERR1)
            
        user_collection = db[self.user_details_collection]              
        
        query =  {"email": user_data_obj.email,**Utility.get_delete_false_query()}
        existing_user = Mongo_DB_Manager.read_one_document(user_collection, query)
        if existing_user :
            raise Custom_Error(CONSTANTS.USER_EXISTS)
                           
        random_password = Utility.generate_random_password()  
        user_data_obj.password = bcrypt.hashpw(random_password.encode('utf-8'), bcrypt.gensalt())
        user_data_obj.created_on = Utility.get_current_time()
        user_data_obj.created_by = email_from_token
        user_data_obj.is_deleted = False
        user_data_obj.notes = random_password
        
        if not hasattr(user_data_obj, 'client_id') or getattr(user_data_obj, 'client_id') is None:
            user_data_obj.client_id = identity_data_obj.client_id
            if client_db_name is not None and client_db_name != self.config.neo_db:
                user_data_obj.client_id = self.get_client_id_by_db(identity_data_obj.client_db_name)
                       
        attributes_to_delete = ["token","updated_by","updated_on","_id","entity_id","otp","otp_timestamp","client_name","db_name","new_password","current_password","client_domain","photo_file_name","photo","visibility"]
        if not hasattr(user_data_obj, 'photo_id') or getattr(user_data_obj, 'photo_id') is None:
            attributes_to_delete.append('photo_id')
        DB_Utility.delete_attributes_from_obj(user_data_obj,attributes_to_delete)
        
        _id = Mongo_DB_Manager.create_document(user_collection,user_data_obj.__dict__)
        if not _id:
            raise Custom_Error('Could not add user')
        
        self.common_service.add_user_permission_for_user(DB_Utility.obj_id_to_str(_id),email_from_token,db)
    
    def get_client_id_by_db(self, client_db_name):
        query = {"db_name": client_db_name, **Utility.get_delete_false_query()}
        client_info = Mongo_DB_Manager.read_one_document(self.client_collection,query)
        if not client_info:
            raise Custom_Error('domain not mapped')
        client_id = DB_Utility.obj_id_to_str(client_info['_id'])
        return client_id
        
    def update_user_details(self,identity_data, user_data, db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        user_data_obj = USER_DETAILS(**user_data)
               
        user_data_request = update_user_request(user_data) 
        user_data_request.parse_request()
        user_data_request.validate_request()
        
        if role_from_token == CONSTANTS.USER:
            raise Custom_Error(CONSTANTS.UPDATE_USER_ERR2)
        
        client_db_name = identity_data_obj.client_db_name
        if role_from_token == CONSTANTS.PRODUCT_USER:
            if user_data_obj.role != CONSTANTS.USER or client_db_name == self.config.neo_db or client_db_name is None:
                raise Custom_Error('product-user can update client viewer only')
        
        if role_from_token == CONSTANTS.ADMIN:
            
            if user_data_obj.role != CONSTANTS.USER:
                raise Custom_Error(CONSTANTS.UPDATE_USER_ERR1)
            
        user_collection = db[self.user_details_collection]   
        
        _id = DB_Utility.str_to_obj_id(user_data_obj._id)
        list_of_keys = ["email","phone"]
        query1 = DB_Utility.update_keys_check(user_data_obj,list_of_keys,_id)
        
        cursor = Mongo_DB_Manager.read_documents(user_collection,query1)
        existing_users = list(cursor)
        
        if _id not in [user['_id'] for user in existing_users]:
            raise Custom_Error(CONSTANTS.USER_NOT_FOUND)                                  
        
        for existing_user in list(existing_users):
            existing_user_obj = USER_DETAILS(**existing_user)    
            if existing_user_obj._id != _id:
                raise Custom_Error(f'email or phone already exists for the _id {existing_user_obj._id}')
                
        query =  {"_id": _id}             
        
        DB_Utility.remove_extra_attributes(user_data_obj.__dict__,user_data)
        del user_data_obj._id 
                
        user_data_obj.updated_on = Utility.get_current_time()
        user_data_obj.updated_by = email_from_token
        
        result = Mongo_DB_Manager.update_document(user_collection, query, user_data_obj.__dict__)
        if result == 0:
            raise Custom_Error('Could not update user') 
            
    def delete_user(self,identity_data,user_data,db):

        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)

        user_data_request = delete_user_request(user_data) 
        user_data_request.parse_request()
        user_data_request.validate_request()
            
        user_data_obj = USER_DETAILS(**user_data)
        
        query = {"_id": DB_Utility.str_to_obj_id(user_data_obj._id)}
        
        if role_from_token == CONSTANTS.USER:
            raise Custom_Error(CONSTANTS.DELETE_USER_ERR)  
            
        user_collection = db[self.user_details_collection]
            
        existing_user = Mongo_DB_Manager.read_one_document(user_collection,query)
        
        if not existing_user:
            raise Custom_Error(CONSTANTS.USER_NOT_FOUND)
        
        existing_user_obj = USER_DETAILS(**existing_user)
        if existing_user_obj.is_deleted == CONSTANTS.TRUE:
            raise Custom_Error(CONSTANTS.USER_ALREADY_DELETED)
                       
        user_data_obj.updated_on = Utility.get_current_time()
        user_data_obj.updated_by = email_from_token
        user_data_obj.is_deleted = True
                    
        if role_from_token == CONSTANTS.ADMIN:
            
            if existing_user_obj.role != CONSTANTS.USER:
                raise Custom_Error(CONSTANTS.DELETE_USER_ERR2)

        else:
            if role_from_token == CONSTANTS.PRODUCT_USER:
                client_db_name = identity_data_obj.client_db_name
                if existing_user_obj.role != CONSTANTS.USER or client_db_name is None or client_db_name == self.config.neo_db:
                    raise Custom_Error('product-user can delete client-users only')
                                             
        _id = user_data_obj._id
        
        query = {"_id":DB_Utility.str_to_obj_id(_id)}
                
        attributes_to_delete = ["_id","name","phone","email","password","role","status","client_id","token","notes","created_on","created_by","entity_id","otp","otp_timestamp","client_name","db_name","new_password","current_password","client_domain","photo_id","photo_file_name","photo","visibility"]
        DB_Utility.delete_attributes_from_obj(user_data_obj,attributes_to_delete)   
                  
        result = Mongo_DB_Manager.update_document(user_collection, query, user_data_obj.__dict__)
        if result != 1:
            raise Custom_Error('Could not delete user')
        self.delete_user_permissions_for_user(_id,db)
                
    def get_user_list(self,identity_data,request_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        pagination = Pagination(**request_data) 
        self.common_service.create_log_details(email_from_token,request_data,"get_user_list",db)
        client_db_name = identity_data_obj.client_db_name
        user_collection = db[self.user_details_collection]
        if role_from_token == CONSTANTS.USER or (role_from_token == CONSTANTS.PRODUCT_USER and (client_db_name is None or client_db_name == self.config.neo_db)):
            result = self.get_user_by_email(user_collection,email_from_token,db)
            if result:
                docs,count = result,1
                return docs,count  
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
        
        if role_from_token == CONSTANTS.ADMIN or role_from_token == CONSTANTS.PRODUCT_USER:
            if pagination.filter_by is None:
                filter_by = [{"role": ["viewer"]}]   
            else:
                filter_by = pagination.filter_by.copy()
                filter_by.append({"role": ["viewer"]})
            pagination.filter_by = filter_by
            
        return self.get_all_users(pagination,user_collection,db)   

    def get_all_users(self,pagination,user_collection,db):
        
        query = DB_Utility.frame_get_query(pagination,self.key_map)         
        docs,count = Mongo_DB_Manager.get_paginated_data1(user_collection,query,pagination) 
        
        new_docs=[]
        for doc in docs:
            if 'created_on' in doc and isinstance(doc['created_on'], datetime):
                doc['created_on'] = doc['created_on'].strftime("%Y-%m-%d %H:%M:%S.%f")
            if 'updated_on' in doc and isinstance(doc['updated_on'], datetime):
                doc['updated_on'] = doc['updated_on'].strftime("%Y-%m-%d %H:%M:%S.%f")

            docs_obj = USER_DETAILS(**doc)
            docs_obj._id = DB_Utility.obj_id_to_str(docs_obj._id)
                        
            attributes_to_delete = ['notes', 'password', 'otp', 'otp_timestamp', 'token', 'entity_id', 'new_password', 'current_password',"db_name",'client_domain','client_name']                
            DB_Utility.delete_attributes_from_obj(docs_obj,attributes_to_delete)
            new_docs.append(docs_obj.__dict__)
                
                
        if new_docs and len(new_docs)>0:
            Mongo_DB_Manager.attachment_details(db["ATTACHMENT_DETAILS"],new_docs,["photo"])
            #count = Mongo_DB_Manager.count_documents(user_collection,query)
            if pagination.is_download==True:
                return new_docs,count
            return new_docs,count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
    
    def get_user_by_email(self,collection, email,db):
        
        user_document = collection.find_one({"email": email},{"password": 0, "token": 0, 'otp':0, 'otp_timestamp':0})
        if not user_document:
            return None
        user_document['_id'] = DB_Utility.obj_id_to_str(user_document['_id'])
        if 'created_on' in user_document and isinstance(user_document['created_on'], datetime):
            user_document['created_on'] = user_document['created_on'].strftime("%Y-%m-%d %H:%M:%S.%f")
        if 'updated_on' in user_document and isinstance(user_document['updated_on'], datetime):
            user_document['updated_on'] = user_document['updated_on'].strftime("%Y-%m-%d %H:%M:%S.%f")

        Mongo_DB_Manager.attachment_details(db["ATTACHMENT_DETAILS"],[user_document],["photo"])     
        
        return user_document
        
    def upload_user_data(self, request, identity_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        if role_from_token == CONSTANTS.USER:
            raise Custom_Error('User cannot add/update new users')
                   
        user_data_request = upload_user_request(request) 
        user_data_request.parse_request()
        user_data_request.validate_request()
        
        documents,column_names=DB_Utility.read_excel(request)   
        if column_names:
            missing_keys = [key for key in CONSTANTS.USER_KEYS if key not in column_names]
            if missing_keys:
                raise Custom_Error(CONSTANTS.EXCEL_NOT_VALID)
           
                     
        result_dict = {}
        for index, doc in enumerate(documents):
            
            #print("index::",index)
            #print("DOC::",doc)
            
            doc = USER_DETAILS(**doc)
            
            if self.check_required_field_for_user(CONSTANTS.REGISTER_USER_REQ_FIELDS_ADMIN ,doc,index, result_dict):
                continue
            
            client_db_name = identity_data_obj.client_db_name
            if role_from_token == CONSTANTS.PRODUCT_USER:
                if doc.role != CONSTANTS.USER or client_db_name is None or client_db_name == self.config.neo_db:
                    status = f'product-user can save client-users only'
                    DB_Utility.update_status(doc, result_dict, index,status)      
                    continue                
                
            if role_from_token == CONSTANTS.ADMIN: 
                
                if doc.client_id:
                    if doc.client_id != identity_data_obj.client_id:
                        status=f"Cannot add/edit user from another client for the index {index}"
                        DB_Utility.update_status(doc, result_dict, index,status)      
                        continue   
                             
                if doc.role != CONSTANTS.USER:
                    status = f"Admin can add/update users only for the index {index}"
                    DB_Utility.update_status(doc, result_dict, index,status)      
                    continue
                
            if not hasattr(doc, 'client_id') or getattr(doc, 'client_id') is None:
                doc.client_id= identity_data_obj.client_id
                if client_db_name is not None and client_db_name != self.config.neo_db:
                    doc.client_id = self.get_client_id_by_db(identity_data_obj.client_db_name)
                    
            user_collection = db[self.user_details_collection]
                        
            if self.check_duplicate_value_for_user(doc,index, result_dict,user_collection):
                continue                
                
            if not doc._id:
                doc.is_deleted = False
                random_password = Utility.generate_random_password()  
                doc.password = bcrypt.hashpw(random_password.encode('utf-8'), bcrypt.gensalt())
                doc.created_on = Utility.get_current_time()
                doc.created_by = email_from_token
                doc.notes = random_password 
                attributes_to_delete_update = None
                add_attributes_to_delete = ["token","updated_by","updated_on","_id","entity_id","otp","otp_timestamp","client_name","db_name","new_password","current_password","client_domain","visibility"]          
                if not hasattr(doc, 'photo_id') or getattr(doc, 'photo_id') is None:
                    add_attributes_to_delete.append('photo_id')
            else:
                doc.updated_by = email_from_token
                doc.updated_on = Utility.get_current_time()
                attributes_to_delete_update = ["created_by","created_on","_id","entity_id","otp","otp_timestamp","client_name","db_name","new_password","current_password","password","role","client_id","token","notes","is_deleted","client_domain","visibility"]  
                if not hasattr(doc, 'photo_id') or getattr(doc, 'photo_id') is None:
                    attributes_to_delete_update.append('photo_id')
                add_attributes_to_delete = None
                                            
            result_dict[index] = self.insert_or_update_obj(doc,result_dict,attributes_to_delete_update,add_attributes_to_delete,user_collection)
        
        json_result_dict = json.loads(json.dumps(result_dict, default=DB_Utility.custom_encoder, indent=4)) 
        response_data = {"data": json_result_dict, "count": len(result_dict)}
        return response_data 
    
    def insert_or_update_obj(self, doc, result_dict,attributes_to_delete_update=None,add_attributes_to_delete=None,user_collection=None):
        
        doc_id = doc._id        
        if doc_id:
            _id = DB_Utility.str_to_obj_id(doc._id)
            query = {'_id': _id}            
            DB_Utility.delete_attributes_from_obj(doc,attributes_to_delete_update) 
            result = Mongo_DB_Manager.update_document(user_collection, query, doc.__dict__)
            
        else:
            DB_Utility.delete_attributes_from_obj(doc,add_attributes_to_delete)
            result = Mongo_DB_Manager.create_document(user_collection,doc.__dict__)
                
        if result != 0:
            status_code = "200"
            if doc_id:
                status = f'Updated successfully for the _id {doc_id}'
            else:
                status = f'Inserted successfully and generated _id is {result}'
        else:
            status_code = "500"
            status = "Failed to insert or update object"

        doc.status_code = status_code
        doc.status = status
        return doc
              
    def check_required_field_for_user(self,fields,doc,index, result_dict):
                                 
        for field in fields :
            if not hasattr(doc, field):
                status=f"Missing required field: {field} for the index {index}"
                DB_Utility.update_status(doc,result_dict, index,status)                
                return True
                              
    def check_duplicate_value_for_user(self,doc,index, result_dict,collection):
        doc_dict = {key: getattr(doc, key) for key in doc.__dict__ if key not in CONSTANTS.KEYS_TO_REMOVE}
        if doc._id is not None:
            _id = DB_Utility.str_to_obj_id(doc._id)
            list_of_keys = ["email","phone"]
            query = DB_Utility.update_keys_check(doc,list_of_keys,_id)
            
            cursor = Mongo_DB_Manager.read_documents(collection,query)
            existing_users = list(cursor)
           
            if _id not in [user['_id'] for user in existing_users]:
                status = f'The id {doc._id} does not exist'
                DB_Utility.update_status(doc, result_dict, index,status)                            
                return True
            else:
                for existing_user in list(existing_users):
                    existing_user_obj = USER_DETAILS(**existing_user)    
                    if existing_user_obj._id != _id:
                        status = f'email or phone already exists for the _id {existing_user_obj._id}'
                        DB_Utility.update_status(doc, result_dict, index,status)                            
                        return True
            result_dict = {key: value for key, value in result_dict.items() if key not in CONSTANTS.KEYS_TO_REMOVE}
                                          
            if result_dict == doc_dict:
                status=f'No update required for the {doc._id}'
                DB_Utility.update_status(doc,result_dict, index,status)
                return True  
        
        else:
            fields_to_check = ["email", "phone"]
            query = {"$or": [{field: getattr(doc, field)} for field in fields_to_check], "is_deleted": False}
            existing_user = Mongo_DB_Manager.read_one_document(collection, query)
            if existing_user:
                field = next((field for field in fields_to_check if getattr(doc, field) == existing_user[field]), None)
                if field:
                    status = f'{field} already exists in the db for _id {existing_user["_id"]},incase of  update,the _id {existing_user["_id"]} is missing in the request '
                    DB_Utility.update_status(doc, result_dict, index,status)
                return True  
            
        return False
        
    def delete_user_permissions_for_user(self,user_id,db):
        query = {"user_id": user_id}
        deleted_count = Mongo_DB_Manager.delete_documents(db['USER_PERMISSION'],query) 
        return True
              
                  
        