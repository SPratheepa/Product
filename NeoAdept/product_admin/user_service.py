import bcrypt

from flask import json
from bson import ObjectId

from ..gbo.bo import Pagination
from ..pojo.access_token import ACCESS_TOKEN
from ..requests.user_request import create_user_request,update_user_request,delete_user_request
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..gbo.common import Custom_Error
from ..pojo.user_details import USER_DETAILS
from ..pojo.client_details import CLIENT_DETAILS

class User_Service:  
    def __init__(self,config,logger,db,client_db,keyset_map):
        self.logger = logger
        self.config = config
        self.db = db
        self.client_db = client_db
        self.user_details_collection = self.db.get_collection("USER_DETAILS")
        self.client_details_collection = self.db.get_collection("CLIENT_DETAILS")    
        self.key_nested_key_map = keyset_map
        
           
    def add_new_user(self,identity_data,user_data):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        user_data_obj = USER_DETAILS(**user_data)           
        
        user_data_request = create_user_request(user_data) 
        user_data_request.parse_request()
        user_data_request.validate_request()
        
                
        if role_from_token == CONSTANTS.PRODUCT_ADMIN:
            return self.add_user_data(user_data_obj,email_from_token)
        
        elif role_from_token == CONSTANTS.ADMIN:
            
            if user_data_obj.role == CONSTANTS.USER:
                user_data_obj.client_id == identity_data_obj.client_id
                return self.add_user_data(user_data_obj,email_from_token)
            
            else:
                raise Custom_Error(CONSTANTS.ADD_USER_ERR1)
            
        else:
            
            client_name = self.get_client_name_by_id(identity_data_obj.client_id)
            
            if client_name == 'NeoCV':
                if user_data_obj.role == CONSTANTS.USER and user_data_obj.client_id!=identity_data_obj.client_id:
                    return self.add_user_data(user_data_obj,email_from_token)
                else:
                    raise Custom_Error('product-user can add client-users only')
            else:
                raise Custom_Error(CONSTANTS.ADD_USER_ERR2)
            
                
    def add_user_data(self,user_data_obj,email_from_token):
        
        query =  {"email": user_data_obj.email,**Utility.get_delete_false_query()}
        existing_user = Mongo_DB_Manager.read_one_document(self.user_details_collection, query)
        if existing_user :
            raise Custom_Error(CONSTANTS.USER_EXISTS)
                           
        random_password = Utility.generate_random_password()  
        user_data_obj.password = bcrypt.hashpw(random_password.encode('utf-8'), bcrypt.gensalt())
        user_data_obj.created_on,user_data_obj.created_by,user_data_obj.status = Utility.settings_for_data_operation(email_from_token, CONSTANTS.ADD)
        user_data_obj.is_deleted = False
        user_data_obj.notes = random_password
        
        attributes_to_delete = ["token","updated_by","updated_on","_id","entity_id","otp","otp_timestamp","client_name","db_name","new_password","current_password"]
        [delattr(user_data_obj, attr) for attr in attributes_to_delete]
        
        user_id = Mongo_DB_Manager.create_document(self.user_details_collection,user_data_obj.__dict__)
        if not user_id:
            raise Custom_Error('Could not add user')
        
    def get_client_name_by_id(self, client_id):
        
        query =  {"_id": DB_Utility.str_to_obj_id(client_id)}
        client = Mongo_DB_Manager.read_one_document(self.client_details_collection,query)
        client_obj = CLIENT_DETAILS(**client)
        return client_obj.client_name if client_obj.client_name else None
        
    def update_user_details(self,identity_data, user_data):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        user_data_obj = USER_DETAILS(**user_data)
               
        user_data_request = update_user_request(user_data) 
        user_data_request.parse_request()
        user_data_request.validate_request()
              
        query =  {"_id": DB_Utility.str_to_obj_id(user_data_obj._id)}
        existing_user = Mongo_DB_Manager.read_one_document(self.user_details_collection, query)
        if not existing_user:
            raise Custom_Error(CONSTANTS.USER_NOT_FOUND)
        
        user_data_obj.updated_on,user_data_obj.updated_by = Utility.settings_for_data_operation(email_from_token, CONSTANTS.UPDATE)
        
        if role_from_token == CONSTANTS.PRODUCT_ADMIN:
            self.edit_user(user_data_obj, query)
            
        elif role_from_token == CONSTANTS.ADMIN:
            
            if user_data_obj.role != CONSTANTS.USER:
                raise Custom_Error(CONSTANTS.UPDATE_USER_ERR1)
            
            if user_data_obj.client_id:
                if user_data_obj.client_id != identity_data_obj.client_id:
                    raise Custom_Error(CONSTANTS.UPDATE_USER_ERR)
            else:
                user_data_obj.client_id = identity_data_obj.client_id
            
            self.edit_user(user_data_obj, query)    
        
        else:
            client_name = self.get_client_name_by_id(identity_data_obj.client_id)
            if client_name != 'NeoCV':
                raise Custom_Error(CONSTANTS.UPDATE_USER_ERR2)
            
            if user_data_obj.role == CONSTANTS.USER and user_data_obj.client_id!=identity_data_obj.client_id:
                self.edit_user(user_data_obj, query)
            else:
                raise Custom_Error('product-user can update client-users only')
            
     
    def edit_user(self, user_data_obj, query):
        attributes_to_delete = ["created_by","created_on","_id","entity_id","otp","otp_timestamp","client_name","db_name","new_password","current_password","password","role","client_id","token","notes","is_deleted"]                                                                                                                
        [delattr(user_data_obj, attr) for attr in attributes_to_delete]
        result = Mongo_DB_Manager.update_document(self.user_details_collection, query, user_data_obj.__dict__)
        if result == 0:
            raise Custom_Error('Could not update user')

    def delete_user(self,identity_data,user_data):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        if role_from_token != CONSTANTS.PRODUCT_ADMIN:
            raise Custom_Error(CONSTANTS.DELETE_USER_ERR)
        
        user_data_request = delete_user_request(user_data) 
        user_data_request.parse_request()
        user_data_request.validate_request()
            
        user_data_obj = USER_DETAILS(**user_data)
        
        query = {"_id": DB_Utility.str_to_obj_id(user_data_obj._id)}
        existing_user = Mongo_DB_Manager.read_one_document(self.user_details_collection,query)
            
        if not existing_user:
            raise Custom_Error(CONSTANTS.USER_NOT_FOUND)
        if existing_user.get(CONSTANTS.IS_DELETED) == CONSTANTS.TRUE:
            raise Custom_Error(CONSTANTS.USER_ALREADY_DELETED)
        
               
        user_data_obj.updated_on,user_data_obj.updated_by,user_data_obj.is_deleted = Utility.settings_for_data_operation(email_from_token, CONSTANTS.DELETE,is_delete=True)

        attributes_to_delete = ["_id","name","phone","email","password","role","status","client_id","token","notes","created_on","created_by","entity_id","otp","otp_timestamp","client_name","db_name","new_password","current_password"]
        [delattr(user_data_obj, attr) for attr in attributes_to_delete]  
            
        result=Mongo_DB_Manager.update_document(self.user_details_collection, query, user_data_obj.__dict__)
        if result != 1:
            raise Custom_Error('Could not delete user')
                             
        
    def get_user_list(self,identity_data,request_data):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token, user_id_from_token = Utility.get_data_from_identity(identity_data_obj,include_user_id=True)
            
        if role_from_token == 'product-user':
            result = self.get_user_by_id(user_id_from_token)
            if result:
                docs,count = result,1
                return DB_Utility.convert_doc_to_cls_obj(docs,USER_DETAILS),count
                
        pagination = Pagination(**request_data) 
        
        query = {}
        
        if pagination.filter_by is None:
            updated_filter_by = [{CONSTANTS.IS_DELETED: [False]}]
        else:
            updated_filter_by = pagination.filter_by.copy()
            updated_filter_by.append({CONSTANTS.IS_DELETED: [False]})
            
        query = DB_Utility.build_filtered_data_query(updated_filter_by)
                    
        docs = Mongo_DB_Manager.get_paginated_data(self.user_details_collection,query,pagination) 
        
        new_docs=[]
        for doc in docs:
            docs_obj = USER_DETAILS(**doc)
            docs_obj._id = DB_Utility.obj_id_to_str(docs_obj._id)
            attributes_to_delete = ['notes', 'password', 'otp', 'otp_timestamp', 'token', 'entity_id', 'new_password', 'current_password','client_name']
            [delattr(docs_obj, attr) for attr in attributes_to_delete]
            new_docs.append(docs_obj.__dict__)

        if pagination.is_download == True:
            return DB_Utility.get_data_in_excel(new_docs,"user_details")

        
        if new_docs and len(new_docs)>0:
            count = Mongo_DB_Manager.count_documents(self.user_details_collection,query)
            return new_docs,count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
   
        
    def upload_users(self,json_documents,identity_data):
       try:          
           result_dict = {}
           for index, doc in enumerate(json_documents):
                try:
                    print("index::",index)
                    if not isinstance(doc, dict):
                        result_dict[index] = "Skipping non-dictionary entry"
                        continue                    
                    role = doc.get(CONSTANTS.ROLE)
                    email_from_token,role_from_token = Utility.get_data_from_identity(identity_data)
                    if role_from_token == CONSTANTS.PRODUCT_ADMIN:
                        if self.check_required_field_for_user(CONSTANTS.REGISTER_USER_REQ_FIELDS,doc,index,result_dict):
                            print("index in required_field_for_produtct-admin ::",index)
                            continue
                        email=doc.get("email")
                        validate_data={"email":email,"is_deleted":False}
                        if self.check_duplicate_value_for_user(validate_data,doc,index, result_dict,self.user_login_collection):
                            print("index in checkid ::",index)# check is_deleted = false
                            continue
                        doc_id=doc.get("id")
                        if doc_id:
                            if mongodb.check_id(doc,index, result_dict,self.client_collection):
                                print("index in checkid ::",index)
                                continue                        
                        else:
                            doc= self.insert_user(doc)
                            
                    elif role_from_token==CONSTANTS.ADMIN:
                         if self.check_required_field_for_user(CONSTANTS.REGISTER_USER_REQ_FIELDS_ADMIN ,doc,index, result_dict):
                            print("index in checkid ::",index)
                            continue
                         client_id_from_token = identity_data.get(CONSTANTS.CLIENT_ID)                        
                         client_id=doc.get(CONSTANTS.CLIENT_ID)
                         if client_id:
                            if client_id != client_id_from_token:
                                status=f"{CONSTANTS.UPDATE_USER_ERR} for the index {index}"
                                mongodb.update_status(doc, result_dict, index,status)      
                                print("index in update by wrong client_id ::",index)
                                continue                        
                         if role==CONSTANTS.USER:
                            doc_id=doc.get("id")
                            if doc_id:
                                if mongodb.check_id(doc,index, result_dict,self.user_login_collection):
                                    print("index in checkid ::",index)
                                    continue                             
                                else:
                                    doc.update({'client_id':client_id_from_token})
                                    doc= self.insert_user(doc)
                            else:
                                status=f"{CONSTANTS.ADD_USER_ERR1} for the index {index}"
                                mongodb.update_status(doc, result_dict, index,status)                                                
                                print("index in required_field_for_admin ::",index)
                                continue                         
                    
                    
                    client_data={"email_from_token":email_from_token}
                    result_dict[index] = mongodb.insert_or_update_obj(self.user_login_collection,doc, client_data)

                except Exception as e:
                    self.logger.error(f"Error processing document at index {index}: {e}")
            
            
                json_result_dict = json.loads( json.dumps(result_dict, default=Utility.custom_encoder, indent=4))            
                response_data = {"data": json_result_dict, "count": len(result_dict)}
                return Utility.generate_success_response_for_crud(success_message="Upload function executed",result_field="response", results=response_data)
       except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e) 
    
    def get_user_by_id(self, user_id):
        
        user_document = self.user_details_collection.find_one({"_id": ObjectId(user_id)},{"password": 0, "token": 0, 'otp':0, 'otp_timestamp':0})
        return user_document
        
        
    def insert_user(self,doc):
         try:                         
            random_password = Utility.generate_random_password()  
            hashed_password = bcrypt.hashpw(random_password.encode('utf-8'), bcrypt.gensalt())
            if hashed_password is not None:
                    doc.update({"password":hashed_password})       
            doc.update({"status":"active","notes":random_password,"is_deleted":False})   
            return doc   
         except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def update_user(self,doc):
         try:                         
            random_password = Utility.generate_random_password()  
            hashed_password = bcrypt.hashpw(random_password.encode('utf-8'), bcrypt.gensalt())
            if hashed_password is not None:
                    doc.update({"password":hashed_password})       
            doc.update({"status":"active","notes":random_password,"is_deleted":False})   
            return doc   
         except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
        
    def check_required_field_for_user(self,fields,doc,index, result_dict):
        try:                         
            for field in fields :
                if field not in doc:
                    doc["status_code"] = "201"
                    doc["status"] = f"Missing required field: {field} for the index {index}"
                    result_dict[index] = doc                
                    return True           
                    
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)
    
    def check_duplicate_value_for_user(self,validate_data,doc,index, result_dict,collection):
        try:
            if validate_data:
                result = collection.find_one(validate_data)
            if result:                    
                if doc.get("_id") is not None:                      
                    if result["_id"] != ObjectId(doc["_id"]):
                        doc["status_code"] = "201"
                        doc["status"] = f'Either the domain or the api_url already exists in the db for the _id {result["_id"]}'
                        result_dict[index] = doc
                        return True  # Duplicate file name
                else:
                    doc["status_code"] = "201"
                    doc["status"] = f'Either the domain or the api_url already exists in the db {result["_id"]},incase of  update,the id {result["_id"]} is missing in the request '
                    result_dict[index] = doc
                    return True  # Missing doc id for update
            return False
        except Exception as e:
            self.logger.error(e)
            return Utility.generate_exception_response(e)