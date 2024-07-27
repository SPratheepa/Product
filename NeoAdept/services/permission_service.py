from flask import current_app
from flask_jwt_extended import get_jwt
import jwt

from ..gbo.bo import Pagination
from ..pojo.permission_details import ROLE_PERMISSION
from ..pojo.user_details import USER_DETAILS
from ..pojo.access_token import ACCESS_TOKEN
from ..utilities.collection_names import COLLECTIONS
from ..utilities.constants import CONSTANTS
from ..gbo.common import Custom_Error
from ..utilities.utility import Utility
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager

class Permission_Service:  
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,logger,db,keyset_map):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.logger = logger
            self.keyset_map = keyset_map
            self.key_map = self.keyset_map[COLLECTIONS.MASTER_ROLE_PERMISSION]
            #self.get_user_permission_view(db)
            
    def get_permissions(self, request_data, db):
        filters = request_data.get('filter_by', [])
        user_id = None

        for filter_item in filters:
            if 'user_id' in filter_item:
                user_id_list = filter_item['user_id']
                if user_id_list:
                    user_id = user_id_list[0]
                    break

        if not user_id:
            raise ValueError("user_id not found in request data")
        
        user_permissions = self.get_user_permissions(db,user_id)   
        
        if user_permissions:
            # Remove 'Common' module from permissions if it exists
            if 'Common' in user_permissions:
                del user_permissions['Common']
                
            permission_info = {
            'user_id':user_id,
            'permissions':user_permissions
            }
            return [permission_info]
        
    def get_user_permissions(self, db, user_id=None):
        query = {}
        if user_id is not None:
            query = {'_id': DB_Utility.str_to_obj_id(user_id)}
        user_doc = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_USER_DETAILS],query)
        if user_doc:
            return user_doc.get("permissions",[])        
        return {}
    
    def get_role_permissions(self, role_name, db):
        query = {'role_name': role_name}
        role_permissions_doc = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_ROLE_PERMISSION], query)
        if role_permissions_doc:
            return role_permissions_doc.get('permissions', [])
        return {}
    
    def save_user_permission(self,identity_data,data,db):      
        
        new_per_obj = USER_DETAILS(permissions = data.get("permissions")) 
       
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token = identity_data_obj.email
       
        user_query = {'_id': DB_Utility.str_to_obj_id(data.get("user_id"))}  
        user_info = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_USER_DETAILS],user_query)        
        if not user_info:
            raise Custom_Error('User not found')
        
        existing_permissions =  self.convert_permission_values(user_info.get("permissions"))
        new_permissions =  self.convert_permission_values(new_per_obj.permissions)
        if existing_permissions: 
           
           merged_permissions = self.merge_permissions(existing_permissions, new_permissions,db)
           update_query = {"permissions": merged_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
        else:             
            update_query = {"permissions": new_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
           
        result = Mongo_DB_Manager.update_document(db[COLLECTIONS.MASTER_USER_DETAILS],user_query,update_query)
        if result == 0:
            raise Custom_Error("Could not update permissions")    
    
        if user_info:
            if 'token' in user_info:
                token = user_info['token']               
                try:
                    # Decode the token to get the JWT ID (jti)
                    decoded_token = jwt.decode(token, options={"verify_signature": False})
                    
                    jti = decoded_token.get('jti')
                    if jti:
                        current_app.blacklist.add(jti)
                except jwt.DecodeError:
                    self.logger.error(f"Failed to decode token for user {DB_Utility.obj_id_to_str(user_info['_id'])}")
        
    def save_role_permission(self,identity_data,data,db): 
        
        data_obj = ROLE_PERMISSION(**data) 
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token = identity_data_obj.email
        role_name = self.get_role_name(data_obj.role_id,db)
        if not role_name:
            raise Custom_Error('Role not found')
        
        data_obj.role_name = role_name
        
        query = {'role_name':role_name}

        existing_doc = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_ROLE_PERMISSION],query)
        
        if existing_doc:
            existing_doc_obj = ROLE_PERMISSION(**existing_doc)
            existing_permissions = self.convert_permission_values(existing_doc_obj.permissions)
            new_permissions = self.convert_permission_values(data_obj.permissions)
            merged_permissions = self.merge_permissions(existing_permissions, new_permissions,db,data_obj.role_name)
            data['role_name'] = role_name
            DB_Utility.remove_extra_attributes(data_obj.__dict__,data)
            
            update_query = {"permissions": merged_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
            result = Mongo_DB_Manager.update_document(db[COLLECTIONS.MASTER_ROLE_PERMISSION],query,update_query)
            if result == 0:
                raise Custom_Error("Could not update permissions")
        else:      
            data_obj.created_on = Utility.get_current_time()
            data_obj.created_by = email_from_token
            attributes_to_delete = ["updated_by","updated_on","_id"]
            data_obj = DB_Utility.delete_attributes_from_obj(data_obj,attributes_to_delete)
            role_perm_id = Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_ROLE_PERMISSION],data_obj.__dict__)
            if not role_perm_id:
                raise Custom_Error('Could not add role permission info')
        
        user_query = {'role': role_name}
        users_with_role = Mongo_DB_Manager.read_documents(db[COLLECTIONS.MASTER_USER_DETAILS], user_query)
        if users_with_role:
            for user in users_with_role:
                if 'token' in user:
                    token = user['token']
                    try:
                        # Decode the token to get the JWT ID (jti)
                        decoded_token = jwt.decode(token, options={"verify_signature": False})
                        jti = decoded_token.get('jti')
                        if jti:
                            current_app.blacklist.add(jti)
                    except jwt.DecodeError:
                        self.logger.error(f"Failed to decode token for user {DB_Utility.obj_id_to_str(user['_id'])}")
                        
    def merge_permissions(self, existing_permissions, new_permissions, db, role_name=None):
        existing_permissions_dict = existing_permissions.copy()  # Create a copy to avoid modifying the original
            
        for module, permissions in new_permissions.items():
            if module in existing_permissions_dict:
                self.update_permissions(existing_permissions_dict[module], permissions)
            else:
                existing_permissions_dict[module] = self.convert_permission_values(permissions)
    
        return existing_permissions_dict
    
    def update_permissions(self, existing_permissions, new_permissions):
        for key, value in new_permissions.items():
            if isinstance(value, dict):
                if key in existing_permissions and isinstance(existing_permissions[key], dict):
                    self.update_permissions(existing_permissions[key], value)
                else:
                    existing_permissions[key] = self.convert_permission_values(value)
            else:
                existing_permissions[key] = value
    
    def convert_permission_values(self, input_dict):
        output_dict = {}
        if input_dict:
            for key, value in input_dict.items():
                if isinstance(value, dict):
                    output_dict[key] = self.convert_permission_values(value)
                else:
                    if value in ('true', True):
                        output_dict[key] = True
                    elif value in ('false', False):
                        output_dict[key] = False
                    else:
                        output_dict[key] = value  # Keeping as is if not a boolean
    
        return output_dict

    def get_role_permission(self,request_data,db):
        
        pagination = Pagination(**request_data) 
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[COLLECTIONS.MASTER_ROLE_PERMISSION],query,pagination) 
        
        if docs and len(docs)>0:
            # Remove 'Common' module from permissions if it exists
            for doc in docs:
                if 'permissions' in doc and 'Common' in doc['permissions']:
                    del doc['permissions']['Common']
            #count = Mongo_DB_Manager.count_documents(db[COLLECTIONS.MASTER_ROLE_PERMISSION],query)
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,ROLE_PERMISSION),count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
    
    def get_role_name(self, role_id,db):
        query = {"_id": DB_Utility.str_to_obj_id(role_id)}
        role_collection = db[COLLECTIONS.MASTER_ROLE]
        role_info = Mongo_DB_Manager.read_one_document(role_collection,query)
        if not role_info:
            return None
        return role_info['name'] if role_info['name'] else None