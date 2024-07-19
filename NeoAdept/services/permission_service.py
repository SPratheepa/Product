from NeoAdept.gbo.bo import Pagination
from NeoAdept.pojo.permission.permission_details import ROLE_PERMISSION

from NeoAdept.pojo.user.user_details import USER_DETAILS
from NeoAdept.pojo.user.access_token import ACCESS_TOKEN
from NeoAdept.utilities.constants import CONSTANTS
from ..gbo.common import Custom_Error
from ..utilities.utility import Utility
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from flask import current_app
from flask_jwt_extended import get_jwt
import jwt

class Permission_Service:  
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,logger,db,keyset_map):
        if not hasattr(self, 'initialized'):
            self.logger = logger
            self.role_permission_details = "ROLE_PERMISSION"
            self.user_permission_details = "USER_PERMISSION"
            self.user_details_collection = "USER_DETAILS"
            self.role_collection = "ROLE"
            self.key_nested_key_map = keyset_map
            if "ROLE_PERMISSION" in keyset_map:
                self.key_map = self.key_nested_key_map["ROLE_PERMISSION"]
            if "USER_PERMISSION" in keyset_map:
                self.key_user_map = self.key_nested_key_map["USER_PERMISSION"]
            #self.get_user_permission_view(db)
    '''        
    def save_user_permission(self,identity_data,data,db):      
        
        new_per_obj = USER_DETAILS(permissions = data.get("permissions")) 
       
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token = identity_data_obj.email
       
        user_query = {'_id': DB_Utility.str_to_obj_id(data.get("user_id"))}  
        user_info = Mongo_DB_Manager.read_one_document(db[self.user_details_collection],user_query)        
        if not user_info:
            raise Custom_Error('User not found')
        
        existing_permissions = user_info.get("permissions")
        new_permissions = new_per_obj.permissions
        if existing_permissions: 
           
           merged_permissions = self.merge_permissions(existing_permissions, new_permissions,db)
           update_query = {"permissions": merged_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
        else:             
            update_query = {"permissions": new_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
           
        result = Mongo_DB_Manager.update_document(db[self.user_details_collection],user_query,update_query)
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
        
    def save_user_permission2(self,identity_data,data,db): 
        
        data_obj = USER_PERMISSION(**data) 
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token = identity_data_obj.email
        
        user_query = {'_id': DB_Utility.str_to_obj_id(data_obj.user_id)}
        user_info = Mongo_DB_Manager.read_one_document(db[self.user_details_collection], user_query)
        if not user_info:
            raise Custom_Error('User not found')

        query = {'user_id':data_obj.user_id}
        
        existing_doc = Mongo_DB_Manager.read_one_document(db[self.user_permission_details],query)
        if existing_doc:
            existing_doc_obj = USER_PERMISSION(**existing_doc)
            existing_permissions = existing_doc_obj.permissions
            new_permissions = data_obj.permissions
            merged_permissions = self.merge_permissions(existing_permissions, new_permissions,db)
            DB_Utility.remove_extra_attributes(data_obj.__dict__,data)
            #del data_obj._id
            update_query = {"permissions": merged_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
            result = Mongo_DB_Manager.update_document(db[self.user_permission_details],query,update_query)
            if result == 0:
                raise Custom_Error("Could not update permissions")
        else:      
            data_obj.created_on = Utility.get_current_time()
            data_obj.created_by = email_from_token
            attributes_to_delete = ["updated_by","updated_on","_id"]
            data_obj = DB_Utility.delete_attributes_from_obj(data_obj,attributes_to_delete)
            user_perm_id = Mongo_DB_Manager.create_document(db[self.user_permission_details],data_obj.__dict__)
            if not user_perm_id:
                raise Custom_Error('Could not add role permission info')

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

        existing_doc = Mongo_DB_Manager.read_one_document(db[self.role_permission_details],query)
        
        if existing_doc:
            existing_doc_obj = ROLE_PERMISSION(**existing_doc)
            existing_permissions = existing_doc_obj.permissions
            new_permissions = data_obj.permissions
            merged_permissions = self.merge_permissions(existing_permissions, new_permissions,db,data_obj.role_name)
            data['role_name'] = role_name
            DB_Utility.remove_extra_attributes(data_obj.__dict__,data)
            
            update_query = {"permissions": merged_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
            result = Mongo_DB_Manager.update_document(db[self.role_permission_details],query,update_query)
            if result == 0:
                raise Custom_Error("Could not update permissions")
        else:      
            data_obj.created_on = Utility.get_current_time()
            data_obj.created_by = email_from_token
            attributes_to_delete = ["updated_by","updated_on","_id"]
            data_obj = DB_Utility.delete_attributes_from_obj(data_obj,attributes_to_delete)
            role_perm_id = Mongo_DB_Manager.create_document(db[self.role_permission_details],data_obj.__dict__)
            if not role_perm_id:
                raise Custom_Error('Could not add role permission info')
        
        user_query = {'role': role_name}
        users_with_role = Mongo_DB_Manager.read_documents(db[self.user_details_collection], user_query)
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

    def merge_permissions(self,existing_permissions, new_permissions,db,role_name=None):
        # Create a dictionary for quick lookup of existing permissions by module
        existing_permissions_dict = existing_permissions

        for module, permissions in new_permissions.items():
            permission = self.convert_permission_values(permissions)
            if module in existing_permissions_dict:
                # Update the existing permission with new values
                existing_permissions_dict[module].update(permission)
            else:
                # Add new permission if it doesn't exist in existing permissions
                existing_permissions_dict[module] = permission

        return existing_permissions_dict
    
    def convert_permission_values(self,input_dict):
        for key, value in input_dict.items():
            if value in ('true', True):
                input_dict[key] = True
            elif value in ('false',False):
                input_dict[key] = False
            else:
                input_dict[key] = "default"      
        return input_dict
    
    def get_role_permission(self,request_data,db):
        
        pagination = Pagination(**request_data) 
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[self.role_permission_details],query,pagination) 

        if docs and len(docs)>0:
            #count = Mongo_DB_Manager.count_documents(db[self.role_permission_details],query)
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,ROLE_PERMISSION),count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
    
    def get_user_permission(self,request_data,db):
        pagination = Pagination(**request_data) 
                       
        query = DB_Utility.frame_get_query(pagination,self.key_user_map)
           
        #docs = Mongo_DB_Manager.get_paginated_data(db[self.user_permission_details],query,pagination) 
        docs,count = Mongo_DB_Manager.get_paginated_data1(db['USER_PERMISSION_VIEW'],query,pagination) 
        if docs and len(docs)>0:
            #count = Mongo_DB_Manager.count_documents(db[self.user_permission_details],query) 
            #count = Mongo_DB_Manager.count_documents(db['USER_PERMISSION_VIEW'],query) 
            
            if pagination.is_download==True:
                return docs,count
            #return DB_Utility.convert_doc_to_cls_obj(docs,USER_PERMISSION),count
            return DB_Utility.convert_doc_to_cls_obj(docs,USER_PERMISSION_VIEW),count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
    
    def get_user_permission_view(self,db):
        if 'USER_PERMISSION_VIEW' not in db.list_collection_names() or Mongo_DB_Manager.is_collection_empty(db['USER_PERMISSION_VIEW']):
            self.create_user_permission_view(db) 
            
    def create_user_permission_view(self,db):
        pipeline = [
            {
                "$lookup": {
                    "from": "role_permission_collection",
                    "localField": "user_id",
                    "foreignField": "role_name",
                    "as": "role_permissions"
                }
            },
            {
                "$unwind": "$role_permissions"
            },
            {
                "$addFields": {
                    "permissions": {
                        "$let": {
                            "vars": {
                                "userPermissions": "$permissions",
                                "rolePermissions": "$role_permissions.permissions"
                            },
                            "in": {
                                "$arrayToObject": {
                                    "$map": {
                                        "input": { "$objectToArray": "$$rolePermissions" },
                                        "as": "rolePermission",
                                        "in": {
                                            "k": "$$rolePermission.k",
                                            "v": {
                                                "$let": {
                                                    "vars": {
                                                        "userPermission": { "$ifNull": [ { "$arrayElemAt": [ { "$filter": { "input": { "$objectToArray": "$$userPermissions" }, "as": "userPermission", "cond": { "$eq": [ "$$userPermission.k", "$$rolePermission.k" ] } } }, 0 ] }, { "v": "default" } ] }
                                                    },
                                                    "in": {
                                                        "$cond": [
                                                            { "$in": [ "$$userPermission.v", [ True, False ] ] },
                                                            "$$userPermission.v",
                                                            "$$rolePermission.v"
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            {
                "$project": {
                    "role_permissions": 0
                }
            }
        ]
        db.command('create', 'USER_PERMISSION_VIEW', viewOn='USER_PERMISSION', pipeline=pipeline)
        #user_permission_view = db.user_permission_collection.aggregate(pipeline)
        #return user_permission_view
    '''
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
        
    '''def get_combined_permissions(self, request_data, db):
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
        user_permissions = self.get_user_permissions(db, user_id)
        
        query = {'_id' : DB_Utility.str_to_obj_id(user_id)}
        user = Mongo_DB_Manager.read_one_document(db[self.user_details_collection],query)
        if not user:
            raise Custom_Error('User not found')
        role_name = user['role']
        role_permissions = self.get_role_permissions(role_name, db)
        
        combined_permissions = role_permissions.copy()
        
        # Merge user permissions with role permissions
        for module, user_perm in user_permissions.items():
            if module not in combined_permissions:
                combined_permissions[module] = user_perm.copy()
            else:
                combined_perm = combined_permissions[module]
                for key, value in user_perm.items():
                    if value != "default":
                        combined_perm[key] = value
                    elif key in role_permissions.get(module, {}):
                        combined_perm[key] = role_permissions[module][key]
                    
        for module, combined_perm in combined_permissions.items():
            user_perm = user_permissions.get(module, {})  # Get user permissions for this module (if any)
            role_perm = role_permissions.get(module, {})
            for action in ['add', 'update', 'delete', 'view', 'upload']:
                if action not in combined_perm:
                    if action in user_perm:
                        combined_perm[action] = user_perm[action]
                    elif action in role_perm:
                        combined_perm[action] = role_perm[action]
                    else:
                        combined_perm[action] = False
          
        permission_info = {
            'user_id':user_id,
            'permissions':combined_permissions
            }
        return [permission_info]'''
    
    
    def get_user_permissions(self, db, user_id=None):
        query = {}
        if user_id is not None:
            query = {'_id': DB_Utility.str_to_obj_id(user_id)}
        user_doc = Mongo_DB_Manager.read_one_document(db[self.user_details_collection],query)
        if user_doc:
            return user_doc.get("permissions",[])        
        return {}
    
    def get_role_permissions(self, role_name, db):
        query = {'role_name': role_name}
        role_permissions_doc = Mongo_DB_Manager.read_one_document(db["ROLE_PERMISSION"], query)
        if role_permissions_doc:
            return role_permissions_doc.get('permissions', [])
        return {}
    
    def save_user_permission(self,identity_data,data,db):      
        
        new_per_obj = USER_DETAILS(permissions = data.get("permissions")) 
       
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token = identity_data_obj.email
       
        user_query = {'_id': DB_Utility.str_to_obj_id(data.get("user_id"))}  
        user_info = Mongo_DB_Manager.read_one_document(db[self.user_details_collection],user_query)        
        if not user_info:
            raise Custom_Error('User not found')
        
        existing_permissions =  self.convert_permission_values(user_info.get("permissions"))
        new_permissions =  self.convert_permission_values(new_per_obj.permissions)
        if existing_permissions: 
           
           merged_permissions = self.merge_permissions(existing_permissions, new_permissions,db)
           update_query = {"permissions": merged_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
        else:             
            update_query = {"permissions": new_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
           
        result = Mongo_DB_Manager.update_document(db[self.user_details_collection],user_query,update_query)
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

        existing_doc = Mongo_DB_Manager.read_one_document(db[self.role_permission_details],query)
        
        if existing_doc:
            existing_doc_obj = ROLE_PERMISSION(**existing_doc)
            existing_permissions = self.convert_permission_values(existing_doc_obj.permissions)
            new_permissions = self.convert_permission_values(data_obj.permissions)
            merged_permissions = self.merge_permissions(existing_permissions, new_permissions,db,data_obj.role_name)
            data['role_name'] = role_name
            DB_Utility.remove_extra_attributes(data_obj.__dict__,data)
            
            update_query = {"permissions": merged_permissions,"updated_on":Utility.get_current_time(),"updated_by":email_from_token}
            result = Mongo_DB_Manager.update_document(db[self.role_permission_details],query,update_query)
            if result == 0:
                raise Custom_Error("Could not update permissions")
        else:      
            data_obj.created_on = Utility.get_current_time()
            data_obj.created_by = email_from_token
            attributes_to_delete = ["updated_by","updated_on","_id"]
            data_obj = DB_Utility.delete_attributes_from_obj(data_obj,attributes_to_delete)
            role_perm_id = Mongo_DB_Manager.create_document(db[self.role_permission_details],data_obj.__dict__)
            if not role_perm_id:
                raise Custom_Error('Could not add role permission info')
        
        user_query = {'role': role_name}
        users_with_role = Mongo_DB_Manager.read_documents(db[self.user_details_collection], user_query)
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
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[self.role_permission_details],query,pagination) 
        
        if docs and len(docs)>0:
            # Remove 'Common' module from permissions if it exists
            for doc in docs:
                if 'permissions' in doc and 'Common' in doc['permissions']:
                    del doc['permissions']['Common']
            #count = Mongo_DB_Manager.count_documents(db[self.role_permission_details],query)
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,ROLE_PERMISSION),count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
    
    def get_role_name(self, role_id,db):
        query = {"_id": DB_Utility.str_to_obj_id(role_id)}
        role_collection = db[self.role_collection]
        role_info = Mongo_DB_Manager.read_one_document(role_collection,query)
        if not role_info:
            return None
        return role_info['name'] if role_info['name'] else None