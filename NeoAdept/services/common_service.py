import mimetypes,os,jwt

from datetime import datetime, timedelta
from flask import json, make_response, send_file,current_app
from bson import ObjectId
from uuid import uuid4
from dateutil.parser import parse as parse_date
from werkzeug.utils import secure_filename

from ..utilities.collection_names import COLLECTIONS
from ..pojo.activity_details import ACTIVITY_DETAILS
from ..pojo.directory import DIRECTORY
from ..pojo.module_details import ACCESS_DETAILS, MODULE_DETAILS
from ..pojo.access_token import ACCESS_TOKEN
from ..pojo.search_details import SEARCH_DETAILS
from ..pojo.log_details import LOG_DETAILS
from ..gbo.bo import Pagination
from ..gbo.common import Custom_Error
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..pojo.directory import DIRECTORY
from ..pojo.access_token import ACCESS_TOKEN

class Common_Service:  
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
            self.db = db
            self.directory = DIRECTORY()
            self.keyset_map = keyset_map
            self.projection = {"JOB_DETAILS":None,"CONTACT_DETAILS_VIEW": {"company_name":1,"contact_id":1,"email":1,"name":1,"phone":1,"comments":1},"CANDIDATE_DETAILS":None,"COMPANY_DETAILS": {"company_name":1,"_id":1}}
            self.recent_search_id_map = {"CONTACT_DETAILS_VIEW": "contact_id"}
            self.key_module_map = self.keyset_map[COLLECTIONS.MASTER_MODULE_DETAILS]
            self.attachment_folder = self.directory.create_folder('attachment')
            self.update_module_details()# only for one time update use
    
    def upload_attachments(self,request,db):
        module_type = request.form.get('type')
        if not module_type:
            raise Custom_Error("Type is missing in the request JSON")
    
        module_folder = self.directory.create_folder(module_type,parent_folder=self.attachment_folder)
        
        attachment_list = []
        for file in request.files.getlist('file'):
            if not file.filename:
                continue  
                
            original_filename = secure_filename(file.filename)
            file_id = str(uuid4())
            file_extension = original_filename.rsplit('.', 1)[1] if '.' in original_filename else ''
            new_filename = f"{file_id}.{file_extension}"
            file_path = self.directory.get_folder(new_filename,parent_folder=module_folder)
            file_type = Utility.get_file_type(new_filename)
            
            file.save(file_path)

            attachment_data = {
                                "file_id": file_id,
                                "file_name": original_filename,
                                "file": new_filename,
                                "file_type": file_type,
                                "module_type": module_type
                            }
            attachment_list.append(attachment_data)
        
        if not attachment_list:
            raise Custom_Error(CONSTANTS.INVALID_INPUT)
        
        inserted_ids = Mongo_DB_Manager.create_documents(db[COLLECTIONS.MASTER_ATTACHMENT_DETAILS],attachment_list)
        if inserted_ids is None:
            raise Custom_Error('Could not add attachment info to collection')
        
        return DB_Utility.convert_object_ids_to_strings(attachment_list)
        
    def get_doc(self,module_type,filename):
        attachment_folder = self.directory.get_folder('attachment')
        module_folder = self.directory.get_folder(module_type,parent_folder=attachment_folder) 
        file_path = self.directory.get_folder(filename,parent_folder = module_folder) 
        
        if not os.path.exists(file_path):
            raise Custom_Error("File not found")

        mime_type, _ = mimetypes.guess_type(file_path) or ('application/octet-stream', None)
        disposition = 'inline' if mime_type.startswith('image/') else 'attachment'
            
        response = make_response(send_file(file_path, mimetype=mime_type))
        response.headers['Content-Disposition'] = f'{disposition}; filename="{filename}"'
        return response       
    
    def create_log_details(self,email_from_token,request_info,api_name,db):
        log_data = {
            "request_info": request_info,
            "user": email_from_token,
            "api_name": api_name,
            "current_time": Utility.get_current_time()
        }
        log_data_obj = LOG_DETAILS(**log_data) 
        del log_data_obj._id
        Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_LOG_DETAILS],log_data_obj.__dict__)
        return True
    
    def get_log_details(self,identity_data,request_data,db):
        pagination = Pagination(**request_data) 
        #self.create_log_details(identity_data_obj.email,request_data,"get_log_details",db)
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[COLLECTIONS.MASTER_LOG_DETAILS],query,pagination) 

        if not docs:
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)

        if pagination.is_download:
            return docs, count
        return DB_Utility.convert_doc_to_cls_obj(docs, LOG_DETAILS), count
    
    def add_module_details(self,module_info,db): 
        if not module_info:
            raise Custom_Error('No module info provided')
        
        access_details = [ACCESS_DETAILS(**ad).to_dict() for ad in module_info.get('access', [])]
        module = module_info.get('module')
        module_name = module.lower().replace(' ','_')
        current_time = Utility.get_current_time()
        
        module_details_obj = MODULE_DETAILS(
            module=module,
            access=access_details,
            created_by="admin",
            created_on=current_time
        )

        attributes_to_delete = ["updated_by","updated_on","_id"]
        module_details_obj = DB_Utility.delete_attributes_from_obj(module_details_obj,attributes_to_delete)
        
        module_id = Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_MODULE_DETAILS],module_details_obj.__dict__)
        if not module_id:
            raise Custom_Error('Could not module info')

        roles = Mongo_DB_Manager.read_documents(db[COLLECTIONS.MASTER_ROLE],{'is_deleted': False})
        for role in roles:
            role_id = DB_Utility.obj_id_to_str(role['_id'])
            role_name = role['name']
        
            role_permission = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_ROLE_PERMISSION],{'role_id': role_id})
            
            #module_permissions = {ad['submodule_name']: False for ad in module_info.get('access', [])}
            module_permissions = {"name":module}
            for ad in module_info.get('access', []):
                sub_module_permissions = {}
                sub_module_permissions['name']= module
                sub_module_permissions['value']= "default" 
                module_permissions[ad["api_name"]] = sub_module_permissions
            query = {'_id':user['_id']}
            update = {f'permissions.{module_name}': module_permissions}
            
            if role_permission:
                role_permission['permissions'][module_name] = module_permissions         
                Mongo_DB_Manager.update_document(db[COLLECTIONS.MASTER_ROLE_PERMISSION],{'_id': role_permission['_id']},{'permissions': role_permission['permissions']})
            else:
                new_role_permission = {
                                        'role_id': role_id,
                                        'role_name': role_name,
                                        'permissions': {
                                            module_name: module_permissions
                                        },
                                        'created_by': "admin",
                                        'created_on': current_time
                                    }
                Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_ROLE_PERMISSION],new_role_permission)
                
        query = {'is_deleted': False}
        users = Mongo_DB_Manager.read_documents(db[COLLECTIONS.MASTER_USER_DETAILS],query)
        for user in users:
            user_id = DB_Utility.obj_id_to_str(user['_id'])           
            #user_permissions = {ad['submodule_name']: "default" for ad in module_info.get('access', [])}
            user_permissions = {"name":module}
            for ad in module_info.get('access', []):
                sub_module_permissions = {}
                sub_module_permissions['name']= module
                sub_module_permissions['value']= "default" 
                user_permissions[ad["api_name"]] = sub_module_permissions
            query = {'_id':user['_id']}
            update = {f'permissions.{module_name}': user_permissions}
            print(update)
            Mongo_DB_Manager.update_document(db[COLLECTIONS.MASTER_USER_DETAILS],query,update)
        
    def get_module_details(self,identity_data,request_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        role_from_token = identity_data_obj.role
        
        pagination = Pagination(**request_data) 
                    
        query = DB_Utility.frame_get_query(pagination,self.key_module_map)
        
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[COLLECTIONS.MASTER_MODULE_DETAILS],query,pagination) 

        if docs and len(docs)>0:
            #count = Mongo_DB_Manager.count_documents(db[COLLECTIONS.MASTER_MODULE_DETAILS],query)
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,MODULE_DETAILS),count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)

    def global_search_details(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)        
        global_search_req_obj = Pagination(**request_data)
        module_collection = global_search_req_obj.module_collection
        data = {}
        #print(f"Available globals: {list(globals().keys())}")
        for mod_col in module_collection:
            count,doc_objs = 0,[]
            if mod_col in self.keyset_map:
                key_map = self.keyset_map[mod_col]
                query = DB_Utility.frame_get_query(global_search_req_obj,key_map)
                docs,count = Mongo_DB_Manager.get_paginated_data1(db[mod_col],query,global_search_req_obj,self.projection[mod_col],key_map)
                if docs and len(docs)>0:
                    #count = Mongo_DB_Manager.count_documents(db[mod_col],query)
                    data_class = globals().get(mod_col)
                    doc_objs = DB_Utility.convert_doc_to_cls_obj(docs,data_class,self.projection[mod_col])
            data.update({mod_col:{"data":doc_objs,"count":count}})
        return data

    def save_search_details(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)        
        search_details_obj = SEARCH_DETAILS(**request_data)
        search_details_obj.user = identity_data_obj.email
        search_details_obj.search_time = Utility.get_current_time()
        del search_details_obj._id
        check_limitation_query = {"user":search_details_obj.user,"module":search_details_obj.module }
        user_mod_count = Mongo_DB_Manager.count_documents(db[COLLECTIONS.ATS_SEARCH_DETAILS],check_limitation_query)
        if user_mod_count >= 20:
            oldest_docs = Mongo_DB_Manager.get_last_n_docs_from_collection(db[COLLECTIONS.ATS_SEARCH_DETAILS],check_limitation_query,19)
            ids_to_delete = [doc['_id'] for doc in oldest_docs]
            Mongo_DB_Manager.delete_documents(db[COLLECTIONS.ATS_SEARCH_DETAILS],ids_to_delete)
        check_limitation_query.update({"search_id":search_details_obj.search_id}) 
        doc = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.ATS_SEARCH_DETAILS],check_limitation_query)
        if doc is None:
            inserted_id = Mongo_DB_Manager.create_document(db[COLLECTIONS.ATS_SEARCH_DETAILS],search_details_obj.__dict__)
        
    def recent_search_details(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)        
        docs = list(Mongo_DB_Manager.read_documents(db[COLLECTIONS.SEARCH_DETAILS_VIEW],{"user":identity_data_obj.email}))
        result_dict = {}
        for doc in docs:
            module = doc['module']
            search_id = doc['search_id']
            # Check if the module exists in the dictionary
            if module in result_dict:
                result_dict[module].extend(search_id)
            else:
                result_dict[module] = search_id
        data = {}
        for mod_col,proj in self.projection.items():
            count,doc_objs = 0,[]
            if mod_col in result_dict:
                ids = result_dict[mod_col] if self.recent_search_id_map.get(mod_col) else DB_Utility.str_id_list_to_obj_list(result_dict[mod_col])
                query = {self.recent_search_id_map.get(mod_col,"_id"): {'$in': ids }}
                docs = Mongo_DB_Manager.get_paginated_data(db[mod_col],query,None,proj)
                if docs and len(docs)>0:
                    count = len(docs)
                    data_class = globals().get(mod_col)
                    doc_objs = DB_Utility.convert_doc_to_cls_obj(docs,data_class,proj)
            data.update({mod_col:{"data":doc_objs,"count":count}})
        return data
    
    def save_activity_details(self,data,db):
        
        activity_data_obj = ACTIVITY_DETAILS(**data) 
        DB_Utility.remove_extra_attributes(activity_data_obj.__dict__,data)
        activity_id = Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_ACTIVITY_DETAILS],activity_data_obj.__dict__)
        return True
        
    def add_role_permission_for_role(self, role_id, role_name, email,db):
        module_info = Mongo_DB_Manager.read_documents(db[COLLECTIONS.MASTER_MODULE_DETAILS], {})
        permissions = {}
    
        for module in module_info:
            module_name = module['module']
            if module_name == 'Common':
                default_value = True
            elif module_name == 'Permissions' and role_name in ['product-admin', 'product-user', 'admin']:
                default_value = True
            else:
                default_value = False
            #default_value = True if module_name == 'Common' else False
            permissions[module['module']] = self.get_submodule_structure(module['access'], default_value)

        new_role_permission = {
            'role_id': role_id,
            'role_name': role_name,
            'permissions': permissions,
            'created_by': email,
            'created_on': Utility.get_current_time(),
            'updated_by': email,
            'updated_on': Utility.get_current_time()
        }

        Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_ROLE_PERMISSION], new_role_permission)  
        
    def get_submodule_structure(self, submodules, default_value):
        submodule_permissions = {}

        for submodule in submodules:
            api_name = submodule['api_name']
            submodule_name = submodule['submodule_name']
            nested_access = submodule.get('access', [])
    
            key_name = submodule_name if submodule_name else api_name
    
            if nested_access:
                submodule_permissions[key_name] = self.get_submodule_structure(nested_access, default_value)
            else:
                submodule_permissions[key_name] = default_value
    
        return submodule_permissions
                
    '''def add_user_permission_for_user(self, user_id, email,db):
        module_info = Mongo_DB_Manager.read_documents(db[COLLECTIONS.MASTER_MODULE_DETAILS], {})
        permissions = {}
        for module in module_info:
            permissions[module['module']] = self.get_submodule_structure(module['access'], "default")
        
        query = {'_id': DB_Utility.str_to_obj_id(user_id)}
        update_query = {'permissions': permissions, 'updated_by': email, 'created_on': Utility.get_current_time()}
    
        modified_count = Mongo_DB_Manager.update_document(db[COLLECTIONS.MASTER_USER_DETAILS],query,update_query)'''
        
    def get_document(self,folder,filename):
        
        folder_path = self.directory.get_folder(folder)
        file_path = self.directory.get_folder(filename,parent_folder = folder_path) 
        
        if not os.path.exists(file_path):
            raise Custom_Error("File not found")

        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type is None:
            mime_type = 'application/octet-stream'
            
        if mime_type.startswith('image/'):
            disposition = 'inline'
        else:
            disposition = 'attachment'
            
        # Serve the file inline
        response = make_response(send_file(file_path, mimetype=mime_type))
        response.headers['Content-Disposition'] = f'{disposition}; filename="{filename}"'

        return response       
    
    def update_column_settings(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)  
        email = identity_data_obj.email
        fn_type = request_data.get("func_type")
        # if fn_type in the request is order,then add the entire request under visiblity>>table_name>>columns without altering widget_enablealready existing
        # if fn_type in the request is show/hide, add that db_coumn alone under  visiblity>>table_name>>columns without altering widget_enable already existing
        request_data["user_id"] = self.get_user_id(db, email)
        user_id = request_data["user_id"]
        
        dynamic_keys = [key for key in request_data if key not in ["user_id", "func_type"]]
        #print("dynamic_key",dynamic_keys)
        
        if not dynamic_keys:
            raise Custom_Error("No field to update found in request_data.")
        
        dynamic_key = dynamic_keys[0]            
                
        column_visibility_data = request_data[dynamic_key]    

        # Find existing document in USER_COULMN_VISIBILITY collection
        column_document = db[COLLECTIONS.MASTER_USER_DETAILS].find_one({'_id': DB_Utility.str_to_obj_id(request_data["user_id"])})
        visibility = column_document.get("visibility", {}) if column_document else {}
        
        if not column_document or "visibility" not in column_document:
            # Initialize visibility if not present
            visibility = {}
            
        if dynamic_key not in visibility:
            visibility[dynamic_key] = { "columns": []}
            
        visibility_path = f"visibility.{dynamic_key}.columns"
    
        if fn_type == "order":            
            update_operation = {
                '$set': {
                    visibility_path: column_visibility_data
                }
            }
            db[COLLECTIONS.COLLECTIONS.MASTER_USER_DETAILS].update_one(
                {'_id': DB_Utility.str_to_obj_id(user_id)},
                update_operation, upsert=True
            )
        elif fn_type == "view":
# Construct the update filter and operation for each client detail
            filter_criteria = {
                    "_id": ObjectId(user_id),visibility_path: 1
                }

            visibility_path_exists = db[COLLECTIONS.COLLECTIONS.MASTER_USER_DETAILS].find_one(
            filter_criteria,
            projection={ visibility_path: 1}
        )
            #print("visibility_path_exists-----------------",visibility_path_exists,visibility_path)
            if not visibility_path_exists:
            # Initialize visibility if it doesn't exist
                db[COLLECTIONS.COLLECTIONS.MASTER_USER_DETAILS].update_one(
                    {"_id": ObjectId(user_id)},
                    {"$set": { visibility_path: []}}
                )

            for detail in column_visibility_data:
                db_column = detail.get("db_column")
                enable = detail.get("enable")

                # Check if db_column exists in the array
                db_column_exists = db[COLLECTIONS.COLLECTIONS.MASTER_USER_DETAILS].count_documents({
                    "_id": ObjectId(user_id),
                    f"{visibility_path}.db_column": db_column
                })
                #print(f"{visibility_path}.db_column: {db_column}, db_column_exists: {db_column_exists}")
                if db_column_exists:
                    # Update existing db_column
                    update_operation = {
                        "$set": {
                            f"{visibility_path}.$[elem].{db_column}": enable
                        }
                    }
                    array_filters = [{"elem.db_column": db_column}]
                else:
                    # Add new db_column
                    update_operation = {
                        "$addToSet": {
                            visibility_path: detail
                        }
                    }
                    array_filters = None

                # Execute the update operation
                result = db[COLLECTIONS.COLLECTIONS.MASTER_USER_DETAILS].update_one(
                    {"_id": ObjectId(user_id)},
                    update_operation,
                    array_filters=array_filters
                )

    def get_user_id(self, db, email):       
        document = db[COLLECTIONS.COLLECTIONS.MASTER_USER_DETAILS].find_one({'email': email})
        if not document:
            raise Custom_Error(f"No user found with email: {email}")
        return DB_Utility.obj_id_to_str(document["_id"])
    
    def enable_user_widget(self,identity_data, request_data,db):
        user_id = request_data.get('user_id')
        collections_data = request_data.get('collections')
        if not user_id or not collections_data:
            raise Custom_Error("user_id and collections data are required")
        query = {'_id':DB_Utility.str_to_obj_id(user_id)}
        user = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.COLLECTIONS.MASTER_USER_DETAILS],query)
        if not user:
            raise Custom_Error("User not found")
        
        if 'visibility' not in user:
            user['visibility'] = {}

        for collection_data in collections_data:
            collection_name = collection_data.get('collection_name')
            collection_widget_enable = collection_data.get('widget_enable')
            columns_data = collection_data.get('columns', [])
            
            if not collection_name:
                continue
            
            # Initialize collection visibility if it doesn't exist
            if collection_name not in user['visibility']:
                if collection_widget_enable is not None:
                    user['visibility'][collection_name] = {
                        "widget_enable": collection_widget_enable,
                        "columns": []
                    }
                else:
                    user['visibility'][collection_name] = {
                        "columns": []
                    }
                # Update collection widget_enable if provided
            elif collection_widget_enable is not None:
                user['visibility'][collection_name]['widget_enable'] = collection_widget_enable

            # Update columns visibility
            existing_columns = {col['db_column']: col for col in user['visibility'][collection_name]['columns']}
            for column_data in columns_data:
                db_column = column_data.get('db_column')
                widget_enable = column_data.get('widget_enable')

                if db_column in existing_columns:
                    # Update existing column visibility
                    if widget_enable is not None:
                        existing_columns[db_column]['widget_enable'] = widget_enable
                else:
                    # Add new column visibility
                    new_column = {
                        "db_column": db_column,
                        "widget_enable": widget_enable if widget_enable is not None else True
                    }
                    user['visibility'][collection_name]['columns'].append(new_column)
        
        # Update the user document
        update_query = {"visibility": user['visibility']}
        updated_count = Mongo_DB_Manager.update_document(db[COLLECTIONS.COLLECTIONS.MASTER_USER_DETAILS],query,update_query)

        if updated_count == 0:
            raise Custom_Error('Could not update user-based widget visibility')
            
        if user:
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

    def get_history(self,identity_data,request_data,db):        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        role_from_token = identity_data_obj.role  
        
        collection_name = request_data.get("collection")
        ref_id = request_data.get("ref_id")        
        from_date = request_data.get("from_date")
        to_date = request_data.get("to_date")
        group_by_minutes = request_data.get("group_by_minutes")
        if from_date:
            from_date_obj = parse_date(from_date)
        else:
            from_date_obj = None

        if to_date:
            to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
            to_date_obj = to_date_obj + timedelta(days=1) - timedelta(seconds=1)
        else:
            to_date_obj = None

        if not collection_name or not ref_id:
            raise Custom_Error("One or more required parameters are missing or empty")

        match_stage = {
        "collection": collection_name,
        ref_id: {"$exists": True}
    }

        date_match_stage = {}
        if from_date_obj and to_date_obj:
            date_match_stage[f"{ref_id}.updated_on"] = {
                "$gte": from_date_obj,
                "$lte": to_date_obj
            }
        elif from_date_obj:
            date_match_stage[f"{ref_id}.updated_on"] = {"$gte": from_date_obj}
        elif to_date_obj:
            date_match_stage[f"{ref_id}.updated_on"] = {"$lte": to_date_obj}

        print("match_stage ", match_stage)
        print("date_match_stage ", date_match_stage)

        pipeline = [
            # Step 1: Match by collection, ref_id, and date range if provided
            {
                "$match": match_stage
            },
            # Step 2: Unwind the array to process each entry separately
            {
                "$unwind": f"${ref_id}"
            },
            # Step 3: Filter by date range after unwinding, if provided
            {
                "$match": date_match_stage if date_match_stage else {}
            },
            # Step 4: Group by updated_by and time intervals
            {
                "$group": {
                    "_id": {
                        "updatedBy": f"${ref_id}.updated_by",
                        "interval": {
                            "$dateTrunc": {
                                "date": f"${ref_id}.updated_on",
                                "unit": "minute",
                                "binSize": group_by_minutes
                            }
                        }
                    },
                    "updates": {
                        "$push": {
                            "field": f"${ref_id}.field",
                            "ov": f"${ref_id}.ov",
                            "nv": f"${ref_id}.nv"
                        }
                    },
                    "max_updated_on": {
                        "$max": f"${ref_id}.updated_on"
                    }
                }
            },
            # Step 5: Project the desired output format
            {
                "$project": {
                    "_id": 0,
                    "updated_by": "$_id.updatedBy",
                    "updated_on": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                            "date": "$max_updated_on",
                            "timezone": "UTC"
                        }
                    },
                    "updates": 1
                }
            },
            # Step 6: Sort by updated_on in descending order
            {
                "$sort": {
                    "updated_on": -1
                }
            }
        ]

    # Run the aggregation pipeline
        print("pipeline ",pipeline)    
        history_entries = list(db[COLLECTIONS.ATS_HISTORY].aggregate(pipeline))
        
        if not history_entries:
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
        return DB_Utility.convert_object_ids_to_strings(history_entries)
        
    def get_db_details(self,identity_data,request_data,db):      
        data,count = Mongo_DB_Manager.get_paginated_data1(db[COLLECTIONS.CONFIG_DB_DETAILS],{},projection={"_id":1,"db_name":1,"db_type":1},sample_doc=self.keyset_map["db_details"])
        if data:
            return DB_Utility.convert_object_ids_to_strings(list(data)),count 
        raise Custom_Error("No Data Found")
    
    def update_module_details(self):
        mod_details_docs = Mongo_DB_Manager.read_documents(self.db[COLLECTIONS.MASTER_MODULE_DETAILS],{})
        permissions = {}
        for module_info in mod_details_docs:
            module = module_info.get('module')
            module_permissions = {"name":module}
            for ad in module_info.get('access', []):
                sub_module_permissions = {}
                sub_module_permissions['name']= ad["submodule_name"] 
                sub_module_permissions['value']= True 
                module_permissions[ad["api_name"]] = sub_module_permissions
            module_name = module.lower().replace(' ','_')
            permissions[module_name] = module_permissions 
        print(Mongo_DB_Manager.update_document(self.db[COLLECTIONS.MASTER_ROLE_PERMISSION],{},{'permissions': permissions}))                
        print(Mongo_DB_Manager.update_document(self.db[COLLECTIONS.MASTER_USER_DETAILS],{},{'permissions':permissions}))
        
            