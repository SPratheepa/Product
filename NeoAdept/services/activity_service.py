from ..services.common_service import Common_Service
from ..pojo.access_token import ACCESS_TOKEN
from ..gbo.bo import Pagination
from ..gbo.common import Custom_Error
from ..requests.activity_request import create_activity_request
from ..utilities.constants import CONSTANTS
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..utilities.utility import Utility
from ..pojo.activity_details import ACTIVITY_DETAILS
from ..utilities.collection_names import COLLECTIONS

class Activity_Service:
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,keyset_map,logger,db):
        if not hasattr(self, 'initialized'):
            self.logger = logger
            #self.common_service = Common_Service(logger,db,keyset_map)
            self.key_map = keyset_map[COLLECTIONS.MASTER_ACTIVITY_DETAILS]
            self.initialized = True
            
    def save_activity(self, activity_request, identity_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        activity_data = create_activity_request(activity_request)
        activity_data.parse_request()
        activity_data.validate_request()
        
        _id = activity_request.get('_id')
        if _id:
            activity_data_obj = ACTIVITY_DETAILS(**activity_request)
            query = {"_id": DB_Utility.str_to_obj_id(_id)}
            
            if not Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_ACTIVITY_DETAILS], query):
                raise Custom_Error(CONSTANTS.NO_DATA_FOUND)

            DB_Utility.remove_extra_attributes(activity_data_obj.__dict__,activity_request)
            del activity_data_obj._id        
        
            activity_data_obj.updated_on = Utility.get_current_time()
            activity_data_obj.updated_by = email_from_token
            
            modified_count = Mongo_DB_Manager.update_document(db[COLLECTIONS.MASTER_ACTIVITY_DETAILS], query, activity_data_obj.__dict__)
            if modified_count == 0:
                raise Custom_Error(CONSTANTS.NO_CHANGES_FOUND)
        else:
            activity_data_obj = ACTIVITY_DETAILS(**activity_request)
            activity_data_obj.created_on = Utility.get_current_timestamp()
            activity_data_obj.created_by = email_from_token
            activity_data_obj.is_deleted = False
            del activity_data_obj._id
            
            inserted_id = Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_ACTIVITY_DETAILS],activity_data_obj.__dict__)
            
            if not inserted_id:
                raise Custom_Error(CONSTANTS.NO_DATA_ADDED)

        data = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_ACTIVITY_DETAILS],{'_id':DB_Utility.str_to_obj_id(inserted_id)})
        return DB_Utility.convert_obj_id_to_str_id(data)  
        
    def delete_activity(self,activity_data,identity_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)

        query = {"_id":DB_Utility.str_to_obj_id(activity_data.get('_id'))}
        existing_document = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_ACTIVITY_DETAILS], query)
        
        if not existing_document:
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
        if existing_document.get(CONSTANTS.IS_DELETED) == CONSTANTS.TRUE:
            raise Custom_Error("Data already deleted")
        activity_data_obj = ACTIVITY_DETAILS(**activity_data)
        activity_data_obj.is_deleted = True
        activity_data_obj.updated_by = email_from_token
        activity_data_obj.updated_on = Utility.get_current_time()
        attributes_to_delete = ["_id","subject","type","date_time","duration_minutes","job_info","job_id","job_name","job_candidate_list","internal_users","candidate_info","candidate_id","comments","company_info","company_id","company_name","company_contact_list","created_by","created_on"]
        DB_Utility.delete_attributes_from_obj(activity_data_obj,attributes_to_delete)  

        deleted_count = Mongo_DB_Manager.update_documents(db[COLLECTIONS.MASTER_ACTIVITY_DETAILS],query,activity_data_obj.__dict__)
        if deleted_count == 0:
            raise Custom_Error('No update happened')
    
    def get_all_activity(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        pagination = Pagination(**request_data) 
        #self.common_service.create_log_details(identity_data_obj.email,request_data,"get_all_activity",db)
        activity_collection = db[COLLECTIONS.MASTER_ACTIVITY_DETAILS]
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        docs,count = Mongo_DB_Manager.get_paginated_data1(activity_collection,query,pagination) 
        if count > 0:
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,ACTIVITY_DETAILS),count
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 