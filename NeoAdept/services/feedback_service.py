import os
from uuid import uuid4
from werkzeug.utils import secure_filename

from NeoAdept.services.common_service import Common_Service

from ..pojo.access_token import ACCESS_TOKEN
from ..config import Config
from ..gbo.bo import Common_Fields, Pagination
from ..gbo.common import Custom_Error
from ..pojo.feedback_details import FEEDBACK_DETAILS
from ..requests.feedback_request import create_feedback_request
from ..utilities.constants import CONSTANTS
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..utilities.utility import Utility

class Feedback_Service():  
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,logger,db,config:Config,keyset_map):
        
        if not hasattr(self, 'initialized'):
            self.logger = logger
            self.feedback_collection = "FEEDBACK_DETAILS"
            #self.feedback_collection = db["FEEDBACK_DETAILS"]
            self.attachment_path = config.attachment_path
            self.key_nested_key_map = keyset_map
            self.common_service = Common_Service(logger,db,keyset_map)

    def save_feedback_details(self, feedback_data, identity_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        feedback_data_request = create_feedback_request(feedback_data)
        feedback_data_request.parse_request()
        feedback_data_request.validate_request() 

        for attachment in feedback_data_request.feedback_details_obj.attachment: #notes:it is dic , change to object
           file_type=Utility.get_file_type(attachment.get("file_name"))
           attachment['file_type'] = file_type
        
        feedback_data['client_name'] = identity_data_obj.client_name
        common_fields = Common_Fields(created_by=email_from_token, created_on=Utility.get_current_timestamp())
        feedback_data.update(common_fields.__dict__)

        inserted_id = Mongo_DB_Manager.create_document(db[self.feedback_collection], feedback_data)
        if inserted_id is None:
            raise Custom_Error("Failed to save feedback details")
     
    def upload_attachment(self, files):
        attachments = []
        path_dir = os.path.dirname(__file__)
        neoadept_dir = os.path.abspath(os.path.join(path_dir, '..','..'))
        save_pic_path = os.path.join(neoadept_dir, self.attachment_path)
        os.makedirs(save_pic_path, exist_ok=True)

        for file in files.getlist('file'):
            if file.filename == '':
                continue  # Skip empty files
            id = str(uuid4())
            filename = id + '_' + secure_filename(file.filename)
            file_location_path = os.path.join(save_pic_path, filename)
            location_path = os.path.join(self.attachment_path, filename)
            location_path = location_path.replace("\\", "/")
            file.save(file_location_path)
            attachments.append({"id": id , "file_name":filename, "file_location_path":location_path})
        
        if not attachments:
           raise Custom_Error(CONSTANTS.CRDTS_ERR)
        return attachments
     
    def get_feedback_list(self, request_data , identity_data,db):
        filter_by = request_data.get('filter_by', [])
        request_data['filter_by'] = [f for f in filter_by if 'client_id' and 'is_deleted' not in f]
        #print(request_data)

        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        self.common_service.create_log_details(email_from_token,request_data,"get_feedback_list",db)
        
        if role_from_token == CONSTANTS.PRODUCT_ADMIN:
            return self.perform_pagination(request_data,db)
        
        else:
            filter_by = {"created_by": [email_from_token]}
            if 'filter_by' not in request_data: 
                request_data["filter_by"] = [filter_by]
            else:
                request_data["filter_by"].append(filter_by)
            return self.perform_pagination(request_data,db)

        
    def perform_pagination(self, request,db):
        pagination = Pagination(**request)
        query = {}
        if pagination.filter_by:
                updated_filter_by = Utility.update_filter_keys(pagination.filter_by,self.key_nested_key_map["FEEDBACK_DETAILS"])
                query = DB_Utility.build_filtered_data_query(updated_filter_by)        
        
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[self.feedback_collection],query,pagination)
        if docs and len(docs)>0:
            #count = Mongo_DB_Manager.count_documents(db[self.feedback_collection], query)
            return DB_Utility.convert_doc_to_cls_obj(docs,FEEDBACK_DETAILS),count
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
    
    