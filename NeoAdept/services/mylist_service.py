import copy, pandas as pd
from NeoAdept.pojo.list_details import FILE_EMAIL_GROUPING, FILE_GROUP_VIEW, LIST_GROUP
from NeoAdept.services.common_service import Common_Service

from bson import ObjectId
from flask import send_file

from NeoAdept.utilities.collection_names import COLLECTIONS

from ..pojo.access_token import ACCESS_TOKEN
from ..config import Config
from ..gbo.bo import Common_Fields, Pagination
from ..gbo.common import Custom_Error
from ..requests.mylist_request import MoveToListRequest, add_cv_list_request, create_list_request, delete_list_request, remove_cv_list_request, update_list_request
from ..utilities.constants import CONSTANTS
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..utilities.utility import Utility

class My_List_Service():
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,config,logger,db,keyset_map):
        if not hasattr(self, 'initialized'):
            self.logger = logger
            self.my_list_file = config.list_group_file_name
            self.keyset_map = keyset_map
            self.key_map = keyset_map[COLLECTIONS.ATS_LIST_GROUP]
            self.key_file_map = keyset_map[COLLECTIONS.FILE_GROUP_VIEW]
            #self.common_service = Common_Service(logger,db,keyset_map)
            self.get_file_group_view(db)

    def add_new_group(self, list_data,identity_data,db): #add condition already available name and deleted name available ,store data or not
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)

        list_data_obj = LIST_GROUP(**list_data) 
        
        list_data_request = create_list_request(list_data) 
        list_data_request.parse_request()
        list_data_request.validate_request()
      
        
        fields_to_check = ["list_name"]
        query = DB_Utility.fields_to_check(list_data_obj,fields_to_check)
        existing_list = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.ATS_LIST_GROUPn], query)
                
        if existing_list:
            raise Custom_Error(CONSTANTS.LIST_NAME_EXISTS)
        
        list_data_obj.created_on = Utility.get_current_time()
        list_data_obj.created_by = email_from_token
        list_data_obj.is_deleted = False
        list_data_obj.candidate_count = 0
        
        attributes_to_delete = ["updated_by","updated_on","_id","list_id"]
        list_data_obj = DB_Utility.delete_attributes_from_obj(list_data_obj,attributes_to_delete)
                
        list_id = Mongo_DB_Manager.create_document(db[COLLECTIONS.ATS_LIST_GROUPn],list_data_obj.__dict__)
        if not list_id:
            raise Custom_Error('Could not add list')
        
    
    def rename_group_list(self, list_data,identity_data,db):
       
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        
        list_data_request = update_list_request(list_data) 
        list_data_request.parse_request()
        list_data_request.validate_request()

        list_data_obj = LIST_GROUP(**list_data) 
        
        _id = DB_Utility.str_to_obj_id(list_data_obj._id)
        query = DB_Utility.update_keys_check(list_data_obj,['list_name'],_id)
        
        cursor = Mongo_DB_Manager.read_documents(db[COLLECTIONS.ATS_LIST_GROUPn],query)
        existing_lists = list(cursor)
                 
        if _id not in [list['_id'] for list in existing_lists]:
            raise Custom_Error('List not found')
        
        for existing_list in list(existing_lists):
            if existing_list["_id"] != _id:
                raise Custom_Error('list_name already exists for other documents')

        DB_Utility.remove_extra_attributes(list_data_obj.__dict__,list_data)
        del list_data_obj._id        
             
        list_data_obj.updated_on = Utility.get_current_time()
        list_data_obj.updated_by = email_from_token

        query =  {"_id": _id}  
        result = Mongo_DB_Manager.update_document(db[COLLECTIONS.ATS_LIST_GROUPn], query, list_data_obj.__dict__)
        if result == 0:
            raise Custom_Error('Could not update list name')  
        
        update_grouping_query = {"list_id": DB_Utility.obj_id_to_str(_id)}
        document_count = Mongo_DB_Manager.count_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING], update_grouping_query)

        if document_count > 0:
            update_data = {"list_name": list_data_obj.list_name}
            Mongo_DB_Manager.update_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING], update_grouping_query, update_data)

                
    def remove_id_from_obj(self,my_list_details_obj):
        cloned_grouplist_data = copy.deepcopy(my_list_details_obj)
        cloned_grouplist_data.__delattr__('_id')
        return cloned_grouplist_data
    
    def delete_list(self, list_data,identity_data,db):

        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)

        list_data_request = delete_list_request(list_data) 
        list_data_request.parse_request()
        list_data_request.validate_request()
                   
        #list_data_obj = LIST_GROUP(**list_data)

        _id_list = list_data.get('_id')  #muliple id ex:refere candidate list delete
        id_objects_list = DB_Utility.str_id_list_to_obj_list(_id_list)
       
        query = {"_id":{'$in':id_objects_list}}
        existing_documents = Mongo_DB_Manager.read_documents(db[COLLECTIONS.ATS_LIST_GROUPn],query)
        if not existing_documents:
            raise Custom_Error("List not found")
        
        delete_list_details_list = []
        delete_file_group_details_list = []
        for document in existing_documents:
            doc_obj = LIST_GROUP(**document)
            if doc_obj.is_deleted == CONSTANTS.TRUE:
                raise Custom_Error("Data already deleted")
            
            
            _id = DB_Utility.str_to_obj_id(doc_obj._id)
            attributes_to_delete = ["_id","list_name","created_by","created_on","list_id"]
            DB_Utility.delete_attributes_from_obj(doc_obj,attributes_to_delete)  
            
            query =  {"_id": _id}
            doc_obj.updated_on = Utility.get_current_time()
            doc_obj.updated_by = email_from_token
            doc_obj.is_deleted = True
            doc_obj.candidate_count = 0
            update_one_operation = DB_Utility.build_update_query_for_bulk_opr({'_id':_id}, doc_obj.__dict__)
            delete_list_details_list.append(update_one_operation)
            update_grouping_query = {'list_id':DB_Utility.obj_id_to_str(_id)}
            document_count = Mongo_DB_Manager.count_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING], update_grouping_query)
            if document_count > 0:
                update_group_operation = DB_Utility.build_update_query_for_bulk_opr(update_grouping_query, doc_obj.__dict__)
                delete_file_group_details_list.append(update_group_operation)
        
        if len(delete_list_details_list) > 0:
            deleted_count = Mongo_DB_Manager.bulk_write_operations(db[COLLECTIONS.ATS_LIST_GROUPn],delete_list_details_list)
            
            if deleted_count.modified_count != len(_id_list):
                raise Custom_Error(CONSTANTS.FEW_RECORDS_NOT_DEL)
            
            if len(delete_file_group_details_list) > 0:
                query = {"list_id": DB_Utility.obj_id_to_str(_id)}
                deleted_count = Mongo_DB_Manager.delete_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING],query)
                #self.update_candidate_count(db,DB_Utility.obj_id_to_str(_id))
                #deleted_count = Mongo_DB_Manager.bulk_write_operations(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING],delete_file_group_details_list)
            
            
    def get_group_list(self,identity_data, request_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        pagination = Pagination(**request_data) 
        ##self.common_service.create_log_details(identity_data_obj.email,request_data,"get_group_list",db)
        list_collection = db[COLLECTIONS.ATS_LIST_GROUPn]
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        
        docs,count = Mongo_DB_Manager.get_paginated_data1(list_collection,query,pagination) 

        if docs and len(docs)>0:
            
            #count = Mongo_DB_Manager.count_documents(list_collection,query)
            for doc in docs:
                doc['list_id'] = DB_Utility.obj_id_to_str(doc['_id'])
            if pagination.is_download==True:
                return docs,count
                
            return DB_Utility.convert_doc_to_cls_obj(docs,LIST_GROUP),count
        
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
                      
    def get_data_in_excel(self,data):
        df = pd.DataFrame(data)
        df.to_excel(self.my_list_file,index=False)  
        return send_file(self.my_list_file, as_attachment=True, download_name="group_list.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    '''    
    def get_list_view(self,db):
        #self.drop_collection(db)
        if CONSTANTS.LIST_VIEW not in db.list_collection_names() or Mongo_DB_Manager.is_collection_empty(db[self.list_view_collection]):
            self.create_list_view(db)
    
    

    def create_list_view(self,db):
        
        pipeline = [
            {
                '$lookup': {
                    'from': 'FILE_EMAIL_GROUPING',
                    'localField': 'list_name',
                    'foreignField': 'list_name',
                    'as': 'list_info'
                }
            },
            {
                '$unwind': {
                    'path': '$list_info',
                    'preserveNullAndEmptyArrays': True
                }
            },    
            {
                '$group': {
                    '_id': '$_id',
                    'list_id': { '$first': { '$toString': '$_id' } },
                    'list_name': { '$first': '$list_name' },
                    'is_deleted': { '$first': '$is_deleted' },
                    'created_by': { '$first': '$created_by' },
                    'created_on': { '$first': '$created_on' },
                    'updated_by': { '$first': '$updated_by' },
                    'updated_on': { '$first': '$updated_on' },
                    #'candidate_info': {
                    #    '$push': {
                    #        'candidate_id': '$list_info.candidate_id',
                    #        'email': '$list_info.email'
                    #    }
                    #},
                    'candidate_count': {
                        '$sum': 1  
                    },
                }
            },
            {
            '$replaceWith': {
                '_id': '$_id',
                'list_id': '$list_id',
                'list_name': '$list_name',
                #'candidate_info': '$candidate_info',
                'candidate_count': '$candidate_count',
                'is_deleted': '$is_deleted',
                'created_by': '$created_by',
                'created_on': '$created_on',
                'updated_by': '$updated_by',
                'updated_on': '$updated_on'
            }
        }
            ]
        
        db.command('create', 'LIST_VIEW', viewOn='LIST_GROUP', pipeline=pipeline)
        return True
    
    def create_list_view1(self, db):
        pipeline = [
            {
                '$lookup': {
                    'from': 'FILE_EMAIL_GROUPING',
                    'let': {'list_name': '$list_name'},
                    'pipeline': [
                        {'$match': {'$expr': {'$eq': ['$list_name', '$$list_name']}}}
                    ],
                    'as': 'list_info'
                }
            },
            {
                '$unwind': {
                    'path': '$list_info',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$addFields': {
                    'list_info': {
                        '$ifNull': ['$list_info', []]
                    }
                }
            },
            {
                '$addFields': {
                    'candidate_count': {
                        '$cond': {
                            'if': {'$isArray': '$list_info'},
                            'then': {'$size': '$list_info'},
                            'else': 0
                        }
                    }
                }
            },
            {
                '$group': {
                    '_id': '$_id',
                    'list_id': { '$first': { '$toString': '$_id' } },
                    'list_name': { '$first': '$list_name' },
                    'is_deleted': { '$first': '$is_deleted' },
                    'created_by': { '$first': '$created_by' },
                    'created_on': { '$first': '$created_on' },
                    'updated_by': { '$first': '$updated_by' },
                    'updated_on': { '$first': '$updated_on' },
                    'candidate_count': { '$first': '$candidate_count' },
                }
            },
            {
                '$replaceWith': {
                    '_id': '$_id',
                    'list_id': '$list_id',
                    'list_name': '$list_name',
                    'candidate_count': '$candidate_count',
                    'is_deleted': '$is_deleted',
                    'created_by': '$created_by',
                    'created_on': '$created_on',
                    'updated_by': '$updated_by',
                    'updated_on': '$updated_on'
                }
            }
        ]

        db.command('create', 'LIST_VIEW', viewOn='LIST_GROUP', pipeline=pipeline)
        return True
    '''           
    def add_cv_list_to_group(self, list_data,db):
        
        list_data_request = add_cv_list_request(list_data) 
        list_data_request.parse_request()
        list_data_request.validate_request()
        
        list_data_obj = FILE_EMAIL_GROUPING(**list_data)
        
        _id = list_data_obj.list_id
        query = {"_id": DB_Utility.str_to_obj_id(_id)}
                
        existing_list = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.ATS_LIST_GROUPn],query)
        if not existing_list:
            raise Custom_Error("List not found")

        list_id = list_data_obj.list_id
        candidate_ids = list_data_obj.candidate_id

        existing_list_obj = LIST_GROUP(**existing_list)       
        list_name = existing_list_obj.list_name
        new_grouping_details_list = []
        for candidate_id in candidate_ids:
            query = {'candidate_id': candidate_id, 'list_id': list_id}
            if Mongo_DB_Manager.count_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING], query) > 0:
                continue

            query = {'_id': DB_Utility.str_to_obj_id(candidate_id), 'is_deleted': False}
            candidate_info = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.ATS_CANDIDATE_DETAILS], query)
            if not candidate_info:
                raise Custom_Error(f'Candidate info not found for candidate ID {candidate_id}')
            
            email = candidate_info.get(CONSTANTS.EMAIL)
            grouping_data = {"list_id": list_id, "list_name": list_name, "candidate_id": candidate_id, "email": email}
            new_grouping_details_list.append(grouping_data)
        if len(new_grouping_details_list) > 0:
            inserted_ids = Mongo_DB_Manager.create_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING],new_grouping_details_list)
            self.update_candidate_count(db, list_id)
        
    def remove_cv_from_group(self,list_data,db):
        
        list_data_request = remove_cv_list_request(list_data) 
        list_data_request.parse_request()
        list_data_request.validate_request()
        
        list_data_obj = FILE_EMAIL_GROUPING(**list_data)
        
        _id = list_data_obj.list_id
        query = {"_id": DB_Utility.str_to_obj_id(_id)}
                
        existing_list = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.ATS_LIST_GROUPn],query)
        if not existing_list:
            raise Custom_Error("List not found")
        
        list_id = list_data_obj.list_id
        candidate_ids = list_data_obj.candidate_id
        
        files_removed = False
        for candidate_id in candidate_ids:
            query = {'candidate_id': candidate_id, 'list_id': list_id}
            if Mongo_DB_Manager.count_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING],query) > 0 :
                Mongo_DB_Manager.delete_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING],query)
                self.update_candidate_count(db, list_id)
                files_removed = True
        if files_removed!=True:       
            raise Custom_Error(CONSTANTS.NO_FILES_REMOVED)   
        
    def get_file_email_list(self,identity_data, request_data, db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        
        
        #self.common_service.create_log_details(identity_data_obj.email,request_data,"get_cv_group_list",db)
        list_id=request_data.get(CONSTANTS.LIST_ID)
        if not list_id:
            raise Custom_Error(CONSTANTS.LIST_ID_MISSING)
        
        request_data.pop(CONSTANTS.LIST_ID, None)
        pagination = Pagination(**request_data) 
        if pagination.filter_by is None:
            filter_by = [{"list_id": [list_id]}]   
        else:
            filter_by = pagination.filter_by.copy()
            filter_by.append({"list_id": [list_id]})
        pagination.filter_by = filter_by
                 
        candidate_ids_cursor = db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING].find({"list_id": list_id}, {"candidate_id": 1})
        candidate_ids = [str(doc['candidate_id']) for doc in candidate_ids_cursor]
        #if not candidate_ids:
         
        #self.get_file_group_view(db)
        
        file_group_view_collection = db[COLLECTIONS.FILE_GROUP_VIEW]
        
        query = DB_Utility.frame_get_query(pagination,self.key_file_map)
           
        docs,count = Mongo_DB_Manager.get_paginated_data1(file_group_view_collection,query,pagination) 

        if docs and len(docs)>0:
            #count = Mongo_DB_Manager.count_documents(file_group_view_collection,query)
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,FILE_GROUP_VIEW),count
        
        return None,0
        #raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
        
    def get_file_group_view(self,db):
        if 'FILE_GROUP_VIEW' not in db.list_collection_names() or Mongo_DB_Manager.is_collection_empty(db[COLLECTIONS.FILE_GROUP_VIEW]):
            self.create_file_group_view(db)          
                            
    def create_file_group_view(self,db):
        
            pipeline = [  
                            {
                                "$lookup": {
                                    "from": "CANDIDATE_DETAILS",
                                    "localField": "email",
                                    "foreignField": "email",
                                    "as": "candidate_data"
                                }
                            },
                            {
                                "$unwind": {
                                        "path": "$candidate_data",
                                        "preserveNullAndEmptyArrays": True
                                }
                            },
                            {
                                "$match": {
                                    "candidate_data.is_deleted": False  # Include records with is_deleted=False
                                }
                            },
                            {
                                "$project": {
                                    "_id": { "$toString": "$_id" },
                                    "list_id": "$list_id",
                                    "list_name": "$list_name",
                                    "candidate_id":"$candidate_id",
                                    "email": "$email",
                                    "age": "$candidate_data.age",
                                    "alternate_contact_number": "$candidate_data.alternate_contact_number",
                                    "can_work_in_shifts": "$candidate_data.can_work_in_shifts",
                                    "career_break": "$candidate_data.career_break",
                                    "career_break_details_list": "$candidate_data.career_break_details_list",
                                    "caregiver_status": "$candidate_data.caregiver_status",
                                    "certification_details_list": "$candidate_data.certification_details_list",
                                    "created_by": "$candidate_data.created_by",
                                    "created_on": "$candidate_data.created_on",
                                    "ctc_type": "$candidate_data.ctc_type",
                                    "current_ctc": "$candidate_data.current_ctc",
                                    "current_location": "$candidate_data.current_location",
                                    "cwe_city": "$candidate_data.cwe_city",
                                    "cwe_company_name": "$candidate_data.cwe_company_name",
                                    "cwe_country": "$candidate_data.cwe_country",
                                    "cwe_designation": "$candidate_data.cwe_designation",
                                    "cwe_end_year": "$candidate_data.cwe_end_year",
                                    "cwe_func_area": "$candidate_data.cwe_func_area",
                                    "cwe_industry": "$candidate_data.cwe_industry",
                                    "cwe_job_type": "$candidate_data.cwe_job_type",
                                    "cwe_level": "$candidate_data.cwe_level",
                                    "cwe_reason_for_leaving": "$candidate_data.cwe_reason_for_leaving",
                                    "cwe_reported_by": "$candidate_data.cwe_reported_by",
                                    "cwe_reported_to": "$candidate_data.cwe_reported_to",
                                    "cwe_start_year": "$candidate_data.cwe_start_year",
                                    "cwe_state": "$candidate_data.cwe_state",
                                    "cwe_year_of_exp_months": "$candidate_data.cwe_year_of_exp_months",
                                    "cwe_year_of_exp_year": "$candidate_data.cwe_year_of_exp_year",
                                    "date_of_birth": "$candidate_data.date_of_birth",
                                    "educational_details_list": "$candidate_data.educational_details_list",
                                    "expected_ctc": "$candidate_data.expected_ctc",
                                    "expected_ctc_type": "$candidate_data.expected_ctc_type",
                                    "family_income": "$candidate_data.family_income",
                                    "first_name": "$candidate_data.first_name",
                                    "gender": "$candidate_data.gender",
                                    "insert_from": "$candidate_data.insert_from",
                                    "is_active": "$candidate_data.is_active",
                                    "is_cwe_end_year_till": "$candidate_data.is_cwe_end_year_till",
                                    "is_deleted": "$candidate_data.is_deleted",
                                    "is_np_negotiable": "$candidate_data.is_np_negotiable",
                                    "is_person_with_disability": "$candidate_data.is_person_with_disability",
                                    "key": "$candidate_data.key",
                                    "languages_known_list": "$candidate_data.languages_known_list",
                                    "last_name": "$candidate_data.last_name",
                                    "linkedin_profile": "$candidate_data.linkedin_profile",
                                    "marital_status": "$candidate_data.marital_status",
                                    "middle_name": "$candidate_data.middle_name",
                                    "nationality": "$candidate_data.nationality",
                                    "nature_of_disability": "$candidate_data.nature_of_disability",
                                    "negotiable_period": "$candidate_data.negotiable_period",
                                    "notes": "$candidate_data.notes",
                                    "notice_period": "$candidate_data.notice_period",
                                    "onsite_experience_list": "$candidate_data.onsite_experience_list",
                                    "parental_status": "$candidate_data.parental_status",
                                    "passport_no": "$candidate_data.passport_no",
                                    "photo": "$candidate_data.photo",
                                    "photo_file_name": "$candidate_data.photo_file_name",
                                    "photo_id": "$candidate_data.photo_id",
                                    "pref_shift_timings": "$candidate_data.pref_shift_timings",
                                    "preferred_location": "$candidate_data.preferred_location",
                                    "previous_work_experience_list": "$candidate_data.previous_work_experience_list",
                                    "primary_contact_number": "$candidate_data.primary_contact_number",
                                    "primary_skills": "$candidate_data.primary_skills",
                                    "prl_city": "$candidate_data.prl_city",
                                    "prl_country": "$candidate_data.prl_country",
                                    "prl_state": "$candidate_data.prl_state",
                                    "relevant_experience": "$candidate_data.relevant_experience",
                                    "religion": "$candidate_data.religion",
                                    "resume": "$candidate_data.resume",
                                    "resume_file_name": "$candidate_data.resume_file_name",
                                    "resume_id": "$candidate_data.resume_id",
                                    "rural_or_urban": "$candidate_data.rural_or_urban",
                                    "secondary_skills": "$candidate_data.secondary_skills",
                                    "spouse_status": "$candidate_data.spouse_status",
                                    "title": "$candidate_data.title",
                                    "total_experience_months": "$candidate_data.total_experience_months",
                                    "total_experience_year": "$candidate_data.total_experience_year",
                                    "total_job_changed": "$candidate_data.total_job_changed",
                                    "travel_details_int_exp": "$candidate_data.travel_details_int_exp",
                                    "travel_details_nat_exp": "$candidate_data.travel_details_nat_exp",
                                    "updated_by": "$candidate_data.updated_by",
                                    "updated_on": "$candidate_data.updated_on",
                                    "willing_to_relocate": "$candidate_data.willing_to_relocate"
                                }
                            }
                        ]
            db.command('create', 'FILE_GROUP_VIEW', viewOn='FILE_EMAIL_GROUPING', pipeline=pipeline)
            self.logger.info("View 'FILE_GROUP_VIEW' created successfully.")  
            
    def move_cv_to_list(self, list_data, db):
        list_data_request = MoveToListRequest(list_data)
        list_data_request.parse_request()
        list_data_request.validate_request()
    
        from_list_id = list_data_request.cv_list_details_obj.from_list_id
        to_list_id = list_data_request.cv_list_details_obj.to_list_id
        candidate_ids = list_data_request.cv_list_details_obj.candidate_id
    
        # Verify existence of both lists
        from_list_query = {"_id": DB_Utility.str_to_obj_id(from_list_id)}
        to_list_query = {"_id": DB_Utility.str_to_obj_id(to_list_id)}
    
        from_list = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.ATS_LIST_GROUPn], from_list_query)
        to_list = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.ATS_LIST_GROUPn], to_list_query)
    
        if not from_list:
            raise Custom_Error("Source list not found")
    
        if not to_list:
            raise Custom_Error("Destination list not found")
    
        from_list_obj = LIST_GROUP(**from_list)
        to_list_obj = LIST_GROUP(**to_list)
    
        from_list_name = from_list_obj.list_name
        to_list_name = to_list_obj.list_name
    
        new_grouping_details_list = []
        for candidate_id in candidate_ids:
            delete_query = {'candidate_id': candidate_id, 'list_id': from_list_id}
            if Mongo_DB_Manager.count_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING], delete_query) == 0:
                raise Custom_Error(f'Candidate {candidate_id} not found in the source list')
        
            # Delete from the source list
            Mongo_DB_Manager.delete_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING], delete_query)
        
            # Add to the destination list
            candidate_query = {'_id': DB_Utility.str_to_obj_id(candidate_id), 'is_deleted': False}
            candidate_info = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.ATS_CANDIDATE_DETAILS], candidate_query)
        
            if not candidate_info:
                raise Custom_Error(f'Candidate info not found for candidate ID {candidate_id}')
        
            email = candidate_info.get(CONSTANTS.EMAIL)
            grouping_data = {
                "list_id": to_list_id, 
                "list_name": to_list_name, 
                "candidate_id": candidate_id, 
                "email": email
            }
            new_grouping_details_list.append(grouping_data)
    
        if new_grouping_details_list:
            Mongo_DB_Manager.create_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING], new_grouping_details_list)
            self.update_candidate_count(db, from_list_id)
            self.update_candidate_count(db, to_list_id)
            
    def update_candidate_count(self,db, list_id):
        query = {'list_id': list_id}
        candidate_count = Mongo_DB_Manager.count_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING], query)
        query = {'_id': DB_Utility.str_to_obj_id(list_id)}
        update_query =  {"candidate_count": candidate_count}
        Mongo_DB_Manager.update_document(db[COLLECTIONS.ATS_LIST_GROUPn],query,update_query)
        
    def remove_candidates_from_list(self, _id_list, db):
        affected_lists = db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING].distinct("list_id", {"candidate_id": {"$in": _id_list}})
        query = {"candidate_id": {"$in": _id_list}}
        Mongo_DB_Manager.delete_documents(db[COLLECTIONS.ATS_FILE_EMAIL_GROUPING],query)
        for list_id in affected_lists:
            self.update_candidate_count(db,list_id)