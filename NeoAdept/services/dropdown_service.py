from ..utilities.collection_names import COLLECTIONS
from ..gbo.bo import Pagination
from ..utilities.constants import CONSTANTS
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..gbo.common import Custom_Error

class Dropdown_Service():
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,logger,db,keyset_map,filters):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.logger = logger
            self.keyset_map = keyset_map
            self.filters = filters
            self.drop_down_list,self.dd_count = self.get_full_dropdown_list(db)

    def get_dropdown_list(self, get_request_data, db):       
        key = get_request_data.get('key')
        doc = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.CONFIG_COLLECTION_LIST_DROPDOWN], {'key': key})
        if not doc:
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)

        collection_value = doc.get('value')
        if not collection_value:
            raise Custom_Error(f'Value not found for key: {key}')

        query = {}
        request_data = {key: value for key, value in get_request_data.items()}
        pagination = Pagination(**request_data)
                    
        if pagination.filter_by:
            query = DB_Utility.build_filtered_data_query(pagination.filter_by)            
        
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[collection_value],query,pagination)
        if count > 0:
            data = DB_Utility.convert_object_ids_to_strings(docs)
            return data,count
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND)                 

    def get_filters_for_collection(self, request_data, email_from_token,db):
        collection_name = request_data.get("collection_name")
        fields_name = request_data.get("field_name", self.filters.get(collection_name))
        
        if collection_name in self.keyset_map and fields_name is None:
            raise Custom_Error("Please config the details in sample collection (filters) for the given collection or pass field_name in the request")
        
        key_map = self.keyset_map.get(collection_name)
        filters = {}
        
        for key in fields_name:
            distinct_values = Mongo_DB_Manager.get_distinct_values(db[collection_name],key_map.get(key)) 
            filters[key] = distinct_values 
        return filters
    
    def get_full_dropdown_list(self, db):       
        documents = list(Mongo_DB_Manager.read_documents(db[COLLECTIONS.CONFIG_COLLECTION_LIST_DROPDOWN], {}) )
        if not documents:
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
        
        documents.sort(key=lambda doc: doc["_id"])

        result = {}
        for col_doc in documents:
            collection_name = col_doc['value']
            collection = db[collection_name]
            cursor = collection.find()  # Adjust the query as needed
            collection_docs = list(cursor)
            if collection_name == 'LOV_CITIES':
                view_country_state_city_view = db[COLLECTIONS.CONFIG_VIEW_COUNTRY_STATE_CITY]  # Assuming 'LOV_STATES' is the collection name for states                    
                vcs_docs = list(view_country_state_city_view.find())
                country_list= {}
                for vcs_doc in vcs_docs:
                    country = vcs_doc.get('country')
                    if "states" in vcs_doc:
                        states_by_country = {
                                country: {
                                    st_doc["state_name"]: st_doc["cities"]
                                    for st_doc in vcs_doc["states"]
                                }
                        }
                        country_list.update(states_by_country)
                result["country"] = country_list
            elif collection_name not in ['LOV_STATES', 'LOV_COUNTRY']:
                result[col_doc['key']] = [doc['value'] for doc in collection_docs if 'value' in doc]  # Extract 'value' from each document
        return result,len(documents)