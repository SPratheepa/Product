import pymongo

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
            self.logger = logger
            self.keyset_map = keyset_map
            self.filters = filters
            #self.db = db
            #self.dropdown_collection = db["COLLECTION_LIST_DROPDOWN"]
            self.dropdown_collection = "COLLECTION_LIST_DROPDOWN"
            self.drop_down_list,self.dd_count = self.get_full_dropdown_list(db)

    def get_dropdown_list(self, get_request_data, db):       
            key = get_request_data.get('key')
            #if not key:
            #   raise Custom_Error(CONSTANTS.REQUIRED_FIELDS_MISSING)

            key_query = {'key': key}
            doc = Mongo_DB_Manager.read_one_document(db[self.dropdown_collection], key_query)
            if not doc:
                raise Custom_Error(CONSTANTS.NO_DATA_FOUND)

            collection_value = doc.get('value')
            if not collection_value:
                raise Custom_Error(f'Value not found for key: {key}')

            dd_collection = db[collection_value]
            query = {}

            request_data = {key: value for key, value in get_request_data.items()}
            pagination = Pagination(**request_data)
                        
            if pagination.filter_by:
                #updated_filter_by = Utility.update_filter_keys(pagination.filter_by,self.key_nested_key_map)
                query = DB_Utility.build_filtered_data_query(pagination.filter_by)            
            
            docs,count = Mongo_DB_Manager.get_paginated_data1(dd_collection,query,pagination)
            
            if docs and len(docs)>0:
                #count = Mongo_DB_Manager.count_documents(dd_collection,query)
                data = DB_Utility.convert_object_ids_to_strings(docs)
                return data,count
            
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)                 

    def get_filters_for_collection(self, request_data, email_from_token,db):
        filters = {}
        collection_name = request_data["collection_name"]
        if "field_name" in request_data and request_data.get("field_name"):
            fields_name = request_data.get("field_name")
        else:
            fields_name = self.filters.get(collection_name)
        
        if collection_name in self.keyset_map and fields_name is None:
            raise Custom_Error("please config the details in sample collection (filters) for the given collection or pass field_name in the request")
        self.key_map = self.keyset_map.get(collection_name)
        for key in fields_name:
            data = Mongo_DB_Manager.get_distinct_values(db[collection_name],self.key_map.get(key)) 
            filters[key] = data
        return filters
    
    def get_full_dropdown_list(self, db):       
            documents = Mongo_DB_Manager.read_documents(db[self.dropdown_collection], {})            
            if not documents:
                raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
            documents.sort({"_id":1})
            collection_names = list(documents)
            result = {}
            for col_doc in collection_names:
                collection_name = col_doc['value']
                collection = db[collection_name]
                cursor = collection.find()  # Adjust the query as needed
                documents = list(cursor)
                if collection_name == 'LOV_CITIES':
                    view_country_state_city_view = db['view_country_state_city']  # Assuming 'LOV_STATES' is the collection name for states                    
                    vcs_docs = list(view_country_state_city_view.find())
                    country_list= {}
                    for vcs_doc in vcs_docs:
                        country = vcs_doc.get('country')
                        states_by_country = {}
                        if "states" in vcs_doc:
                            st_docs = vcs_doc["states"]
                            state_doc = {}
                            for st_doc in st_docs:
                                state_doc.update({st_doc["state_name"]:st_doc["cities"]})
                            states_by_country.update({country:state_doc})
                        country_list.update(states_by_country)
                    result["country"] = country_list
                elif collection_name == 'LOV_STATES' or collection_name == 'LOV_COUNTRY':
                    pass
                else:
                    result[col_doc['key']] = [doc['value'] for doc in documents if 'value' in doc]  # Extract 'value' from each document
                    '''if 'order' in documents[0]:
                        values_with_order = [(doc['value'], doc['order']) for doc in documents if 'value' in doc and 'order' in doc]
                        sorted_values_with_order = sorted(list(set(values_with_order)), key=lambda x: x[1])
                        sorted_values = [item[0] for item in sorted_values_with_order]
                    else:
                        values = [doc['value'] for doc in documents if 'value' in doc]  # Extract 'value' from each document
                        sorted_values = sorted(list(set(values)))
                        
                    if 'Other' in sorted_values:
                        sorted_values.remove('Other')
                        sorted_values.append('Others')
                    if 'Any' in sorted_values:
                        sorted_values.remove('Any')
                        sorted_values.append('Any')
                    result[col_doc['key']] = sorted_values'''
            return result,len(collection_names)