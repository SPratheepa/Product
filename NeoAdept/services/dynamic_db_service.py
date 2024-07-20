import pymongo
from NeoAdept.gbo.bo import Base_Response, Pagination
from NeoAdept.gbo.common import Custom_Error
from NeoAdept.pojo.access_token import ACCESS_TOKEN
from NeoAdept.utilities.constants import CONSTANTS
from NeoAdept.utilities.db_utility import DB_Utility, Mongo_DB_Manager
from NeoAdept.utilities.utility import Utility


class Dynamic_DB_Service:
    _instance = None  
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance
    
    def __init__(self,logger,db,keyset_map_dt,sql_db,sql_table_list,session):
        if not hasattr(self, 'initialized'):
            self.logger = logger
            self.keyset_map_dt = keyset_map_dt
            self.sample_collection = "sample"
            self.column_visibility = 'COLUMN_VISIBILITY'
            self.session = session
            self.sql_table_list = sql_table_list
            self.operators_map = {
                "bool": ["=", "!="],
                "str": ["=", "!=", "in", "not in"],
                "int": ["=", "!=", ">", "<", ">=", "<=", "in", "not in"],
                "float": ["=", "!=", ">", "<", ">=", "<=", "in", "not in"],
                "date": ["=", "!=", ">", "<", ">=", "<="],
                "datetime": ["=", "!=", ">", "<", ">=", "<="],
                "list": ["in","not in"],
                "default": ["=", "!=", "in", "not in"]
            }
            self.possible_values_map = {
                "bool": [True, False],
                "default": []
            }
    
    '''        
    def get_collection_listabcd(self, request_data, db, identity_data):
        page = int(request_data.get("page", 1))  
        per_page = int(request_data.get("per_page", 10))  
        sort_by = request_data.get("sort_by")
        order_by = request_data.get("order_by")
        filter_by = request_data.get("filter_by", [])

        query = {"widget_enable": True}
        widget_enabled_docs = Mongo_DB_Manager.read_documents(db[self.sample_collection], query)
        
        # Extract the collection names from the widget enabled documents
        widget_enabled_collections = [doc.get("key") for doc in widget_enabled_docs if "key" in doc]
        
        collection_names = db.list_collection_names()

        collections = []
        for collection_name in collection_names:
            
            if collection_name in widget_enabled_collections and collection_name in self.keyset_map_dt:
                columns = self.keyset_map_dt[collection_name]
                collections.append({"name": collection_name, "columns": columns})
                
        if sort_by == "collection_name":
            if order_by == "asc":
                collections.sort(key=lambda x: x["name"])
            elif order_by == "desc":
                collections.sort(key=lambda x: x["name"], reverse=True)
            else:
                raise Custom_Error("Invalid value for 'order_by'")
            
        if filter_by:
            filtered_collections = []
            filter_collection_names = [item["collection_name"] for item in filter_by]
            for inner_list in filter_collection_names:
                for collection_name in inner_list:
                    for collection in collections:
                        if collection_name==collection['name']:
                            filtered_collections.append(collection)
            collections = filtered_collections
        
        if not request_data:  # If request_data is empty, skip pagination
            paginated_collections = collections
        else:
            start_index = (page - 1) * per_page
            end_index = start_index + per_page
            paginated_collections = collections[start_index:end_index]
        
        count = len(collections)
        # Format the data to include columns
        formatted_collections = []
        
        column_visibility = Mongo_DB_Manager.read_one_document(db[self.column_visibility],{})
        for collection in paginated_collections:
            formatted_columns = []
            collection_columns = column_visibility.get(collection['name'], [])
            for column in collection_columns:
                if column.get('widget_enable')!= False:
                    column_name = column.get('db_column')
                    if column_name in collection['columns']:
                        column_datatype = collection['columns'][column_name]
                        formatted_columns.append({
                            "name": column_name,
                            "datatype": column_datatype
                        })
            if "_id" in collection['columns']:
                formatted_columns.append({
                    "name": "_id",
                    "datatype": collection['columns']['_id']
                })
            formatted_collections.append({
                "collection_name": collection['name'],
                "columns": formatted_columns
            })
        return formatted_collections, count
    '''
    def get_collection_list(self, request_data, db):
        page = int(request_data.get("page", 1))  
        per_page = int(request_data.get("per_page", 10))  
        sort_by = request_data.get("sort_by")
        order_by = request_data.get("order_by")
        filter_by = request_data.get("filter_by", [])

        #identity_data_obj = ACCESS_TOKEN(**identity_data)
        #widget_enable_for_db = identity_data_obj.widget_enable_for_db
        if self.session.widget_enable_for_db is None:
            return Base_Response(status=CONSTANTS.FAILED, status_code=403, message="Session expired.Please log in again").__dict__
            #return Utility.generate_error_response("Session expired.Please log in again")
        
        widget_enable_for_db = self.session.widget_enable_for_db
        
        sample_docs = Mongo_DB_Manager.read_documents(db['sample'],{"key": {"$exists": True}})
        sample_map = {doc['key']: doc['collection_description'] for doc in sample_docs}
        
        collections = []
        for collection_name, collection_info in widget_enable_for_db.items():
            if collection_info.get("widget_enable"):
                columns = collection_info.get("columns", [])
                columns_dict = {col["db_column"]: col.get("widget_enable", True) for col in columns}
                collection_description = sample_map.get(collection_name, "No description available")
                collections.append({"name": collection_name, "columns": columns_dict, "description": collection_description})
    
        if sort_by == "collection_name":
            if order_by == "asc":
                collections.sort(key=lambda x: x["name"])
            elif order_by == "desc":
                collections.sort(key=lambda x: x["name"], reverse=True)
            else:
                raise Custom_Error("Invalid value for 'order_by'")
        
        if filter_by:
            filtered_collections = []
            filter_collection_names = [item["collection_name"] for item in filter_by]
            for inner_list in filter_collection_names:
                for collection_name in inner_list:
                    for collection in collections:
                        if collection_name == collection['name']:
                            filtered_collections.append(collection)
            collections = filtered_collections
    
        if not request_data:  # If request_data is empty, skip pagination
            paginated_collections = collections
        else:
            start_index = (page - 1) * per_page
            end_index = start_index + per_page
            paginated_collections = collections[start_index:end_index]
    
        count = len(collections)
    
        # Format the data to include columns
        formatted_collections = []
    
        for collection in paginated_collections:
            formatted_columns = []
            for column_name, widget_enable in collection['columns'].items():
                if widget_enable:
                    datatype = self.keyset_map_dt[collection['name']].get(column_name, "Unknown")
                    operators_allowed = self.operators_map.get(datatype, self.operators_map["default"])
                    possible_values = self.possible_values_map.get(datatype, self.possible_values_map["default"])
                    column_info = {
                        "name": column_name,
                        "datatype": datatype,
                        "operators": operators_allowed
                    }
                    if possible_values:
                        column_info["possible_values"] = possible_values
                    formatted_columns.append(column_info)
            if "_id" not in collection['columns']:
                formatted_columns.append({
                    "name": "_id",
                    "datatype": "Unknown",
                    "operators": self.operators_map["default"]
                })
            # Remove 'widget_enable' column if it exists
            formatted_columns = [col for col in formatted_columns if col["name"] != "widget_enable"]
            formatted_collections.append({
                "collection_name": collection['name'],
                "columns": formatted_columns,
                "description": collection['description']
            })
    
        return formatted_collections, count
    
    def get_collection_columns(self, collection):
        
        columns = {}
        for key in collection.find_one().keys():
            columns[key] = "string"  
        return columns
    
    def get_collection_list1(self,request_data,db,identity_data):
        page = int(request_data.get("page", 1))  
        per_page = int(request_data.get("per_page", 10))  
        sort_by = request_data.get("sort_by")
        order_by = request_data.get("order_by")

        collection_names = db.list_collection_names()

        if sort_by == "collections":
            if order_by == "asc":
                collection_names.sort()  
            elif order_by == "desc":
                collection_names.sort(reverse=True)  
            else:
                raise Custom_Error("Invalid value for 'order_by'")

        start_index = (page - 1) * per_page
        end_index = start_index + per_page

        paginated_collection_names = collection_names[start_index:end_index]
        count = len(collection_names)
        return paginated_collection_names,count

    def get_attribute_list(self, request_data, db):
        print("Entering get_attribute_list")
        page = int(request_data.get("page", 1))
        per_page = int(request_data.get("per_page", 10))
        sort_by = request_data.get("sort_by")
        order_by = request_data.get("order_by")
        filter_by = request_data.get("filter_by", [])

        filter_by = request_data.get('filter_by', [])
        collection_names = []
        for filter_item in filter_by:
            if 'collection' in filter_item:
                collection_names.extend(filter_item['collection'])

        for collection_name in collection_names:
            if collection_name in self.keyset_map_dt:
                attributes = list(self.keyset_map_dt[collection_name].keys())

                if sort_by == "attributes":
                    if order_by == "asc":
                        attributes.sort()  
                    elif order_by == "desc":
                        attributes.sort(reverse=True)  
                    else:
                        raise Custom_Error("Invalid value for 'order_by'")
                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                paginated_attribute_names = attributes[start_index:end_index]
                count = len(attributes)
                return paginated_attribute_names,count
            
            
    """def get_document_list(self,request_data):
        page = int(request_data.get("page", 1))  
        per_page = int(request_data.get("per_page", 10))  
        sort_by = request_data.get("sort_by")
        order_by = request_data.get("order_by")
        filter_by = request_data.get("filter_by", [])

        if filter_by:
            all_documents = []
            total_count = 0

            for criterion in filter_by:
                if "collection" in criterion:
                    collection_names = criterion["collection"]
                    if isinstance(collection_names, list):
                        for collection_name in collection_names:
                            collection = self.db.get_collection(collection_name)
                            query = {}  
                            cursor = collection.find(query).sort(sort_by, 1 if order_by == "asc" else -1).skip((page - 1) * per_page).limit(per_page)
                            documents = list(cursor)
                            count = collection.count_documents(query)  
                            total_count += count
                            all_documents.extend(documents)
                    else:
                        collection_name = collection_names
                        collection = self.db.get_collection(collection_name)
                        query = {} 
                        cursor = collection.find(query).sort(sort_by, 1 if order_by == "asc" else -1).skip((page - 1) * per_page).limit(per_page)
                        documents = list(cursor)
                        count = collection.count_documents(query)  
                        total_count += count
                        all_documents.extend(documents)

            # Convert object IDs to strings
            data = DB_Utility.convert_object_ids_to_strings(all_documents)
            return data, total_count

        raise Custom_Error("No filter criteria provided.")"""

    def get_sql_table_list(self,identity_data,request_data):
        page = int(request_data.get("page", 1))  
        per_page = int(request_data.get("per_page", 10))  
        sort_by = request_data.get("sort_by")
        order_by = request_data.get("order_by")
        filter_by = request_data.get("filter_by", [])
        sql_table_list = self.sql_table_list[request_data.get("db_name")]
        if sort_by == "collection_name":
            if order_by == "asc":
                sql_table_list.sort(key=lambda x: x["name"])
            elif order_by == "desc":
                sql_table_list.sort(key=lambda x: x["name"], reverse=True)
            else:
                raise Custom_Error("Invalid value for 'order_by'")
        collections = sql_table_list
        if filter_by:
            filtered_collections = []
            filter_collection_names = [item["collection_name"] for item in filter_by]
            for inner_list in filter_collection_names:
                for collection_name in inner_list:
                    for collection in collections:
                        if collection_name == collection['name']:
                            filtered_collections.append(collection)
            collections = filtered_collections
    
        if not request_data:  # If request_data is empty, skip pagination
            paginated_collections = collections
        else:
            start_index = (page - 1) * per_page
            end_index = start_index + per_page
            paginated_collections = collections[start_index:end_index]
    
        count = len(collections)    
        return paginated_collections, count