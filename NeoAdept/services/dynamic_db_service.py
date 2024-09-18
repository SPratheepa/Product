from ..gbo.bo import Base_Response
from ..gbo.common import Custom_Error
from ..utilities.collection_names import COLLECTIONS
from ..utilities.constants import CONSTANTS
from ..utilities.db_utility import  Mongo_DB_Manager
class Dynamic_DB_Service:
    _instance = None  
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance
    
    def __init__(self,logger,keyset_map_dt,sql_table_list,session):
        if not hasattr(self, 'initialized'):
            self.logger = logger
            self.keyset_map_dt = keyset_map_dt
            
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
            self.initialized = True
    
    def get_collection_list(self, request_data, db):
        if session.widget_enable_for_db is None:
            return Base_Response(status=CONSTANTS.FAILED, status_code=403, message="Session expired.Please log in again").__dict__
            
        widget_enable_for_db = session.widget_enable_for_db
        sample_docs = Mongo_DB_Manager.read_documents(db[COLLECTIONS.CONFIG_sample],{"key": {"$exists": True}})
        sample_map = {doc['key']: doc['collection_description'] for doc in sample_docs}
        
        collections = [
            {
                "name": collection_name,
                "columns": {col["db_column"]: col.get("widget_enable", True) for col in collection_info.get("columns", [])},
                "description": sample_map.get(collection_name, "No description available")
            }
            for collection_name, collection_info in widget_enable_for_db.items()
            if collection_info.get("widget_enable")
        ]
    
        collections = self._sort_and_filter_collections(collections, request_data.get("sort_by"), request_data.get("order_by"), request_data.get("filter_by"))
        paginated_collections, count = self._paginate_collections(collections, request_data.get("page"), request_data.get("per_page"))
    
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

    def get_sql_table_list(self,identity_data,request_data):        
        sql_table_list = self.sql_table_list[request_data.get("db_name")]
        sql_table_list = self._sort_and_filter_collections(sql_table_list, request_data.get("sort_by"), request_data.get("order_by"), request_data.get("filter_by"))
        return self._paginate_collections(sql_table_list, request_data.get("page"), request_data.get("per_page"))
    
    def _sort_and_filter_collections(self, collections, sort_by, order_by, filter_by):
        if sort_by == "collection_name":
            reverse = order_by == "desc"
            if order_by not in ["asc", "desc"]:
                raise Custom_Error("Invalid value for 'order_by'")
            collections.sort(key=lambda x: x["name"], reverse=reverse)

        if filter_by:
            filter_collection_names = {item["collection_name"] for item in filter_by}
            collections = [collection for collection in collections if collection['name'] in filter_collection_names]
        return collections
    
    def _paginate_collections(self, collections, page, per_page):  
        if page and per_page:
            start_index = (int(page) - 1) * int(per_page)
            end_index = start_index + int(per_page)
            paginated_collections = collections[start_index:end_index]
            return paginated_collections, len(collections)
        return collections, len(collections)