from ..pojo.dynamic_widget import DYNAMIC_WIDGET
from ..pojo.access_token import ACCESS_TOKEN
from ..gbo.bo import Base_Response, Pagination
from ..gbo.common import Custom_Error
from ..requests.dynamic_widget_request import create_dynamic_widget_request, delete_dynamic_widget_request, update_dynamic_widget_request
from ..utilities.collection_names import COLLECTIONS
from ..utilities.constants import CONSTANTS
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..utilities.utility import Utility
class Dynamic_widget_Service:
    
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance
    
    def __init__(self,logger,db,keyset_map,keyset_map_dt,session,sql_db):
        self.initialized = True
        self.logger = logger
        self.keyset_map = keyset_map
        self.keyset_map_dt = keyset_map_dt
        self.datatype_operations = {
                    'str': {'=': '$eq', '==': '$eq', '!=': '$ne', 'in': '$in', 'nin': '$nin', 'exists': '$exists', 'regex': '$regex'},
                    'int': {'=': '$eq', '==': '$eq', '!=': '$ne', '>': '$gt', '<': '$lt', '>=': '$gte', '<=': '$lte', 'in': '$in', 'nin': '$nin', 'exists': '$exists'},
                    'float': {'=': '$eq', '==': '$eq', '!=': '$ne', '>': '$gt', '<': '$lt', '>=': '$gte', '<=': '$lte', 'in': '$in', 'nin': '$nin', 'exists': '$exists'},
                    'bool': {'=': '$eq', '==': '$eq', '!=': '$ne', 'exists': '$exists'},
                    'date': {'=': '$eq', '==': '$eq', '!=': '$ne', '>': '$gt', '<': '$lt', '>=': '$gte', '<=': '$lte', 'exists': '$exists'},
                    'datetime': {'=': '$eq', '==': '$eq', '!=': '$ne', '>': '$gt', '<': '$lt', '>=': '$gte', '<=': '$lte', 'exists': '$exists'},
                    'list': {'in': '$in', 'nin': '$nin', 'exists': '$exists'},
                    'dict': {'exists': '$exists'},
                    'array': {'exists': '$exists'},
                    'objectid': {'=': '$eq', '==': '$eq', '!=': '$ne', 'exists': '$exists'}
                }
        #self.common_service = Common_Service(logger,db,keyset_map)
        self.key_map = self.keyset_map[COLLECTIONS.MASTER_DYNAMIC_WIDGET ]
        
        self.sql_db = sql_db

    def get_dynamic_widget(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        pagination = Pagination(**request_data)
        #self.common_service.create_log_details(identity_data_obj.email,request_data,"get_dynamic_widget",db)
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET],query,pagination) 
        if count > 0:
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,DYNAMIC_WIDGET),count
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
    
    def update_dynamic_widget(self,dynamic_widget_data,db,email_from_token):
        dynamic_widget_request = update_dynamic_widget_request(dynamic_widget_data) 
        dynamic_widget_request.parse_request()
        dynamic_widget_request.validate_request()
        
        dynamic_widget_data_obj = DYNAMIC_WIDGET(**dynamic_widget_data)
        _id = DB_Utility.str_to_obj_id(dynamic_widget_data_obj._id)
        
        list_of_keys = [key for key in ['name'] if getattr(dynamic_widget_data_obj, key, None)]
        query = {"_id": _id} if not list_of_keys else DB_Utility.update_keys_check(dynamic_widget_data_obj, list_of_keys, _id)
    
        existing_dynamic_widgets = list(Mongo_DB_Manager.read_documents(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET], query))
        
        if not any(widget['_id'] == _id for widget in existing_dynamic_widgets):
            raise Custom_Error('Dynamic widget not found')
        
        for widget in existing_dynamic_widgets:
            if widget["_id"] != _id:
                raise Custom_Error('Name already exists for other documents')
            
        if "query_information" in dynamic_widget_data:
            query_information = dynamic_widget_data['query_information']
            collection_name = query_information.get("conditions")[0].get("table")
            is_mongo_db = dynamic_widget_data.get("db_type") != CONSTANTS.SQL
            self.validate_query_information(query_information, collection_name, is_mongo_db)
        
        dynamic_widget_data_obj.updated_on = Utility.get_current_time()
        dynamic_widget_data_obj.updated_by = email_from_token
        dynamic_widget_update_data = dynamic_widget_data_obj.__dict__
        DB_Utility.remove_extra_attributes(dynamic_widget_update_data,dynamic_widget_data)
        del dynamic_widget_data_obj._id  
        result = Mongo_DB_Manager.update_document(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET], {"_id": _id}, dynamic_widget_update_data)
        if result == 0:
            raise Custom_Error('Could not update dynamic widget')  
        data = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET],query)
        return DB_Utility.convert_obj_id_to_str_id(data)
            
    def save_dynamic_widget(self,dynamic_widget_data,db,email_from_token):
        dynamic_widget_request = create_dynamic_widget_request(dynamic_widget_data) 
        dynamic_widget_request.parse_request()
        dynamic_widget_request.validate_request()
        dynamic_widget_obj_details = dynamic_widget_request.dynamic_widget_obj
        query_information = dynamic_widget_data.get("query_information")
        collection_name = query_information.get("conditions")[0].get("table")
        is_mongo_db = dynamic_widget_data.get("db_type") != CONSTANTS.SQL
        self.validate_query_information(query_information, collection_name,is_mongo_db)
        fields_to_check = ["name"]
        query = DB_Utility.fields_to_check(dynamic_widget_obj_details,fields_to_check)
        existing_widget = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET], query)
        if existing_widget:
            field = next((field for field in fields_to_check if getattr(dynamic_widget_obj_details, field) == existing_widget[field]), None)
            if field:
                raise Custom_Error(f"{field} already exists")
        dynamic_widget_obj_details = Utility.upsert_by_on(dynamic_widget_obj_details,"add",email_from_token)         
        dynamic_widget_id = Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET],dynamic_widget_obj_details.__dict__) 
        if not dynamic_widget_id:
            raise Custom_Error('Dynamic_widget is not saved')
        data = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET],query)       
        return DB_Utility.convert_obj_id_to_str_id(data)
        
    def validate_query_information(self, query_information, collection_name,is_mongo_db=True):
        conditions = query_information.get("conditions", [])
        if not conditions:
            raise Custom_Error("Conditions are missing in the query information.")
        for condition in conditions:
            missing_fields = [key for key in ['table', 'field', 'operator', 'value'] if key not in condition]
            if missing_fields:
                raise Custom_Error(f"Invalid query: Missing fields {missing_fields} in condition.")
            if condition['table'] != collection_name:
                raise Custom_Error("Collection names should be same.")
        if is_mongo_db:
            rules = query_information.get("rules", [])
            if rules:
                self.validate_and_process_rules(rules, collection_name) 
        
    def validate_and_process_rules(self, rules, collection_name):
        keyset_map_dt = self.keyset_map_dt[collection_name]
        def validate_condition(condition):
            missing_fields = [key for key in ['table', 'field', 'operator', 'value'] if key not in condition]
            if missing_fields:
                raise Custom_Error(f"Invalid query: Missing fields {missing_fields} in condition.")
            if condition['table'] != collection_name:
                raise Custom_Error("Collection names should be the same.")
        def validate_rule(rule):
            missing_fields = [key for key in ['field', 'operator', 'value'] if key not in rule]
            if missing_fields:
                raise Custom_Error(f"Invalid query: Missing fields {missing_fields} in rule.")
            field = rule['field']
            operator = rule['operator']
            field_datatype = keyset_map_dt.get(field)
            if field_datatype is None:
                raise Custom_Error(f"No datatype found for field '{field}' in the keyset.")
            if operator not in self.datatype_operations.get(field_datatype, []):
                raise Custom_Error(f"Operator '{operator}' not supported for field '{field}' with datatype '{field_datatype}'.")
            if not rule.get('operation'):
                raise Custom_Error("Operation is missing or empty in rule.")
        for rule in rules:
            if 'conditions' in rule:
                nested_conditions = rule['conditions']
                if not nested_conditions:
                    raise Custom_Error("Conditions cannot be empty.")
                for nested_condition in nested_conditions:
                    validate_condition(nested_condition)
                self.validate_and_process_rules(rule['rules'], collection_name)
            else:
                validate_rule(rule)
            
    def delete_dynamic_widget(self,identity_data,dynamic_widget_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
        dynamic_widget_data_request = delete_dynamic_widget_request(dynamic_widget_data) 
        dynamic_widget_data_request.parse_request()
        dynamic_widget_data_request.validate_request()
        dynamic_widget_data_obj = DYNAMIC_WIDGET(**dynamic_widget_data)
        dynamic_widget_collection = db[COLLECTIONS.MASTER_DYNAMIC_WIDGET]
        _id = dynamic_widget_data_obj._id
        query = {"_id": DB_Utility.str_to_obj_id(_id)}
        existing_dynamic_widget = Mongo_DB_Manager.read_one_document(dynamic_widget_collection,query)
        if not existing_dynamic_widget:
            raise Custom_Error("Dynamic_widget not found")
        existing_dynamic_widget_obj = DYNAMIC_WIDGET(**existing_dynamic_widget)
        if existing_dynamic_widget_obj.is_deleted == CONSTANTS.TRUE:
            raise Custom_Error("Dynamic_widget is already deleted")
        dynamic_widget_data_obj.updated_on = Utility.get_current_time()
        dynamic_widget_data_obj.updated_by = email_from_token
        dynamic_widget_data_obj.is_deleted = True                                                                                          
        attributes_to_delete = ["_id","name","file_name","info","type","class_name","description","query_information","visual_type","visual_parameters","created_by","created_on","db_type"]
        DB_Utility.delete_attributes_from_obj(dynamic_widget_data_obj,attributes_to_delete)  
        result = Mongo_DB_Manager.update_document(dynamic_widget_collection, query, dynamic_widget_data_obj.__dict__)
        if result != 1:
            raise Custom_Error('Dynamic_widget is not deleted')
    
    def generate_mongodb_query(self, query_information, keyset_map_dt,key_nested_key_map):
        query = {}
        operation = query_information.get('operation')
        query_operator = '$and' if operation == 'AND' else '$or'
        query[query_operator] = []
        for cond in query_information.get('conditions', []):
            field = cond['field']
            value = cond['value']
            operator = cond['operator'].replace('not in', 'nin')
            field_datatype = keyset_map_dt.get(field)
            init_operations = self.datatype_operations.get(field_datatype, {})
            operation = init_operations.get(operator)
            if operator in ['in', 'nin']:
                if not isinstance(value, list):
                    raise Custom_Error(f"Value for operator '{operator}' must be a list")
            field_name = key_nested_key_map.get(field)
            query_condition = {field_name: {operation: value}}
            query[query_operator].append(query_condition)
        for sub_rule in query_information.get('rules', []):
            sub_query = self.generate_mongodb_query(sub_rule, keyset_map_dt, key_nested_key_map)
            if sub_query:
                query[query_operator].append(sub_query)
        return query 
    
    def get_query_from_dynamic_widget(self,identity_data,id,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data) 
        _id = DB_Utility.str_to_obj_id(id)
        doc = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET], {"_id": _id})
        if not doc:
            raise Custom_Error("No data found for _id")
        query_information = doc.get("query_information")  
        if not query_information:
            raise Custom_Error("query_information not available for widget")
        collection_name = query_information.get("conditions")[0].get("table")
        if not collection_name:
            raise Custom_Error("Table name not available in query_information")
        generate_query = f"select * from {collection_name}"
        if query_information.get('conditions') and query_information.get('rules'):
            query = self.generate_sql_query(query_information)
            if query:
                generate_query = f" where {query}"
        return generate_query
        
    def generate_dynamic_widget_query(self,identity_data,id,request_data,db):
        if session.widget_enable_for_db is None:
            return Base_Response(status=CONSTANTS.FAILED, status_code=403, message="Session expired.Please log in again").__dict__
        identity_data_obj = ACCESS_TOKEN(**identity_data) 
        _id = DB_Utility.str_to_obj_id(id)
        doc = Mongo_DB_Manager.read_one_document(db[COLLECTIONS.MASTER_DYNAMIC_WIDGET], {"_id": _id})
        if not doc:
            raise Custom_Error("No data found for _id")
        if doc.get("db_type") == CONSTANTS.SQL:
            return self.preview_query_result_for_sql(doc,identity_data)
        else:
            return self.preview_query_result(doc,db,identity_data)
        
    def preview_query_result(self,request_data,db,identity_data):
        query_information = request_data.get("query_information")  
        if not query_information:
            raise Custom_Error("query_information not available for widget")
        
        collection_name = query_information.get("conditions")[0].get("table")
        if not collection_name:
            raise Custom_Error("table not available in query_information")
        
        widget_enable_for_db = session.widget_enable_for_db
        if collection_name not in widget_enable_for_db or not widget_enable_for_db[collection_name].get("widget_enable"):
            raise Custom_Error('The user does not have access to collection')
        
        projection = { "_id": 1 }  # Always include the _id field
        for col in widget_enable_for_db[collection_name].get("columns", []):
            if col.get("widget_enable", True):
                projection[col["db_column"]] = 1
        generate_query = {}
        if query_information.get('rules'):
            generate_query = self.generate_mongodb_query(
                query_information,
                self.keyset_map_dt[collection_name],
                self.keyset_map[collection_name]
            )
        
        visualization_type = request_data.get("visual_type") or "Summary"
        visual_parameters = request_data.get("visual_parameters",[])
        req = {}
        if visual_parameters:
            sort_by = visual_parameters[0].get("sort_by_column")
            order_by = visual_parameters[0].get("order_by", "asc")
            if sort_by and order_by:
                req['sort_by'] = sort_by
                req['order_by'] = order_by
        if visualization_type == "Summary":
            req.update({
            'page': request_data.get('page'),
            'per_page': request_data.get('per_page'),
            'order_by': request_data.get('order_by'),
            'sort_by': request_data.get('sort_by'),
            'filter_by': request_data.get('filter_by'),
            'search_by': request_data.get('search_by')
        })     
        else:            
            maximum_no_of_items = visual_parameters[0].get("maximum_no_of_items", None)
            if maximum_no_of_items:
                req.update({
                'page': 1,
                'per_page': maximum_no_of_items
            }) 
        pagination = Pagination(**req)
        key_map = self.keyset_map.get(collection_name, {})
        framed_query = DB_Utility.frame_get_query(pagination, key_map) 
        combined_query = {
        '$and': [
            generate_query.get('$and', [])[0] if generate_query.get('$and') else {},
            framed_query.get('$and', [])[0] if framed_query.get('$and') else {}
        ]
        } if framed_query else generate_query
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[collection_name],combined_query,pagination,projection,self.keyset_map[collection_name])
        if count > 0:            
            return DB_Utility.convert_object_ids_to_strings(docs),count
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
        
    def preview_query_result_for_sql(self,request_data,identity_data=None):
        if identity_data:
            identity_data_obj = ACCESS_TOKEN(**identity_data) 
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)  
        query_information = request_data.get('query_information')
        if not query_information:
            raise Custom_Error("Please provide query_information")
        collection_name = query_information.get("conditions")[0].get("table")
        if not collection_name:
            raise Custom_Error("Collection name is missing in the rules.")
        col_names = Utility.list_to_str(query_information["col_names"]) if "col_names" in query_information and query_information["col_names"] else '*'
        generate_query = f"select {col_names} from {collection_name}"
        if query_information.get('conditions') and query_information.get('rules'):
            query = self.generate_sql_query(query_information)
            if query:
                generate_query = f" {generate_query} where {query}"
        docs = self.sql_db[request_data.get('db_name')].fetch_data(generate_query)
        if docs and len(docs)>0:            
            return docs,len(docs)
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
    
    def generate_sql_query(self,query_information):
        operation = query_information.get('operation')
        query_operator = 'AND' if operation == 'AND' else 'OR'
        conditions = []
        for cond in query_information.get('conditions', []):
            field = cond['field']
            value = cond['value']
            operator = cond['operator']
            if isinstance(value, str):
                value = f"'{value}'"
            condition = f"{field} {operator} {value}"
            conditions.append(condition)
        for sub_rule in query_information.get('rules', []):
            sub_query = self.generate_sql_query(sub_rule)
            if sub_query:
                conditions.append(f"({sub_query})")
        if conditions:
            query = f" {query_operator} ".join(conditions)
            return f"({query})"
        return ""