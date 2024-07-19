import json
import logging
from flask import jsonify
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Table, and_, or_
from NeoAdept.pojo.ui_template.dynamic_widget import DYNAMIC_WIDGET
from NeoAdept.pojo.user.access_token import ACCESS_TOKEN
from NeoAdept.pojo.user.user_details import USER_DETAILS
from NeoAdept.services.common_service import Common_Service
import pymongo
from NeoAdept.gbo.bo import Base_Response, Pagination
from NeoAdept.gbo.common import Custom_Error
from NeoAdept.requests.dynamic_widget_request import create_dynamic_widget_request, delete_dynamic_widget_request, update_dynamic_widget_request
from NeoAdept.utilities.constants import CONSTANTS
from NeoAdept.utilities.db_utility import DB_Utility, Mongo_DB_Manager
from NeoAdept.utilities.utility import Utility


class Dynamic_widget_Service:
     
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance
    
    def __init__(self,logger,db,keyset_map,keyset_map_dt,session,sql_db):
        self.logger = logger
        self.dynamic_widget_collection = "DYNAMIC_WIDGET"
        self.key_nested_key_map = keyset_map
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
        self.common_service = Common_Service(logger,db,keyset_map)
        if "DYNAMIC_WIDGET" in keyset_map:
            self.key_map = self.key_nested_key_map["DYNAMIC_WIDGET"]
        self.session = session
        self.sql_db = sql_db
 
    def get_dynamic_widget(self,identity_data,request_data,db):

        identity_data_obj = ACCESS_TOKEN(**identity_data)
        
        pagination = Pagination(**request_data)
    
        self.common_service.create_log_details(identity_data_obj.email,request_data,"get_dynamic_widget",db)
        
        dynamic_widget_collection = db[self.dynamic_widget_collection]
        
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        
        docs,count = Mongo_DB_Manager.get_paginated_data1(dynamic_widget_collection,query,pagination) 

        if docs and len(docs)>0:
            #count = Mongo_DB_Manager.count_documents(dynamic_widget_collection,query)
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
        
        flag = 0
        list_of_keys = []
        if getattr(dynamic_widget_data_obj, 'name', None):
            list_of_keys.append("name")
            flag = 1
        
            
        if flag == 0:
            query = {"_id": _id}
        else:
            query = DB_Utility.update_keys_check(dynamic_widget_data_obj,list_of_keys,_id)
            
        cursor = Mongo_DB_Manager.read_documents(db[self.dynamic_widget_collection],query)
        existing_dynamic_widgets = list(cursor)
                 
        if _id not in [dynamic_widget['_id'] for dynamic_widget in existing_dynamic_widgets]:
            raise Custom_Error('Dynamic widget not found')
        
        for existing_dynamic_widget in list(existing_dynamic_widgets):
            if existing_dynamic_widget["_id"] != _id:
                raise Custom_Error('name already exists for other documents')
            existing_doc = existing_dynamic_widget
            
        query_information = dynamic_widget_data_obj.query_information
        if query_information:
            collection_name = query_information.get("conditions")[0].get("table")
            is_mongo_db = True
            if "db_type" in dynamic_widget_data and dynamic_widget_data["db_type"]=='SQL':
                is_mongo_db = False
            self.validate_query_information(query_information, collection_name,is_mongo_db)

        DB_Utility.remove_extra_attributes(dynamic_widget_data_obj.__dict__,dynamic_widget_data)
        del dynamic_widget_data_obj._id        
        
        dynamic_widget_data_obj.updated_on = Utility.get_current_time()
        dynamic_widget_data_obj.updated_by = email_from_token
                
        query =  {"_id": _id}  
        result = Mongo_DB_Manager.update_document(db[self.dynamic_widget_collection], query, dynamic_widget_data_obj.__dict__)
        if result == 0:
            raise Custom_Error('Could not update dynamic widget')  

        data = Mongo_DB_Manager.read_one_document(db[self.dynamic_widget_collection],query)
        
        return DB_Utility.convert_obj_id_to_str_id(data)
            
    def save_dynamic_widget(self,dynamic_widget_data,db,email_from_token):

        dynamic_widget_request = create_dynamic_widget_request(dynamic_widget_data) 
        dynamic_widget_request.parse_request()
        dynamic_widget_request.validate_request()

        dynamic_widget_obj_details = dynamic_widget_request.dynamic_widget_obj

        query_information = dynamic_widget_data.get("query_information")
        collection_name = query_information.get("conditions")[0].get("table")
        is_mongo_db = True
        if "db_type" in dynamic_widget_data and dynamic_widget_data["db_type"]=='SQL':
            is_mongo_db = False
        self.validate_query_information(query_information, collection_name,is_mongo_db)
            
        fields_to_check = ["name"]
        query = DB_Utility.fields_to_check(dynamic_widget_obj_details,fields_to_check)
        existing_widget = Mongo_DB_Manager.read_one_document(db[self.dynamic_widget_collection], query)
        if existing_widget:
            field = next((field for field in fields_to_check if getattr(dynamic_widget_obj_details, field) == existing_widget[field]), None)
            if field:
                raise Custom_Error(f"{field} already exists")
            
        dynamic_widget_obj_details = Utility.upsert_by_on(dynamic_widget_obj_details,"add",email_from_token)         
        dynamic_widget_id = Mongo_DB_Manager.create_document(db[self.dynamic_widget_collection],dynamic_widget_obj_details.__dict__) 
        if not dynamic_widget_id:
            raise Custom_Error('Could not add dynamic_widget')
        
        data = Mongo_DB_Manager.read_one_document(db[self.dynamic_widget_collection],query)       
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
        rules = query_information.get("rules", [])
        if rules and is_mongo_db:
            self.validate_and_process_rules(rules, collection_name)      
        
    def validate_and_process_rules(self, rules, collection_name):
        keyset_map_dt = self.keyset_map_dt[collection_name]
        for rule in rules:
            if 'conditions' in rule:
                nested_conditions = rule['conditions']
                if not nested_conditions:
                    raise Custom_Error("conditions cannot be empty.")
                for nested_condition in nested_conditions:
                    missing_fields = [key for key in ['table', 'field', 'operator', 'value'] if key not in nested_condition]
                    if missing_fields:
                        raise Custom_Error(f"Invalid query: Missing fields {missing_fields} in nested condition.")
                    if nested_condition['table'] != collection_name:
                        raise Custom_Error("Collection names passed should be same.")

                self.validate_and_process_rules(rule['rules'], collection_name)
            else:
                missing_fields = [key for key in ['field', 'operator', 'value'] if key not in rule]
                if missing_fields:
                    raise Custom_Error(f"Invalid query: Missing fields {missing_fields} in rule.")

                field = rule['field']
                operator = rule['operator']

                field_datatype = keyset_map_dt.get(field)

                if field_datatype is None:
                    raise Custom_Error(f"No datatype found for field '{field}' in the keyset.")

                init_operations = self.datatype_operations.get(field_datatype)

                if operator not in init_operations:
                    raise Custom_Error(f"Operator '{operator}' not supported for field '{field}' with datatype '{field_datatype}'.")
            operation = rule.get('operation')
            if not operation:
                raise Custom_Error("Operation is missing or empty in rule.")
            
    def delete_dynamic_widget(self,identity_data,dynamic_widget_data,db):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)
                        
        dynamic_widget_data_request = delete_dynamic_widget_request(dynamic_widget_data) 
        dynamic_widget_data_request.parse_request()
        dynamic_widget_data_request.validate_request()
                   
        dynamic_widget_data_obj = DYNAMIC_WIDGET(**dynamic_widget_data)
        
        dynamic_widget_collection = db[self.dynamic_widget_collection]
        _id = dynamic_widget_data_obj._id
        query = {"_id": DB_Utility.str_to_obj_id(_id)}
                
        existing_dynamic_widget = Mongo_DB_Manager.read_one_document(dynamic_widget_collection,query)
        if not existing_dynamic_widget:
            raise Custom_Error("dynamic_widget not found")

        existing_dynamic_widget_obj = DYNAMIC_WIDGET(**existing_dynamic_widget)
        if existing_dynamic_widget_obj.is_deleted == CONSTANTS.TRUE:
            raise Custom_Error("dynamic_widget is already deleted")
                       
        dynamic_widget_data_obj.updated_on = Utility.get_current_time()
        dynamic_widget_data_obj.updated_by = email_from_token
        dynamic_widget_data_obj.is_deleted = True
                                                                                                    
        attributes_to_delete = ["_id","name","file_name","info","type","class_name","description","query_information","visual_type","visual_parameters","created_by","created_on","db_type"]
        DB_Utility.delete_attributes_from_obj(dynamic_widget_data_obj,attributes_to_delete)  
        
        result = Mongo_DB_Manager.update_document(dynamic_widget_collection, query, dynamic_widget_data_obj.__dict__)
        if result != 1:
            raise Custom_Error('Could not delete dynamic_widget')
    
    def generate_mongodb_query1(self, query_information, keyset_map_dt,key_nested_key_map):
        
        query = {}
        operation = query_information.get('operation')
        query_operator = '$and' if operation == 'AND' else '$or'
        query[query_operator] = []
        
        for cond in query_information.get('conditions', []):
            field = cond['field']
            value = cond['value']
            operator = cond['operator']
            
            # Map 'not in' to '$nin'
            if operator == 'not in':
                operator = 'nin'
            
            field_datatype = keyset_map_dt.get(field)
            field_name = key_nested_key_map.get(field)
        
            init_operations = self.datatype_operations.get(field_datatype)
            operation = init_operations.get(operator)
            
            # Adjusting condition based on the operator type
            if operator in ['in', 'nin']:
                # Ensure value is a list for 'in' and 'not in' operators
                if not isinstance(value, list):
                    raise Custom_Error(f"Value for operator '{operator}' must be a list")
                query_condition = {field_name: {operation: value}}
            else:
                query_condition = {field_name: {operation: value}}
        
            #query_condition = {field_name: {operation: value}}
            query[query_operator].append(query_condition)
        
        for sub_rule in query_information.get('rules', []):
            sub_query = self.generate_mongodb_query1(sub_rule, keyset_map_dt, key_nested_key_map)
            if sub_query:
                query[query_operator].append(sub_query)
        
        return query 
    
    def get_query_from_dynamic_widget(self,identity_data,id,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data) 
        _id = DB_Utility.str_to_obj_id(id)
        query = {"_id": _id}
        doc = Mongo_DB_Manager.read_one_document(db[self.dynamic_widget_collection], query)
        if not doc:
            raise Custom_Error("No data found for _id")
        
        query_information = doc.get("query_information")  
        if not query_information:
            raise Custom_Error("query_information not available for widget")
        
        collection_name = query_information.get("conditions")[0].get("table")
        if not collection_name:
            raise Custom_Error("collection not available in query_information")
        
        generate_query = "select * from {collection_name}"
        if query_information.get('conditions') and query_information.get('rules'):
            query = self.generate_sql_query(query_information)
            if query:
                generate_query = f" where {query}"
                
        return generate_query
        
    def generate_dynamic_widget_query1(self,identity_data,id,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data) 
        _id = DB_Utility.str_to_obj_id(id)
        query = {"_id": _id}
        
        doc = Mongo_DB_Manager.read_one_document(db[self.dynamic_widget_collection], query)
        if not doc:
            raise Custom_Error("No data found for _id")
        
        query_information = doc.get("query_information")  
        if not query_information:
            raise Custom_Error("query_information not available for widget")
        
        collection_name = query_information.get("conditions")[0].get("table")
        if not collection_name:
            raise Custom_Error("collection not available in query_information")
        
        if "db_type" in doc and doc["db_type"]=="SQL":
            return self.preview_query_result_for_sql(doc,identity_data)
        
        generate_query = {}
        if not query_information.get('conditions') and not query_information.get('rules'):
            query = {}
        else:
            generate_query = self.generate_mongodb_query1(query_information, self.keyset_map_dt[collection_name], self.key_nested_key_map[collection_name])
        
        #print("dynamic query",generate_query)
        # Determine projection based on identity_data_obj.widget_enable_for_db
        #widget_enable_for_db = identity_data_obj.widget_enable_for_db
        if self.session.widget_enable_for_db is None:
            return Base_Response(status=CONSTANTS.FAILED, status_code=403, message="Session expired.Please log in again").__dict__
            #return Utility.generate_error_response("Session expired.Please log in again")
        widget_enable_for_db = self.session.widget_enable_for_db
        projection = { "_id": 1 }  # Always include the _id field
        if collection_name in widget_enable_for_db and widget_enable_for_db[collection_name].get("widget_enable"):
            for col in widget_enable_for_db[collection_name].get("columns", []):
                if col.get("widget_enable", True):
                    projection[col["db_column"]] = 1
        else:
            raise Custom_Error('The user does not have access to collection')
        
        visualization_type = doc.get("visual_type") or "Summary"
        visual_parameters = doc.get("visual_parameters",[])
        req = {}
        if visual_parameters:
            sort_by = visual_parameters[0].get("sort_by_column")
            order_by = visual_parameters[0].get("order_by", "asc")
            if sort_by and order_by:
                req['sort_by'] = sort_by
                req['order_by'] = order_by
        
        if visualization_type == "Summary":
            req['page']=request_data.get('page')
            req['per_page'] = request_data.get('per_page')
            if request_data.get('order_by'):
                req['order_by'] = request_data.get('order_by')
            if request_data.get('sort_by'):
                req['sort_by'] = request_data.get('sort_by')
            if request_data.get('filter_by'):
                req['filter_by'] = request_data.get('filter_by')
            if request_data.get('search_by'):
                req['search_by'] = request_data.get('search_by')
                
        else:            
        #if visualization_type == 'Ring_Chart':
            maximum_no_of_items = visual_parameters[0].get("maximum_no_of_items", None)
            if maximum_no_of_items:
                req['page'] = 1
                req['per_page'] = maximum_no_of_items
            
        pagination = Pagination(**req)
        
        if collection_name in self.key_nested_key_map:
            key_map = self.key_nested_key_map[collection_name]
            
        query = DB_Utility.frame_get_query(pagination,key_map)
        #print("framed query::",query)  
       
        if query:
            combined_query = {
                '$and': [
                    generate_query['$and'][0],  # Extract the condition from dynamic_query
                    query['$and'][0]    # Extract the condition from framed_query
                ]
            }
        else:
            combined_query = generate_query
        #print("combined_query:::",combined_query)

        docs,count = Mongo_DB_Manager.get_paginated_data1(db[collection_name],combined_query,pagination,projection,self.key_nested_key_map[collection_name])
               
        if docs and len(docs)>0:            
            return DB_Utility.convert_object_ids_to_strings(docs),count
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
    
    def preview_query_result(self,request_data,db,identity_data):
        
        identity_data_obj = ACCESS_TOKEN(**identity_data) 
        email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)  
        
        query_information = request_data.get('query_information')
        if not query_information:
            raise Custom_Error("Please provide query_information")
        
        collection_name = query_information.get("conditions")[0].get("table")
        if not collection_name:
            raise Custom_Error("Collection name is missing in the rules.")

        if not query_information.get('conditions') and not query_information.get('rules'):
            query = {}
        else:
            generate_query = self.generate_mongodb_query1(query_information, self.keyset_map_dt[collection_name], self.key_nested_key_map[collection_name])
        
        # Determine projection based on identity_data_obj.widget_enable_for_db
        #widget_enable_for_db = identity_data_obj.widget_enable_for_db
        if self.session.widget_enable_for_db is None:
            return Base_Response(status=CONSTANTS.FAILED, status_code=403, message="Session expired.Please log in again").__dict__
            #return Utility.generate_error_response("Session expired.Please log in again")
        
        widget_enable_for_db = self.session.widget_enable_for_db
        
        projection = { "_id": 1 }  # Always include the _id field
        if collection_name in widget_enable_for_db and widget_enable_for_db[collection_name].get("widget_enable"):
            for col in widget_enable_for_db[collection_name].get("columns", []):
                if col.get("widget_enable", True):
                    projection[col["db_column"]] = 1
        else:
            raise Custom_Error('The user does not have access to collection')
        
        visualization_type = request_data.get("visual_type") or "Summary"
        visual_parameters = request_data.get("visual_parameters",[])
        req = {}
        if visual_parameters:
            sort_by_column = visual_parameters[0].get("sort_by_column")
            order_by = visual_parameters[0].get("order_by", "asc")
            if sort_by_column and order_by:
                req['sort_by'] = sort_by_column
                req['order_by'] = order_by
        
        if visualization_type == "Summary":
            req['page']=request_data.get('page')
            req['per_page'] = request_data.get('per_page')
            if request_data.get('order_by'):
                req['order_by'] = request_data.get('order_by')
            if request_data.get('sort_by'):
                req['sort_by'] = request_data.get('sort_by')
            if request_data.get('filter_by'):
                req['filter_by'] = request_data.get('filter_by')
            if request_data.get('search_by'):
                req['search_by'] = request_data.get('search_by')
        
        else:            
        #if visualization_type == 'Ring_Chart':
            maximum_no_of_items = visual_parameters[0].get("maximum_no_of_items", None)
            if maximum_no_of_items:
                req['page'] = 1
                req['per_page'] = maximum_no_of_items
        #allowed_fields = {'page', 'per_page', 'sort_by', 'order_by', 'filter_by', 'search_by'}
        #request_data = {key: value for key, value in request_data.items() if key in allowed_fields}
        
        pagination = Pagination(**req)
        
        if collection_name in self.key_nested_key_map:
            key_map = self.key_nested_key_map[collection_name]
            
        query = DB_Utility.frame_get_query(pagination,key_map)
       
        if query:
            combined_query = {
                '$and': [
                    generate_query['$and'][0],  # Extract the condition from dynamic_query
                    query['$and'][0]    # Extract the condition from framed_query
                ]
            }
        else:
            combined_query = generate_query
        
        docs,count = Mongo_DB_Manager.get_paginated_data1(db[collection_name],combined_query,pagination,projection,self.key_nested_key_map[collection_name])
        #print(docs,count)      
        if docs and len(docs)>0:            
            return DB_Utility.convert_object_ids_to_strings(docs),count
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
        
    def preview_query_result_for_sql(self,request_data,identity_data=None):
        
        if identity_data:
            identity_data_obj = ACCESS_TOKEN(**identity_data) 
            email_from_token, role_from_token = Utility.get_data_from_identity(identity_data_obj)  
        
        query_information = request_data.get('query_information')
        if not query_information:
            raise Custom_Error("Please provide query_information")
        
        #collection_name = query_information.get("collection")
        collection_name = query_information.get("conditions")[0].get("table")
        if not collection_name:
            raise Custom_Error("Collection name is missing in the rules.")
        
        col_names = '*'
        if "col_names" in query_information and query_information["col_names"]:
            col_names = Utility.list_to_str(query_information["col_names"])

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
                
    def get_table(self,table_name):
        if table_name not in self.metadata.tables:
            columns = []
            for column_info in self.inspector.get_columns(table_name):
                name = column_info['name']
                col_type = column_info['type']
                if isinstance(col_type, String):
                    col_type = String(col_type.length)
                elif isinstance(col_type, Integer):
                    col_type = Integer()
                elif isinstance(col_type, Boolean):
                    col_type = Boolean()
                elif isinstance(col_type, DateTime):
                    col_type = DateTime()
                elif isinstance(col_type, Float):
                    col_type = Float()
                else:
                    col_type = String()  # Default type if unrecognized
                columns.append(Column(name, col_type))
            table = Table(table_name, self.metadata, *columns, autoload_with=self.engine)
        else:
            table = self.metadata.tables[table_name]
        return table

    def build_condition(self,condition):
        #table = self.get_table(condition['table'])
        #column = table.c[condition['field']]
        column = condition['field']
        operator = condition['operator']
        value = condition['value']
        
        if operator == '=':
            return column == value
        elif operator == '!=':
            return column != value
        elif operator == '>':
            return column > value
        elif operator == '<':
            return column < value
        elif operator == '>=':
            return column >= value
        elif operator == '<=':
            return column <= value
        elif operator == 'like':
            return column.like(value)
        # Add more operators as needed

    def build_conditions(self,conditions, operation):
        condition_list = [self.build_condition(cond) for cond in conditions]
        if operation == 'AND':
            return and_(*condition_list)
        elif operation == 'OR':
            return or_(*condition_list)

    def build_rules(self,rules, operation):
        rule_list = []
        for rule in rules:
            conditions = self.build_conditions(rule['conditions'], rule['operation'])
            nested_rules = self.build_rules(rule['rules'], rule['operation']) if rule['rules'] else None
            if nested_rules:
                rule_list.append(and_(conditions, nested_rules) if rule['operation'] == 'AND' else or_(conditions, nested_rules))
            else:
                rule_list.append(conditions)
        if operation == 'AND':
            return and_(*rule_list)
        elif operation == 'OR':
            return or_(*rule_list)
        
"""def generate_mongodb_query(self,rules,keyset_map_dt):
        query = {}        
        condition = rules.get('condition', 'and').lower()
        query_operator = '$and' if condition == 'and' else '$or'
        query[query_operator] = []
        for rule in rules['rules']:
            if 'condition' in rule:
                sub_query = self.generate_mongodb_query(rule,keyset_map_dt)
                if sub_query:
                    sub_query_operator = None
                    for op in ['$and', '$or']:
                        if op in sub_query and sub_query[op]:
                            sub_query_operator = op
                            break
                    if sub_query_operator:
                        query[query_operator].append(sub_query)
            else:
                missing_fields = [key for key in ['field', 'operator', 'value'] if key not in rule]
                if missing_fields:
                    raise ValueError(f"Invalid query: Missing fields {missing_fields} in {rule}")
                field = rule['field'] 
                value = rule['value']
                operator = rule['operator']
                field_datatype = keyset_map_dt[field]
                if field_datatype is not None:
                    field_with_datatype = f"{field}: {field_datatype}"
                    print("field_with_datatype_______________________________", field_with_datatype)
                else:
                    print(f"No datatype found for field '{field}' in the keyset.")

                if field_datatype == 'str':
                    print("Performing string related operations...")
        # Example operations for string datatype
                    if operator == '=':
                        query[query_operator].append({field: value})
                    elif operator == '==':
                        query[query_operator].append({field: {'$eq': value}})
                    elif operator == '!=':
                        query[query_operator].append({field: {'$ne': value}})
                    elif operator == 'in':
                        query[query_operator].append({field: {'$in': value}})
                    elif operator == 'nin':
                        query[query_operator].append({field: {'$nin': value}})
                    elif operator == 'exists':
                        query[query_operator].append({field: {'$exists': value}})
                    elif operator == 'regex':
                        query[query_operator].append({field: {'$regex': value}})
                    else:
                        print(f"Operator '{operator}' not supported for string datatype.")
                
                elif field_datatype == 'int' or field_datatype == 'float':
                    # Integer related operations
                    if operator == '=':
                        query[query_operator].append({field: value})
                    elif operator == '==':
                        query[query_operator].append({field: {'$eq': value}})
                    elif operator == '!=':
                        query[query_operator].append({field: {'$ne': value}})
                    elif operator == '>':
                        query[query_operator].append({field: {'$gt': value}})
                    elif operator == '<':
                        query[query_operator].append({field: {'$lt': value}})
                    elif operator == '>=':
                        query[query_operator].append({field: {'$gte': value}})
                    elif operator == '<=':
                        query[query_operator].append({field: {'$lte': value}})
                    elif operator == 'in':
                        query[query_operator].append({field: {'$in': value}})
                    elif operator == 'nin':
                        query[query_operator].append({field: {'$nin': value}})
                    elif operator == 'exists':
                        query[query_operator].append({field: {'$exists': value}})
                    else:
                        print(f"Operator '{operator}' not supported for integer datatype.")
               
                elif field_datatype == 'bool':
                    print("Performing boolean related operations...")
        # Example operations for bool datatype
                    if operator == '=':
                        query[query_operator].append({field: value})
                    elif operator == '==':
                        query[query_operator].append({field: {'$eq': value}})
                    elif operator == '!=':
                        query[query_operator].append({field: {'$ne': value}})
                    elif operator == 'exists':
                        query[query_operator].append({field: {'$exists': value}})
                    else:
                        print(f"Operator '{operator}' not supported for boolean datatype.")
                    # Boolean related operations
                    print("Performing boolean related operations...")
                elif field_datatype == 'date' or field_datatype == 'datetime':
                    if operator == '=':
                        query[query_operator].append({field: value})
                    elif operator == '==':
                        query[query_operator].append({field: {'$eq': value}})
                    elif operator == '!=':
                        query[query_operator].append({field: {'$ne': value}})
                    elif operator == '>':
                        query[query_operator].append({field: {'$gt': value}})
                    elif operator == '<':
                        query[query_operator].append({field: {'$lt': value}})
                    elif operator == '>=':
                        query[query_operator].append({field: {'$gte': value}})
                    elif operator == '<=':
                        query[query_operator].append({field: {'$lte': value}})
                    elif operator == 'exists':
                        query[query_operator].append({field: {'$exists': value}})
                    else:
                        print(f"Operator '{operator}' not supported for date datatype.")
                elif field_datatype == 'list':
                    # Example operations for list datatype
                    if operator == 'in':
                        query[query_operator].append({field: {'$in': value}})
                    elif operator == 'nin':
                        query[query_operator].append({field: {'$nin': value}})
                    elif operator == 'exists':
                        query[query_operator].append({field: {'$exists': value}})
                    else:
                        print(f"Operator '{operator}' not supported for list datatype.")
                elif field_datatype == 'dict' or field_datatype == 'array':
                    if operator == 'exists':
                        query[query_operator].append({field: {'$exists': value}})
                    else:
                        print(f"Operator '{operator}' not supported for dictionary datatype or array.")
                elif field_datatype == 'objectid':
                    if operator == '=':
                        query[query_operator].append({field: value})
                    elif operator == '==':
                        query[query_operator].append({field: {'$eq': value}})
                    elif operator == '!=':
                        query[query_operator].append({field: {'$ne': value}})
                    elif operator == 'exists':
                        query[query_operator].append({field: {'$exists': value}})
                    else:
                        print(f"Operator '{operator}' not supported for ObjectId datatype.")
                else:
                    print(f"No datatype found for field '{field}' in the keyset.")
                    
        return query  """


"""main_collection = query_information.get("main_collection")
        rules = query_information.get("rules", [])

        queries = []

        for rule in rules:
            if "field" in rule:
                field = rule.get("field")
                operator = rule.get("operator")
                value = rule.get("value")

                if field and operator and value:
                    match_query = {field: {f"${operator}": value}}
                    queries.append({"collection": main_collection, "match": match_query})

            elif "lookup_collection" in rule:
                lookup_collection = rule.get("lookup_collection")
                nested_rules = rule.get("rules", [])
                nested_queries = []

                for nested_rule in nested_rules:
                    print("nested_rule", nested_rule)
                    if 'condition' in nested_rule and 'rules' in nested_rule:
                        # Handle condition and rules
                        condition = nested_rule['condition']
                        sub_rules = nested_rule['rules']

                        # Generate MongoDB query for sub-rules recursively
                        sub_queries = self.generate_nested_queries(sub_rules, lookup_collection)
                        
                        # Check if there are any sub-queries generated
                        if sub_queries:
                # Apply the condition to the sub-queries
                            if condition == 'and':
                                nested_queries.extend(sub_queries)
                            elif condition == 'or':
                                nested_queries.append({"$or": sub_queries})
                    elif nested_rule.get("field") and nested_rule.get("operator") and nested_rule.get("value"):
            # Handle individual nested rule
                        nested_match_query = {nested_rule["field"]: {f"${nested_rule['operator']}": nested_rule["value"]}}
                        nested_queries.append({"collection": lookup_collection, "match": nested_match_query})

                # Append the nested queries if there are any
                if nested_queries:
                    queries.append({"nested_queries": nested_queries})
            elif "condition" in rule:
                condition = rule.get("condition")
                sub_rules = rule.get("rules", [])

                sub_queries = self.generate_mongodb_query({"main_collection": main_collection, "rules": sub_rules})

                if condition == "or":
                    queries.append({"$or": sub_queries})
                elif condition == "and":
                    queries.append({"$and": sub_queries})

        return queries
    
    def generate_nested_queries(self, rules, lookup_collection):
        nested_queries = []
        for nested_rule in rules:
            if 'condition' in nested_rule and 'rules' in nested_rule:
                # Handle condition and rules
                condition = nested_rule['condition']
                sub_rules = nested_rule['rules']

                # Generate MongoDB query for sub-rules recursively
                sub_queries = self.generate_nested_queries(sub_rules, lookup_collection)
                
                # Check if there are any sub-queries generated
                if sub_queries:
                    # Apply the condition to the sub-queries
                    if condition == 'and':
                        nested_queries.extend(sub_queries)
                    elif condition == 'or':
                        nested_queries.append({"$or": sub_queries})
            elif nested_rule.get("field") and nested_rule.get("operator") and nested_rule.get("value"):
                # Handle individual nested rule
                nested_match_query = {
                    nested_rule["field"]: {f"${nested_rule['operator']}": nested_rule["value"]}
                }
                nested_queries.append({"collection": lookup_collection, "match": nested_match_query})

        return nested_queries"""



            
"""def generate_mongodb_query(self,json_structure):

        pipeline = []

        # Recursively build pipeline stages
        for rule in json_structure["rules"]:
            if "collection" in rule:
                # Handle nested rules
                if "rules" in rule:
                    nested_pipeline = self.generate_mongodb_query(rule)
                    if rule["condition"] == "and":
                        pipeline.extend(nested_pipeline)
                    elif rule["condition"] == "or":
                        pipeline.append({"$or": nested_pipeline})
                else:
                    if "collection" in json_structure:
                        collection = json_structure["collection"]
                        # Construct match stage
                        match_stage = {"$match": {rule["field"]: {f"${rule['operator']}": rule['value']}}}
                        pipeline.append(match_stage)
                    else:
                        raise ValueError("Collection not specified in rule")
            else:
                raise ValueError("Collection not specified in rule")

        return pipeline"""


        
    