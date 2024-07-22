from contextlib import contextmanager
import re,pymongo, pandas as pd,numpy as np,io

from ..gbo.common import Custom_Error
from flask import json, send_file
from datetime import datetime
from bson import ObjectId
from pymongo import DESCENDING, MongoClient,UpdateOne
from pymongo.database import Database


from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from contextlib import contextmanager
from sqlalchemy.engine import reflection

from ..gbo.bo import Base_Response
from ..config import Config
from ..gbo.bo import Pagination
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility

class SQL_Connection_Manager:
    _instance = None  # Class variable to store the singleton instance
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance
    
    def __init__(self,db_url):
        if not hasattr(self, 'initialized'):
            self.db_url = db_url
            self.engine = create_engine(self.db_url, pool_pre_ping=True)
            self.scoped_session = scoped_session(sessionmaker(bind=self.engine))
            self.inspector = reflection.Inspector.from_engine(self.engine) 
        
    @contextmanager
    def create_session(self):
        session = self.scoped_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            #print(f"Database error: {str(e)}")
            raise e
        finally:
            session.close()

class SQL_Utility:
    _instance = None  # Class variable to store the singleton instance
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,connection_manager):
        if not hasattr(self, 'initialized'):
            self.connection_manager = connection_manager
        
    def fetch_data(self,query):
        try:
            with self.connection_manager.create_session() as session:
                if type(query) is text:
                    result_proxy = session.execute(query)
                else:
                    result_proxy = session.execute(text(query))
            columns = result_proxy.keys()
            values = result_proxy.fetchall()
            rows = [dict(zip(columns, row)) for row in values]
        except Exception as e:
            print(f"Database error: {str(e)}")
            return Utility.generate_exception_response(e)
        return rows

    def get_count(self,query):
        try:
            session=self.db.get_session()
            total_count = session.execute(text(query)).scalar()
            return total_count
        except Exception as e:
        # Handle database exceptions
            print(f"Database error: {str(e)}")
        finally:
            session.close()

class Collection_Manager:
    def __init__(self,config:Config=None):
        if config:
            self.config = config
        
    def connect_db(self,db_name):         
        db = self.configure_client(self.config.db_url,self.config.max_pool_size)[db_name]
        # self.add_collection(db,self.config.collection_names)
        return db
    
    def configure_client(self,db_url,max_pool_size):
        return MongoClient(db_url,maxPoolSize=max_pool_size)
    
    @staticmethod
    def create_view(db,collection_name,view_name,pipeline):
        if view_name not in db.list_collection_names() or Mongo_DB_Manager.is_collection_empty(db[view_name]) : 
            db.command({
    "create": view_name,  # Name of the view
    "viewOn": collection_name,       # Name of the original collection
    "pipeline": pipeline
})

class DB_Utility:
    
    id_map = {None:'contact_id'}
    
    @staticmethod
    def fields_to_check(obj,fields_to_check):
        return {"$or": [{field: getattr(obj, field)} for field in fields_to_check], "is_deleted": False}

    @staticmethod
    def delete_attributes_from_obj(obj,attributes_to_delete):
        [delattr(obj, attr) for attr in attributes_to_delete]
        return obj
        
    @staticmethod
    def frame_get_query(pagination:Pagination,key_map):
        query = {}
        if pagination.filter_by:
            updated_filter_by = Utility.update_filter_keys(pagination.filter_by,key_map)
            for item in updated_filter_by:
                if "_id" in item:
                    item["_id"] = [ObjectId(str(val)) for val in item["_id"]]
            filter_query = DB_Utility.build_filtered_data_query(updated_filter_by) 
            query['$and'] = [filter_query]
        if pagination.search_by and pagination.search_by != "":                
            search_query = DB_Utility.frame_search_query(pagination.search_by,key_map,pagination.search_type)
            if '$and' in query:
                query['$and'].append(search_query)  # Add to $and if it already exists
            else:
                query = search_query 
        return query

    @staticmethod
    def convert_object_ids_to_strings(data,id="_id"): 
        for item in data:
            if item.items():
                for key, value in item.items():
                    if key==id or isinstance(value, ObjectId):
                        break
                if value:
                    item["_id"] = str(value)  # Convert ObjectId to string
        return data

    @staticmethod
    def convert_doc_to_cls_obj(documents,data_class_obj,projection=None):
        id = DB_Utility.id_map.get(data_class_obj,"_id")
        dataclass_objects = []
        if not isinstance(documents, list):
            documents = Utility.ensure_list(documents)
        if projection:
            return DB_Utility.convert_object_ids_to_strings(documents,id)
        else:
            for document in documents:           
                if id in document:               
                    document['_id'] = str(document[id])
                for key, value in document.items():
                    if isinstance(value, datetime):
                        document[key] = value.strftime("%Y-%m-%d %H:%M:%S.%f")
                dataclass_objects.append(data_class_obj(**document))
        if not dataclass_objects:
            print("error")
        return dataclass_objects
    
    @staticmethod
    def convert_obj_id_to_str_id(document):
        if '_id' in document:
                document['_id'] = str(document['_id'])
        return document

    @staticmethod
    def str_to_obj_id(_id):
        return ObjectId(_id)

    @staticmethod
    def obj_id_to_str(_id):
        return str(_id)
    
    @staticmethod
    def str_id_list_to_obj_list(_id_list):
        return [ObjectId(_id) for _id in _id_list]

    @staticmethod
    def custom_encoder(obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()

    @staticmethod
    def build_aggregation_pipeline(field_path):
        #handle array of object keys
        pipeline = []
        for field in reversed(field_path):
            if isinstance(field, dict):
                # Handle nested objects
                for key in field.keys():
                    pipeline.append({"$unwind": f"${key}"})
            else:
                pipeline.append({"$group": {"_id": f"${field}"}})
        pipeline.append({"$project": {"_id": 0, "distinct_values": "$_id"}})
        return pipeline

    @staticmethod    
    def create_query(filter_by):
        query = {}
        for filter_item in filter_by:
            for key, value in filter_item.items():
                if isinstance(value, dict):  # If value is a dictionary, it indicates nested keys
                    nested_query = DB_Utility.create_query([value])  # Recursively create query for nested keys
                    query[key] = nested_query
                elif isinstance(value, list):  # If value is a list, it indicates multiple values
                    query[key] = {"$in": value}  # Use $in operator to match any value in the list
                else:
                    query[key] = value
        return query

    @staticmethod
    def build_filtered_data_query(filter_by_req):
        query = {}      
        if len(filter_by_req) > 0:
            filter_by_req = Utility.ensure_list(filter_by_req)
            for filter_item in filter_by_req:
                for key, values in filter_item.items():
                    query[key] = {"$in": values}
        return query
    
    @staticmethod
    def build_update_query_for_bulk_opr(query,data):
        return UpdateOne(query, {'$set':data})

    @staticmethod
    def extract_all_keys_from_json(doc, parent_key=""):
        keys = {}
        if isinstance(doc, dict):
            for key, value in doc.items():
                current_key = f"{parent_key}.{key}" if parent_key else key
                if isinstance(value, (dict, list)):
                    nested_keys = DB_Utility.extract_all_keys_from_json(value, parent_key=current_key)
                    keys.update(nested_keys)
                else:
                    keys[key] = current_key
        elif isinstance(doc, list):
            if all(isinstance(item, (int, float, str, bool)) for item in doc):
                keys[parent_key] = parent_key
            else:
                if all(isinstance(item, dict) for item in doc):
                    for i, item in enumerate(doc):
                        current_key = f"{parent_key}"
                        nested_keys = DB_Utility.extract_all_keys_from_json(item, parent_key=current_key)
                        keys.update({f"{k}": v for k, v in nested_keys.items()})
                else:
                    keys[parent_key] = parent_key
        return keys
    
    @staticmethod
    def extract_all_keys_from_json_with_dt(doc, parent_key=""):
        keys = {}
        if isinstance(doc, dict):
            for key, value in doc.items():
                current_key = f"{parent_key}.{key}" if parent_key else key
                if isinstance(value, (dict, list)):
                    nested_keys = DB_Utility.extract_all_keys_from_json_with_dt(value, parent_key=current_key)
                    keys.update(nested_keys)
                else:
                    #keys[current_key] = (current_key, type(value).__name__)
                    keys[key] = type(value).__name__
        elif isinstance(doc, list):
            if all(isinstance(item, (int, float, str, bool)) for item in doc):
                keys[parent_key] = type(doc[0]).__name__
            else:
                if all(isinstance(item, dict) for item in doc):
                    for i, item in enumerate(doc):
                        current_key = f"{parent_key}"
                        nested_keys = DB_Utility.extract_all_keys_from_json_with_dt(item, parent_key=current_key)
                        keys.update({f"{k}": v for k, v in nested_keys.items()})
                else:
                    keys[parent_key] = type(item).__name__
        return keys
    
    @staticmethod
    def extract_all_keys_from_json_with_values(doc, parent_key=""):
        keys = {}
        if isinstance(doc, dict):
            for key, value in doc.items():
                current_key = f"{parent_key}.{key}" if parent_key else key
                if isinstance(value, (dict, list)):
                    nested_keys = DB_Utility.extract_all_keys_from_json_with_values(value, parent_key=current_key)
                    keys.update(nested_keys)
                else:
                    keys[current_key] = str(value)
        elif isinstance(doc, list):
            grouped_values = {}  # Initialize grouped_values outside the loop
            for i, item in enumerate(doc):
                if isinstance(item, dict):
                    for key, value in item.items():
                        nested_key = f"{parent_key}.{key}"
                        if isinstance(value, (dict, list)):
                            nested_keys = DB_Utility.extract_all_keys_from_json_with_values(value, parent_key=nested_key)
                            for nested_key, nested_value in nested_keys.items():
                                if nested_key not in grouped_values:
                                    grouped_values[nested_key] = []
                                grouped_values[nested_key].extend(nested_value)  # Extend the list with nested values
                        else:
                            if nested_key not in grouped_values:
                                grouped_values[nested_key] = []
                            grouped_values[nested_key].append(str(value))       
                else:
                    if parent_key not in grouped_values:
                        grouped_values[parent_key] = []
                    grouped_values[parent_key].append(str(item)) 
            for key, value in grouped_values.items():
                grouped_values[key] = ', '.join(value)
            keys.update(grouped_values)
        return keys
    
    @staticmethod
    def extract_parent_keys(doc):
        return doc.keys()
    
    def check_token(token,email,user_collection):
            query = {"email": email}
            current_user = Mongo_DB_Manager.read_one_document(user_collection,query)
            if current_user and current_user.get('token') != token:
                return Base_Response(status=CONSTANTS.FAILED,status_code=403,message="Please login again").__dict__
            return None
    
    @staticmethod
    def check_id_exists(id_list,collection) :
        id_list = Utility.ensure_list(id_list)        
        query = {'_id': {'$in': DB_Utility.str_id_list_to_obj_list(id_list) },**Utility.get_delete_false_query()}       
        return Mongo_DB_Manager.read_documents(collection,query) 
    
    @staticmethod
    def read_excel(request):
        
        excel_file = request.files['file']
        if excel_file and excel_file.filename.endswith('.xlsx'):
            
            df = pd.read_excel(excel_file)
            df = df.replace({np.nan: None})
            column_names = df.columns.tolist()
            documents = df.to_dict(orient='records')
            return documents,column_names
            
    @staticmethod
    def check_null_value_or_invalid_status_from_excel(doc, index, result_dict,keys):
        for key in keys:
            value = getattr(doc, key)
            
            if key != '_id' and  key not in CONSTANTS.KEYS_TO_REMOVE and (value is None or value == ''):
                status=f'Null or empty value found in document at index {index} for key {key}'
                DB_Utility.update_status(doc,result_dict, index,status)                
                return True 
            
            if key == 'status' and value not in ['active', 'inactive']:
                status = f'Invalid value found for status in document at index {index} for key {key}'
                DB_Utility.update_status(doc, result_dict, index, status)
                return True            
        return False
    
    @staticmethod
    def is_valid_email(email,doc,result_dict,index):
        if re.match(r'^[\w\.-]+@[a-zA-Z\d\.-]+\.[a-zA-Z]{2,}$', email):
            return False
        else:
            status = f'The email at index {index} is not valid'
            DB_Utility.update_status(doc, result_dict, index, status)
            return True

    @staticmethod
    def is_valid_mobile(mobile,doc,result_dict,index):
        if re.match(r'^[0-9]{10}$', str(mobile)):
            return False
        else:
            status = f'The mobile number at index {index} is not valid'
            DB_Utility.update_status(doc, result_dict, index, status)
            return True

    @staticmethod
    def update_status(doc, result_dict, index,status):
        doc.status_code = "201"
        doc.status = status
        result_dict[index] = doc.__dict__
        
    @staticmethod
    def get_data_in_excel(data,collection):
        # Convert DataFrame to Excel file in memory
        excel_buffer = io.BytesIO()
        json_data = json.loads(json.dumps(data, default=DB_Utility.custom_encoder, indent=4))
        df = pd.DataFrame(json_data)
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)  # Move the cursor to the beginning of the buffer
        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name=f'{collection}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    @staticmethod
    def update_keys_check(data_obj,list_of_keys,_id):
        
        or_conditions = [{"_id": _id}]
        and_condition = {
            "$and": [
                {"$or": [{key: getattr(data_obj, key)} for key in list_of_keys]},
                {"_id": {"$ne": _id}}
            ]
        }
        or_conditions.append(and_condition)
        query = {"$or": or_conditions}
        return query
    
    @staticmethod
    def remove_extra_attributes(obj_dict, reference_dict):
        
        attributes_to_delete = [key for key in obj_dict if key not in reference_dict]
        
        for attr in attributes_to_delete:
            del obj_dict[attr]
        return obj_dict
                
    def ids_exists(ids,collection):
            cursor = DB_Utility.check_id_exists(ids,collection)           
            existing_ids = [doc['_id'] for doc in cursor]           
            ids = DB_Utility.str_id_list_to_obj_list(ids)
            missing_ids = [_id for _id in ids if _id not in existing_ids]            
            return missing_ids if missing_ids else None
    
    def update_filter_by(key,value,request_data):
        filter_by = {key: [value]}
        if 'filter_by' not in request_data:            
            request_data ={"filter_by" :filter_by}
        else:
            request_data["filter_by"].append(filter_by)
        return request_data
    
    @staticmethod
    def remove_null_attributes(obj_dict):
        attributes_to_delete = [key for key, value in obj_dict.items() if value is None or value == "" or (isinstance(value, list) and not value)]
        for attr in attributes_to_delete:
            del obj_dict[attr]
        return obj_dict
    
    def apply_paginated_for_data(cursor,pagination:Pagination):
        page_number = pagination.page
        page_size = pagination.per_page
        sortby = pagination.sort_by
        order = pagination.order_by
        
        if sortby and order:
            if order.lower() == "asc":
                cursor = cursor.sort([(sortby, pymongo.ASCENDING)])
            elif order.lower() == "desc":
                cursor = cursor.sort([(sortby, pymongo.DESCENDING)])       
        
        if page_number and page_size:
            skip = (page_number - 1) * page_size
            cursor = cursor.skip(skip).limit(page_size)
        
        # Convert cursor to list of documents
        paginated_data = list(cursor)
        return paginated_data
    
    @staticmethod
    def frame_search_query(search_by,key_map,search_type="or"):
        search_condition = {}            
        conditions = []
        search_terms = search_by
        if isinstance(search_by,str):
            search_terms = [term.strip() for term in search_by.split(',')]
        for term in search_terms:
            term_conditions = [{value: {'$regex': term, '$options': 'i'}} for key, value in key_map.items()]
            conditions.append({'$or': term_conditions})
        if search_type == "or" :
            search_type = "$or"
        else:
            search_type = "$and"
        search_condition = {search_type: conditions}
        return search_condition
    
    @staticmethod
    def flatten_structure(value, prefix=''):
        flat_dict = {}
        if isinstance(value, dict):
            for k, v in value.items():
                flat_dict.update(DB_Utility.flatten_structure(v, prefix + k + '_'))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                flat_dict.update(DB_Utility.flatten_structure(item, prefix + f'item[{i}]_'))
        else:
            flat_dict[prefix[:-1]] = value  # Remove the trailing underscore from prefix
        return flat_dict

    @staticmethod
    def download(data, collection):
        # Convert nested dictionaries to flattened structure
        flattened_data = []
        for item in data:
            flat_dict = DB_Utility.flatten_structure(item)
            list_items = {key: flat_dict.pop(key) for key in flat_dict if isinstance(flat_dict[key], list)}
            flat_dict.update(list_items)
            flattened_data.append(flat_dict)

        # Convert flattened data to DataFrame
        df = pd.DataFrame(flattened_data)

        # Convert DataFrame to Excel file in memory
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)  # Move the cursor to the beginning of the buffer
        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name=f'{collection}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    @staticmethod
    def check_permissions(permissions,api_name,module_details_map):
        
        for module_name, module_details in module_details_map.items():
            for access_entry in module_details.get('access', []):
                if api_name == access_entry['api_name']:
                    if module_name not in permissions:
                        return Base_Response(status=CONSTANTS.FAILED, status_code=405, message="Insufficient permissions").__dict__
                
                    required_permission = access_entry['submodule_name']
                    user_permissions = permissions[module_name]
                
                    if not user_permissions.get(required_permission, False):
                        return Base_Response(status=CONSTANTS.FAILED, status_code=405, message="Insufficient permissions").__dict__
        return None
    
    def extract_key_values_from_objects(objects,key='_id',is_str=False):
        """
        Extracts IDs from a list of objects.

        Args:
        objects (list): List of objects (dictionaries) from which to extract IDs.

        Returns:
        list: List of extracted IDs.
        """
        if is_str:
            return [str(obj[key]) for obj in objects]
        else:
            id_list = []
            for obj in objects:
                if isinstance(obj[key], str):
                    id_list.extend([ObjectId(obj[key])])
                elif isinstance(obj[key], list):
                    id_list.extend([ObjectId(o) if isinstance(o, str) else o for o in obj[key]])
                else: 
                    id_list.extend(obj[key])
                
            return id_list
        
    def update_history_data(existing_doc, req_data, function, keys_to_remove):
        history_data ={}
        key, value = next(iter(req_data.items()))            
        if key not in keys_to_remove:              
            old_value = existing_doc.get(key, "") if function == "update" else None
            history_data ={
                "field": key,
                "ov": old_value,
                "nv": value
            }    
        return history_data if history_data else None
       
        
class Mongo_DB_Manager:

    def create_document(collection, document):
        #document = {"name": "Alice", "age": 30, "city": "New York"}
        result = collection.insert_one(document)
        return result.inserted_id
    
    def create_documents(collection, documents):
        #document = [{"name": "Alice", "age": 30, "city": "New York"},{"name": "Alice", "age": 30, "city": "New York"}]
        result = collection.insert_many(documents)
        return result.inserted_ids
    
    def read_one_document(collection, query):
        #query = {"name": "Alice"}
        document = collection.find_one(query)
        return document

    def read_documents(collection, query):
        #query = {"name": "Alice"}/query = {"age": {"$gt": 30}}
        documents = collection.find(query)
        return documents

    def update_document(collection, query, update):
        '''query = {"name": "Alice"}
        update = {"$set": {"age": 31}}'''
        result = collection.update_one(query, {'$set': update})
        return result.modified_count

    def update_documents(collection, query, update):
        '''query = {"age": {"$gt":10}}
        update = {"$set": {"age": 31}}'''
        result = collection.update_many(query, {'$set': update})
        return result.modified_count

    def delete_document(collection, query):
        #query = {"name": "Alice"}
        result = collection.delete_one(query)
        return result.deleted_count

    def delete_documents(collection, query):
        #query = {"name": "Alice"}/query = {"age": {"$gt": 30}}
        result = collection.delete_many(query)
        return result.deleted_count

    def bulk_write_operations(collection, operations):
        '''for single doc update : operations = [
    InsertOne({"name": "Alice", "age": 30}),
    InsertOne({"name": "Bob", "age": 35}),
    UpdateOne({"name": "Charlie"}, {"$set": {"age": 40}}),
    DeleteOne({"name": "David"})
]
for multiple docs update: operations = [
    InsertMany([
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 35},
        {"name": "Charlie", "age": 40}
    ]),
    UpdateMany({"age": {"$lt": 35}}, {"$set": {"status": "inactive"}}),
    DeleteMany({"age": {"$gte": 40}})
]
'''
        result = collection.bulk_write(operations)
        '''results can be taken from print("Bulk write result:")
print("Inserted:", result.inserted_count)
print("Matched:", result.matched_count)
print("Modified:", result.modified_count)
print("Deleted:", result.deleted_count)'''

        return result

    def get_distinct_values(collection,field):
        #for nested attribute "nested_field.nested_subfield"
        return collection.distinct(field)

    def get_distinct_values_array_of_keys(collection, field_path):
        #field_path = ["f1", "f2", {"$f3": "$f4"}] for  ["f1":["f2":["f3":{"f4"}]]] 
        pipeline = Mongo_DB_Manager.build_aggregation_pipeline(field_path)
        distinct_values = list(collection.aggregate(pipeline))
        return distinct_values

    def count_documents(collection,query):
        return collection.count_documents(query)
    
    def apply_search(collection, search_value):
        
        distinct_keys= Mongo_DB_Manager.get_field_names(collection)           
        or_conditions = []
        for key in distinct_keys:
            or_conditions.append({key: {'$regex': search_value, '$options': 'i'}})
        return {'$or': or_conditions}
    
    def get_field_names(collection):    
        distinct_keys = set()
        for document in collection.find():
            distinct_keys.update(document.keys())
        return distinct_keys

    def get_paginated_data(collection,query, pagination:Pagination,projection=None):
        if pagination:
            page_number = pagination.page
            page_size = pagination.per_page
            sortby = pagination.sort_by
            order = pagination.order_by        
        
        cursor = collection.find(query,projection)
        
        if pagination:   
            if sortby and order:
                if order.lower() == "asc":
                    cursor = cursor.sort([(sortby, pymongo.ASCENDING)])
                elif order.lower() == "desc":
                    cursor = cursor.sort([(sortby, pymongo.DESCENDING)])       
            
            if page_number and page_size:
                skip = (page_number - 1) * page_size
                cursor = cursor.skip(skip).limit(page_size)
            
        # Convert cursor to list of documents
        paginated_data = list(cursor)
        return paginated_data

    def get_paginated_data1(collection,query, pagination:Pagination=None,projection=None,sample_doc=None):
        projection_query = {}
        pagination_query = []
        
        if pagination:
            page_number = pagination.page
            page_size = pagination.per_page
            sortby = pagination.sort_by
            order = pagination.order_by   
                
            if sortby and order:
                order = 1 if order.lower() == "asc" else -1 if order.lower() == "desc" else None
                pagination_query.append({"$sort": {sortby: order}})
                
            if page_number and page_size:
                skip = (page_number - 1) * page_size 
                pagination_query.append({"$skip": skip})
                pagination_query.append({"$limit": page_size})
        #print(query,pagination_query)
        pipeline = [{   "$match": query                },
                {"$facet": {"count": [  {  "$count": "count"   }   ],"docs": pagination_query }   },
                { '$project': {'count': {'$arrayElemAt': ['$count.count', 0]}, 'docs': 1  } }]
        if projection:
            if sample_doc:
                for field,db_field in sample_doc.items():
                    if field in projection and projection[field] == 1:
                        projection_query[db_field] = f'$$doc.{db_field}'  # Include the field
                projection_query['_id'] = {'$toString': '$$doc._id'}   
            pipeline.append({ '$addFields': {
            'docs': {
                '$map': {
                    'input': '$docs',
                    'as': 'doc',
                    'in': {
                        '$mergeObjects': [
                            '$$doc',
                            {'_id': {'$toString': '$$doc._id'}}
                        ]
                    }
                }
            }
        }})
            pipeline.append( {
                '$set': {
                    'docs': {
                        '$map': {
                            'input': '$docs',
                            'as': 'doc',
                            'in': projection_query
                        }
                    }
                }
            })
        else:
            pipeline[1]["$facet"]["docs"].append({
        '$addFields': {
            '_id': {'$toString': '$_id'}  # Convert _id to string
        }
    })
        #print("pipeline==========",pipeline)
        results = collection.aggregate(pipeline);
        result_set = list(results)
        result, count = [],0
        if result_set and result_set[0]:
            count = result_set[0].get('count',0)
            result = result_set[0].get('docs',[])
            if result and projection:
                sort_order = list(projection.keys())
                ordered_result = []
                for doc in result:
                    ordered_doc = {key: doc.get(key) for key in sort_order if key in doc}
                    ordered_result.append(ordered_doc)
                
                # Now `ordered_result` contains the documents with fields in the desired order
                result = ordered_result
        return result,count
        
    def search_content(collection, key,search_term):
    # Construct a regex pattern for case-insensitive search
        regex_pattern = re.compile(search_term, re.IGNORECASE)
        # Define the query to search for the content
        query = {key: {"$regex": regex_pattern}}
        # Perform the search
        search_results = collection.find(query)
        return search_results

    def call_aggregate(collection,pipeline):
        return list(collection.aggregate(pipeline))

    def maintain_request_order_by_id(collection, ids, error_message="No documents found"):       
        ids = DB_Utility.str_id_list_to_obj_list(ids)      
        pipeline = [
            {"$match": {"_id": {"$in": ids}, "is_deleted": False}},
            {"$addFields": {"_order": {"$indexOfArray": [ids, "$_id"]}}},
            {"$sort": {"_order": 1}}
        ]
        result_list = Mongo_DB_Manager.call_aggregate(collection,pipeline)             
        if not result_list:
            raise Custom_Error(error_message)
        return result_list

    def attachment_details(attachment_collection,documents,file_type_list):
        id_key_to_file = {}
        obj_id_values = []

        for doc in documents:
            #doc["_id"] = DB_Utility.obj_id_to_str(doc["_id"])
            for key, value in doc.items():
                if isinstance(value, datetime):
                    doc[key] = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            for file_type in file_type_list:
                id_key = file_type + "_id"
                if id_key in doc:
                    obj_id = DB_Utility.str_to_obj_id(doc[id_key])
                    obj_id_values.append(obj_id)
                    id_key_to_file[(doc[id_key], file_type)] = doc

        if obj_id_values:
            # Fetch all attachments in a single call
            attachments = attachment_collection.find({"_id": {"$in": obj_id_values}})

            # Create a dictionary to map attachment _id to attachment
            attachment_dict = {str(attachment["_id"]): attachment for attachment in attachments}

            # Update documents with attachment details
            for (id_key_value, file_type), doc in id_key_to_file.items():
                attachment = attachment_dict.get(str(DB_Utility.str_to_obj_id(id_key_value)))
                if attachment:                    
                    doc[file_type] = attachment['module_type']+"/"+attachment["file"]
                    doc[file_type + "_file_name"] = attachment["file_name"]
       
    def is_collection_empty(collection):
        return collection.count_documents({}) == 0    
    
    def is_collection_exists(db: Database, collection_name: str) -> bool:
         return collection_name in db.list_collection_names()
    
    def get_last_n_docs_from_collection(collection,query,n=0):
        return collection.find(query).sort("_id", DESCENDING).skip(n)
    
    def date_history(db, collection_name, reference_id, filtered_data, updated_on, updated_by):        
        filtered_data["updated_on"] = updated_on
        filtered_data["updated_by"] = updated_by
        document_found = Mongo_DB_Manager.read_one_document(db["HISTORY"],{"collection" : collection_name})   
       
        needs_update = False     
        if not document_found:           
            new_doc = {
                "collection": collection_name,
                reference_id: [filtered_data]
            }
            # Insert the new document into the MongoDB collection
            Mongo_DB_Manager.create_document(db["HISTORY"],new_doc)
        else:
            # Update the existing collection
            history_found = document_found.get(reference_id)               
            if history_found:
                # If the reference_id exists, append the new update to its list
                    document_found[reference_id].append(filtered_data)
                    needs_update = True
            else:
                # If the reference_id does not exist, add a new reference with the update
                document_found[reference_id] =  [filtered_data]
                needs_update = True
        if needs_update:
                # Update the existing document in the MongoDB collection
                query = {"_id": document_found["_id"]}
                update = {reference_id: document_found[reference_id]}
                Mongo_DB_Manager.update_document(db["HISTORY"],query,update)
                

             

     
        
        
