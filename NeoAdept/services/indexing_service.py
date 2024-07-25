import os

from datetime import datetime
from whoosh.index import open_dir
from whoosh.fields import TEXT, Schema,DATETIME
from whoosh.qparser import QueryParser 
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT
from whoosh import scoring
from whoosh.query import And

from ..config import Config
from ..utilities.collection_names import COLLECTIONS
from ..gbo.common import Custom_Error
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..utilities.utility import Utility

class Indexing_Service:
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,logger,db,key_set_map,key_set_index_map,config:Config):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.index_folder = config.index_folder
            self.collection = db[COLLECTIONS.ATS_CANDIDATE_DETAILS]
            self.search_history_collection = db[COLLECTIONS.ATS_SEARCH_HISTORY]
            self.sample_collection = db[COLLECTIONS.CONFIG_SAMPLE]
            self.key_set_map = key_set_map
            self.key_set_index_map =  key_set_index_map
            #if self.key_set_index_map and "CANDIDATE_DETAILS" in self.key_set_index_map:
            if not os.path.exists(self.index_folder):
                print("creating index")
                os.mkdir(self.index_folder)
                self.create_index()
            else:
                print("updating index")
                self.update_index()
    
    def get_field_type(self,value):
        if isinstance(value, str):
            return TEXT(stored=True)
        elif isinstance(value, bool):
            return TEXT(stored=True)
        elif isinstance(value, int) or isinstance(value, float):
            return TEXT(stored=True)
        elif isinstance(value, list):
            return TEXT(stored=True)
        elif isinstance(value, datetime):
            return DATETIME(stored=True)
        else:
            return TEXT(stored=True)

    def generate_schema(self,document):
        fields = {}
        for key, value in document.items():
                fields[key] = self.get_field_type(value)
        fields['path'] = TEXT(stored=True)
        del fields['_id']        
        return Schema(**fields)

    def create_index(self):
        try:
            ix = create_in(self.index_folder, self.generate_schema(self.key_set_index_map))
            writer = ix.writer()
            for document in self.collection.find():
                doc =  DB_Utility.extract_all_keys_from_json_with_values(document)
                try:
                    if doc:
                        doc['path'] = doc['_id']
                        del doc['_id']
                        writer.add_document(**doc)
                except Exception as e:
                    Utility.printExceptionStackTrace(e)
            writer.commit()
            self.update_batch_time()
            
        except Exception as e:
            raise Custom_Error(str(e))
    
    def get_last_indexed_time(self):
        doc = self.sample_collection.find_one({'batch_name': 'search_index'})
        return doc['last_modified']
    
    def find_batch(self):
        return self.sample_collection.find({'batch_name': 'search_index' })

    def get_updated_documents(self,last_indexed_time):
        query = {'$or': [
            { 'created_on': { '$gt': last_indexed_time } },
            { 'updated_on': { '$gt': last_indexed_time } }
        ] }
        return Mongo_DB_Manager.read_documents(self.collection,query)
    
    def update_batch_time(self):
        self.sample_collection.update_one({'batch_name': 'search_index'}, {'$set': {'last_modified':Utility.get_current_time()} })
    
    def update_index(self):        
        ix = open_dir(self.index_folder)
        writer = ix.writer()
        last_indexed_time = self.get_last_indexed_time()
        #print("last_indexed_time",last_indexed_time)
        updated_documents = list(self.get_updated_documents(last_indexed_time))
        #print("updated_documents",updated_documents)
        if updated_documents:
            for updated_document in updated_documents:
                    #print("updated_document",updated_document)
                    if updated_document:
                        path = str(updated_document['_id'])
                        if updated_document.get('is_deleted', False):
                            # If the document is marked as deleted, remove it from the index
                            writer.delete_by_term('path', path)
                            print(f"Document with path {path} has been deleted from the index.")
                        else:
                            doc = DB_Utility.extract_all_keys_from_json_with_values(updated_document)
                            #print("doc-path",doc['path'])
                            existing_doc = self._find_existing_document(ix, path)
                            print(f"Document with path {path} has been updated in the index.")
                            if existing_doc:
                                writer.delete_by_term('path', path)
                                writer.add_document(**doc)
                                #writer.update_document(**doc)
                            else:
                                print(f"Document with path {path} has been added in the index.")
                                writer.add_document(**doc)
            writer.commit()
            self.update_batch_time()

    def _find_existing_document(self, ix, path):
        with ix.searcher() as searcher:
            query = QueryParser("path", ix.schema).parse(path)
            results = searcher.search(query)
            return len(results) > 0
    
    def search_candidates(self,req_data,user_id):
            ix = open_dir(self.index_folder)
            parsed_queries = []

            for field, values in req_data["filter_by"].items():       
                qp = QueryParser(self.key_set_map.get(field), schema=ix.schema)
                query_string = " OR ".join('"{0}"'.format(value) for value in values)
                parsed_query = qp.parse(query_string)
                parsed_queries.append(parsed_query)
            # Combine parsed queries using AND operator
            combined_query = And(parsed_queries)
            result_set = []

            with ix.searcher(weighting=scoring.TF_IDF()) as searcher:
                results = searcher.search(combined_query,limit=None)
                for hit in results:
                    result_set.append({"_id":hit['path'],"score":hit.score}) 
                search_log_data = {"user_id":user_id,"req_data":req_data,"result_set":result_set,"search_time":Utility.get_current_time()}
                Mongo_DB_Manager.create_document(self.search_history_collection,search_log_data)
            if len(result_set)>0:
                return result_set,len(result_set)
            raise Custom_Error("No data found")
       