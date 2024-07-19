import copy
from NeoAdept.services.common_service import Common_Service
from NeoAdept.utilities.constants import CONSTANTS
from flask import json
from bson import ObjectId

from ..gbo.common import Custom_Error
from ..gbo.bo import Common_Fields,Pagination
from ..pojo.ui_template.sub_menu import SUB_MENU
from ..pojo.ui_template.menu import MENU
from ..pojo.ui_template.page import PAGE
from ..pojo.ui_template.role import ROLE
from ..pojo.ui_template.widget import WIDGET
from ..requests.widget_request import create_widget_request
from ..requests.sub_menu_request import create_sub_menu_request
from ..requests.page_request import create_page_request
from ..requests.menu_request import create_menu_request
from ..requests.role_request import create_role_request
from ..utilities.db_utility import Mongo_DB_Manager,DB_Utility
from ..utilities.utility import Utility

class UI_Template_Service():  
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,logger,db,keyset_map):
        if not hasattr(self, 'initialized'):
            self.key_nested_key_map = keyset_map         
            self.role_key_map = keyset_map.get("ROLE", {})
            self.menu_key_map = keyset_map.get("MENU", {})
            self.sub_menu_key_map = keyset_map.get("SUB_MENU", {})
            self.widget_key_map = keyset_map.get("WIDGET", {})
            self.page_key_map = keyset_map.get("PAGE", {})
            self.logger = logger            
            self.widget_collection = "WIDGET"       
            self.page_collection = "PAGE"
            self.sub_menu_collection = "SUB_MENU"
            self.menu_collection = "MENU"
            self.role_collection = "ROLE"
            self.common_service = Common_Service(logger,db,keyset_map)
            self.collections_dict = {         
                                        "role":db[self.role_collection],
                                        "widget":db[self.widget_collection],
                                        "page":db[self.page_collection],
                                        "menu":db[self.menu_collection],
                                        "sub_menu":db[self.sub_menu_collection]
}
    
    def upsert_widget(self,widget_data,email_from_token,db):              
        widget_request = create_widget_request(widget_data) 
        widget_request.parse_request()
        widget_request.validate_request()

        widgetObj = widget_request.widget_obj
        _id = widgetObj._id
       
        if _id: 
                status = self.check_duplicates(db[self.widget_collection],widgetObj,"file_name",_id)
                if status is not None : 
                     raise Custom_Error(status)
                update_query = {'_id': DB_Utility.str_to_obj_id(_id)}                 
                cloned_widget_data = copy.deepcopy(widgetObj)
                cloned_widget_data = self.upsert_by_on(cloned_widget_data,"update",email_from_token)    
                
                Mongo_DB_Manager.update_document(db[self.widget_collection],update_query,cloned_widget_data.__dict__)           
        else:
                status = self.check_duplicates(db[self.widget_collection],widgetObj,"file_name")
                if status is not None : 
                     raise Custom_Error(status)
                widgetObj = self.upsert_by_on(widgetObj,"add",email_from_token)                 
                Mongo_DB_Manager.create_document(db[self.widget_collection],widgetObj.__dict__)              

    def upsert_page(self,page_data,email_from_token,db):
        page_request = create_page_request(page_data)           
        page_request.parse_request()           
        page_request.validate_request()         
            
        pageObj = page_request.page_obj 
        _id = pageObj._id
           
        widget_ids = pageObj.widget_ids           
        status = self.ids_exists(widget_ids,db[self.widget_collection],value = "widget")
        if status is not None : 
                     raise Custom_Error(status)
            
        if _id:            
            status = self.check_duplicates(db[self.page_collection],pageObj,"router_link",_id)
            if status is not None : 
                     raise Custom_Error(status)
            update_query = {'_id': DB_Utility.str_to_obj_id(_id)}                 
            cloned_page_data = copy.deepcopy(pageObj)
            cloned_page_data = self.upsert_by_on(cloned_page_data,"update",email_from_token)                
            Mongo_DB_Manager.update_document(db[self.page_collection],update_query,cloned_page_data.__dict__)                 
        else:
            
            status = self.check_duplicates(db[self.page_collection],pageObj,"router_link")
            if status is not None : 
                     raise Custom_Error(status)
            pageObj = self.upsert_by_on(pageObj,"add",email_from_token)                 
            Mongo_DB_Manager.create_document(db[self.page_collection],pageObj.__dict__)                  
    
    def upsert_sub_menu(self,sub_menu_data,email_from_token,db):                    
            sub_menu_request = create_sub_menu_request(sub_menu_data) 
            sub_menu_request.parse_request()
            sub_menu_request.validate_request()
            
            sub_menuObj = sub_menu_request.sub_menu_obj
            _id = sub_menuObj._id
           
            page_id = sub_menuObj.page_id           
            doc = DB_Utility.check_id_exists([page_id],db[self.page_collection])               
            if not doc or list(doc)==[]:
                raise Custom_Error(f"The page id {page_id} does not exists")     
            
            if _id:
                status = self.check_duplicates(db[self.sub_menu_collection],sub_menuObj,"name",_id)
                if status is not None : 
                     raise Custom_Error(status)
                update_query = {'_id': DB_Utility.str_to_obj_id(_id)}                 
                cloned_sub_menu_data = copy.deepcopy(sub_menuObj)
                cloned_sub_menu_data = self.upsert_by_on(cloned_sub_menu_data,"update",email_from_token)
                
                Mongo_DB_Manager.update_document(db[self.sub_menu_collection],update_query,cloned_sub_menu_data.__dict__)                
            else:               
                status = self.check_duplicates(db[self.sub_menu_collection],sub_menuObj,"name")
                if status is not None : 
                     raise Custom_Error(status)
                sub_menuObj = self.upsert_by_on(sub_menuObj,"add",email_from_token)                              
                Mongo_DB_Manager.create_document(db[self.sub_menu_collection],sub_menuObj.__dict__)                  
                    
    
    def upsert_menu(self,menu_data,email_from_token,db):                    
           menu_request = create_menu_request(menu_data) 
           menu_request.parse_request()
           menu_request.validate_request()        
          
           menuObj = menu_request.menu_obj
           _id = menuObj._id
           
           is_sub_menu = menuObj.is_sub_menu
           if is_sub_menu:
                sub_menu_ids = menuObj.sub_menu_ids
                status = self.ids_exists(sub_menu_ids,db[self.sub_menu_collection],value = "sub_menu")
                if status is not None : 
                     raise Custom_Error(status)
                del menuObj.page_id               
           else:
               page_id = menuObj.page_id           
               doc = DB_Utility.check_id_exists([page_id],db[self.page_collection])               
               if not doc or list(doc)==[]:
                     raise Custom_Error(f"The page id {page_id} does not exists")  
               del menuObj.sub_menu_ids
           
           if _id:
                status = self.check_duplicates(db[self.menu_collection],menuObj,"name",_id)
                if status is not None : 
                     raise Custom_Error(status)
                update_query = {'_id': DB_Utility.str_to_obj_id(_id)}                 
                cloned_menu_data = copy.deepcopy(menuObj)
                cloned_menu_data = self.upsert_by_on(cloned_menu_data,"update",email_from_token)
                Mongo_DB_Manager.update_document(db[self.menu_collection],update_query,cloned_menu_data.__dict__)            
           else:
                 status = self.check_duplicates(db[self.menu_collection],menuObj,"name")
                 if status is not None : 
                     raise Custom_Error(status)
                 menuObj = self.upsert_by_on(menuObj,"add",email_from_token)                              
                 Mongo_DB_Manager.create_document(db[self.menu_collection],menuObj.__dict__)  
    
    def upsert_role(self,role_data,email_from_token,db):                    
           role_request = create_role_request(role_data) 
           role_request.parse_request()
           role_request.validate_request()
           
           roleObj = role_request.role_obj
           _id = roleObj._id
           
           menu_ids = roleObj.menu_ids           
           status = self.ids_exists(menu_ids,db[self.menu_collection],value = "menu")
           if status is not None : 
                     raise Custom_Error(status)
           
           if _id:                            
                status = self.check_duplicates(db[self.role_collection],roleObj,"name",_id) 
                if status is not None : 
                     raise Custom_Error(status)
                update_query = {'_id': DB_Utility.str_to_obj_id(_id)}                 
                cloned_role_data = copy.deepcopy(roleObj)
                cloned_role_data = self.upsert_by_on(cloned_role_data,"update",email_from_token)
                Mongo_DB_Manager.update_document(db[self.role_collection],update_query,cloned_role_data.__dict__)   
                self.update_role_permissions_for_role(_id, roleObj.name, email_from_token, db)
           else:                
                 status = self.check_duplicates(db[self.role_collection],roleObj,"name")
                 if status is not None : 
                     raise Custom_Error(status)
                 roleObj =  self.upsert_by_on(roleObj,"add",email_from_token) 
                 role_id = Mongo_DB_Manager.create_document(db[self.role_collection],roleObj.__dict__)     
                 self.common_service.add_role_permission_for_role(DB_Utility.obj_id_to_str(role_id),roleObj.name,email_from_token,db)
    
    def ids_exists(self,ids,collection,is_upload = False,doc = None,result_dict = None,index = 0,value = None):         
           cursor = DB_Utility.check_id_exists(ids,collection)           
           existing_ids = [doc['_id'] for doc in cursor]           
           ids = DB_Utility.str_id_list_to_obj_list(ids)
           missing_ids = [_id for _id in ids if _id not in existing_ids]            
           if missing_ids:
               status = f"The {value} ids {missing_ids} does not exists" 
               if is_upload :                       
                    DB_Utility.update_status(doc,result_dict,index, status)
                    return True  
               return status
           return None
        
          
    
    def upsert_by_on(self,obj,type,email_from_token):
        if type == "update" :              
              attributes_to_delete = ["created_by","created_on","_id"]             
              [delattr(obj, attr) for attr in attributes_to_delete]
              obj.updated_by = email_from_token
              obj.updated_on = Utility.get_current_timestamp()
        elif type == "add":
              del obj._id
              obj.created_by = email_from_token
              obj.created_on = Utility.get_current_timestamp()
        return obj
     
    def delete_obj(self,collection_name,_id,email_from_token,db) :
       
        collection = self.collections_dict.get(collection_name)
        if _id:
               doc = DB_Utility.check_id_exists(_id,collection)            
               if not doc or list(doc) == []:
                     raise Custom_Error(f"The id {_id} does not exists")
               
               common_fields = Common_Fields(updated_by = email_from_token,updated_on = Utility.get_current_timestamp())
               data = {"is_deleted" : True}
               data.update(common_fields.__dict__)

               query = {'_id': DB_Utility.str_to_obj_id(_id)}
               result_modified_count = Mongo_DB_Manager.update_document( collection,query,data)
               
               if result_modified_count > 0:
                    self.delete_references(collection,_id,db)
                   
    def delete_references(self,collection,_id,db):
    
            if collection == db[self.widget_collection]:
                   db[self.page_collection].update_many({"widget_ids": _id},{ "$pull": { "widget_ids":_id}})
                   
            if collection == db[self.menu_collection]:
                    db[self.role_collection].update_many({"menu_ids": _id},{ "$pull": { "menu_ids":_id}})
                    
            if collection == db[self.sub_menu_collection]:
                    db[self.menu_collection].update_many({"sub_menu_ids": _id},{ "$pull": { "sub_menu_ids":_id}}) 
                    
            if collection == db[self.page_collection]:                
                    db[self.sub_menu_collection].update_many({"page_id": _id}, {"$unset": {"page_id": ""}})
                    db[self.menu_collection].update_many({"page_id": _id}, {"$unset": {"page_id": ""}})


    def load_widgets(self,db,request_data = None,widget_ids=None):
        widget_objects = []  
        if widget_ids and widget_ids:            
            get_function,count = False,0           
            widget_list = Mongo_DB_Manager.maintain_request_order_by_id(db[self.widget_collection], widget_ids, error_message="No documents found for widget id")            
        else:            
            get_function = True
            pagination = Pagination(**request_data)   
            #query = DB_Utility.build_filtered_data_query(pagination.filter_by) if pagination.filter_by else {}
            query = DB_Utility.frame_get_query(pagination,self.widget_key_map)         
            widget_list = Mongo_DB_Manager.get_paginated_data(db[self.widget_collection],query,pagination)
            
            if not widget_list: 
               raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
            count = Mongo_DB_Manager.count_documents(db[self.widget_collection], query)
            
        widget_objects = self.load_and_process_data(db,widget_list, WIDGET,None, None,None,get_function)   
        return widget_objects,count
    
    def load_pages(self,db,request_data = None,page_id = None):
        count = 0
        if page_id:
            page = db[self.page_collection].find_one({"_id":ObjectId(page_id)},{"is_deleted": False})
            if not page :
                raise Custom_Error("page not found for the page id")
            data = self.load_and_process_data(db,[page],PAGE,self.load_widgets,"widget_ids","widgets",False)   
            page_objects = data[0]
        else:            
            pagination = Pagination(**request_data)            
            #query = DB_Utility.build_filtered_data_query(pagination.filter_by) if pagination.filter_by else {}
            query = DB_Utility.frame_get_query(pagination,self.page_key_map)  
            page_data = Mongo_DB_Manager.get_paginated_data(db[self.page_collection],query,pagination)          
            if not page_data: 
                raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
            page_objects = self.load_and_process_data(db,page_data,PAGE,self.load_widgets,"widget_ids","widgets",False)
            count = Mongo_DB_Manager.count_documents(db[self.page_collection], query)
        return  page_objects,count
    
    def load_sub_menus(self,db,request_data = None,sub_menu_ids = None):
        if sub_menu_ids and sub_menu_ids:            
            get_function,count = False,0            
            sub_menu_list = Mongo_DB_Manager.maintain_request_order_by_id(db[self.sub_menu_collection], sub_menu_ids, error_message="No data found for the sub_menu id")         
        else:
            get_function = True            
            pagination = Pagination(**request_data)      
           # query = DB_Utility.build_filtered_data_query(pagination.filter_by) if pagination.filter_by else {}
            query = DB_Utility.frame_get_query(pagination,self.sub_menu_key_map)  
            count = Mongo_DB_Manager.count_documents(db[self.sub_menu_collection],query)          
            sub_menu_list = Mongo_DB_Manager.get_paginated_data(db[self.sub_menu_collection],query,pagination)
            if not sub_menu_list: 
                raise Custom_Error(CONSTANTS.NO_DATA_FOUND)         
        sub_menu_objects = self.load_and_process_data(db,sub_menu_list,SUB_MENU,self.load_pages,"page_id","page",get_function)        
        return sub_menu_objects,count
       

    def load_menus(self,db,request_data = None,menu_ids = None):
        if menu_ids is not None and len(menu_ids) > 0:
            get_function,count = False,0
            menu_list =Mongo_DB_Manager.maintain_request_order_by_id(db[self.menu_collection], menu_ids, error_message="No data found for the menu id")
        else:      
            get_function = True
            pagination = Pagination(**request_data)       
           # query = DB_Utility.build_filtered_data_query(pagination.filter_by) if pagination.filter_by else {}
            query = DB_Utility.frame_get_query(pagination,self.menu_key_map)   
            
            count = Mongo_DB_Manager.count_documents(db[self.menu_collection], query)        
            menu_list = Mongo_DB_Manager.get_paginated_data(db[self.menu_collection],query,pagination)       
            if not menu_list: 
               raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
            
        menu_objects = []      
        for menu in menu_list:
            menu_obj = MENU(**menu)
            menu_obj._id = DB_Utility.obj_id_to_str(menu_obj._id)
            if get_function:
                     self.remove_attributes(menu_obj,'_order')                     
            if menu_obj.is_sub_menu:                
                sub_menu_data,count_menu = self.load_sub_menus(db,sub_menu_ids = menu_obj.sub_menu_ids)
                menu_obj.sub_menus = sub_menu_data
                self.remove_attributes(menu_obj, 'page_id', 'page', 'sub_menu_ids')                
                menu_objects.append(menu_obj.__dict__)             
            else:   
                page_data,count_page = self.load_pages(db,page_id = menu_obj.page_id)                          
                menu_obj.page = page_data
                self.remove_attributes(menu_obj, 'page_id', 'sub_menus', 'sub_menu_ids')               
                menu_objects.append(menu_obj.__dict__)   
        
        return menu_objects,count
    
    def load_roles(self,db,request_data = None, role_name = None):
        if role_name:
            count = 0
            role_data = db[self.role_collection].find_one({"name":role_name},{"is_deleted" : False})
            if not role_data:
                return None,count
            role_objects = self.load_and_process_data(db,[role_data],ROLE,self.load_menus,"menu_ids","menus",False)
        else:           
            pagination = Pagination(**request_data)
           # query = DB_Utility.build_filtered_data_query(pagination.filter_by) if pagination.filter_by else {}
            query = DB_Utility.frame_get_query(pagination,self.role_key_map)   
            
            count = Mongo_DB_Manager.count_documents(db[self.role_collection], query)            
            role_data = Mongo_DB_Manager.get_paginated_data(db[self.role_collection],query,pagination)       
            if not role_data: 
                raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
           
            role_objects = self.load_and_process_data(db,role_data,ROLE,self.load_menus,"menu_ids","menus",False)                 
        return role_objects,count    

    def load_and_process_data(self,db,data, obj_class,load_function, del_attr_keys,set_attr_keys,is_remove_order_key = False):
        obj_array = []        
        for item in data:
            obj = obj_class(**item)
            setattr(obj, "_id", DB_Utility.obj_id_to_str(getattr(obj, "_id")))
            if is_remove_order_key :
                self.remove_attributes(obj,"_order") 
            if load_function:
                related_data, count_related = load_function(db,**{del_attr_keys: getattr(obj, del_attr_keys)})
                setattr(obj,set_attr_keys, related_data)  
                delattr(obj, del_attr_keys)
            obj_array.append(obj.__dict__)
        return obj_array

    def remove_attributes(self,obj, *attributes):
        for attribute in attributes:
            delattr(obj, attribute)       
    
    def upload_excel_data(self,request_data,email_from_token,db):
         
         self.file_type = request_data.form.get('type')         
         file_types = {
            "page": {
            "keys": CONSTANTS.PAGE_KEYS,
            "upload_method": self.upload_pages
            },
            "widget": {
            "keys": CONSTANTS.WIDGET_KEYS,
            "upload_method": self.upload_widgets
             },
            "sub_menu": {
            "keys": CONSTANTS.SUB_MENU_KEYS,
            "upload_method": self.upload_sub_menus
             },
            "menu": {
            "keys": CONSTANTS.MENU_KEYS,
            "upload_method": self.upload_menus
            },
            "role": {
            "keys": CONSTANTS.ROLE_KEYS,
            "upload_method": self.upload_roles
             } 
         }

         if  self.file_type.lower() in file_types:
            json_documents, column_names = DB_Utility.read_excel(request_data)
    
            self.file_info = file_types[ self.file_type.lower()]
            self.key_constants = self.file_info["keys"]
            self.upload_method = self.file_info["upload_method"]
            
            if column_names:
                missing_keys = [key for key in self.key_constants if key not in column_names]
                if missing_keys:
                    raise Custom_Error(CONSTANTS.EXCEL_NOT_VALID)
                
            if not json_documents:
                raise Custom_Error("Excel file read failed")
    
            return self.upload_method(json_documents, email_from_token,db)
         else:
            raise Custom_Error("Invalid file type")
        
        

    def upload_widgets(self, json_documents, email_from_token,db):        
            result_dict = {}
            for index, doc in enumerate(json_documents):
                try:
                    print("index::",index)
                    is_upload = True
                    if not isinstance(doc, dict):
                        result_dict[index] = "Skipping non-dictionary entry"
                        continue
                    doc = WIDGET(**doc)
                    
                    null_check_keys = ["name","file_name"]
                    if DB_Utility.check_null_value_or_invalid_status_from_excel(doc,index, result_dict,null_check_keys):
                        print("index in null value check ::",index)                        
                        continue
                   
                    if self.check_duplicates(db[self.widget_collection],doc,"file_name",doc._id,is_upload,result_dict,index):
                        print("index in duplicate values ::",index)                      
                        continue
                    
                    result_dict[index] = self.insert_or_update_obj(db[self.widget_collection],doc, email_from_token)

                except Exception as e:
                    self.logger.error(f"Error processing document at index {index}: {e}")
            
           
            json_result_dict = json.loads(json.dumps(result_dict, default=DB_Utility.custom_encoder, indent=4)) 
            response_data = {"data": json_result_dict, "count": len(result_dict)}           
            return response_data
   
         
    def upload_pages(self, json_documents, email_from_token,db):        
            result_dict = {}
            for index, doc in enumerate(json_documents):
                try:
                    print("index::",index)
                    is_upload = True
                    if not isinstance(doc, dict):
                        result_dict[index] = "Skipping non-dictionary entry"
                        continue
                    
                    doc = PAGE(**doc)
                    widget_ids = doc.widget_ids           
                    if widget_ids:
                         
                         new_widget_ids = widget_ids.split(',')                        
                         del doc.widget_ids                        
                         doc.widget_ids = new_widget_ids
                         print("doc",doc.__dict__)
                         if self.ids_exists(new_widget_ids,db[self.widget_collection],is_upload,doc,result_dict,index,value = "widget"):
                             print("widget_id does not  exists::",index)                        
                             continue         
                    
                    null_check_keys = ["name","router_link"]
                    if DB_Utility.check_null_value_or_invalid_status_from_excel(doc,index, result_dict,null_check_keys):
                        print("index in null value check ::",index)                        
                        continue
                   
                    if self.check_duplicates(db[self.page_collection],doc,"router_link",doc._id,is_upload,result_dict,index):
                        print("index in duplicate values ::",index)                      
                        continue
                    
                    result_dict[index] = self.insert_or_update_obj(db[self.page_collection],doc, email_from_token)

                except Exception as e:
                    self.logger.error(f"Error processing document at index {index}: {e}")
            
            
            json_result_dict = json.loads(json.dumps(result_dict, default=DB_Utility.custom_encoder, indent=4)) 
            response_data = {"data": json_result_dict, "count": len(result_dict)}           
            return response_data
    
    def upload_sub_menus(self, json_documents, email_from_token,db):        
            result_dict = {}
            for index, doc in enumerate(json_documents):
                try:
                    print("index::",index)
                    is_upload = True
                    if not isinstance(doc, dict):
                        result_dict[index] = "Skipping non-dictionary entry"
                        continue
                    
                    doc = SUB_MENU(**doc)
                    page_id = doc.page_id
                    if page_id:                         
                        if self.ids_exists([page_id],db[self.page_collection],is_upload,doc,result_dict,index,value = "page"):
                             print("page_id does not  exists::",index)                        
                             continue     
                    
                    null_check_keys = ["name"]
                    if DB_Utility.check_null_value_or_invalid_status_from_excel(doc,index, result_dict,null_check_keys):
                        print("index in null value check ::",index)                        
                        continue
                   
                    if self.check_duplicates(db[self.sub_menu_collection],doc,"name",doc._id,is_upload,result_dict,index):
                        print("index in duplicate values ::",index)                      
                        continue
                    
                    result_dict[index] = self.insert_or_update_obj(db[self.sub_menu_collection],doc, email_from_token)

                except Exception as e:
                    self.logger.error(f"Error processing document at index {index}: {e}")
            
            
            json_result_dict = json.loads(json.dumps(result_dict, default=DB_Utility.custom_encoder, indent=4)) 
            response_data = {"data": json_result_dict, "count": len(result_dict)}           
            return response_data
    
    def upload_menus(self, json_documents, email_from_token,db):        
            result_dict = {}
            for index, doc in enumerate(json_documents):
                try:
                    print("index::",index)
                    is_upload = True
                    if not isinstance(doc, dict):
                        result_dict[index] = "Skipping non-dictionary entry"
                        continue
                    
                    doc = MENU(**doc)
                    is_sub_menu=doc.is_sub_menu
                    sub_menu_ids = doc.sub_menu_ids
                    page_id = doc.page_id
                    print(is_sub_menu,sub_menu_ids,page_id,index)
                    
                    if  self.check_menu_attribute_combination(doc,is_sub_menu,page_id, sub_menu_ids,result_dict,index):
                        print("index in check_menu_attribute_combination ::",index)
                        continue
                    
                    if page_id:                         
                        if self.ids_exists([page_id],db[self.page_collection],is_upload,doc,result_dict,index,value = "page"):
                             print("page_id does not  exists::",index)                        
                             continue
                        
                    if sub_menu_ids:
                         new_sub_menu_ids = sub_menu_ids.split(',')                     
                         if self.ids_exists(new_sub_menu_ids,db[self.sub_menu_collection],is_upload,doc,result_dict,index,value = "sub_menu"):
                             print("sub_menu_ids does not  exists::",index)                        
                             continue
                         doc.sub_menu_ids = new_sub_menu_ids
                    print("doc001 :",doc)     
                    
                    
                 
                    if DB_Utility.check_null_value_or_invalid_status_from_excel(doc,index, result_dict,["name"]):
                        print("index in null value check ::",index)                        
                        continue
                   
                    if self.check_duplicates(db[self.sub_menu_collection],doc,"name",doc._id,is_upload,result_dict,index):
                        print("index in duplicate values ::",index)                      
                        continue
                    print("doc :",doc)
                    result_dict[index] = self.insert_or_update_obj(db[self.sub_menu_collection],doc, email_from_token)

                except Exception as e:
                    self.logger.error(f"Error processing document at index {index}: {e}")
            
           
            json_result_dict = json.loads(json.dumps(result_dict, default=DB_Utility.custom_encoder, indent=4)) 
            response_data = {"data": json_result_dict, "count": len(result_dict)}           
            return response_data
    
    def upload_roles(self, json_documents, email_from_token,db):        
            result_dict = {}
            for index, doc in enumerate(json_documents):
                try:
                    print("index::",index)
                    is_upload = True
                    if not isinstance(doc, dict):
                        result_dict[index] = "Skipping non-dictionary entry"
                        continue
                    
                    doc = ROLE(**doc)                   
                    menu_ids = doc.sub_menus
                   
                    if menu_ids:
                         new_menu_ids = menu_ids.split(',')                         
                         del doc.menu_ids
                         doc.menu_ids = new_menu_ids 
                         if self.ids_exists(new_menu_ids,db[self.sub_menu_collection],is_upload,doc,result_dict,index,value = "menu"):
                             print("sub_menu_ids does not  exists::",index)                        
                             continue
                    
                    if DB_Utility.check_null_value_or_invalid_status_from_excel(doc,index, result_dict,["name"]):
                        print("index in null value check ::",index)                        
                        continue
                   
                    if self.check_duplicates(db[self.sub_menu_collection],doc,"name",doc._id,is_upload,result_dict,index):
                        print("index in duplicate values ::",index)                      
                        continue
                    
                    result_dict[index] = self.insert_or_update_obj(db[self.sub_menu_collection],doc, email_from_token)

                except Exception as e:
                    self.logger.error(f"Error processing document at index {index}: {e}")
            
           
            json_result_dict = json.loads(json.dumps(result_dict, default=DB_Utility.custom_encoder, indent=4)) 
            response_data = {"data": json_result_dict, "count": len(result_dict)}           
            return response_data
    
         
    def check_duplicates(self,collection,obj,column_name,_id = None,is_upload = False,result_dict = None,index = 0):
       try:
            print("ID ",_id)
            if _id :                    
                _id = DB_Utility.str_to_obj_id(_id)
                query = {
                "$or": [
                    {"_id": _id, "is_deleted": False},
                    {"$and": [
                        {column_name: getattr(obj, column_name, None)},                       
                        {"is_deleted": False},
                        {"_id": {"$ne": _id}}
                    ]}                          
                ]
            }
                print("QUERY ",query)
                cursor = Mongo_DB_Manager.read_documents(collection,query)
                existing_docs = list(cursor)
                print("EXISTING DOCS ::",existing_docs)
                doc_found = False

                for doc in existing_docs:
                        if doc["_id"] != _id:
                            status = f'{column_name} already exists for other documents'
                            if is_upload :                           
                                DB_Utility.update_status(doc, result_dict, index, status)
                                return True  
                            return status

                        else:
                            obj.created_by = doc["created_by"]
                            obj.created_on = doc["created_on"]
                            doc_found = True

                if not doc_found:
                    status = f"The id {_id} does not exists"
                    if is_upload :                           
                        DB_Utility.update_status(obj, result_dict, index, status)
                        return True  
                    return status
                return None
                
            else:              
                 check_query = {column_name:  getattr(obj, column_name, None),"is_deleted": False} 
                 print("QUERY ",check_query)
                 cursor = Mongo_DB_Manager.read_documents(collection,check_query)
                 result = list(cursor)
                 if len(result) > 0 :                    
                     status = f"The {column_name} already exists"                     
                     if is_upload :                       
                        DB_Utility.update_status(obj,result_dict,index, status)
                        return True  
                     return status
                 return None
       except Exception as e:
                print(e)
                self.logger.error(e)
    
    def insert_or_update_obj(self,collection, doc, email_from_token):
            try:
                doc_id = doc._id
                if doc_id:                   
                    doc.updated_by = email_from_token
                    doc.updated_on = Utility.get_current_timestamp()
                    del doc._id
                    update_query = {'_id': DB_Utility.str_to_obj_id(doc_id)}                  

                    result = collection.replace_one(update_query, doc.__dict__, upsert=True)
                else:
                    doc = self.upsert_by_on(doc,"add",email_from_token)
                    result = collection.insert_one(doc.__dict__)
                    

                if result.acknowledged :
                    status_code = "200"
                    if doc_id:
                        print("UPDATE")
                        status = f'Updated successfully for the _id {doc_id}'
                    else:
                        print("ADD")
                        status = f'Inserted successfully and generated _id is {result.inserted_id}'
                else:
                    status_code = "500"
                    status = "Failed to insert or update object"

                doc.status_code = status_code
                doc.status = status
                return doc.__dict__
            except Exception as e:
                print(e)
                self.logger.error(e)
                
    def  check_menu_attribute_combination(self,doc=None,is_sub_menu=False,page_id=None, sub_menu_ids=None,result_dict=None,index=None):
     
            doc.status_code = "201"
            if not is_sub_menu and (sub_menu_ids is not None and sub_menu_ids != []):
                doc.status = f'Cannot add menus as submenu is set to False'
                result_dict[index] = doc.__dict__
                return True  # Found null or empty value
            if not is_sub_menu and not page_id:
                doc.status  = f'page_id linked to the menu is missing'
                result_dict[index] = doc.__dict__
                return True  # Found null or empty value
            if  is_sub_menu and page_id:
                doc.status  = f'page_id cannot be added if the is_sub_menu set to True'
                result_dict[index] = doc.__dict__
                return True  # Found null or empty value
            if is_sub_menu and sub_menu_ids == []:
                doc.status  = f'as submenu is set to True,please provide sub_menu_ids'
                result_dict[index] = doc.__dict__
                return True  # Found null or empty value
            return False
    
    def delete_role_permissions_for_role(self,_id,db):
        query = {"role_id": _id}
        deleted_count = Mongo_DB_Manager.delete_documents(db['ROLE_PERMISSION'],query) 
        return True
       
    def update_role_permissions_for_role(self, role_id, role_name, email_from_token, db):
        query = {"role_id": role_id}
        update = {"role_name": role_name, "updated_by": email_from_token, "updated_on":Utility.get_current_time()}
        Mongo_DB_Manager.update_documents(db['ROLE_PERMISSION'], query, update)
        return True