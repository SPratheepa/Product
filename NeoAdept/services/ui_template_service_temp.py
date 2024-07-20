import copy

from numpy import empty
from NeoAdept.services.common_service import Common_Service
from NeoAdept.utilities.constants import CONSTANTS
from flask import json
from bson import ObjectId

from ..gbo.common import Custom_Error
from ..gbo.bo import Common_Fields,Pagination
from ..pojo.sub_menu import SUB_MENU
from ..pojo.menu import MENU
from ..pojo.page import PAGE
from ..pojo.role import ROLE
from ..pojo.widget import WIDGET
from ..requests.widget_request import create_widget_request
from ..requests.sub_menu_request import create_sub_menu_request
from ..requests.page_request import create_page_request
from ..requests.menu_request import create_menu_request
from ..requests.role_request import create_role_request
from ..utilities.db_utility import Mongo_DB_Manager,DB_Utility
from ..utilities.utility import Utility

class UI_Template_Service_temp():  
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

    def load_widgets(self,db,request_data = None):       
        pagination = Pagination(**request_data) 
        query = DB_Utility.frame_get_query(pagination,self.widget_key_map)       
        widget_list,count = Mongo_DB_Manager.get_paginated_data1(db[self.widget_collection],query,pagination)
        if not widget_list: 
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
        #count = Mongo_DB_Manager.count_documents(db[self.widget_collection], query)
        #for widget in widget_list:         
            #widget= DB_Utility.convert_obj_id_to_str_id(widget)                
        return widget_list,count    

    
    def load_pages(self,db,request_data = None):       
            pagination = Pagination(**request_data)            
            query = DB_Utility.frame_get_query(pagination,self.page_key_map)  
            page_data,count = Mongo_DB_Manager.get_paginated_data1(db[self.page_collection],query,pagination)          
            if not page_data: 
                raise Custom_Error(CONSTANTS.NO_DATA_FOUND)  
            all_widget_ids = set()
            for page in page_data:
                widget_ids = page.get("widget_ids", [])
                if widget_ids:
                    # Convert string IDs to ObjectIDs in batch
                    converted_widget_ids = DB_Utility.str_id_list_to_obj_list(widget_ids)
                    all_widget_ids.update(converted_widget_ids)
            
        # Fetch all widgets in one query
            if all_widget_ids:
                widget_query = {"_id": {"$in": list(all_widget_ids)}}
                all_widgets = Mongo_DB_Manager.read_documents(db[self.widget_collection], widget_query)

                # Convert widgets to a dictionary keyed by their string ID for easy lookup
                widget_dict = {
                    str(widget["_id"]): DB_Utility.convert_obj_id_to_str_id(widget)
                    for widget in all_widgets
                }           
            # Attach widgets to their respective pages
            page_data_list = []
            for page in page_data:
                widget_ids = page.get("widget_ids", [])                
                widgetList = [widget_dict[str(widget_id)] for widget_id in widget_ids if str(widget_id) in widget_dict]                
                page["widgets"] = widgetList
                del page["widget_ids"]
                page_new = DB_Utility.convert_obj_id_to_str_id(page)               
                page_data_list.append(page_new)      
            #page_data = self.process_page(page_data,db)             
            #count = Mongo_DB_Manager.count_documents(db[self.page_collection], query)
            return  page_data_list,count
    
    def load_sub_menus(self,db,request_data = None):                
        pagination = Pagination(**request_data)    
        query = DB_Utility.frame_get_query(pagination,self.sub_menu_key_map)  
        #count = Mongo_DB_Manager.count_documents(db[self.sub_menu_collection],query)          
        sub_menu_list,count = Mongo_DB_Manager.get_paginated_data1(db[self.sub_menu_collection],query,pagination)
        if not sub_menu_list: 
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)       
        sub_menu_list = self.process_sub_menu1(sub_menu_list,db)           
        return sub_menu_list,count

    def load_menus(self,db,request_data = None):
        pagination = Pagination(**request_data)                 
        query = DB_Utility.frame_get_query(pagination,self.menu_key_map)  
        #count = Mongo_DB_Manager.count_documents(db[self.menu_collection], query)        
        menu_list,count = Mongo_DB_Manager.get_paginated_data1(db[self.menu_collection],query,pagination)       
        if not menu_list: 
            raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
        menu_list = self.process_menu(list(menu_list),db)
        return menu_list, count
    
    def load_roles(self,db,request_data = None, role_name = None):      
        if role_name:
            role_data = db[self.role_collection].find_one({"name": role_name, "is_deleted": False})
            count = 1 if role_data else 0
            if not role_data:
                return None, count
            role_data = [role_data] 
        else:           
            pagination = Pagination(**request_data)
            query = DB_Utility.frame_get_query(pagination, self.role_key_map)
            #count = Mongo_DB_Manager.count_documents(db[self.role_collection], query)
            role_data,count = Mongo_DB_Manager.get_paginated_data1(db[self.role_collection], query, pagination)
            if not role_data:
                raise Custom_Error(CONSTANTS.NO_DATA_FOUND)
    
        role_data = Utility.ensure_list(role_data)
    
        all_menu_ids = {menu_id for role in role_data for menu_id in role.get("menu_ids", [])}
        if all_menu_ids:
            converted_menu_ids = DB_Utility.str_id_list_to_obj_list(list(all_menu_ids))
            menu_query = {"_id": {"$in": converted_menu_ids}, "is_deleted": False}
            all_menus = Mongo_DB_Manager.read_documents(db[self.menu_collection], menu_query)
            processed_menus = self.process_menu(list(all_menus), db)
        
            menu_dict = {str(menu["_id"]): DB_Utility.convert_obj_id_to_str_id(menu) for menu in processed_menus}
        else:
            menu_dict = {}
        role_list = []
        for role in role_data:
            menu_ids = role.get("menu_ids", [])
            if menu_ids:
                menu_list = [menu_dict.get(str(menu_id)) for menu_id in menu_ids if str(menu_id) in menu_dict]
                if menu_list:
                    role["menus"] = menu_list
                    del role["menu_ids"]
            role_new = DB_Utility.convert_obj_id_to_str_id(role)
            role_list.append(role_new)
        return role_list, count

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
    
    def process_page(self,page_data,db):           
            all_widget_ids = set()
            for page in page_data:
                widget_ids = page.get("widget_ids", [])
                if widget_ids:
                    # Convert string IDs to ObjectIDs in batch
                    converted_widget_ids = DB_Utility.str_id_list_to_obj_list(widget_ids)
                    all_widget_ids.update(converted_widget_ids)
            
        # Fetch all widgets in one query
            if all_widget_ids:
                widget_query = {"_id": {"$in": list(all_widget_ids)}}
                all_widgets = Mongo_DB_Manager.read_documents(db[self.widget_collection], widget_query)

                # Convert widgets to a dictionary keyed by their string ID for easy lookup
                widget_dict = {
                    str(widget["_id"]): DB_Utility.convert_obj_id_to_str_id(widget)
                    for widget in all_widgets
                }           
            # Attach widgets to their respective pages
            page_data_list = []
            for page in page_data:
                widget_ids = page.get("widget_ids", [])                
                widgetList = [widget_dict[str(widget_id)] for widget_id in widget_ids if str(widget_id) in widget_dict]                
                page["widgets"] = widgetList
                del page["widget_ids"]
                page_new = DB_Utility.convert_obj_id_to_str_id(page)               
                page_data_list.append(page_new)
            return page_data_list
    
    def process_sub_menu(self,sub_menu_list,db):
            all_page_ids = set()
            for submenu in sub_menu_list:       
                page_id = submenu.get("page_id", None)
                if page_id: 
                    page_id = DB_Utility.str_to_obj_id(page_id)
                    all_page_ids.add((page_id))
                if all_page_ids:
                    all_page_ids_list = list(all_page_ids)
                    page_query = {"_id": {"$in": all_page_ids_list}, "is_deleted": False}
                    all_pages = Mongo_DB_Manager.read_documents(db[self.page_collection], page_query)
                    all_pages = self.process_page(list(all_pages),db)                
                page_dict = {
                str(page["_id"]): DB_Utility.convert_obj_id_to_str_id(page)
                for page in all_pages
            }
            for submenu in sub_menu_list:  
                page_id = submenu.get("page_id",None)
                if str(page_id) in page_dict :
                    page_data = page_dict[str(page_id)] 
                    submenu["page"] = page_data
                    del submenu["page_id"]
                    submenu = DB_Utility.convert_obj_id_to_str_id(submenu)
            return sub_menu_list
        
    def process_sub_menu1(self,sub_menu_list,db):
            all_page_ids = set()
            for submenu in sub_menu_list:       
                page_id = submenu.get("page_id", None)
                if page_id: 
                    page_id = DB_Utility.str_to_obj_id(page_id)
                    all_page_ids.add((page_id))
            if all_page_ids:
                    all_page_ids_list = list(all_page_ids)
                    page_query = {"_id": {"$in": all_page_ids_list}, "is_deleted": False}
                    all_pages = Mongo_DB_Manager.read_documents(db[self.page_collection], page_query)
                    all_pages = self.process_page(list(all_pages),db)                
                    page_dict = {
                str(page["_id"]): DB_Utility.convert_obj_id_to_str_id(page)
                for page in all_pages
            }
            for submenu in sub_menu_list:  
                page_id = submenu.get("page_id",None)
                if str(page_id) in page_dict :
                    page_data = page_dict[str(page_id)] 
                    submenu["page"] = page_data
                    del submenu["page_id"]
                    submenu = DB_Utility.convert_obj_id_to_str_id(submenu)
            return sub_menu_list
        
    def process_menu(self,menu_list,db):
        all_sub_menu_ids = set()
        all_page_ids = set()
        sub_menu_dict = {}
        menulist = []
        for menu in menu_list:
            sub_menu_ids = menu.get("sub_menu_ids", [])
            if sub_menu_ids:
                all_sub_menu_ids.update(sub_menu_ids)
            else:
                page_id = menu.get("page_id", None)
                if page_id: 
                    page_id = DB_Utility.str_to_obj_id(page_id)
                    all_page_ids.add((page_id)) 
        
        if all_sub_menu_ids:
            converted_sub_menu_ids = DB_Utility.str_id_list_to_obj_list(list(all_sub_menu_ids))
            sub_menu_query = {"_id": {"$in": converted_sub_menu_ids}, "is_deleted": False}
            all_sub_menus = Mongo_DB_Manager.read_documents(db[self.sub_menu_collection], sub_menu_query)
            processed_sub_menus = self.process_sub_menu1(list(all_sub_menus), db)           
            sub_menu_dict = {
                str(sub_menu["_id"]): DB_Utility.convert_obj_id_to_str_id(sub_menu)
                for sub_menu in processed_sub_menus
            }
        if all_page_ids:
            all_page_ids_list = list(all_page_ids)
            page_query = {"_id": {"$in": all_page_ids_list}, "is_deleted": False}
            all_pages = Mongo_DB_Manager.read_documents(db[self.page_collection], page_query)
            all_pages = self.process_page(list(all_pages),db)                
            page_dict = {
            str(page["_id"]): DB_Utility.convert_obj_id_to_str_id(page)
            for page in all_pages
            }
        for menu in menu_list:
            sub_menu_ids = menu.get("sub_menu_ids", [])
            if sub_menu_ids:                
                sub_menu_list = [sub_menu_dict.get(str(sub_menu_id)) for sub_menu_id in sub_menu_ids if str(sub_menu_id) in sub_menu_dict]
                if sub_menu_list:
                    menu["sub_menus"] = sub_menu_list
                del menu["sub_menu_ids"]
            
            else:
                page_id = menu.get("page_id")
                if page_id and str(page_id) in page_dict:
                    menu["page"] = page_dict[str(page_id)]
                    del menu["page_id"]
            
            menu_new = DB_Utility.convert_obj_id_to_str_id(menu)
            menulist.append(menu_new) 
        return menulist