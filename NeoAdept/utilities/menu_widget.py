import os
import pandas as pd
import numpy as np
from ..utilities.constants import CONSTANTS
from ..utilities.db_utility import DB_Utility,Mongo_DB_Manager
from ..utilities.utility import Utility
from NeoAdept. pojo.common.directory import DIRECTORY
from ..gbo.common import Custom_Error
from NeoAdept.pojo.ui_template.page import PAGE
from NeoAdept.pojo.ui_template.role import ROLE
from NeoAdept.pojo.ui_template.sub_menu import SUB_MENU
from NeoAdept.pojo.ui_template.menu import MENU
from NeoAdept.pojo.ui_template.widget import WIDGET



class Menu_Widget:   
 
   def __init__(self,ui_template_file):
        if not hasattr(self, 'initialized'): 
            self.directory = DIRECTORY()            
            self.excel_file =  self.get_ui_template_file(ui_template_file)         
            self.EXCEL_KEYS = {
            'WIDGET': ['class_name', 'description', 'file_name', 'info', 'name', 'type'],
            'PAGE': ['class_name', 'description', 'router_link', 'info', 'name', 'widget_mapping'],
            'MENU': ['name', 'icon', 'sub_menus', 'page_mapping'],
            'SUB_MENU': ['name', 'icon', 'page_mapping'],
            'ROLE': ['name', 'description', 'menus']
              }      
   
   def load_widget_menu(self,db) :       
        file_name_to_widget_ids = router_link_to_page_ids = name_to_sub_menu_ids = name_to_menu_ids = None
       
        df_widgets = self.load_excel_sheet("WIDGET",self.EXCEL_KEYS["WIDGET"])
        df_pages = self.load_excel_sheet('PAGE',self.EXCEL_KEYS['PAGE'])
        df_sub_menus = self.load_excel_sheet('SUB_MENU',self.EXCEL_KEYS['SUB_MENU'])
        df_menus = self.load_excel_sheet('MENU',self.EXCEL_KEYS['MENU'])
        df_roles = self.load_excel_sheet('ROLE',self.EXCEL_KEYS['ROLE'])
        
        for collection_name in CONSTANTS.UI_COLLECTIONS:     
                if collection_name == 'WIDGET':                    
                    self.validate_objects(df_widgets,collection_name,CONSTANTS.WIDGET_MANDATORY_EXCEL_KEYS)
                    
                   
                elif collection_name == 'PAGE':                     
                      self.validate_objects(df_pages,collection_name,CONSTANTS.PAGE_MANDATORY_EXCEL_KEYS)
                      
                elif collection_name == 'SUB_MENU':                     
                      self.validate_objects(df_sub_menus,collection_name,CONSTANTS.SUB_MENU_MANDATORY_EXCEL_KEYS)
                      
                elif collection_name == 'MENU':                     
                      self.validate_objects(df_menus,collection_name,CONSTANTS.MENU_MANDATORY_EXCEL_KEYS)
                     
                elif collection_name == 'ROLE':                      
                      self.validate_objects(df_roles,collection_name,CONSTANTS.ROLE_MANDATORY_EXCEL_KEYS)
                    
                   
        widget_list = self.create_objects(df_widgets, 'WIDGET', WIDGET)            
        widget_ids = Mongo_DB_Manager.create_documents(db['WIDGET'],widget_list)            
        file_name_to_widget_ids = {row.file_name : str(widget_id) for row, widget_id in zip(df_widgets.itertuples(index=False), widget_ids)}        
             
        page_list = self.create_objects(df_pages, 'PAGE', PAGE, extra_processing=self.process_page_doc, extra_args=(file_name_to_widget_ids,))
        page_ids = Mongo_DB_Manager.create_documents(db['PAGE'], page_list)
        router_link_to_page_ids = {row.router_link: str(page_id) for row, page_id in zip(df_pages.itertuples(index=False), page_ids)}            
                    
        sub_menu_list = self.create_objects(df_sub_menus, 'SUB_MENU', SUB_MENU, extra_processing=self.process_sub_menu_doc, extra_args=(router_link_to_page_ids,))
        sub_menu_ids = Mongo_DB_Manager.create_documents(db['SUB_MENU'], sub_menu_list)
        name_to_sub_menu_ids = {row.name: str(sub_menu_id) for row, sub_menu_id in zip(df_sub_menus.itertuples(index=False), sub_menu_ids)}            
              
        menu_list  = self.create_objects(df_menus, 'MENU', MENU, extra_processing=self.process_menu_doc, extra_args=(name_to_sub_menu_ids, router_link_to_page_ids))
        menu_ids = Mongo_DB_Manager.create_documents(db['MENU'], menu_list)
        name_to_menu_ids = {row.name: str(menu_id) for row, menu_id in zip(df_menus.itertuples(index=False), menu_ids)}            
          
        role_list = self.create_objects(df_roles, 'ROLE', ROLE, extra_processing=self.process_role_doc, extra_args=(name_to_menu_ids,))
        role_ids = Mongo_DB_Manager.create_documents(db['ROLE'],role_list)
        print("basic widgets, pages,menus,submenus,role created")
        if role_ids:
            query = {'is_deleted': False}
            roles = Mongo_DB_Manager.read_documents(db['ROLE'],query)
            for role in roles:
                role_id = DB_Utility.obj_id_to_str(role['_id'])
                role_name = role['name']
                self.add_role_permission_for_role(role_id,role_name,"admin",db) 
            print("role permissions created")
        
   def add_role_permission_for_role(self, role_id, role_name, email, db):
       module_info = Mongo_DB_Manager.read_documents(db['MODULE_DETAILS'], {})
       permissions = {}
       
       for module in module_info:
           module_name = module['module']
           if module_name == 'Common':
                default_value = True
           elif module_name == 'Permissions' and role_name in ['product-admin', 'product-user', 'admin']:
                default_value = True
           else:
                default_value = False
           #default_value = True if module_name == 'Common' else False
           permissions[module['module']] = self.get_submodule_structure(module['access'],default_value)

       new_role_permission = {
            'role_id': role_id,
            'role_name': role_name,
            'permissions': permissions,
            'created_by': email,
            'created_on': Utility.get_current_time(),
            'updated_by': email,
            'updated_on': Utility.get_current_time()
       }

       Mongo_DB_Manager.create_document(db['ROLE_PERMISSION'], new_role_permission)    
       
   def get_submodule_structure(self, submodules,default_value):
        submodule_permissions = {}

        for submodule in submodules:
            api_name = submodule['api_name']
            submodule_name = submodule['submodule_name']
            nested_access = submodule.get('access', [])

            key_name = submodule_name if submodule_name else api_name

            if nested_access:
                submodule_permissions[key_name] = self.get_submodule_structure(nested_access)
            else:
                submodule_permissions[key_name] = default_value

        return submodule_permissions

   def get_ui_template_file(self,ui_template_file):     
        files_folder = self.directory.get_folder('files')       
        file_path = self.directory.get_folder(ui_template_file,parent_folder = files_folder)       
        if not os.path.exists(file_path):
                raise Custom_Error("File not found") 
        return file_path

   def load_excel_sheet(self,sheet_name,expected_keys):
        df = pd.read_excel(self.excel_file, sheet_name=sheet_name).replace({np.nan: None})
        columns = df.columns.tolist()
        if not all(key in columns for key in expected_keys):
            raise Custom_Error(f"Excel Invalid for sheet: {sheet_name}")       
        return df 
   
   def prepare_document(self,row, key_map):
    return {key: row[key] for key in key_map if key in row}
   
   def validate_objects(self,df,collection_name,columns) :            
        value = self.check_duplicates(df,columns)      
        if value is not None:
            raise Custom_Error(f'Duplicate values found in {collection_name } column ::{value}')      
        for index, row in df.iterrows():
            if any(row[key] in (None, "") for key in columns):
                raise Custom_Error(f'Either of the values in {collection_name} : {columns} is null or empty for the index {index}')
            if collection_name == 'MENU':
                self.check_sub_menu_page_combination(row,index)
   
   def create_objects(self, df, key, obj_class, extra_processing=None, extra_args=()):
        obj_list = []
        for index, row in df.iterrows():
            doc = self.prepare_document(row, self.EXCEL_KEYS[key])
            if extra_processing:
                doc = extra_processing(doc, row, *extra_args)
            obj = obj_class(**doc)
            obj.created_on, obj.created_by, obj = Utility.settings_for_data_operation("client-admin", "add", None, obj)
            obj_list.append(obj.__dict__)
        return obj_list

   def process_page_doc(self, doc, row, file_name_to_widget_ids):
        
        doc['widget_ids'] = [file_name_to_widget_ids[widget.strip()] for widget in row['widget_mapping'].split(',')] if row['widget_mapping'] else []
        doc.pop("widget_mapping")
        return doc

   def process_sub_menu_doc(self, doc, row, router_link_to_page_ids):
        doc['page_id'] = router_link_to_page_ids[row["page_mapping"]]
        doc.pop('page_mapping')
        return doc

   def process_menu_doc(self, doc, row, name_to_sub_menu_ids, router_link_to_page_ids):
        doc['sub_menu_ids'] = [name_to_sub_menu_ids[sub_menu.strip()] for sub_menu in row['sub_menus'].split(',')] if row['sub_menus'] else []
        doc['is_sub_menu'] = bool(doc['sub_menu_ids'])
        if row["page_mapping"]:
            doc["page_id"] = router_link_to_page_ids[row["page_mapping"]]
        doc.pop("page_mapping", None)
        doc.pop("sub_menus", None)
        return doc

   def process_role_doc(self, doc, row, name_to_menu_ids):
        doc['menu_ids'] = [name_to_menu_ids[menu.strip()] for menu in row['menus'].split(',')]
        doc.pop('menus')
        return doc
   
   def check_duplicates(self,df, columns):      
      for column in columns:         
          duplicate_check = not df[column].is_unique         
          if duplicate_check:
            return column
      return None
  
   def check_sub_menu_page_combination(self,row,index):
        page_mapping_filled = pd.notnull(row['page_mapping']) and row['page_mapping'] != ''
        sub_menu_filled = pd.notnull(row['sub_menus']) and row['sub_menus'] != ''
        
        # Check if both page 1 and page 2 are filled
        if page_mapping_filled and sub_menu_filled:
            raise ValueError(f"Error at row {index}: Both 'page_mapping' and 'sub_menus' are filled.")
        
        # Check if neither page 1 nor page 2 are filled
        if not page_mapping_filled and not sub_menu_filled:
            raise ValueError(f"Error at row {index}: Neither 'page_mapping' nor 'sub_menus' is filled.")
        
        # Check if either page 1 or page 2 is filled
        if not (page_mapping_filled or sub_menu_filled):
            raise ValueError(f"Error at row {index}: Neither 'page_mapping' nor 'sub_menus' is filled.")
        
  