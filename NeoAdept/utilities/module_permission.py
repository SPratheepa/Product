import os
import pandas as pd
import numpy as np
from ..utilities.constants import CONSTANTS
from ..utilities.db_utility import DB_Utility,Mongo_DB_Manager
from ..utilities.utility import Utility
from NeoAdept. pojo.directory import DIRECTORY
from ..gbo.common import Custom_Error

class Module_Permission:   
 
    def __init__(self,role_permission_file):
        if not hasattr(self, 'initialized'): 
            self.directory = DIRECTORY()            
            self.excel_file =  self.get_role_permission_file(role_permission_file)         
            self.EXCEL_KEYS = {
            'ROLE_PERMISSION': ['role_name', 'module', 'submodule_level_1', 'submodule_1_access']
              }     
            self.role_collection = "ROLE"
            self.module_details_collection = "MODULE_DETAILS"
   
    def load_role_permission(self,db) :     
        df_role_permission = self.load_excel_sheet("ROLE_PERMISSION",self.EXCEL_KEYS["ROLE_PERMISSION"]) 
        self.validate_objects(df_role_permission,"ROLE_PERMISSION",CONSTANTS.ROLE_PERMISSION_MANDATORY_EXCEL_KEYS)
        
        #df = pd.read_excel(self.excel_file)
        grouped = df_role_permission.groupby(['role_name'])
        
        for role_name, group in grouped:
            if isinstance(role_name, tuple):
                role_name = role_name[0]
            query = {'name':role_name}
            role_data = Mongo_DB_Manager.read_one_document(db[self.role_collection],query)
            if not role_data:
                raise Custom_Error(f"Role with name '{role_name}' not found in collection")
        
        for role_name, group in grouped:
            if isinstance(role_name, tuple):
                role_name = role_name[0]
            query = {'name':role_name}
            role_data = Mongo_DB_Manager.read_one_document(db[self.role_collection],query)
            role_id = DB_Utility.obj_id_to_str(role_data["_id"])
            
            role_permission_query = {'role_id': role_id}
            existing_permission = Mongo_DB_Manager.read_one_document(db["ROLE_PERMISSION"], role_permission_query)
            
            if existing_permission:
                # Update existing role permissions
                for _, row in group.iterrows():
                    module = row['module']
                    submodule = row['submodule_level_1']
                    access = row['submodule_1_access']
                    
                    if module not in existing_permission['permissions']:
                        existing_permission['permissions'][module] = {}
                    existing_permission['permissions'][module][submodule] = access
                Mongo_DB_Manager.update_document(db["ROLE_PERMISSION"], {'_id': existing_permission['_id']}, existing_permission)
            else:
                doc = {
                        "role_id": role_id,
                        "role_name": role_name,
                        "permissions": {},
                        "created_by": "admin",  
                        "created_on": Utility.get_current_time()
                    }
                for _, row in group.iterrows():
                    module = row['module']
                    submodule = row['submodule_level_1']
                    access = row['submodule_1_access']
                    
                    if module not in doc['permissions']:
                        doc['permissions'][module] = {}
                    doc['permissions'][module][submodule] = access
                Mongo_DB_Manager.create_document(db["ROLE_PERMISSION"],doc)

    def get_role_permission_file(self,role_permission_file):     
        files_folder = self.directory.get_folder('files')       
        file_path = self.directory.get_folder(role_permission_file,parent_folder = files_folder)       
        if not os.path.exists(file_path):
                raise Custom_Error("File not found") 
        return file_path

    def load_excel_sheet(self,sheet_name,expected_keys):
        df = pd.read_excel(self.excel_file, sheet_name=sheet_name).replace({np.nan: None})
        columns = df.columns.tolist()
        if not all(key in columns for key in expected_keys):
            raise Custom_Error(f"Excel Invalid for sheet: {sheet_name}")       
        return df 
   
    def validate_objects(self,df,collection_name,columns) :  
        for index, row in df.iterrows():
            if any(row[key] in (None, "") for key in columns):
                raise Custom_Error(f'Either of the values in {collection_name} : {columns} is null or empty for the index {index}')    
        '''value = self.check_duplicates(df,'module','role_name')      
        if value is not None:
            raise Custom_Error(f'Duplicate values found in {collection_name } column ::{value}')  '''    
   
    def check_duplicates(self,df, column_to_check,group_by_column):      
        # Group data by the specified column (e.g., role_name)
        grouped_data = df.groupby(group_by_column)

        # Iterate through each group
        for _, group_df in grouped_data:
            # Check if the specified column has duplicates within the group
            if not group_df[column_to_check].is_unique:
                # Return the column name if duplicates are found
                return column_to_check

        # Return None if no duplicates are found
        return None
  
    def load_module_details(self, db):
        df_modules = self.load_excel_sheet("MODULES", self.get_module_excel_keys())
        
        for _, row in df_modules.iterrows():
            module_name = row['module']
            access_details = self.process_module_row(row)
            module_document = {
                "module": module_name,
                "access": access_details,
                "created_by": "admin",
                "created_on": Utility.get_current_time()
            }
            Mongo_DB_Manager.create_document(db[self.module_details_collection], module_document)

    def get_module_excel_keys(self):
        base_keys = ['module']
        access_keys = [f'access{i}.{key}' for i in range(1, 18) for key in ['api_name', 'submodule_name', 'api_access', 'collection']]
        return base_keys + access_keys

    def validate_module_data(self, df):
        required_keys = self.get_module_excel_keys()
        for index, row in df.iterrows():
            for key in required_keys:
                if key in row and (row[key] is None or row[key] == ""):
                    raise Custom_Error(f"Value for {key} is missing in row {index}")

    def process_module_row(self, row):
        access_details = []
        for i in range(1, 18):
            api_name_key = f'access{i}.api_name'
            submodule_name_key = f'access{i}.submodule_name'
            api_access_key = f'access{i}.api_access'
            collection_key = f'access{i}.collection'
            if row[api_name_key]:
                access_details.append({
                    "api_name": row[api_name_key],
                    "submodule_name": row[submodule_name_key],
                    "api_access": row[api_access_key].split(','),
                    "collection": row[collection_key].split(',')
                })
        return access_details