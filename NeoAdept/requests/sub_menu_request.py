from ..gbo.common import Base_Request
from ..pojo.ui_template.sub_menu import SUB_MENU

class create_sub_menu_request(Base_Request):
    
    def parse_request(self):       
        self.sub_menu_details = self.request_data
        self.sub_menu_obj = SUB_MENU(**self.sub_menu_details)
        
     
    def validate_request(self):
         
        if not isinstance(self.sub_menu_obj, SUB_MENU):
            raise ValueError("Invalid Object")     
        
        string_check_fields= ["name","icon","page_id"] 
        for field in string_check_fields:
            if not isinstance( self.sub_menu_details[field], str):
                 raise ValueError(f"{field} must be an string")
            
        if not all(self.sub_menu_details[field] for field in string_check_fields):
            raise ValueError(f"One of the {string_check_fields} is empty or null")
        
        
       