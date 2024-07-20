from ..gbo.common import Base_Request
from ..pojo.role import ROLE

class create_role_request(Base_Request):
    
    def parse_request(self):       
        self.role_details = self.request_data
        self.role_obj = ROLE(**self.role_details)
        
     
    def validate_request(self):
         
        if not isinstance(self.role_obj, ROLE):
            raise ValueError("Invalid Object")     
        
        string_check_fields= ["name","description"] 
        for field in string_check_fields:
            if not isinstance( self.role_details[field], str):
                 raise ValueError(f"{field} must be an string")
            
        if not all(self.role_details[field] for field in string_check_fields):
            raise ValueError(f"One of the {string_check_fields} is empty or null")
        
        menu_ids = self.role_obj.menu_ids
        if menu_ids :
            self.validate_menu_ids(menu_ids)
        

    def validate_menu_ids(self, menu_ids):
        if menu_ids is None:
            raise ValueError("menu_ids must not be None")
        if not menu_ids:
            raise ValueError("menu_ids must not be empty")
        if not isinstance(menu_ids, list):
            raise ValueError("menu_ids must be a list")
        if not all(isinstance(item, str) for item in menu_ids):
            raise ValueError("All elements in menu_ids must be strings")

    
  
           
            
       