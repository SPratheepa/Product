from ..gbo.common import Base_Request
from ..pojo.menu import MENU

class create_menu_request(Base_Request):
    
    def parse_request(self):       
        self.menu_details = self.request_data
        self.menu_obj = MENU(**self.menu_details)
        
     
    def validate_request(self):
         
        if not isinstance(self.menu_obj, MENU):
            raise ValueError("Invalid Object")     
        
        string_check_fields= ["name","icon"] 
        for field in string_check_fields:
            if not isinstance( self.menu_details[field], str):
                 raise ValueError(f"{field} must be an string")
            
        if not all(self.menu_details[field] for field in string_check_fields):
            raise ValueError(f"One of the {string_check_fields} is empty or null")
        
        print("IS_SUB_MENU::",self.menu_obj.is_sub_menu)
        if not self.menu_obj.is_sub_menu and (self.menu_obj.sub_menu_ids is not None and self.menu_obj.sub_menu_ids != []):
               raise ValueError('Cannot add sub_menus as submenu is set to False')
        if not self.menu_obj.is_sub_menu and not self.menu_obj.page_id:
               raise ValueError('page_id linked to the menu is missing')
        if  self.menu_obj.is_sub_menu and self.menu_obj.page_id:
                 raise ValueError('page_id cannot be added if the is_sub_menu set to True')
        if self.menu_obj.is_sub_menu and (self.menu_obj.sub_menu_ids == [] or self.menu_obj.sub_menu_ids is None):
                raise ValueError('as submenu is set to True,please provide sub_menu_ids')