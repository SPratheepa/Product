from ..gbo.common import Base_Request
from ..pojo.page import PAGE
class create_page_request(Base_Request):
    
    def parse_request(self):
        print("self.request_data",self.request_data)
        self.page_details = self.request_data
        self.page_obj = PAGE(**self.page_details)
        
     
    def validate_request(self):
         
        if not isinstance(self.page_obj, PAGE):
            raise ValueError("Invalid Object")     
        
        string_check_fields= ["name","router_link"] 
        for field in string_check_fields:
            if not isinstance( self.page_details[field], str):
                 raise ValueError(f"{field} must be an string")
            
        if not all(self.page_details[field] for field in string_check_fields):
            raise ValueError(f"One of the {string_check_fields} is empty or null")
        
        widget_ids = self.page_obj.widget_ids
        self.validate_widget_ids(widget_ids)
        

    def validate_widget_ids(self, widget_ids):
        if widget_ids is None:
            raise ValueError("widget_ids must not be None")
        if not widget_ids:
            raise ValueError("widget_ids must not be empty")
        if not isinstance(widget_ids, list):
            raise ValueError("widget_ids must be a list")
        if not all(isinstance(item, str) for item in widget_ids):
            raise ValueError("All elements in widget_ids must be strings")

    
  
           
            
       