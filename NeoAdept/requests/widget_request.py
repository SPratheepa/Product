from ..gbo.common import Base_Request
from ..pojo.widget import WIDGET

class create_widget_request(Base_Request):
    
    def parse_request(self):
        print("self.request_data",self.request_data)
        self.widget_details = self.request_data
        self.widget_obj = WIDGET(**self.widget_details)
        
     
    def validate_request(self):
         
        if not isinstance(self.widget_obj, WIDGET):
            raise ValueError("Invalid Object")     
        
        string_check_fields = ["name", "file_name"]

        for field in string_check_fields:
            if not isinstance(self.widget_details[field], str):
                raise ValueError(f"{field} must be a string")

        if not all(self.widget_details[field] for field in string_check_fields):
            raise ValueError(f"One of the fields {string_check_fields} is empty or null")

           
            
       