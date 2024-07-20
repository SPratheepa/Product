from NeoAdept.pojo.registration_details import REGISTRATION_DETAILS
from ..utilities.constants import CONSTANTS
from ..gbo.common import Base_Request
from ..gbo.common import Custom_Error

class register_client_request(Base_Request):
    def parse_request(self):
        self.client_details = self.request_data
        self.client_details_obj = REGISTRATION_DETAILS(**self.client_details)
     
    def validate_request(self):
        required_fields = ["name","email","phone","company"]
        for field in required_fields:
            if not getattr(self.client_details_obj, field):
                raise ValueError(f"Missing required field: {field}")
            
class update_client_status_request(Base_Request):
    def parse_request(self):
        self.client_details = self.request_data
        self.client_details_obj = REGISTRATION_DETAILS(**self.client_details)
     
    def validate_request(self):
        required_fields = ["_id","status","comments"]
        for field in required_fields:
            if not getattr(self.client_details_obj, field):
                raise ValueError(f"Missing required field: {field}")
            
   
