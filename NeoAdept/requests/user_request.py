from ..gbo.common import Base_Request, Custom_Error
from ..pojo.user_details import USER_DETAILS
from ..utilities.constants import CONSTANTS

class create_user_request(Base_Request):
    def parse_request(self):
        self.user_details = self.request_data
        self.user_details_obj = USER_DETAILS(**self.user_details)
        self.required_attributes = ["name", "phone", "email","role","status"]
     
    def validate_request(self):
        for attribute in self.required_attributes:
            value = getattr(self.user_details_obj, attribute)
            if not value or not value.strip():
                error_msg = f"Missing {attribute} in request"
                raise Custom_Error(error_msg)
                   
class update_user_request(Base_Request):
    def parse_request(self):
        self.user_details = self.request_data
        self.user_details_obj = USER_DETAILS(**self.user_details)
        self.required_fields = ["email", "name","status","phone","_id","role"]
     
    def validate_request(self):
        
        for field in self.required_fields:
            if not getattr(self.user_details_obj, field):
                raise ValueError(f"Missing required field: {field}")
            value = getattr(self.user_details_obj, field)
            if not value or not value.strip():
                error_msg = f"Missing {field} in request"
                raise Custom_Error(error_msg)
            
        
class delete_user_request(Base_Request):
    def parse_request(self):
        self.user_details = self.request_data
        self.user_details_obj = USER_DETAILS(**self.user_details)
        
    def validate_request(self):
        if not self.user_details_obj._id or self.user_details_obj._id.strip() == "":
            raise Custom_Error("Missing _id in request")
    
class upload_user_request(Base_Request):
    def parse_request(self):
        self.request = self.request_data
        if 'file' not in self.request.files:
            raise Custom_Error(CONSTANTS.NO_FILE)
        
        excel_file = self.request.files['file']
        if excel_file.filename == '':
            raise Custom_Error('Excel not found')
                
    def validate_request(self):
        excel_file = self.request.files['file']
        if not (excel_file and excel_file.filename.endswith('.xlsx')):
            raise Custom_Error('Excel not found')    
