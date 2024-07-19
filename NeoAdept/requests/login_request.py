from ..utilities.constants import CONSTANTS
from ..gbo.common import Base_Request
from ..pojo.user.user_details import USER_DETAILS
from ..gbo.common import Custom_Error

class create_product_admin_request(Base_Request):
    def parse_request(self):
        self.login_details = self.request_data
        self.login_details_obj = USER_DETAILS(**self.login_details)
        
    def validate_request(self):
        required_fields = ["name", "phone", "email", "password"]
        for field in required_fields:
            if not getattr(self.login_details_obj, field):
                raise ValueError(f"Missing required field: {field}")
        
        if self.login_details:
            self.validate_prod_admin_request(self.login_details_obj)
    
    def validate_prod_admin_request(self, login_details_obj):
        if not all(getattr(login_details_obj, field) and getattr(login_details_obj, field).strip() for field in ['name', 'phone', 'email', 'password']):
            raise Custom_Error(CONSTANTS.CRDTS_ERR)

class login_request(Base_Request):
    def parse_request(self):
        self.login_details = self.request_data
        self.login_details_obj = USER_DETAILS(**self.login_details)

    def validate_request(self):
        required_fields =["email","password"]
        for field in required_fields:
            if not getattr(self.login_details_obj, field):
                raise ValueError(f"Missing required field: {field}")
        
        if self.login_details:
            self.validate_login_request(self.login_details_obj)

    
    def validate_login_request(self,login_details_obj):
        if not all(getattr(login_details_obj, field) and getattr(login_details_obj, field).strip() for field in ['email', 'password']):
            raise Custom_Error(CONSTANTS.CRDTS_ERR)

class forgot_password_request(Base_Request):
    def parse_request(self):
        self.login_details = self.request_data
        self.login_details_obj = USER_DETAILS(**self.login_details)
     
    def validate_request(self):
        required_fields =["email"]
        for field in required_fields:
            if not getattr(self.login_details_obj, field):
                raise ValueError(f"Missing required field: {field}")
        
        if self.login_details:
             self.validate_forgot_password_request(self.login_details_obj)
    
    def validate_forgot_password_request(self,login_details_obj):
        if not all(getattr(login_details_obj, field) and getattr(login_details_obj, field).strip() for field in ['email']):
            raise Custom_Error(CONSTANTS.CRDTS_ERR)
                              
class verify_otp_request(Base_Request):
    def parse_request(self):
        self.login_details = self.request_data
        self.login_details_obj = USER_DETAILS(**self.login_details)
     
    def validate_request(self):
        required_fields = ["email", "otp","new_password"]
        for field in required_fields:
            if not getattr(self.login_details_obj, field):
                raise ValueError(f"Missing required field: {field}")
              
        if self.login_details:
             self.validate_verify_otp_request(self.login_details_obj)
     
    def validate_verify_otp_request(self,login_details_obj):
        if not all(getattr(login_details_obj, field) and getattr(login_details_obj, field).strip() for field in ["email", "otp","new_password"]):
            raise Custom_Error(CONSTANTS.CRDTS_ERR)
            
        
class change_password_request(Base_Request):
    def parse_request(self):
        self.login_details = self.request_data
        self.login_details_obj = USER_DETAILS(**self.login_details)
     
    def validate_request(self):
        required_fields = ["current_password","new_password"]
        for field in required_fields:
            if not getattr(self.login_details_obj, field):
                raise ValueError(f"Missing required field: {field}")
       
        if self.login_details:
             self.validate_verify_otp_request(self.login_details_obj)
     
    def validate_verify_otp_request(self,login_details_obj):
        if not all(getattr(login_details_obj, field) and getattr(login_details_obj, field).strip() for field in ["current_password","new_password"]):
            raise Custom_Error(CONSTANTS.CRDTS_ERR)
        