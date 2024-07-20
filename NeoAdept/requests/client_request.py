from ..gbo.common import Base_Request
from ..pojo.client_details import CLIENT_DETAILS,SUBSCRIPTION_DETAILS
from ..gbo.common import Custom_Error
from ..utilities.constants import CONSTANTS

class create_client_request(Base_Request):
    def parse_request(self):
        self.client_details = self.request_data
        self.client_details_obj = CLIENT_DETAILS(**self.client_details)
        
        self.subscription_details =  self.client_details_obj.subscription_details
        if self.subscription_details:
            self.subscription_details_obj = [SUBSCRIPTION_DETAILS(**sub_detail) for sub_detail in self.subscription_details]
     
    def validate_request(self):
        if not isinstance(self.client_details_obj, CLIENT_DETAILS):
            raise ValueError("Invalid Object")
        if self.client_details_obj:
             self.validate_add_client_request(self.client_details_obj)
        
        if self.subscription_details:
            if not all(isinstance(sub_detail, SUBSCRIPTION_DETAILS) for sub_detail in self.subscription_details_obj):
                raise ValueError("Invalid SUBSCRIPTION_DETAILS Object")
        
    def validate_add_client_request(self,client_details_obj):       
        required_attributes = [
            'client_name',
            'client_address',
            'contact_person',
            'email',
            'api_url',
            'domain',
            'status'
        ]

        # Check each attribute, raising an error if it's missing or empty
        for attribute in required_attributes:
            value = getattr(client_details_obj, attribute)
            if not value or not value.strip():
                error_msg = f"Missing {attribute} in request"
                raise Custom_Error(error_msg)
        
class create_client_subscription_request(Base_Request):
    def parse_request(self):
        self.client_details = self.request_data
        self.client_details_obj = CLIENT_DETAILS(**self.client_details)
        
        self.subscription_details =  self.client_details_obj.subscription_details
        if self.subscription_details:
            self.subscription_details_obj = [SUBSCRIPTION_DETAILS(**sub_detail) for sub_detail in self.subscription_details]
     
    def validate_request(self):
        if not isinstance(self.client_details_obj, CLIENT_DETAILS):
            raise ValueError("Invalid Object")
        if self.client_details_obj:
             self.validate_add_subscriptions_request(self.client_details_obj)
        
        if self.subscription_details:
            if not all(isinstance(sub_detail, SUBSCRIPTION_DETAILS) for sub_detail in self.subscription_details_obj):
                raise ValueError("Invalid SUBSCRIPTION_DETAILS Object")
     
    def validate_add_subscriptions_request(self, client_details_obj):
        required_attributes = [
            '_id',
            'subscription_details'
        ]
        for attribute in required_attributes:
            value = getattr(client_details_obj, attribute)
            if not value:
                error_msg = f"Missing {attribute} in request"
                raise Custom_Error(error_msg)
            elif isinstance(value, list) and not value:
                error_msg = f"{attribute} cannot be empty"
                raise Custom_Error(error_msg)
            
        for sub_detail in self.subscription_details_obj:
            if not hasattr(sub_detail, 'start_date') or not hasattr(sub_detail, 'end_date'):
                raise Custom_Error(CONSTANTS.START_END_DATE_MISSING)    
            
class update_client_request(Base_Request):
    def parse_request(self):
        self.client_details = self.request_data
        self.client_details_obj = CLIENT_DETAILS(**self.client_details)
        
    def validate_request(self):
        if not isinstance(self.client_details_obj, CLIENT_DETAILS):
            raise ValueError("Invalid Object")
        if self.client_details_obj:
             self.validate_update_client_request(self.client_details_obj)
     
    def validate_update_client_request(self,client_details_obj):
        required_attributes = [ '_id', 'client_name','client_address','contact_person','email','api_url','domain','status' ]

        for attribute in required_attributes:
            value = getattr(client_details_obj, attribute)
            if not value or not value.strip():
                error_msg = f"Missing {attribute} in request"
                raise Custom_Error(error_msg)
                    
class delete_client_request(Base_Request):
    def parse_request(self):
        self.client_details = self.request_data
        self.client_details_obj = CLIENT_DETAILS(**self.client_details)
     
    def validate_request(self):
        if not isinstance(self.client_details_obj, CLIENT_DETAILS):
            raise ValueError("Invalid Object")
        if self.client_details_obj:
             self.validate_delete_client_request(self.client_details_obj)
     
    def validate_delete_client_request(self,client_details_obj):
        if not client_details_obj._id or client_details_obj._id.strip() == "":
            raise Custom_Error("Missing _id in request")

class upload_client_request(Base_Request):
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
 
'''                  
class update_settings_request(Base_Request):
    def parse_request(self):
        self.settings = self.request_data
        self.settings_obj = CLIENT_DETAILS(**self.settings)
     
    def validate_request(self):
        if not isinstance(self.settings_obj, CLIENT_DETAILS):
            raise ValueError("Invalid Object")
'''
