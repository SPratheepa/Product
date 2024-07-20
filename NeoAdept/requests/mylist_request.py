from ..gbo.common import Base_Request, Custom_Error
from ..pojo.list_details import FILE_EMAIL_GROUPING, LIST_GROUP, MoveToListDetails

class create_list_request(Base_Request):

    def parse_request(self):
        self.list_details = self.request_data
        self.list_details_obj = LIST_GROUP(**self.list_details)

    def validate_request(self):
        if not isinstance(self.list_details_obj, LIST_GROUP):
            raise ValueError("Invalid LIST_DETAILS Object") 
        self.validate_add_list_details(self.list_details_obj)
        
    def validate_add_list_details(self,list_details_obj):
        if not list_details_obj.list_name or not list_details_obj.list_name.strip():
            error_msg = "Missing list_name in request"
            raise Custom_Error(error_msg)
        
class update_list_request(Base_Request):

    def parse_request(self):
        self.list_details = self.request_data
        self.list_details_obj = LIST_GROUP(**self.list_details)
        
    def validate_request(self):
        if not isinstance(self.list_details_obj, LIST_GROUP):
            raise ValueError("Invalid LIST_DETAILS Object") 
        self.validate_update_list_details(self.list_details_obj)
        
    def validate_update_list_details(self,list_details_obj):
        for field in ['_id', 'list_name']:
            if not getattr(list_details_obj, field) or not getattr(list_details_obj, field).strip():
                raise Custom_Error(f"Missing or empty field: {field} in request")
            
class delete_list_request(Base_Request):
    def parse_request(self):
        if '_id' not in self.request_data:
            raise Custom_Error("Missing _id in request data")
        self.list_details = self.request_data['_id']
        #self.list_details = self.request_data
     
    def validate_request(self):
        if not self.list_details:
            raise Custom_Error("Empty request data")
        self.validate_delete_list_request(self.list_details)
     
    def validate_delete_list_request(self,list_details):
        if not isinstance(list_details, list):
            raise Custom_Error("Invalid list_details format. Expecting an array of IDs.")

        # Validate if each element in the list is a string 
        for item in list_details:
            if not isinstance(item, str):
                raise Custom_Error("Invalid ID format. Expecting strings in the list.")

        # Validate if any ID is empty or whitespace-only
        if any(not item.strip() for item in list_details):
            raise Custom_Error("Missing or empty _id in request")

class add_cv_list_request(Base_Request):
    def parse_request(self):
        self.cv_list_details = self.request_data
        self.cv_list_details_obj = FILE_EMAIL_GROUPING(**self.cv_list_details)

    def validate_request(self):
        if not isinstance(self.cv_list_details_obj, FILE_EMAIL_GROUPING):
            raise ValueError("Invalid Cv list Details Object")
        
        if not self.cv_list_details_obj.list_id:
            raise Custom_Error("List ID cannot be empty")
        
        if not self.cv_list_details_obj.candidate_id or not isinstance(self.cv_list_details_obj.candidate_id, list):
            raise ValueError("candidate_id should be a non-empty list")

        if not all(self.cv_list_details_obj.candidate_id):
            raise ValueError("candidate_id list should not contain empty values")
        
class remove_cv_list_request(Base_Request):
    def parse_request(self):
        self.cv_list_details = self.request_data
        self.cv_list_details_obj = FILE_EMAIL_GROUPING(**self.cv_list_details)

    def validate_request(self):
        if not isinstance(self.cv_list_details_obj, FILE_EMAIL_GROUPING):
            raise ValueError("Invalid Cv list Details Object")
        
        if not self.cv_list_details_obj.list_id:
            raise Custom_Error("List ID cannot be empty")
        
        if not self.cv_list_details_obj.candidate_id or not isinstance(self.cv_list_details_obj.candidate_id, list):
            raise ValueError("candidate_id should be a non-empty list")

        if not all(self.cv_list_details_obj.candidate_id):
            raise ValueError("candidate_id list should not contain empty values")
        
class MoveToListRequest(Base_Request):
    def parse_request(self):
        self.cv_list_details = self.request_data
        self.cv_list_details_obj = MoveToListDetails(**self.cv_list_details)

    def validate_request(self):
        if not isinstance(self.cv_list_details_obj, MoveToListDetails):
            raise ValueError("Invalid Move to List Details Object")
        
        if not self.cv_list_details_obj.from_list_id:
            raise Custom_Error("From List ID cannot be empty")
        
        if not self.cv_list_details_obj.to_list_id:
            raise Custom_Error("To List ID cannot be empty")
        
        if not self.cv_list_details_obj.candidate_id or not isinstance(self.cv_list_details_obj.candidate_id, list):
            raise ValueError("candidate_id should be a non-empty list")
        
        if not all(self.cv_list_details_obj.candidate_id):
            raise ValueError("candidate_id list should not contain empty values")
