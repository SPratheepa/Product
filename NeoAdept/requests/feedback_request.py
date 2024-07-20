from NeoAdept.pojo.feedback_details import FEEDBACK_DETAILS
from NeoAdept.gbo.common import Base_Request


class create_feedback_request(Base_Request):
    def parse_request(self):
        self.feedback_details = self.request_data
        self.feedback_details_obj = FEEDBACK_DETAILS(**self.feedback_details)

    def validate_request(self):
        if not isinstance(self.feedback_details_obj, FEEDBACK_DETAILS):
            raise ValueError("Invalid Feedback Details Object") 
        #missing_attributes = Base_Request.check_missing_elements(FEEDBACK_DETAILS,self.feedback_details)
        #if len(missing_attributes)>0:
            #raise ValueError("Missing attributes ",missing_attributes)
        
    
        