class Custom_Error(Exception):
    def __init__(self, message):
        super().__init__(message)

class Base_Request:
    def __init__(self, request_data):
        self.request_data = request_data
        #print("Base_request",self.request_data)

    def parse_request(self):
        raise NotImplementedError("Subclasses must implement parse_request method.")

    def validate_request(self):
        raise NotImplementedError("Subclasses must implement validate_request method.")
   

