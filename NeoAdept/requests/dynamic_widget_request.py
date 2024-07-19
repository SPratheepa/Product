from NeoAdept.gbo.common import Base_Request, Custom_Error
from NeoAdept.pojo.ui_template.dynamic_widget import DYNAMIC_WIDGET


class create_dynamic_widget_request(Base_Request):

    def parse_request(self):
        self.dynamic_widget_details = self.request_data
        self.dynamic_widget_obj = DYNAMIC_WIDGET(**self.dynamic_widget_details)

    def validate_request(self):

        if not isinstance(self.dynamic_widget_obj, DYNAMIC_WIDGET):
            raise Custom_Error("Invalid Object")
        
        #required_fields = ["name","file_name","info","type","class_name","description","query_information"]
        required_fields = ["name","query_information"]
        for field in required_fields:
            if not getattr(self.dynamic_widget_obj, field):
                raise Custom_Error(f"Missing required field: {field}")
            
        # Additional validation for query_information
        query_information = self.dynamic_widget_obj.query_information
        collection_name = query_information.get("conditions")[0].get("table")
        if not collection_name:
            raise Custom_Error("Collection name is missing in the query information.")

        operation = query_information.get('operation', '').lower()
        if operation not in ['and', 'or']:
            raise Custom_Error(f"Invalid condition: {operation}.")
        
        # Validate visualization_type specific fields
        visual_type = self.dynamic_widget_obj.visual_type
        visual_parameters = self.dynamic_widget_obj.visual_parameters
        if visual_parameters:
            if not isinstance(visual_parameters, list) or not all(isinstance(item, dict) for item in visual_parameters):
                raise Custom_Error("visual_parameters should be a list of objects.")
            '''
            for params in visual_parameters:
                if visual_type == "Ring":
                    required_fields = ["main_field", "series_value_field", "sort_by", "order_by", "maximum_no_of_items"]
                    for field in required_fields:
                        #if field not in params:
                            #raise Custom_Error(f"Missing required field in visual_parameters for RingChart: {field}")
                        if field == "maximum_no_of_items" and not isinstance(params[field], int):
                            raise Custom_Error(f"Field {field} should be an integer.")
                elif visual_type == "Summary":
                    # For table, the fields are optional but must be valid if present
                    optional_fields = ["page", "per_page", "order_by", "sort_by"]
                    for field in optional_fields:
                        if field in params and not isinstance(params[field], (str, int)):
                            raise Custom_Error(f"Field {field} should be a string or an integer.")
                else:
                    required_fields = ["main_field", "series_value_field"]
                    for field in required_fields:
                        if field not in params:
                            raise Custom_Error(f"Missing required field in visual_parameters for {visual_type}: {field}")'''
                    
class update_dynamic_widget_request(Base_Request):

    def parse_request(self):
        self.dynamic_widget_details = self.request_data
        self.dynamic_widget_obj = DYNAMIC_WIDGET(**self.dynamic_widget_details)
        self.query_information =  self.dynamic_widget_obj.query_information
        self.visual_type = self.dynamic_widget_obj.visual_type
        self.visual_parameters = self.dynamic_widget_obj.visual_parameters

    def validate_request(self):

        
        if not isinstance(self.dynamic_widget_obj, DYNAMIC_WIDGET):
            raise Custom_Error("Invalid Object")
        
        if self.query_information:            
            # Additional validation for query_information
            query_information = self.dynamic_widget_obj.query_information
            collection_name = query_information.get("conditions")[0].get("table")
            if not collection_name:
                raise Custom_Error("Collection name is missing in the query information.")

            operation = query_information.get('operation', '').lower()
            if operation not in ['and', 'or']:
                raise Custom_Error(f"Invalid condition: {operation}.")
        
        if self.visual_parameters:
            if not isinstance(self.visual_parameters, list) or not all(isinstance(item, dict) for item in self.visual_parameters):
                raise Custom_Error("visual_parameters should be a list of objects.")
            
class delete_dynamic_widget_request(Base_Request):
    def parse_request(self):
        self.dynamic_widget_details = self.request_data
        self.dynamic_widget_details_obj = DYNAMIC_WIDGET(**self.dynamic_widget_details)
     
    def validate_request(self):
        if not isinstance(self.dynamic_widget_details_obj, DYNAMIC_WIDGET):
            raise Custom_Error("Invalid Object")
        if self.dynamic_widget_details_obj:
             self.validate_delete_dynamic_widget_request(self.dynamic_widget_details_obj)
     
    def validate_delete_dynamic_widget_request(self,dynamic_widget_details_obj):
        if not dynamic_widget_details_obj._id or dynamic_widget_details_obj._id.strip() == "":
            raise Custom_Error("Missing _id in request")
