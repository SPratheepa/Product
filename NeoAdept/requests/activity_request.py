from NeoAdept.gbo.common import Base_Request
from NeoAdept.pojo.activity.activity_details import ACTIVITY_DETAILS


class create_activity_request(Base_Request):
    def parse_request(self):
        self.activity_details = self.request_data
        self.activity_details_obj = ACTIVITY_DETAILS(**self.activity_details)

    def validate_request(self):
        if not isinstance(self.activity_details_obj, ACTIVITY_DETAILS):
            raise ValueError("Invalid activity Details Object") 