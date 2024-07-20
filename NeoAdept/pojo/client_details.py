from dataclasses import dataclass, field, fields,_MISSING_TYPE
from datetime import datetime
from typing import List, Optional

@dataclass
class SUBSCRIPTION_DETAILS:
    start_date: str
    end_date: str

@dataclass
class CLIENT_DETAILS:
    client_name: Optional[str] = None
    api_url: Optional[str] = None
    domain: Optional[str] = None
    db_name: Optional[str] = None
    status: Optional[str] = None
    is_deleted: Optional[bool]= False
    client_address: Optional[str]= None
    contact_person: Optional[str]= None
    phone: Optional[str]= None
    email: Optional[str]= None
    created_by: Optional[str] = None
    created_on: Optional[datetime] = None
    updated_by: Optional[str] = None
    updated_on: Optional[datetime] = None
    subscription_details: Optional[List[SUBSCRIPTION_DETAILS]] =  field(default_factory=list)
    _id: Optional[str] = None 
    #client_id: Optional[str] = None

    def __post_init__(self):
        # Check for missing mandatory attributes
        missing_mandatory = [field.name for field in fields(self) if field.default == _MISSING_TYPE and not hasattr(self, field.name)]
        if missing_mandatory:
            raise ValueError(f"Missing mandatory attributes: {', '.join(missing_mandatory)}")
        