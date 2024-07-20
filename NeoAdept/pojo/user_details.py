from dataclasses import dataclass, fields,_MISSING_TYPE,field
from typing import Optional,Dict,List,Any
from xmlrpc.client import DateTime

@dataclass
class ColumnVisibility:
    db_column: Optional[str]
    ui_column: Optional[str]
    order: Optional[int]
    enable: Optional[bool]
    default_enable: Optional[bool]
    widget_enable: Optional[bool]

@dataclass
class CollectionVisibility:
    widget_enable: Optional[bool]
    columns: Optional[List[ColumnVisibility]]
    
@dataclass
class Permissions:
  # Use Dict[str, Any] to represent dynamic permission names and values
  permissions: Dict[str, Any] = None
    
@dataclass
class USER_DETAILS:
    _id: Optional[str] = None
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    status: Optional[str] = None
    client_id: Optional[str] = None
    token: Optional[str] = None
    notes: Optional[str] = None
    is_deleted: Optional[bool] = None
    created_on: Optional[DateTime] = None
    created_by: Optional[str] = None
    updated_on: Optional[DateTime] = None
    updated_by: Optional[str] = None
    entity_id: Optional[str] = None
    otp: Optional[str] = None
    otp_timestamp: Optional[DateTime] = None
    client_name: Optional[str] = None
    db_name: Optional[str] = None
    new_password: Optional[str] = None
    current_password: Optional[str] = None
    #_id: Optional[str] = None
    client_domain: Optional[str] = None
    #file_id: Optional[str] = None
    photo_id: Optional[str] = None
    photo_file_name: Optional[str] = None
    photo: Optional[str] = None
    portal_view_id: Optional[str] = None
    portal_view_role: Optional[str] = None
    visibility: Optional[Dict[str, CollectionVisibility]] = None
    permissions: Optional[Permissions] = None

    def __post_init__(self):
        # Check for missing mandatory attributes
        missing_mandatory = [field.name for field in fields(self) if field.default == _MISSING_TYPE and not hasattr(self, field.name)]
        if missing_mandatory:
            raise ValueError(f"Missing mandatory attributes: {', '.join(missing_mandatory)}")
