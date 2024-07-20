
from dataclasses import dataclass,field
from typing import Optional, List,Dict

"""@dataclass
class RULE:
    field: Optional[str] = None
    field_value: Optional[str] = None
    operator: Optional[str] = None  # operator like ==, !=, >, <, >=, <=

@dataclass
class CONDITION:
    condition: Optional[str] = None  # AND/OR
    rules: Optional[List[RULE]] = None"""

@dataclass
class DYNAMIC_WIDGET:
    name: Optional[str] = None
    file_name: Optional[str] = None
    info: Optional[str] = None
    type: Optional[str] = None
    class_name: Optional[str] = None
    description: Optional[str] = None
    query_information: Optional[dict] = None
    visual_type: Optional[str] = field(default="")
    visual_parameters: Optional[List[Dict]] = field(default_factory=list)
    is_deleted: Optional[bool] = False
    _id: Optional[str] = None
    created_by: Optional[str] = None
    created_on: Optional[str] = None
    updated_by: Optional[str] = None
    updated_on: Optional[str] = None
    db_type: Optional[str] = None
    db_name: Optional[str] = None

    