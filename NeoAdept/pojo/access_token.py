from dataclasses import dataclass, fields, _MISSING_TYPE
from typing import Optional

@dataclass
class ACCESS_TOKEN:
    
    email: str  
    role: str
    phone: str = None
    _id: Optional[str] = None
    client_name: Optional[str] = None
    client_id: Optional[str] = None
    db_name: Optional[str] = None
    client_db_name: Optional[str] = None
    #permissions: Optional[str] = None
    #widget_enable_for_db: Optional[str] = None

    def __post_init__(self):
        # Check for missing mandatory attributes
        missing_mandatory = [field.name for field in fields(self) if field.default == _MISSING_TYPE and not hasattr(self, field.name)]
        if missing_mandatory:
            raise ValueError(f"Missing mandatory attributes: {', '.join(missing_mandatory)}")