from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime

@dataclass
class ACCESS_DETAILS:
    api_name: str
    submodule_name: str
    api_access: Optional[List[str]] =  field(default_factory=list)
    collection: Optional[List[str]] = field(default_factory=list)

    def to_dict(self):
        return {"api_name": self.api_name,"submodule_name": self.submodule_name, "api_access": self.api_access, "collection": self.collection}

@dataclass
class MODULE_DETAILS:
    _id: Optional[str] = None
    module: Optional[str] = None
    access: Optional[List[ACCESS_DETAILS]] =  field(default_factory=list)
    created_by: Optional[str] = None
    created_on: Optional[datetime] = None
    updated_by: Optional[str] = None
    updated_on: Optional[datetime] = None