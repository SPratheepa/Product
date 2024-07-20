from dataclasses import dataclass, field
from typing import Optional, Dict, List, Union, Any
from datetime import datetime
   
@dataclass
class Permissions:
  # Use Dict[str, Any] to represent dynamic permission names and values
  permissions: Dict[str, Any] = None

@dataclass
class ROLE_PERMISSION:
    _id: Optional[str] = None
    role_id: Optional[str] = None
    role_name: Optional[str] = None
    permissions: Optional[Permissions] = None
    created_by: Optional[str] = None
    created_on: Optional[datetime] = None
    updated_by: Optional[str] = None
    updated_on: Optional[datetime] = None
    
