from dataclasses import dataclass, field, fields, _MISSING_TYPE
from typing import Optional, Dict, Any
from datetime import datetime

@dataclass
class SEARCH_DETAILS:
    _id: Optional[str] = None
    user: Optional[str] = None
    module: Optional[str] = None
    search_id: Optional[str] = None
    search_time: Optional[datetime] = None
