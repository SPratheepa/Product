from dataclasses import dataclass, field, fields, _MISSING_TYPE
from typing import Optional, Dict, Any
from datetime import datetime

@dataclass
class LOG_DETAILS:
    request_info: Optional[Dict[Any, Any]] = None
    _id: Optional[str] = None
    user: Optional[str] = None
    api_name: Optional[str] = None
    current_time: Optional[datetime] = None
