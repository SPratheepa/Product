from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class REGISTRATION_DETAILS:
    
    name: Optional[str] = None  
    email: Optional[str] = None  
    phone: Optional[str] = None   
    company: Optional[str] = None 
    created_on: Optional[datetime] = None
    _id: Optional[str] = None  # Unique identifier (typically populated by MongoDB)
    status: Optional[str] = None
    comments: Optional[str] = None
    updated_on: Optional[datetime] = None
    updated_by: Optional[str] = None