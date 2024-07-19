from typing import Optional
from dataclasses import dataclass
from NeoAdept.pojo.ui_template.page import PAGE
@dataclass
class SUB_MENU:
        name: str 
        icon: str         
        is_deleted : bool = False
        page_id : Optional[str] = None        
        _id: Optional[str] = None
        page : Optional[PAGE] = None 
        _order : Optional[int] = None
        created_by: Optional[str] = None
        created_on: Optional[str] = None
        updated_by: Optional[str] = None
        updated_on: Optional[str] = None        
        
        
                
       