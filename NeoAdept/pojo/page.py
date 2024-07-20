from typing import Optional,List
from dataclasses import dataclass
from NeoAdept.pojo.widget import WIDGET

@dataclass
class PAGE:
        name: str 
        router_link: str         
        is_deleted : bool = False
        class_name:Optional[str] = None
        info :  Optional[str] = None    
        description:Optional[str] = None        
        widget_ids : Optional[List[str]] = None 
        widgets : Optional[List[WIDGET]] = None
        _id: Optional[str] = None
        created_by: Optional[str] = None
        created_on: Optional[str] = None
        updated_by: Optional[str] = None
        updated_on: Optional[str] = None
       
        
       
        
                
       