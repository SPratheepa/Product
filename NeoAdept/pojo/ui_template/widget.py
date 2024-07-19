from typing import Optional
from dataclasses import dataclass

@dataclass
class WIDGET:
        name: str 
        file_name: str 
        type :  str       
        class_name: str
        info : str      
        description:str
        is_deleted : bool = False        
        _id: Optional[str] = None 
        _order : Optional[int] = None
        created_by: Optional[str] = None
        created_on: Optional[str] = None
        updated_by: Optional[str] = None
        updated_on: Optional[str] = None

       
        
                
       