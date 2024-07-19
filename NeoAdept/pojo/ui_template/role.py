from typing import Optional,List
from dataclasses import dataclass
from NeoAdept.pojo.ui_template.menu import MENU

@dataclass
class ROLE:
        name: str         
        description:str  
        menu_ids : Optional[List[str]] = None
        is_deleted : bool = False           
        menus : Optional[List[MENU]] = None
        _id: Optional[str] = None
        created_by: Optional[str] = None
        created_on: Optional[str] = None
        updated_by: Optional[str] = None
        updated_on: Optional[str] = None
       