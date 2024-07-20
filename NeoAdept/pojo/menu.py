from typing import Optional, List
from dataclasses import dataclass
from NeoAdept.pojo.page import PAGE
from NeoAdept.pojo.sub_menu import SUB_MENU


@dataclass
class MENU:
        name: str 
        icon: str         
        is_sub_menu : bool
        is_deleted : bool = False
        page_id :Optional[str] = None
        page : Optional[PAGE] = None
        sub_menu_ids : Optional[List[str]] = None
        sub_menus : Optional[List[SUB_MENU]] = None
        _order : Optional[int] = None       
        _id: Optional[str] = None
        created_by: Optional[str] = None
        created_on: Optional[str] = None
        updated_by: Optional[str] = None
        updated_on: Optional[str] = None
        
        
        
       
        
                
       