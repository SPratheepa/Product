from dataclasses import dataclass, field
import os
from typing import Optional
      
@dataclass
class DIRECTORY:
    project_root: str = field(default_factory=lambda: os.path.join(os.getcwd()))
    folder_name: Optional[str] = None
    
    def get_folder(self, folder_name: Optional[str] = None, parent_folder: Optional[str] = None) -> str:
        base_path = parent_folder if parent_folder else self.project_root
        if not os.path.exists(base_path):
            raise ValueError(f"Specified path does not exist: {base_path}")
        if folder_name:
            return os.path.join(base_path, folder_name)
        return base_path
    
    def create_folder(self, folder_name: str, parent_folder: Optional[str] = None) -> str:
        base_path = parent_folder if parent_folder else self.project_root
        attachment_folder = os.path.join(base_path, folder_name)
        if not os.path.exists(attachment_folder):
            os.makedirs(attachment_folder)
        return attachment_folder
