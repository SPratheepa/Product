
from dataclasses import dataclass, field
from typing import List, Optional

from NeoAdept.pojo.attachment_details import Attachment


@dataclass
class FEEDBACK_DETAILS:
    _id: Optional[str] = None
    created_by: Optional[str] = None
    created_on: Optional[str] = None
    updated_by: Optional[str] = None
    updated_on: Optional[str] = None
    #common_fields : Optional[Common_Fields] = None
    rating: Optional[int] = 0
    feedback_type: Optional[str] = None
    content: Optional[str] = None
    attachment: Optional[List[Attachment]] = field(default_factory=list)
    key: Optional[str] = None
    client_name: Optional[str] = None




        