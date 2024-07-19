from dataclasses import dataclass


@dataclass
class Attachment:
    file_name: str
    file_type: str
    id: str