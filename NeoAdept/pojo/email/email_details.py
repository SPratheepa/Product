from dataclasses import dataclass
from typing import Optional,List,Dict
from datetime import datetime

@dataclass
class EMAIL_DETAILS:
    
    from_email: str  # Email address of the sender
    send_to: List[str]  # List of recipient email addresses
    subject: str  # Subject of the email
    content: str  # Body content of the email (HTML or plain text)  # Updated field
    sent_on: datetime  # Date and time the email was sent
    attachments: Optional[List[str]] = None  # List of attachment file paths (if any)
    _id: Optional[str] = None  # Unique identifier (typically populated by MongoDB)