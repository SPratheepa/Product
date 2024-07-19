from argparse import OPTIONAL
from dataclasses import dataclass
from typing import List, Optional,Dict,Union
from xmlrpc.client import DateTime


@dataclass
class ACTIVITY_DETAILS:
    _id: Optional[str] = None
    subject: str = None
    type : Optional[str] = None
    date_time : Optional[DateTime] = None
    duration_minutes : Optional[int] = None
    #job_info : Optional[Dict] = None
    job_id: Optional[str] = None
    #job_name: Optional[str] = None
    #job_candidate_list: Optional[List[str]] = None
    #internal_users : Optional[List[str]] = None
    candidate_info: Optional[Dict] = None 
    candidate_id: Optional[Union[str, List[str]]] = None
    comments: Optional[str] = None
    #company_info: Optional[Dict] = None
    company_id: Optional[str] = None
    #company_name: Optional[str] = None
    #company_contact_list: Optional[List[str]] = None
    created_by : Optional[str]= None
    created_on : Optional[DateTime]= None
    updated_by : Optional[str]= None
    updated_on : Optional[DateTime]= None
    is_deleted: Optional[bool] = False