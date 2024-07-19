from typing import Optional,List,Any

from dataclasses import dataclass

@dataclass
class ACADEMIC_RECORD:
        id: Optional[int] = 0
        course: Optional[str] = None
        course_name: Optional[str] = None
        institute: Optional[str] = None
        completion_year: Optional[int] = None
        specification: Optional[str] = None
        mode_of_education: Optional[str] = None

@dataclass
class CTC:
        ctc_type: Optional[str] = None
        current_ctc: Optional[float] = 0.0
        expected_ctc: Optional[float] = 0.0
        family_income: Optional[float] = 0.0
        expected_ctc_type: Optional[str] = None

@dataclass
class LOCATION:
        id: Optional[int] =  0
        country: Optional[str] =  None
        state: Optional[str] =  None
        city: Optional[str] =  None

@dataclass
class ONSITE_EXPERIENCE:
        oe_id: Optional[int]= 0
        country_visited: Optional[str]= None
        location: Optional[str]= None
        visa_details: Optional[str]= None
        visa_type: Optional[str]= None

@dataclass
class TRAVEL_DETAILS:    
        passport_no: Optional[str]= None
        int_exp: Optional[int]= 0
        nat_exp: Optional[int]= 0

@dataclass
class YEAR_EXPERIENCE:
        year : Optional[int]= 0
        months : Optional[int]= 0

@dataclass
class WORK_EXPERIENCE:
        company_name: Optional[str] = None
        start_year: Optional[int] = 0
        end_year: Optional[int] = 0
        designation: Optional[str] = None
        level: Optional[str] = None
        job_type: Optional[str] = None
        industry: Optional[str] = None
        func_area: Optional[str] = None
        reason_for_leaving: Optional[str] = None
        year_of_exp: Optional[YEAR_EXPERIENCE] = None

@dataclass
class CAREER_BREAK_DETAILS:
        cbd_id: Optional[int] = 0
        cbd_start_year: Optional[int] = 0
        cbd_end_year: Optional[int] = 0
        cbd_reason: Optional[str] = None

@dataclass
class Common_Fields:
        created_by : Optional[str]= None
        created_on : Optional[str]= None
        updated_by : Optional[str]= None
        updated_on : Optional[str]= None

@dataclass  
class Pagination:
        page : Optional[int]= None
        per_page : Optional[int]= None
        sort_by : Optional[str]= None
        order_by : Optional[str]= None
        filter_by : Optional[List[dict]]= None
        search_by : Optional[str]= None
        key : Optional[str]= None
        is_download: Optional[bool] = False
        module_collection: Optional[List[str]] = None
        search_type : Optional[str] = None    
        
@dataclass
class Base_Response:
        status : str
        status_code : str
        message : str
        data : Optional[List[Any]] = None
        count : Optional[int] = 0