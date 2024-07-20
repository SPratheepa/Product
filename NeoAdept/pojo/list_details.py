from datetime import datetime
from dataclasses import dataclass,field
from token import OP
from typing import Optional,List,Dict

@dataclass
class LIST_GROUP:
    _id: Optional[str] = None
    list_name: Optional[str] = None
    is_deleted: Optional[bool] = None
    created_by: Optional[str] = None
    created_on: Optional[datetime] = None
    updated_by: Optional[str] = None
    updated_on: Optional[datetime] = None
    candidate_count: Optional[int] = 0
    list_id : Optional[str] = None
    
@dataclass
class CandidateInfo:
    candidate_id: Optional[str] = None
    email: Optional[str] = None
    

@dataclass
class FILE_EMAIL_GROUPING:
    _id: Optional[str] = None
    list_id: Optional[str] = None
    list_name: Optional[str] = None
    candidate_id: Optional[List[str]] = None
    is_deleted: Optional[bool] = None
    created_by: Optional[str] = None
    created_on: Optional[str] = None
    updated_by: Optional[str] = None
    updated_on: Optional[str] = None
    
@dataclass
class FILE_GROUP_VIEW:
    _id: Optional[str] = None
    list_id: Optional[str] = None
    list_name: Optional[str] = None
    candidate_id: Optional[str] = None
    email: Optional[str] = None
    age: Optional[int] = None
    alternate_contact_number: Optional[str] = None
    can_work_in_shifts: Optional[str] = None
    career_break: Optional[str] = None
    career_break_details_list: List = field(default_factory=list)
    caregiver_status: Optional[str] = None
    certification_details_list: List = field(default_factory=list)
    created_by: Optional[str] = None
    created_on: Optional[str] = None
    ctc_type: Optional[str] = None
    current_ctc: Optional[float] = None
    current_location: Optional[str] = None
    cwe_city: Optional[str] = None
    cwe_company_name: Optional[str] = None
    cwe_country: Optional[str] = None
    cwe_designation: Optional[str] = None
    cwe_end_year: Optional[int] = None
    cwe_func_area: Optional[str] = None
    cwe_industry: Optional[str] = None
    cwe_job_type: Optional[str] = None
    cwe_level: Optional[str] = None
    cwe_reason_for_leaving: Optional[str] = None
    cwe_reported_by: Optional[str] = None
    cwe_reported_to: Optional[str] = None
    cwe_start_year: Optional[int] = None
    cwe_state: Optional[str] = None
    cwe_year_of_exp_months: Optional[int] = None
    cwe_year_of_exp_year: Optional[int] = None
    date_of_birth: Optional[str] = None
    educational_details_list: List = field(default_factory=list)
    expected_ctc: Optional[float] = None
    expected_ctc_type: Optional[str] = None
    family_income: Optional[float] = None
    first_name: Optional[str] = None
    gender: Optional[str] = None
    insert_from: Optional[str] = None
    is_active: Optional[bool] = None
    is_cwe_end_year_till: Optional[str] = None
    is_deleted: Optional[bool] = None
    is_np_negotiable: Optional[str] = None
    is_person_with_disability: Optional[str] = None
    key: Optional[str] = None
    languages_known_list: List[str] = field(default_factory=list)
    last_name: Optional[str] = None
    linkedin_profile: Optional[str] = None
    marital_status: Optional[str] = None
    middle_name: Optional[str] = None
    nationality: Optional[str] = None
    nature_of_disability: Optional[str] = None
    negotiable_period: Optional[int] = None
    notes: Optional[Dict[str, str]] = None
    notice_period: Optional[int] = None
    onsite_experience_list: List = field(default_factory=list)
    parental_status: Optional[str] = None
    passport_no: Optional[str] = None
    photo: Optional[str] = None
    photo_file_name: Optional[str] = None
    photo_id: Optional[str] = None
    pref_shift_timings: Optional[str] = None
    preferred_location: Optional[str] = None
    previous_work_experience_list: List = field(default_factory=list)
    primary_contact_number: Optional[str] = None
    primary_skills: List[str] = field(default_factory=list)
    prl_city: Optional[str] = None
    prl_country: Optional[str] = None
    prl_state: Optional[str] = None
    relevant_experience: Optional[int] = None
    religion: Optional[str] = None
    resume: Optional[str] = None
    resume_file_name: Optional[str] = None
    resume_id: Optional[str] = None
    rural_or_urban: Optional[str] = None
    secondary_skills: Optional[str] = None
    spouse_status: Optional[str] = None
    title: Optional[str] = None
    total_experience_months: Optional[int] = None
    total_experience_year: Optional[int] = None
    total_job_changed: Optional[int] = None
    travel_details_int_exp: Optional[int] = None
    travel_details_nat_exp: Optional[int] = None
    updated_by: Optional[str] = None
    updated_on: Optional[str] = None
    willing_to_relocate: Optional[str] = None
    
@dataclass
class MoveToListDetails:
    from_list_id: str
    to_list_id: str
    candidate_id: List[str]