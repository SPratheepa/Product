import copy
import hashlib
import string,random,traceback,logging,docx2txt,os,re
import base64
import secrets
import requests
import uuid
from typing import Any, Dict, List

from PyPDF2 import PdfReader
from flask import json, jsonify,current_app
from datetime import date, datetime, timedelta

from ..gbo.bo import YEAR_EXPERIENCE, Base_Response
from ..utilities.constants import CONSTANTS
#from ..utilities.key_generator import public_key_path,private_key_path # this should be the client public key.as of now using server public key for code purpose
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes


class Utility:
    COND_ANY = "any"
    COND_ALL = "all"   
    
    def get_origin(origin):
        print(origin)
        return origin.split(':')[1].replace("//","")
    
    @staticmethod
    def list_to_str(lst):
        # Convert each element of the list to a string
        str_list = [str(item) for item in lst]
        # Join the elements into a single string
        result_str = ', '.join(str_list)  # You can choose your delimiter
        return result_str

    @staticmethod
    def update_filter_keys(filter_by_req,key_mapping):
            updated_filter_items = []  # List to store modified filter items
            filter_by_req =  Utility.ensure_list(filter_by_req)
            for filter_item in filter_by_req:               
                updated_filter_item = filter_item.copy()  # Create a copy to avoid modifying the original dictionary
                for old_key, new_key in key_mapping.items():
                    if old_key in updated_filter_item:
                        updated_filter_item[new_key] = updated_filter_item.pop(old_key)                       
                updated_filter_items.append(updated_filter_item)  # Add the modified filter item to the list
            return updated_filter_items

    @staticmethod
    def filterObjects(records,attributesList,cond):
        if cond == Utility.COND_ANY:
            return [record for record in records if any(key in record for key in attributesList)]
        return  [record for record in records if all(key in record for key in attributesList)]
    
    @staticmethod
    def isValidFilter(requestList,mustAttributes):
        return all(key in requestList for key in mustAttributes)

    @staticmethod
    def ensure_list(value):
        if isinstance(value, list):
            return value
        return [value]

    @staticmethod
    def generate_random_password(length=12):
        characters = string.ascii_letters + string.digits #+ string.punctuation
        random_password = ''.join(random.choice(characters) for _ in range(length))
        return random_password
    
    @staticmethod
    def printExceptionStackTrace(e):
        print(e)
        error_details = str(e)
        traceback_str = traceback.format_exc()
        logging.error("Error during registration: %s", error_details)
        logging.error("Traceback: %s", traceback_str)
    
    @staticmethod
    def buildResponseMessage(key,value,status,status_code,message):
        response = {}
        if key:
            response.update({key:value})
        response.update({"status_code":status_code,"status":status,"message": message})
        return response
    
    @staticmethod
    def get_file_type(filename):
    # Split the filename by dots
        parts = filename.split('.')
        file_type = "Unknown"
        # Extract the last part as the file type
        if len(parts) > 1:
            file_type = parts[-1].lower()
        return file_type
        
    @staticmethod
    def generate_success_response(is_table,message=None,data=None,count=0):
        response = Base_Response(status=CONSTANTS.SUCCESS,status_code=CONSTANTS.SUCCESS_STATUS_CODE,message=message,data=data,count=count)
        if not is_table:
            if data is None:
                response.__delattr__('data')
            response.__delattr__('count')
        return response.__dict__

    @staticmethod
    def generate_error_response(error_message):
        status = CONSTANTS.FAILED
        status_code = CONSTANTS.FUNCTIONAL_ERR_STATUS_CODE
        
        if error_message == CONSTANTS.NO_DATA_FOUND:
            status = CONSTANTS.SUCCESS  
            status_code = CONSTANTS.SUCCESS_STATUS_CODE
        response = Base_Response(status=status,status_code=status_code,message=error_message,data=None,count=0)
        response.__delattr__('data')
        response.__delattr__('count')
        return response.__dict__

    @staticmethod
    def generate_success_response_for_crud(success_message,result_field=None,results=None):
        response_data = {
                CONSTANTS.STATUS: CONSTANTS.SUCCESS,
                CONSTANTS.STATUS_CODE: 200,
                CONSTANTS.MESSAGE: success_message
            }
        if result_field and results is not None:
            response_data[result_field]=results
        results=response_data
        return jsonify(results), 200

    @staticmethod
    def generate_exception_response(e):
       Utility.printExceptionStackTrace(e)
       return jsonify({CONSTANTS.STATUS:CONSTANTS.FAILED,CONSTANTS.STATUS_CODE:500,CONSTANTS.MESSAGE:str(e)}), 500  
 
    @staticmethod
    def is_jti_blacklisted(jti):
        if jti in current_app.blacklist:
            return True
        return False

    @staticmethod
    def handle_blacklisted_jti(jti):
        if Utility.is_jti_blacklisted(jti):
            return jsonify({CONSTANTS.STATUS: CONSTANTS.FAILED, CONSTANTS.STATUS_CODE: 403, CONSTANTS.MESSAGE: CONSTANTS.TOKEN_EXPIRED_MESSAGE}), 201

    @staticmethod
    def get_data_from_identity(identity_data_obj, include_user_id=False):
        email = identity_data_obj.email
        role = identity_data_obj.role
        _id = identity_data_obj._id if include_user_id else None
        if include_user_id:
            return email, role, _id
        return email, role
        
    @staticmethod
    def settings_for_data_operation(email_from_token, operation_type=None,is_delete=None,obj = None):
        current_time = Utility.get_current_time()
        if operation_type is None:
            return current_time,email_from_token
        if operation_type == 'add': 
            if obj is not None:
                del obj._id
                return current_time, email_from_token, obj
            return current_time, email_from_token, 'active'
        elif operation_type == 'update':
            return current_time, email_from_token
        elif operation_type == 'delete':
            status = 'inactive'
            if is_delete is not None:
                status=True               
            return current_time, email_from_token, status

    @staticmethod
    def get_current_time():
        return datetime.now()
    
    @staticmethod
    def get_current_timestamp():
        return Utility.get_current_time().strftime("%Y-%m-%d %H:%M:%S.%f")
    
    @staticmethod
    def get_current_date():
        return Utility.get_current_time().strftime("%Y-%m-%d")
    
    @staticmethod
    def get_is_deleted_false_query():
        return {"is_deleted": False}
    
    @staticmethod
    def get_active_data_query():
        return {"status": "active"}
    
    @staticmethod
    def get_delete_false_query():
        return {"is_deleted":bool(False)}

    @staticmethod
    def get_active_and_not_deleted_query():
        active_query = Utility.get_active_data_query()
        active_query.update(Utility.get_delete_false_query())       
        return active_query

    def update_request_data(data, operation_type, updated_on=None, updated_by=None, status=None, created_on=None, created_by=None, password=None, is_delete=None):
        if operation_type in {CONSTANTS.ADD, CONSTANTS.UPDATE, CONSTANTS.DELETE}:
            if operation_type == CONSTANTS.ADD:
                data.update({"created_on": created_on, "created_by": created_by, "status": status})
                if password is not None:
                    data.update({"password":password})
                
                
            elif operation_type == CONSTANTS.UPDATE:
                data.update({"updated_on": updated_on, "updated_by": updated_by})
                
                
            elif operation_type == CONSTANTS.DELETE:
                if is_delete is None:
                    data.update({"updated_on": updated_on, "updated_by": updated_by, "status": status})
                else:
                    data.update({"updated_on": updated_on, "updated_by": updated_by, "is_deleted": status})
        return data
    
    @staticmethod
    def clean_up_blacklist(current_app):
        expired_tokens = [token for token in current_app.blacklist if Utility.is_token_expired(token)]
        for token in expired_tokens:
            current_app.blacklist.remove(token)
    
    @staticmethod
    def schedule_blacklist_cleanup(current_app):
        if not hasattr(current_app, "last_cleanup"):
            current_app.last_cleanup = datetime.now()
        cleanup_interval = timedelta(days=1)
        if datetime.now() - current_app.last_cleanup > cleanup_interval:
            Utility.clean_up_blacklist(current_app)
            current_app.last_cleanup = datetime.now()
    
    @staticmethod
    def extract_text_from_pdf(pdf_path):
        text = ""
        try:
            with open(pdf_path, 'rb') as file:
                    pdf_reader = PdfReader(file)
                    page = pdf_reader.pages[0]  # Assuming profile picture is on the first page
                
                    num_pages = len(pdf_reader.pages)
                    for page_num in range(num_pages):
                        page = pdf_reader.pages[page_num]
                        text += page.extract_text()
            text = text.replace('\n',' ').replace('\t',' ')  
        except Exception as e:
            Utility.printExceptionStackTrace(e)
            text = None
        return text
    
    @staticmethod
    def read_word(file):
        text = ""
        try:     
            text = docx2txt.process(file)
            text = text.replace('\n',' ').replace('\t',' ')  
        except Exception as e:
            Utility.printExceptionStackTrace(e)
            text = None
        return text

    @staticmethod
    def extract_unique_keys(data, prefix=''):
        keys = set()
        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                keys.add(full_key)
                keys.update(Utility.extract_unique_keys(value, prefix=full_key))
        elif isinstance(data, list):
            # Check if all elements in the list are dictionaries
            if all(isinstance(item, dict) for item in data):
                # Extract keys from the dictionaries in the list
                for item in data:
                    keys.update(Utility.extract_unique_keys(item, prefix=prefix))
            else:
                # If elements are not dictionaries, just add the key as it is
                keys.add(prefix)
                # Add keys with array indices
                for i, item in enumerate(data):
                    keys.update(Utility.extract_unique_keys(item, prefix=full_key))
        return keys

    @staticmethod
    def extract_unique_keys_with_types(data, prefix='', result_dict=None):
        if result_dict is None:
            result_dict = {}
        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                Utility.extract_unique_keys_with_types(value, prefix=full_key, result_dict=result_dict)
        elif isinstance(data, list):
            result_dict[prefix] = 'list'
            # Check if all elements in the list are dictionaries
            if all(isinstance(item, dict) for item in data):
                # Extract keys from the dictionaries in the list
                for item in data:
                    Utility.extract_unique_keys_with_types(item, prefix=prefix, result_dict=result_dict)
        else:
            result_dict[prefix] = 'value'
        return result_dict

    @staticmethod
    def get_content_of_doc(file_path,file_name):
            content = ""
            try:
                if not os.path.exists(file_path):
                    return None
                if file_name.endswith('.docx'):
                    content = Utility.read_word(file_path)
                elif file_name.endswith('.pdf'):
                    content = Utility.extract_text_from_pdf(file_path)
            except Exception as e:
                #print(f"Error processing file: {filepath}. Skipping... Error: {e}")
                Utility.print_exception(e)
            return content      
    
    @staticmethod
    def extract_entities(doc,sentence,entity_list):
        entities = []    
        for token in doc:
            #   print ("token----->",token)
            if token.like_email:
               entities.append((token.idx, token.idx + len(token.text), 'EMAIL'))
  
        for entity_type, entity_values in entity_list.items():
                #    print ("entity_values----->",entity_values)
                    for value in entity_values:
                        if value in sentence:
                            entities.append((sentence.find(value),sentence.find(value)+len(value),entity_type))
 
        return entities

    @staticmethod
    def iterate_list(lst, indent=""):
        for item in lst:
            if isinstance(item, list):
                Utility.iterate_list(item, indent + "  ")  # Recursive call for nested lists
            else:
                print(indent + "- " + str(item))

    @staticmethod
    def iterate_data_with_nested_keys(data, result=[], current_key=""):
        if isinstance(data, dict):
            for key, value in data.items():
                nested_key = current_key + "." + key if current_key else key
                if isinstance(value, (dict, list)):
                    Utility.iterate_data(value, result, nested_key)  # Recursive call for nested dictionaries and lists
                else:
                    result.append((nested_key.lower(), value.lower()))  # Append tuple containing nested key and value
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    Utility.iterate_data(item, result, current_key)  # Recursive call for nested dictionaries and lists
                else:
                    result.append((current_key.lower(), item.lower()))  # Append tuple containing current key and item as value
        else:
            result.append((current_key.lower(), data.lower()))  #
    
    @staticmethod
    def iterate_data(data, indent=""):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    print(indent + key + " (dict)")
                    Utility.iterate_data(value, indent + "  ")  # Recursive call for nested dictionaries
                elif isinstance(value, list):
                    print(indent + key + " (list)")
                    for item in value:
                        Utility.iterate_data(item, indent + "  ")  # Recursive call for nested lists
                else:
                    print(indent + key + ":", value)
        elif isinstance(data, list):
            for item in data:
                Utility.iterate_data(item, indent + "  ")  # Recursive call for nested lists
        else:
            print(indent + str(data))

    @staticmethod
    def allowed_file(filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in CONSTANTS.ALLOWED_CV_EXTENSIONS

    @staticmethod
    def extract_emails(text):
        unique_emails = []
        # Regular expression pattern to match email addresses
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        # Find all matches of the pattern in the text
        emails = re.findall(pattern, text, flags=re.IGNORECASE)
        if emails:
            seen = set()
            for email in emails:
                if email not in seen:
                    unique_emails.append(email)
                    seen.add(email)  
        return unique_emails

    @staticmethod
    def extract_emails_from_text(text):
        email_list = Utility.extract_emails(text)
        return email_list[0] if email_list and len(email_list)>0  else None

    @staticmethod
    def extract_month_year_from_text(text):
        month,year = None,None
        matches = re.findall(r'([A-Za-z]+)?\.? (\d{4})', text)
        if matches:
            for match in matches:
                if match[0]:  # If month is present
                    month = match[0]
                    year = match[1]
                else:  # If month is not present
                    year = match[1]
        return month,year

    @staticmethod
    def calculate_duration(start_date, end_date=None):
        
        if isinstance(start_date, int):
            start_date = date(start_date, 1, 1)
        if isinstance(end_date, int):
            end_date = date(end_date, 1, 1)
        if isinstance(start_date, str):
            start_date = Utility.format_date(start_date)
        if isinstance(end_date, str):
            end_date = Utility.format_date(end_date)
        
        # Calculate the difference in months
        if end_date is None:
            end_date = date.today()
            
        months_difference = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)

        # Calculate the number of years and remaining months
        years_difference = months_difference // 12
        remaining_months = months_difference % 12

        return years_difference, remaining_months

    @staticmethod
    def calculate_years_months(text):
        month_mapping = {
    'jan': 'January',
    'feb': 'February',
    'mar': 'March',
    'apr': 'April',
    'may': 'May',
    'jun': 'June',
    'jul': 'July',
    'aug': 'August',
    'sep': 'September',
    'oct': 'October',
    'nov': 'November',
    'dec': 'December'
}
        text = text.lower().replace('.', '').replace(',', '').replace('-', '')
        match = re.search(r'([a-zA-Z]+)?\s*(\d+)\s*(?:-|to)?\s*([a-zA-Z]+)?\.?\s*(\d+)?', text)
        if match:
            start_month = match.group(1)
            start_year = match.group(2)

            if start_month:
                start_month = month_mapping.get(start_month.lower(), month_mapping.get(start_month.lower()[:3]))  
            else:
                start_month = "January"  # Default to January if month is not provided
            if match.group(3) and match.group(4):
                end_month = match.group(3)
                end_year = match.group(4)
                if end_month:
                    end_month = month_mapping.get(end_month.lower(), month_mapping.get(end_month.lower()[:3]))  
                else:
                    end_month = "December"  # Default to December if month is not provided
            else:
            # If end month is not provided, consider it as "Present"
                end_month = "Present"
                end_year = datetime.now().year

            start_date_str = f"{start_month} {start_year}"
            end_date_str = f"{end_month} {end_year}"

            try:
                start_date = datetime.strptime(start_date_str, '%B %Y')
            except ValueError:
                start_day = int(start_year)
                start_year = int(f'20{start_day}') if start_day < 100 else start_day
                start_date_str = f"{start_month} {start_year}"
                start_date = datetime.strptime(start_date_str, '%B %Y')

            if end_month != "Present":
                try:
                    end_date = datetime.strptime(end_date_str, '%B %Y')
                except ValueError:
                    end_day = int(end_year)
                    end_year = int(f'20{end_day}') if end_day < 100 else end_day
                    end_date_str = f"{end_month} {end_year}"
                    end_date = datetime.strptime(end_date_str, '%B %Y')
            else:
                end_date = datetime.now()
            if start_date > end_date:
                start_date, end_date = end_date, start_date
            years_difference , remaining_months = Utility.calculate_duration(start_date,end_date)
        
            return YEAR_EXPERIENCE(years_difference,remaining_months),start_date,end_date
        return None,None,None

    def format_date(date_str):
        formats = ["%d/%m/%Y", "%m/%d/%Y","%B %Y","%Y","%m/%Y", "%B/%Y","%Y-%m-%dT%H:%M:%S.%fZ"]
        date_val = None
        for fmt in formats:
            try:
                date_val = datetime.strptime(date_str, fmt)
                break  # Exit the loop if parsing succeeds
            except ValueError:
                date_str_update = Utility.extract_date_from_text(date_str)
                if date_str_update:
                    date_str = date_str_update[0]
                    for fmt_in in formats:
                        try:
                            date_val = datetime.strptime(date_str, fmt_in)
                            break
                        except ValueError:
                            pass
                pass  # Continue to the next format if parsing fails
        return date_val

    def calculate_diff_in_years_months(start_date_str,end_date_str):
          # Add more formats if needed
        start_date = Utility.format_date(start_date_str)
        end_date = Utility.format_date(end_date_str)
        
        if start_date is None or end_date is None:
                date = datetime.strptime(start_date_str, "%m/%y")
                t = date.strftime("%m/%Y")
                start_date = Utility.format_date(t)
                date = datetime.strptime(end_date_str, "%m/%y")
                t = date.strftime("%m/%Y")
                end_date = Utility.format_date(t)
            #raise ValueError("Failed to parse date strings",start_date_str,end_date_str)

        if start_date > end_date:
            start_date, end_date = end_date, start_date

        year_diff = end_date.year - start_date.year
        month_diff = end_date.month - start_date.month

        # Adjust the difference if the month difference is negative
        if month_diff < 0:
            year_diff -= 1
            month_diff += 12

        return YEAR_EXPERIENCE(year_diff,month_diff),start_date,end_date
        
    def split_dates(text):
        dates = text
        if '-' in text:
            dates = text.split("-")
        elif ':' in text:
            dates = text.split(":")
        elif 'to' in text:
            dates = text.split("to")
        if(isinstance(dates,list)):
            start_date = dates[0].strip()
            end_date = dates[1].strip() if len(dates) > 1 else None
            if end_date and 'present' in end_date.lower() or end_date.lower().startswith('till'):
                end_date = str(datetime.now().strftime("%Y"))
            return start_date, end_date
        else:
            if len(dates) == 4:
                return None,text
        return None,None

    def extract_date_from_text(text):
    # Regular expression pattern to match dates in the format 'dd/mm/yyyy' or 'd/m/yyyy'
        pattern = r'\b\d{1,2}/\d{1,2}/\d{4}\b'
        matches = re.findall(pattern, text)
        return matches
    
    @staticmethod
    def assign_uuid(obj, list_attr, id_field):
        if hasattr(obj, list_attr):
            for item in getattr(obj, list_attr):
                if not item.get(id_field) or item[id_field] in [None, "", "null"]:
                    item[id_field] = str(uuid.uuid4())
        return obj
    
    @staticmethod
    def search_documents(documents, search_string):
        search_string = search_string.lower()
        
        def search_in_dict(d, search_string):
            for key, value in d.items():
                if isinstance(value, dict):
                    if search_in_dict(value, search_string):
                        return True
                elif isinstance(value, list):
                    if any(search_in_dict(item, search_string) if isinstance(item, dict) else search_string in str(item).lower() for item in value):
                        return True
                elif search_string in str(value).lower():
                    return True
            return False
        
        return [doc for doc in documents if search_in_dict(doc, search_string)]
    
    @staticmethod
    def filter_documents(documents: List[Dict[str, Any]], criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        def extract_matching_sub_docs(document: Dict[str, Any], criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
            matched_sub_docs = []

            for key, value in criteria.items():
                if '.' in key:
                    sub_keys = key.split('.')
                    sub_doc = document
                    for sub_key in sub_keys[:-1]:
                        if sub_key in sub_doc:
                            sub_doc = sub_doc[sub_key]
                        else:
                            sub_doc = None
                            break
                    if sub_doc:
                        last_key = sub_keys[-1]
                        if isinstance(sub_doc, list):
                            for item in sub_doc:
                                if last_key in item and ((isinstance(value, dict) and '$in' in value and item[last_key] in value['$in']) or item[last_key] == value):
                                    matched_sub_docs.append(item)
                        elif last_key in sub_doc and ((isinstance(value, dict) and '$in' in value and sub_doc[last_key] in value['$in']) or sub_doc[last_key] == value):
                            matched_sub_docs.append(sub_doc)
                else:
                    if document.get(key) == value:
                        matched_sub_docs.append(document)
            
            return matched_sub_docs

        matched_documents = []
        for doc in documents:
            matched_sub_docs = extract_matching_sub_docs(doc, criteria)
            if matched_sub_docs:
                new_doc = doc.copy()
                new_doc['matched_sub_docs'] = matched_sub_docs
                matched_documents.append(new_doc)

        return matched_documents
    @staticmethod
    def upsert_by_on(obj,type,email_from_token):#use common function
        if type == "update" :              
            attributes_to_delete = ["created_by","created_on","_id"]
            [delattr(obj, attr) for attr in attributes_to_delete]
            obj.updated_by = email_from_token
            obj.updated_on = Utility.get_current_timestamp()
        elif type == "add":
            del obj._id
            obj.created_by = email_from_token
            obj.created_on = Utility.get_current_timestamp()
        return obj
    
    @staticmethod
    def frame_email(from_email,subject,to_email,to_name=None,email_template=None,text=None):
        data = {
            "email": {
                "html": email_template,
                "text": text,
                "subject": subject,
                "from": {
                    "email": from_email
                },
                "to": []
            }
        }
    
        # If to_email is a list of emails
        if isinstance(to_email, list):
            for email in to_email:
                recipient = {"email": email}
                if to_name:
                    recipient["name"] = to_name
                data["email"]["to"].append(recipient)
        # If to_email is a single email
        else:
            recipient = {"email": to_email}
            if to_name:
                recipient["name"] = to_name
            data["email"]["to"].append(recipient)
        return data

    @staticmethod
    def third_party_email_function(api_key,api_secret,token_url,email_url,data):
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': api_key,
            'client_secret': api_secret
        }
        response = requests.post(token_url, data=token_data)
        result = response.json() 
        token = result['access_token']  
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        response = requests.post(email_url, headers=headers, data=json.dumps(data)) 
        return response
    
    '''def encrypt_response_data(json_data): # client public key done form the front end
        with open(public_key_path, "wb") as f:
            client_public_key = load_pem_public_key(f.read())
            
        
        json_string = json.dumps(json_data)
    
        encrypted_response = client_public_key.encrypt(
        json_string.encode('utf-8'),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
        )
        return encrypted_response'''
    
    
    def decrypt_request_data(request_data,server_private_key): # server private key
        try :
            #print("request_data ::",request_data)
           # encrypted_data = bytes.fromhex(request_data) 
            encrypted_data = base64.b64decode(request_data)
           # print("encrypted_data (bytes) ::", encrypted_data)
            #print("encrypted_data length ::", len(encrypted_data))
            decrypted_data = server_private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            data = decrypted_data.decode('utf-8')
            return data
        except ValueError as e:
            print("Decryption failed: ValueError")
            traceback.print_exc()  # Print the complete traceback for debugging
            return None
        except Exception as e:
            print("Decryption failed: General Exception")
            traceback.print_exc()  # Print the complete traceback for debugging
            return None

    
    @staticmethod
    def extract_date_from_datetime(datetime_str: str) -> date:
        try:
            datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")           
        except ValueError:
            datetime_obj = Utility.format_date(datetime_str)
        return datetime_obj
    
    @staticmethod
    def calculate_age(dob):
        # Get the current date
        today = datetime.today()
        # Calculate the age
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        
        return age
    
    def convert_years_months_to_float(years, months):
        return round(years + (months / 12),1)
    
    def extract_phone_number(phone_number):
        pattern = re.compile(r'\b\d{10}\b')
        phone_numbers = pattern.findall(phone_number)
        phone_number,al_phone_number = None,None
        # Remove leading "91" or "0" if present
        if phone_numbers:
            if len(phone_numbers)>0:
                phone_number = phone_numbers[0]
                if len(phone_numbers)>1:
                    al_phone_number = phone_numbers[1]
        
        return phone_number,al_phone_number
    
    @staticmethod
    def str_to_list(str_val):
        # Convert each element of the list to a string
        string_list = str_val.split(',')
        return string_list
    
    @staticmethod
    def convert_months_to_year_months(years=0,months=0):
        year,month = 0,int(months)
        if month>=12:
            year = int(month/12)
            month = month%12
        year = year + years
        return year,month
    
    @staticmethod
    def generate_token():
        return secrets.token_urlsafe(16)
    
    @staticmethod
    def get_expiration_time(hours):
        return datetime.now() + timedelta(hours=hours)
    
    @staticmethod
    def generate_cache_key(request,api_name):
        request_data = {
        "api_name":api_name,
        "path": request.path,
        "args": request.args.to_dict(),
        "json": request.get_json() if request.is_json else None
    }
        key_data = json.dumps(request_data, sort_keys=True)
        return hashlib.md5(key_data.encode()).hexdigest()
    
    @staticmethod
    def define_sql_operators():
        return {
            "datetime": ["=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "NOW()", "CURDATE()", "DATE_ADD()", "DATE_SUB()", "DATEDIFF()", "TIMESTAMPDIFF()", "EXTRACT()", "YEAR()", "MONTH()", "DAY()", "HOUR()", "MINUTE()", "SECOND()"],
            "timestamp": ["=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "NOW()", "CURDATE()", "DATE_ADD()", "DATE_SUB()", "DATEDIFF()", "TIMESTAMPDIFF()", "EXTRACT()", "YEAR()", "MONTH()", "DAY()", "HOUR()", "MINUTE()", "SECOND()"],
            "tinyint": ["=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "&", "|", "^", "~", "<<", ">>", "ABS()", "CEIL()", "FLOOR()", "ROUND()", "POWER()", "SQRT()", "MOD()"],
            "integer": ["=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "&", "|", "^", "~", "<<", ">>", "ABS()", "CEIL()", "FLOOR()", "ROUND()", "POWER()", "SQRT()", "MOD()"],
            "bigint": ["=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "&", "|", "^", "~", "<<", ">>", "ABS()", "CEIL()", "FLOOR()", "ROUND()", "POWER()", "SQRT()", "MOD()"],
            "double": ["=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "ABS()", "CEIL()", "FLOOR()", "ROUND()", "POWER()", "SQRT()", "MOD()"],
            "enum": ["=", "!=", "<>", "<", "<=", ">", ">=", "FIELD()", "ELT()"],
            "date": ["=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "CURDATE()", "DATE_ADD()", "DATE_SUB()", "DATEDIFF()", "YEAR()", "MONTH()", "DAY()"],
            "text": ["=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "NOT LIKE", "REGEXP", "NOT REGEXP", "||", "CONCAT()", "LENGTH()", "LOWER()", "UPPER()", "SUBSTRING()", "TRIM()", "REPLACE()", "LEFT()", "RIGHT()", "LPAD()", "RPAD()"],
            "varchar": ["=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "NOT LIKE", "REGEXP", "NOT REGEXP", "||", "CONCAT()", "LENGTH()", "LOWER()", "UPPER()", "SUBSTRING()", "TRIM()", "REPLACE()", "LEFT()", "RIGHT()", "LPAD()", "RPAD()"],
            "boolean": ["=", "!=", "<>", "AND", "OR", "NOT", "BETWEEN", "IN", "IS NULL", "IS NOT NULL", "LIKE", "NOT LIKE"]
        }