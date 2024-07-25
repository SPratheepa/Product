import os

from werkzeug.utils import secure_filename
from flask import current_app
from flask_mail import Message,Mail

from ..gbo.bo import Pagination
from ..gbo.common import Custom_Error
from ..pojo.directory import DIRECTORY
from ..pojo.email_details import EMAIL_DETAILS
from ..pojo.access_token import ACCESS_TOKEN
from ..utilities.collection_names import COLLECTIONS
from ..utilities.db_utility import DB_Utility, Mongo_DB_Manager
from ..utilities.constants import CONSTANTS
from ..utilities.utility import Utility
from ..config import Config

class Email_Service:
    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance
    
    def __init__(self,config:Config,keyset_map,logger,db):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.logger = logger
            self.config = config
            self.db = db
            self.directory = DIRECTORY()
            self.key_nested_key_map = keyset_map
            self.key_map = self.key_nested_key_map[COLLECTIONS.MASTER_EMAIL_DETAILS]
            #self.common_service = Common_Service(logger,db,keyset_map)
            self.SENDPULSE_API_KEY = self.config.sendpulse_api_key
            self.SENDPULSE_API_SECRET = self.config.sendpulse_api_secret
            self.SENDPULSE_TOKEN_URL = self.config.sendpulse_token_url
            self.SENDPULSE_EMAIL_URL = self.config.sendpulse_email_url
            
    def send_email1(self,request,db):
        recipients = request.form.getlist('to')
        subject = request.form.get('subject', 'Default Subject')
        html = request.form.get('html', '<p>Default HTML content</p>')
        body = request.form.get('text','Default text content')
        
        email_data = Utility.frame_email(from_email=self.verified_sender_email,subject=subject,to_email=recipients,to_name=None,email_template=None,text=body)
        
        files = request.files.getlist('attachments')
        saved_attachments = []
        mail_attachments_folder = self.config.mail_attachments_folder
        
        if files:
            os.makedirs(mail_attachments_folder, exist_ok=True)
            for file in files:
                filename = secure_filename(file.filename)
                file_content = file.read()
                email_data['email']['attachments'].append({
                    'name': filename,
                    'type': file.content_type,
                    'content': file_content
                })
                file_path = os.path.join(self.config.mail_attachments_folder, filename)
                file.save(file_path)
                saved_attachments.append(file_path)
        
        response = Utility.third_party_email_function(self.SENDPULSE_API_KEY,self.SENDPULSE_API_SECRET,self.SENDPULSE_TOKEN_URL,self.SENDPULSE_EMAIL_URL,email_data)       
        
        if response.status_code != 200:
            raise Custom_Error(response.json())
        
        email_data = {
            'from_email': self.verified_sender_email,
            'send_to': recipients,
            'subject': subject,
            'content': body,
            'attachments': saved_attachments,
            'sent_on': Utility.get_current_time()
        }
        
        result = Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_EMAIL_DETAILS],email_data)
        
        if not result:
            raise Custom_Error('Could not save mail info in db')
        
    def send_email(self,request,db):
        recipients = request.form.get('to', [])
        subject = request.form.get('subject', 'Default Subject')
        html = request.form.get('html', '<p>Default HTML content</p>')
        text = request.form.get('text', 'Default text content')
        msg = Message(subject=subject, sender=self.config.verified_sender_email, recipients=[recipients])
        msg.html = html
        msg.body = text

        files = request.files.getlist('attachments')
        saved_attachments = []
        if files:
            mail_folder = self.directory.get_folder(self.config.mail_attachments_folder)
            self.directory.create_folder(mail_folder)
            for file in files:
                filename = secure_filename(file.filename)
                file_content = file.read()
                msg.attach(filename, file.content_type, file_content)
                file_path = os.path.join(self.config.mail_attachments_folder, filename)
                file.save(file_path)
                saved_attachments.append(file_path)
                
        mail = Mail(current_app)
        mail.send(msg)
        
        email_data = {
            'from_email': self.config.verified_sender_email,
            'send_to': recipients,
            'subject': subject,
            'content': text,
            'attachments': saved_attachments,
            'sent_on': Utility.get_current_time()
        }
        result = Mongo_DB_Manager.create_document(db[COLLECTIONS.MASTER_EMAIL_DETAILS],email_data)
        if not result:
            raise Custom_Error('Mail info is not saved in db')

    def view_mail_history(self,identity_data,request_data,db):
        identity_data_obj = ACCESS_TOKEN(**identity_data)
        pagination = Pagination(**request_data) 
        ##self.common_service.create_log_details(identity_data_obj.email,request_data,"view_mail_history",db)
        
        email_collection = db[COLLECTIONS.MASTER_EMAIL_DETAILS]
        query = DB_Utility.frame_get_query(pagination,self.key_map)
        
        docs,count = Mongo_DB_Manager.get_paginated_data1(email_collection,query,pagination) 
        if docs:
            if pagination.is_download==True:
                return docs,count
            return DB_Utility.convert_doc_to_cls_obj(docs,EMAIL_DETAILS),count
        raise Custom_Error(CONSTANTS.NO_DATA_FOUND) 
