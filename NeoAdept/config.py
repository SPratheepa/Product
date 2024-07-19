import os
from dotenv import load_dotenv

class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        print("loading .env file")
        load_dotenv()
        self.swagger_url = '/swagger'
        self.api_url = '/static/swagger.json'
        self.log_file_name = os.getenv('LOG_FILE_NAME')
        self.log_max_bytes_size = int(os.getenv('LOG_MAX_BYTES_SIZE', 1000000))
        self.log_path = os.getenv('LOG_PATH')
        self.jwt_secret_key = os.getenv('JWT_SECRET_KEY')
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.secret_key = os.getenv('SECRET_KEY')
        self.otp_expiration_seconds = '300'
        self.max_pool_size = int(os.getenv("MAX_POOL_SIZE")) if os.getenv("MAX_POOL_SIZE") else None
        self.db_url = os.getenv("DB_URL")
        self.db_name = os.getenv("DB_NAME")
        self.app_version = os.getenv("APP_VERSION")
        self.attachment_path = os.getenv("ATTACHMENT_FILE_LOCATION")
        self.list_group_file_name = os.getenv("LIST_GROUP_FILE_NAME")
        self.index_folder = os.getenv("INDEX_FOLDER")
        self.CLIENT_ENV = os.getenv("CLIENT_ENV")
        self.ui_template_file = os.getenv("UI_TEMPLATE_FILE")
        self.sendpulse_api_key = os.getenv("SENDPULSE_API_KEY")
        self.sendpulse_api_secret = os.getenv("SENDPULSE_API_SECRET")
        self.verified_sender_email = os.getenv("VERIFIED_SENDER_EMAIL")
        self.mail_attachments_folder = os.getenv("MAIL_ATTACHMENTS_FOLDER")
        self.sendpulse_email_url = os.getenv("SENDPULSE_EMAIL_URL")
        self.sendpulse_token_url = os.getenv("SENDPULSE_TOKEN_URL")
        self.mail_server = os.getenv("MAIL_SERVER")
        self.mail_port = os.getenv("MAIL_PORT")
        self.mail_password = os.getenv("MAIL_PASSWORD")
        self.mail_use_ssl = os.getenv("MAIL_USE_SSL")
        self.role_permission_file = os.getenv('ROLE_PERMISSION_FILE')
        self.neo_db = os.getenv('NEO_DB')
        self.cache_timeout = os.getenv('cache_timeout')
        self.session_lifetime = os.getenv('session_lifetime')
        self.connect_sql = os.getenv('connect_sql')
        self.is_model_used = os.getenv('is_model_used')
        self.version =  os.getenv('version')
        
