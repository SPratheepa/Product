import logging,os,base64


from flask import Flask,send_from_directory
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_swagger_ui import get_swaggerui_blueprint
from flask_session import Session  # Ensure you have this installed

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

from NeoAdept.config import Config
from NeoAdept.utilities.collection_names import COLLECTIONS
from NeoAdept.utilities.menu_widget import Menu_Widget
from NeoAdept.utilities.module_permission import Module_Permission
from NeoAdept.utilities.key_generator import key_generator
from NeoAdept.routes.dropdown_route import Dropdown_Route
from NeoAdept.routes.dynamic_db_route import Dynamic_DB_Route
from NeoAdept.routes.dynamic_widget_route import Dynamic_widget_Route
from NeoAdept.routes.feedback_route import Feedback_Route
from NeoAdept.utilities.utility import Utility
from NeoAdept.utilities.db_utility import Collection_Manager,DB_Utility,Mongo_DB_Manager, SQL_Connection_Manager, SQL_Utility
from NeoAdept.utilities.constants import CONSTANTS
from NeoAdept.routes.prod_ctrl import Prod_Ctrl_Route
from NeoAdept.routes.login_route import Login_Route
from NeoAdept.routes.client_route import Client_Route
from NeoAdept.routes.user_route import User_Route
from NeoAdept.routes.mylist_route import My_List_Route
from NeoAdept.routes.activity_route import Activity_Route
from NeoAdept.routes.common_route import Common_Route
from NeoAdept.routes.email_route import Email_Route
from NeoAdept.routes.register_route import Register_Route
from NeoAdept.routes.permission_route import Permission_Route

class NeoAdeptApp:
    def __init__(self, app_name):
        self.app = Flask(app_name) 
        self.config = Config()
        connection_manager = Collection_Manager(self.config)
        self.db = connection_manager.connect_db(self.config.db_name)

        self.keyset_map,self.keyset_map_index,self.filters,self.keyset_map_dt = self.load_keyset_mapping()
        self.operators = Utility.define_sql_operators()
        
        self.sql_db,self.sql_table_list = {},{}
        if self.config.connect_sql=="True":
            self.setup_sql_connections()

        self.configure_jwt()
        self.config_session(timedelta(hours=int(self.config.session_lifetime)))    
        self.configure_logger(app_name)
        self.configure_app(app_name) 
        self.initialize_collections()
        self.register_blueprints()
        self.index()
    
    def setup_sql_connections(self):
        self.sql_cm, self.inspector, self.sql_table_dt_list, self.table_list, self.db_session = {}, {}, {}, {}, {}

        db_details = self.db[COLLECTIONS.DB_DETAILS]
        db_doc_list = Mongo_DB_Manager.read_documents(db_details, {})
        
        for db_doc in db_doc_list:
            db_url = db_doc['db_url']
            db_type = db_doc['db_type']
            db_name = db_doc['db_name']
            
            if db_type == CONSTANTS.SQL:
                cm = SQL_Connection_Manager(db_url)
                sql_db = SQL_Utility(cm)
                
                if sql_db and db_name not in self.sql_cm:
                    self.sql_cm[db_name] = cm
                    self.sql_db[db_name] = sql_db
                    inspector = cm.inspector
                    self.inspector[db_name] = inspector
                    self.load_table_info(db_name, inspector)

    def load_table_info(self, db_name, inspector):
        sql_table_list = []
        table_list = inspector.get_table_names()
        self.table_list[db_name] = table_list
        
        for table_name in table_list:
            columns = inspector.get_columns(table_name)
            columns_list = [{"name": col['name'], "datatype": col['type'].__visit_name__.lower(), "operators": self.operators.get(col['type'].__visit_name__.lower(), [])} for col in columns]
            sql_table_list.append({"collection_name": table_name, "columns": columns_list, "description": table_name})
        
        self.sql_table_list[db_name] = sql_table_list
        
    def initialize_collections(self):
        module_collection = self.db[COLLECTIONS.MASTER_MODULE_DETAILS]
        if Mongo_DB_Manager.is_collection_empty(module_collection):
            Module_Permission(self.config.role_permission_file).load_module_details(self.db)
        
        self.app.module_details_map = self.load_module_details()
        if self.config.CLIENT_ENV == CONSTANTS.CLIENT and Mongo_DB_Manager.is_collection_empty(self.db[COLLECTIONS.MASTER_WIDGET]):
            Menu_Widget(self.config.ui_template_file).load_widget_menu(self.db)
            key_generator(self.db)
        
        if Mongo_DB_Manager.is_collection_empty(module_collection):
            self.enable_collections_columns(self.keyset_map, self.db)
    
    def config_session(self,session_lifetime):
        self.app.config['SESSION_TYPE'] = 'filesystem'  # You can use other types like 'redis' if you have Redis installed
        self.app.config['SESSION_PERMANENT'] = True
        self.app.config['SESSION_USE_SIGNER'] = True
        self.app.config['PERMANENT_SESSION_LIFETIME'] = session_lifetime  
        self.session = Session(self.app)  # Initialize the session
        self.session.permissions = None
        self.session.widget_enable_for_db = None
    
    def load_module_details(self):
        module_details_map = {}
        module_details_collection = self.db[COLLECTIONS.MASTER_MODULE_DETAILS]
        modules = Mongo_DB_Manager.read_documents(module_details_collection,{})
        for module in modules:
            module_details_map[module["module"]] = module
        return module_details_map    

    def load_keyset_mapping(self):
        keyset_map, keyset_map_index, filters, keyset_map_dt = {}, {}, {}, {}
        collection_sample_list = Mongo_DB_Manager.read_documents(self.db[COLLECTIONS.CONFIG_SAMPLE],{})
        for collection_sample in collection_sample_list:
            if collection_sample:
                collection_name = collection_sample.get("key")
                keyset_map[collection_name] = DB_Utility.extract_all_keys_from_json(collection_sample)
                keyset_map_index[collection_name] = DB_Utility.extract_all_keys_from_json_with_values(collection_sample)
                keyset_map_dt[collection_name] = DB_Utility.extract_all_keys_from_json_with_dt(collection_sample)
                if CONSTANTS.FILTERS in keyset_map_index[collection_name]:
                    filters[collection_name] = keyset_map_index[collection_name].get(CONSTANTS.FILTERS)
        return keyset_map,keyset_map_index,filters ,keyset_map_dt             
        
    def generate_log_file_path(self):
        try:
            now = Utility.get_current_time()
            year, month, iso_week = now.year, now.strftime("%b"), now.strftime("%W")
            iso_week_month_start = int(datetime(now.year, now.month, 1).strftime("%W"))
            adjusted_week = int(iso_week) - iso_week_month_start + 1

            log_file_name = f'{self.config.log_file_name}_W({adjusted_week})_M({month})_Y({year}).log'
            return os.path.join(self.config.log_path, log_file_name)
        except Exception as e:
            print(e)
            return None

    def configure_logger(self,app_name):
        try:
            log_file_path = self.generate_log_file_path()
            if not log_file_path:
                return
            
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            max_bytes = int(self.config.log_max_bytes_size)
            
            handler = RotatingFileHandler(log_file_path, maxBytes=max_bytes, backupCount=1)
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            
            logger = logging.getLogger(app_name)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            self.logger = logger
        except Exception as e:
            print(e)

    def configure_app(self,app_name):
        self.server_private_key,self.decryption_apis = self.load_private_key(self.db)             

        self.app.config.update(
            MAIL_SERVER=self.config.mail_server,
            MAIL_PORT=self.config.mail_port,
            MAIL_USERNAME=self.config.verified_sender_email,
            MAIL_PASSWORD=self.config.mail_password,
            MAIL_USE_SSL=self.config.mail_use_ssl,
            SECRET_KEY=self.config.secret_key,
            JWT_SECRET_KEY=self.config.jwt_secret_key
        )
        
        CORS(self.app)        
        
        swagger_ui = get_swaggerui_blueprint(self.config.swagger_url, self.config.api_url, config={'app_name': app_name})
        
        self.app.register_blueprint(swagger_ui, url_prefix=self.config.swagger_url)
        
        @self.app.route("/static/swagger.json")
        def specs():
            BASE_PATH = Path(__file__).resolve().parent
            return send_from_directory(BASE_PATH.joinpath('NeoAdept/static'), 'swagger.json')
    
    def register_blueprints(self):
        versioned_prefix = f"/prod/{self.config.version}"

        common_args = (self.config, self.logger, self.db, self.keyset_map,self.session)
        routes_with_extra_args = [
            (Dropdown_Route, ( self.filters,)),
            (Dynamic_DB_Route, (self.keyset_map_dt, self.sql_db, self.sql_table_list)),
            (Dynamic_widget_Route, (self.keyset_map_dt, self.sql_db))
        ]

        for route, extra_args in routes_with_extra_args:
            route_name = route.__name__.lower().replace('_route', '')
            blueprint = route(route_name, __name__, *common_args, *extra_args)
            self.app.register_blueprint(blueprint, url_prefix=f"{versioned_prefix}/{route_name}")
        
        routes_without_extra_args = [
            User_Route,Client_Route, Feedback_Route, Prod_Ctrl_Route,  
            My_List_Route, Activity_Route,  Common_Route, Email_Route, 
            Register_Route, Permission_Route
        ]
        self.app.register_blueprint(Login_Route('login_route', __name__,self.config,self.logger,self.db,self.keyset_map,self.session,self.server_private_key,self.decryption_apis),url_prefix=f"{versioned_prefix}")
        for route in routes_without_extra_args:
            route_name = route.__name__.lower().replace('_route', '')
            blueprint = route(route_name, __name__, *common_args)
            self.app.register_blueprint(blueprint, url_prefix=f"{versioned_prefix}/{route_name}")
        
    def configure_jwt(self):
        jwt_manager = JWTManager(self.app)
        self.app.blacklist = set()

        @self.app.before_request
        def cleanup_blacklist():
            Utility.schedule_blacklist_cleanup(self.app)
    
    def run(self):
        HOST = self.config.host
        PORT = self.config.port
        print(HOST,PORT)
        self.app.run(host=HOST, port=PORT)
    
    def index(self):
        @self.app.route('/', methods=['GET'])
        def index():
            return f"{self.app_name} {self.config.version} {self.config.app_version} is running"
        
    def  load_private_key(self,db) :   
        key_document = db[COLLECTIONS.CONFIG_KEYS].find_one({"server_private_key": {"$exists": True}})
        if key_document is None:
            raise ValueError(f"Server private keys not found in the database{COLLECTIONS.CONFIG_KEYS}.")      
        
        server_private_key_pem = base64.b64decode(key_document["server_private_key"])    
        server_private_key = load_pem_private_key(server_private_key_pem, password=None)
        decryption_apis = key_document.get("decryption_apis",None)    
        return server_private_key,decryption_apis 
    
if __name__ == "__main__":
    NeoAdept_app = NeoAdeptApp('NeoAdept')
    NeoAdept_app.run()


