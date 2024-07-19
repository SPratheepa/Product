import PyPDF2,pymongo, os,re,shutil,traceback,docx2txt,hashlib

from datetime import datetime
from docx import Document
from PyPDF2 import PdfReader
from whoosh.fields import ID,Schema,TEXT,DATETIME
from whoosh.index import create_in,open_dir
from whoosh.qparser import QueryParser,OrGroup
from whoosh.analysis import RegexTokenizer

class Search_Model_Service:

    def __init__(self,is_method):        
        if is_method == True:
            self.index_folder = os.environ['INDEX_FOLDER']
            self.search_folder = os.environ['SEARCH_DOC_FOLDER']
            self.cv_files_folder = os.environ['CV_DOC_FOLDER']
            self.profile_pic_folder = os.environ['PROFILE_PIC_FOLDER']

            if not os.path.exists(self.profile_pic_folder):
                os.mkdir(self.profile_pic_folder)
            if not os.path.exists(self.cv_files_folder):
                os.mkdir(self.cv_files_folder)       
            if not os.path.exists(self.index_folder):
                os.mkdir(self.index_folder)
                #print("creating index")
                self.indexer = self.create_index()
            else:
                #print("updating index")
                self._update_index()

    def create_index(self):
        try:
            #StandardAnalyzer(minsize=1)
            schema = Schema(path=ID(unique=True, stored=True), content=TEXT(stored=True,analyzer=RegexTokenizer(expression=r'\w+')), modified=DATETIME(stored=True), emails=TEXT(stored=True))
            ix = create_in(self.index_folder, schema)
            # Open index writer
            writer = ix.writer()
            
            content_hashes = set()  # To store content hashes
            # Walk through the documents folder and index each document
            for root, dirs, files in os.walk(self.search_folder):
                for file in files:
                    path = os.path.join(root, file)
                    #print("reading ",path)
                    content,emails_in_doc = self.read_document(file,path)
                    #print(emails_in_doc)
                    try:
                        if content:
                                content = content.encode('utf-8', errors='ignore').decode('utf-8')
                                content_hash = hashlib.md5(content.encode()).hexdigest()
                                #print(content_hash,'=====',content_hashes)
                                if content_hash not in content_hashes:
                                    modified_time = datetime.fromtimestamp(os.path.getmtime(path))
                                    writer.add_document(path=path, content=content,modified=modified_time,emails=emails_in_doc)
                                    content_hashes.add(content_hash)
                                    print(f"Started Moving file {path} to {self.cv_files_folder}.")
                                    self.move_file(path,self.cv_files_folder)
                                #else:
                                #    self.move_failed_file(path,self.duplicate_files_folder)
                                    #print(f"Skipping file {path} due to duplicate content.")
                        #else:
                            #self.move_failed_file(path,self.duplicate_files_folder)
                            #print(f"Skipping file {path} due to no content.")
                    except Exception as e:
                        #DocumentLoader.print_exception(e)
                        pass
                        #self.move_failed_file(path,self.failed_files_folder)
                        #print("Error while adding documents:", e)
                        # Commit changes and close writer
            writer.commit()
            
        except Exception as e:
            pass
            #DocumentLoader.print_exception(e)
            #print("Error creating index:", e)
            
    def move_file(self, file_path,to_folder):
        try:
            file_name = os.path.basename(file_path)
            new_path = os.path.join(to_folder, file_name)
            shutil.move(file_path, new_path)
            print(f"Moving file {file_path} to {new_path}.")
        except Exception as e:
            pass
            #DocumentLoader.print_exception(e)
            #print(f"Error moving failed file '{file_path}' to '{to_folder}': {e}")
            
    def read_document(self,file,path):
        content = None
        image_data = []
        emails_in_doc = None
        try:
                #print("read_document====>",file,path)
                if file.endswith('.txt'):  # Consider only text files
                    with open(path, 'r', encoding='utf-8') as f:
                        content = f.read()
                elif file.endswith('.docx'):
                    content = docx2txt.process(path)
                    doc = Document(path)
                    for rel in doc.part.rels:
                        if "image" in doc.part.rels[rel].target_ref:
                            image_data.append(doc.part.rels[rel].target_part.blob)
                elif file.endswith('.pdf'):
                    content,image_data = DocumentLoader.extract_text_from_pdf(path)
                #print("content----->",content)
                if content and len(content)>500:
                    content = re.sub(r'\s+', ' ', content)
                    content = ' '.join(content.split())
                    emails_in_doc = DocumentLoader.extract_emails(content)
                    phone_numbers = DocumentLoader.extract_mobile_numbers(content)
                    content = content.lower()
                #print("emails_in_doc----->",emails_in_doc,"length===>",len(content))
                if emails_in_doc and image_data:
                        profile_pic_path = DocumentLoader.save_profile_pic(self.profile_pic_folder,image_data,emails_in_doc[0])
        except Exception as e:
            pass
            #DocumentLoader.print_exception(e)
            #self.move_failed_file(path,self.failed_files_folder)
            #print(f"Skipping file {path} due to exception.")
        return content,emails_in_doc

    def _update_index(self):
        try:
            ix = open_dir(self.index_folder)
            writer = ix.writer()

            indexed_documents = {}
            for docnum, stored_fields in ix.searcher().iter_docs():
                indexed_documents[stored_fields['path']] = stored_fields.get('modified',None)
            
            content_hashes = set()  # To store content hashes
            for root, dirs, files in os.walk(self.search_folder):
                for file in files:
                    path = os.path.join(root, file)
                    modified_time = datetime.fromtimestamp(os.path.getmtime(path))
                    if path not in indexed_documents or modified_time > indexed_documents[path]:
                        content,emails_in_doc = self.read_document(file,path)
                        try:
                            if content:
                                    content = content.encode('utf-8', errors='ignore').decode('utf-8')
                                    content_hash = hashlib.md5(content.encode()).hexdigest()
                                    #print(content_hash,'====',content_hashes)
                                    if content_hash not in content_hashes:
                                        if path in indexed_documents and modified_time > indexed_documents[path]:
                                            writer.update_document(path=path, content=content, modified=modified_time,emails=emails_in_doc)
                                        else:
                                            writer.add_document(path=path, content=content,modified=modified_time,emails=emails_in_doc)
                                        content_hashes.add(content_hash)
                                        ##print(f"Indexing file {path}")
                                    #else:
                                    #    self.move_failed_file(path,self.duplicate_files_folder)
                                        #print(f"Skipping file {path} due to duplicate content.")
                            #else:
                                #self.move_failed_file(path,self.duplicate_files_folder)
                                #print(f"Skipping file {path} due to no content.")
                        except Exception as e:
                            pass
                            #self.move_failed_file(path,self.failed_files_folder)
                            #DocumentLoader.print_exception(e)
                            #print("Error updating index:", e)
                    #else:
                    #    print(f"Skipping file {path} due to already indexed content.")
            # Check for deleted files
            for indexed_path in indexed_documents.keys():
                if not os.path.exists(indexed_path):
                    writer.delete_by_term('path', indexed_path)
                    #print(f"Deleted file {indexed_path} from index.")
            writer.commit()
            
        except Exception as e:
            pass
            #self.move_failed_file(path,self.failed_files_folder)
            #DocumentLoader.print_exception(e)
            #print("Error updating index:", e)

    def search_files(self,search_text,is_complete_search=None):
        try:            
            ix = open_dir(self.index_folder)
            # Create a QueryParser for the field "content" in the schema
            if is_complete_search is None:
                query_parser = QueryParser("content", schema=ix.schema)
            else:
                query_parser = QueryParser("content", schema=ix.schema,group=OrGroup)
            query = query_parser.parse(search_text)

            # Search
            result_set = []
            with ix.searcher() as searcher:
                results = searcher.search(query,limit=None)
                for hit in results:
                    print(hit['path'],hit.score)
                    file_path = hit['path']
                    filename = os.path.basename(file_path)
                    result_set.append({"file_name":filename,"email":hit['emails'],"score":hit.score}) 
            if result_set:
                return {"file_list":result_set,"count":len(result_set)}
            return {"file_list":[],"count":0}
        except Exception as e:
            DocumentLoader.print_exception(e)
            return {"file_list":[],"count":-1}

class DocumentLoader:
    
    @staticmethod
    def extract_mobile_numbers(text):
    # Regular expression pattern to match email addresses
        pattern = r'\b(?:\+91[-\s]?)?\d{10}\b'
        # Find all matches of the pattern in the text
        mobile_numbers = re.findall(pattern, text)
        unique_mobile_numbers = list(set(mobile_numbers))
        return unique_mobile_numbers
    
    @staticmethod
    def extract_emails(text):
    # Regular expression pattern to match email addresses
        pattern = r'(?:Email:)?\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})'
        #pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        # Find all matches of the pattern in the text
        emails = re.findall(pattern, text)
        unique_emails = list(set(emails))
        return unique_emails

    @staticmethod
    def extract_skills(text):
    # Regular expression pattern to match email addresses
        pattern = r'(?:Email:)?\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})'
        #pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        # Find all matches of the pattern in the text
        emails = re.findall(pattern, text)
        unique_emails = list(set(emails))
        return unique_emails
    
    @staticmethod
    def extract_companies(text):
    # Regular expression pattern to match email addresses
        pattern = r'(?:Email:)?\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})'
        #pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        # Find all matches of the pattern in the text
        emails = re.findall(pattern, text)
        unique_emails = list(set(emails))
        return unique_emails

    @staticmethod
    def extract_experience(text):
    # Regular expression pattern to match email addresses
        pattern = r'(?:Email:)?\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})'
        #pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        # Find all matches of the pattern in the text
        emails = re.findall(pattern, text)
        unique_emails = list(set(emails))
        return unique_emails
    
    @staticmethod
    def extract_experience(text):
    # Regular expression pattern to match email addresses
        pattern = r'(?:Email:)?\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})'
        #pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        # Find all matches of the pattern in the text
        emails = re.findall(pattern, text)
        unique_emails = list(set(emails))
        return unique_emails
    @staticmethod
    def load_documents_from_directory(directory):
        documents = []
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            try:
                if filename.endswith('.docx'):
                    content = docx2txt.process(filepath)
                    documents.append({"title": filename, "content": content,"emails":DocumentLoader.extract_emails(content)})
                elif filename.endswith('.pdf'):
                    content = DocumentLoader.extract_text_from_pdf(filepath)
                    documents.append({"title": filename, "content": content,"emails":DocumentLoader.extract_emails(content)})
            except Exception as e:
                #print(f"Error processing file: {filepath}. Skipping... Error: {e}")
                DocumentLoader.print_exception(e)
        return documents
    @staticmethod
    def extract_text_from_pdf_reader(pdf_path):
        text = ""
        with open(pdf_path, "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)
            for page in pdf_reader.pages:
                text += page.extract_text()
        return text,[]
    @staticmethod
    def extract_text_from_pdf(pdf_path):
        text = ""
        image_data=[]
        '''try:'''
        with open(pdf_path, 'rb') as file:
                pdf_reader = PdfReader(file)
                page = pdf_reader.pages[0]  # Assuming profile picture is on the first page
                
                num_pages = len(pdf_reader.pages)
                for page_num in range(num_pages):
                    page = pdf_reader.pages[page_num]
                    text += page.extract_text()
                    if '/XObject' in page['/Resources']:
                                xobject = page['/Resources']['/XObject']. get_object()
                                for obj in xobject:
                                    if xobject[obj]['/Subtype'] == '/Image':
                                        image_data.append(xobject[obj].get_data())
                                        #image_data = xobject[obj].get_data()
        '''except Exception as e:
            #print("Error while saving pic in the path ",pdf_path,image_data)
            self.move_failed_file(pdf_path,self.failed_files_folder)
            DocumentLoader.print_exception(e)'''
        return text,image_data
    
    @staticmethod
    def save_profile_pic(save_pic_path,image_data,email):
        try:
                for i, image in enumerate(image_data):
                    image_filename = f'{email}_{i}.jpg'  # Include a counter in the filename
                    with open(os.path.join(save_pic_path, image_filename), 'wb') as img_file:
                        img_file.write(image)
                        profile_pic_path = os.path.join(save_pic_path, image_filename)
                            # print(f"Profile picture saved to: {profile_pic_path}")
            
        except Exception as e:
            print("Error while saving pic in the path ",save_pic_path,image_data,email)
            DocumentLoader.print_exception(e)
            
    
    @staticmethod
    def print_exception(e):
        error_details = str(e)
        traceback_str = traceback.format_exc()
        print("error_details",error_details)
        print("traceback_str",traceback_str)

if __name__ == "__main__":
    # Instantiate Search_Model_Service and call necessary methods
    service = Search_Model_Service(is_method=True)