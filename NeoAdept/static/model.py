from collections import defaultdict
import re
import nltk,spacy
from nltk import word_tokenize, pos_tag, ne_chunk,ngrams
from nltk.corpus import stopwords
from nltk.tree import Tree
from transformers import BertTokenizer, BertForTokenClassification
from transformers import pipeline
class Model_Utility:

    _instance = None  # Class variable to store the singleton instance
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            return cls._instance
        return cls._instance

    def __init__(self,pattern_list=None):
        if not hasattr(self, 'initialized'):
            self.nlp = spacy.load("en_core_web_sm")  
            self.ruler = self.nlp.add_pipe("entity_ruler", before="ner")
            self.ruler.from_disk("patterns.jsonl")

    def process_job_desc(self,job_description):
        doc = self.nlp(job_description)        
        print("Entities...............................")
        experience_skills = defaultdict(set)
        current_exp = None
        for ent in doc.ents:
            print(ent.label_,ent.text)
            if ent.label_ == 'DATE':
                if '+' in ent.text:
                    current_exp = int(ent.text.split('+')[0])
                else:
                    current_exp = int(ent.text.split(' ')[0])
            if ent.label_ == 'SKILL':
                if current_exp is None:
                    current_exp = 1
                experience_skills[current_exp].add(ent.text)
            if ent.label_ == 'LOCATION':
                location = ent.text
        for exp in experience_skills:
            experience_skills[exp] = list(experience_skills[exp])
        return experience_skills,location