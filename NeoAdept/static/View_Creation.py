from datetime import datetime
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb+srv://neocv:3WuC33YTWHEjzyvs@cluster0.pabvnx8.mongodb.net/')
db = client['Tezra']
collection = db['CANDIDATE_DETAILS']
new_collection = db['UNIQUE_KEYS_CANDIDATE_DETAILS']

def extract_unique_skills():
    all_skills =  set()
    
    for doc in collection.find():
        if "primary_skills" in doc and doc["primary_skills"]:
            for skill in doc["primary_skills"]:
                if skill and len(skill)<50: 
                    all_skills.add(skill.lower())

        if "secondary_skills" in doc and doc["secondary_skills"]:
            for skill in doc["secondary_skills"]:
                if skill and len(skill)<50: 
                    all_skills.add(skill.lower())
    return list(all_skills)

def insert_unique_data(key_dict):
    for doc in collection.find():
        for key,new_key in key_dict.items():
            if key in doc and doc[key]:
                if isinstance(doc[key],list):
                    for list_data in doc[key]:
                        if list_data and len(list_data)<50: 
                            is_doc_exist = new_collection.find_one({new_key:list_data})
                            if not is_doc_exist:
                                new_collection.insert_one({new_key:list_data})
                else:
                    is_doc_exist = new_collection.find_one({new_key:doc[key]})
                    if not is_doc_exist:
                        new_collection.insert_one({new_key:doc[key]})

# Extract unique values and store them in the new collection
KEY_SET = {"primary_skills":"skills","secondary_skills":"skills","current_location":"current_location"}
insert_unique_data(KEY_SET)
#new_collection.insert_one({"skills":unique_values})

print("Unique values have been extracted and stored in the new collection.")