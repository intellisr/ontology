import pandas as pd
import re
import pymongo
import requests
import json
from langchain_community.llms import Ollama
def prompts(prompt):    
    ollama = Ollama(
            base_url='http://localhost:11434',
            model="llama3"
            )
    print("Invoking..")
    answer=ollama.invoke(prompt)
    return answer


ontology = {
    "labels":[
        {"Question": "Minimum representation of the customer query e.g., create uber account"},
        {"Action": "Represents specific actions mentioned in answers e.g., Create Account, Verify Phone Number, Enter Payment Information."},
        {"Requirement": "Represents the necessary requirements or conditions mentioned in answers e.g., Email Address, Phone Number, Credit Card."},
        {"Step": "Represents list of steps in a process described in answers."},
        {"Entity": "Represents entities involved in the questions and answers e.g., Uber, Customer, Phone, App."},
    ],
    "relationships":[
        "Question includes Action",
        "Action requires Requirement",
        "Action involves Entity",
        "Step involves Action",
    ]
}

client = pymongo.MongoClient("mongodb+srv://manoj:manoj123@cluster0.z04hirk.mongodb.net/")
db = client["sample_mflix"]
collection = db["ont2"]

def upload_dict_as_document(data):
  result = collection.insert_one(data)
  return result.inserted_id

def preprocess_text(text):
    try:
      text = re.sub(r'\s+', ' ', text)  # Remove extra spaces
      text = re.sub(r'\W', ' ', text)  # Remove non-alphanumeric characters
      text = text.lower().strip()  # Convert to lowercase and strip spaces
      return text
    except Exception as e:
      print(e)
      return text


def extract_string_in_double_quotes(text):
    match = re.search(r'"(.*?)"', text)
    if match:
        return match.group(1)
    else:
        return None
    
def extract_list_from_string(text):
  # Remove any leading or trailing whitespace
  text = text.strip()
  first=text.split('[')[1]
  second=text.split(']')[1]
  textA=first.replace(']'+second,"")
  # Split the string on commas
  if textA=="":
    return None
  elements = textA.split('",')
  s_list=[]
  for e in elements:
    e_l=extract_string_in_double_quotes(e+'"')
    s_list.append(e_l)

  # Return the Python list
  return s_list    

def extract_action(question):
  try:
      response = prompts('I want to extract expected action from quection,Write the action that invole with this question "'+question+'" and write it as python variable, this is a example:- Action = "apply_promo_code"')
      return response
  except Exception as e:
      return False

def extract_steps(action,answer,question):

  try:
      response = prompts('Extract steps that need to follow to complete this action "'+action+'" or answer to this question "'+question+'" from the following answer "'+answer+'" and write them as a Python list variable.')
      return response
  except Exception as e:
      return e

def extract_requirement(action,answer):

  try:
      response = prompts('Extract the prerequisites needed to complete this action "'+action+'"  from the following answer "'+answer+'" and write them as a Python list variable.')
      return response
  except Exception as e:
      return e

def extract_entities(action,answer):

  try:
      response = prompts('Extract entities related to the action  "'+action+'"  from the following answer "'+answer+'" and write them as a Python list variable.')
      return response
  except Exception as e:
      return e 
  
# Read the excel file
df = pd.read_excel('uber.xlsx', header=None)
# Extract the two columns
column1 = df[0].tolist()
column2 = df[1].tolist()

def get_data_for_ontology(number):
  pre_question=preprocess_text(column1[number])
  pre_answer=preprocess_text(column2[number])
  print(column1[number])
  action=""
  steps=[]
  req=[]
  entities=[]
  try:
    action_txt=extract_action(pre_question)
    if action_txt:
      action=extract_string_in_double_quotes(action_txt)
      print(action)
  except Exception as e:
    print(e)     
  
  if action:
    actionLower=preprocess_text(action)
    try:
        steps_txt=extract_steps(actionLower,pre_answer,pre_question)
        steps=extract_list_from_string(steps_txt)
    except Exception as e:
      print(e)

    try:
        ent_txt=extract_entities(actionLower,pre_answer)
        entities=extract_list_from_string(ent_txt)
    except Exception as e:
      print(e)                

    try:
        req_txt=extract_requirement(actionLower,pre_answer)
        req=extract_list_from_string(req_txt)
    except Exception as e:
      print(e)  

    print(steps)
    print(entities)
    print(req)

    if len(steps) > 0:
      try:
        upload_dict_as_document({"no": number,"question":pre_question ,"action":action,"steps":steps,"requirements":req,"entities":entities})
      except Exception as e:
        print(e)
mylist=[543,666,969,1009,1107]           
for i in mylist:
    get_data_for_ontology(i)