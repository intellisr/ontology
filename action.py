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
        "Step follows Step",
        "Step involves Action",
    ]
}

client = pymongo.MongoClient("mongodb+srv://manoj:manoj123@cluster0.z04hirk.mongodb.net/")
db = client["sample_mflix"]
collection = db["ont2"]

def upload_dict_as_document(no,action):
  collection.update_one(
        {"no": no},  # Filter to find the document by "no"
        {"$set": {"action2": action}}  # Update operation
  )

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
       
def extract_action(question):
  try:
      response = prompts('extract expected action from this quection "'+question+'" and write it as python variable, this is a example:- Action = "apply_promo_code"')
      return response
  except Exception as e:
      return False

  
# Read the excel file
df = pd.read_excel('uber.xlsx', header=None)
# Extract the two columns
column1 = df[0].tolist()
column2 = df[1].tolist()

def get_data_actions(number):
  pre_question=preprocess_text(column1[number])
  try:
    action_txt=extract_action(pre_question)
    if action_txt:
      action=extract_string_in_double_quotes(action_txt)
      print(action)
  except Exception as e:
    print(e)     
  
  if action:
    actionLower=preprocess_text(action)
    return actionLower

def is_single_word(s):
    return len(s.strip().split()) == 1
  
def addActions(actionList,noList):
  action=""
  actionHaveN=True
  for x in actionList:
    if is_single_word(x):
      action=x
      actionHaveN=False 
  
  if actionHaveN:
    action=get_data_actions(noList[0])
    
  for n in noList:
    print(action,n) 
    upload_dict_as_document(n,action)       
   
      

noList=[]
actionList=[]
answer=""           
for i in range(1,1114):
  if i not in [543,666,969,1009,1107]:
    pre_answer=preprocess_text(column2[i])
    if pre_answer != answer and answer!="":
      print("Execution on:",i-1)    
      addActions(actionList,noList)
      answer=pre_answer
      noList=[]
      actionList=[]
      actionList.append(document['action'])
      noList.append(i)
    else:
      if answer=="":
        answer=pre_answer
      document = collection.find_one({"no": i})
      if document:
        actionList.append(document['action'])  
        noList.append(i)