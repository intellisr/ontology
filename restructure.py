import re
import pymongo

client = pymongo.MongoClient("mongodb+srv://manoj:manoj123@cluster0.z04hirk.mongodb.net/")
db = client["sample_mflix"]
collection = db["ont2"]
collection2 = db["ont"]

def upload_dict_as_document(data):
  result = collection2.insert_one(data)
  return result.inserted_id

def get_max(dict_list):
  sorted_dict = dict(sorted(dict_list.items()))
  key=list(sorted_dict.keys())[-1]
  return sorted_dict[key]

       
for doc in collection.find({}):
  steps={}
  require={}
  ent={}
  questions=[]
  action=doc['action2']
  obj=collection2.find_one({'action':action})
  if obj:
    print("Already Have:"+action)
  else:  
    for doc2 in collection.find({'action2':action}):
      no=doc2['no']
      steps[no]=doc2['steps']
      require[no]=doc2['requirements']
      ent[no]=doc2['entities']
      questions.append(doc2['question'])
    
    l_steps=get_max(steps)
    l_require=get_max(require) 
    l_ent=get_max(ent)
    new_dict={"question":questions ,"action":action,"steps":l_steps,"requirements":l_require,"entities":l_ent} 
    print(new_dict)  
    try:
      upload_dict_as_document(new_dict)
    except Exception as e:
      print(e)  
    
