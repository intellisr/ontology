from py2neo import Graph, Node, Relationship
graph = Graph("bolt+s://d0642942.databases.neo4j.io:7687", auth=("neo4j", "AHHzMMC8_dusNm8T6bdkgs0lClYkdUg3VvChFi8d5GI"))

def get_related_question(question):
    query = """
    CALL db.index.fulltext.queryNodes("questionIndex",$question)
    YIELD node, score
    RETURN node.name, score
    ORDER BY score DESC
    """
    result = graph.run(query, question=question).data()
    return result[0]

def get_related_data(action):
    query = """
    MATCH (a:Action {name: $action})
    OPTIONAL MATCH (a)-[:HAS_STEP]->(s:Step)
    OPTIONAL MATCH (a)-[:HAS_REQUIREMENT]->(r:Requirement)
    OPTIONAL MATCH (a)-[:HAS_KEY]->(k:Keys)
    WITH a, collect(DISTINCT s.name) AS steps, collect(DISTINCT r.name) AS requirements, collect(DISTINCT k.name) AS keys
    RETURN steps, requirements, keys
    """
    result = graph.run(query, action=action).data()
    return result[0]

def get_action(question):
    query = """
    MATCH (q:Question {name: $question})<-[:HAS_QUESTION]-(a:Action)
    RETURN a.name AS action
    """
    result = graph.run(query, question=question).data()
    return result[0]


def get_context(q_txt):
    related_data = get_related_question(q_txt)
    print(related_data['node.name'])
    
    action=get_action(related_data['node.name'])
    print(action['action'])
    
    related_data = get_related_data(action['action'])
    
    key_txt="#Key words:"
    keys=related_data['keys'] 
    for k in keys:
        key_txt=key_txt+k+','
        
    req_txt="#Prerequisites:"
    requirements=related_data['requirements'] 
    for r in requirements:
        req_txt=req_txt+r+','  
        
    step_txt="#Steps to Follow:"
    steps=related_data['steps'] 
    for s in steps:
        step_txt=step_txt+s+','
        
    context=key_txt+'\n'+req_txt+'\n'+step_txt   
    return context

import pandas as pd

# df = pd.read_excel('uber.xlsx', header=None)
# # Extract the two columns
# column1 = df[0].tolist()
# column2 = df[1].tolist()
q_txt="Can you help me with downloading the Uber Driver app on my iPhone?"
context=get_context(q_txt)
answer=""
 
prompt = f"""###System:Read the Context provided and answer the corresponding question.
###Context:
{context}
###Question:
{q_txt}
###Answer:
{answer}"""

print(prompt)    