import pymongo
from py2neo import Graph, Node, Relationship


client = pymongo.MongoClient("mongodb+srv://manoj:manoj123@cluster0.z04hirk.mongodb.net/")
db = client["sample_mflix"]
collection = db["ont"]

# Connect to Neo4j
graph = Graph("bolt+s://d0642942.databases.neo4j.io:7687", auth=("neo4j", "AHHzMMC8_dusNm8T6bdkgs0lClYkdUg3VvChFi8d5GI"))

def transform_document(doc):
    entities = []
    
    # Add Action entity
    if doc["action"]:
        entities.append({"name": doc["action"], "label": "Action"})
        
    if doc["question"] != None:
        for qs in doc.get("question", []):
            if qs:
                entities.append({"name": qs, "label": "Question"})   
    
    # Add Steps entities
    if doc["steps"] != None:
        for step in doc.get("steps", []):
            if step:
                entities.append({"name": step, "label": "Step"})
    
    # Add Requirements entities
    if doc["requirements"] != None:
        for requirement in doc.get("requirements", []):
            if requirement:
                entities.append({"name": requirement, "label": "Requirement"})
    
    # Add Keys entities
    if doc["entities"] != None:        
        for ent in doc.get("entities", []):
            if ent:
                entities.append({"name": ent, "label": "Keys"})        
    
    return entities

documents = collection.find()
transformed_data = [transform_document(doc) for doc in documents]

def create_nodes_and_relationships(entities):
    node_dict = {}
    
    # Create or get nodes
    for entity in entities:
        if entity["name"] not in node_dict:
            node = graph.nodes.match(entity["label"], name=entity["name"]).first()
            if not node:
                node = Node(entity["label"], name=entity["name"])
                graph.create(node)
                print(f"Created node: {entity['label']} - {entity['name']}")
            node_dict[entity["name"]] = node
    
    # Identify the main action node
    action_node = node_dict.get(next(ent["name"] for ent in entities if ent["label"] == "Action"))
    if not action_node:
        return
    
    # Create relationships from action node to other entities
    for entity in entities:
        if entity["label"] == "Question":
            question_node = node_dict[entity["name"]]
            relationship = Relationship(action_node, "HAS_QUESTION", question_node)
            graph.create(relationship)
            print(f"Created relationship: {action_node['name']} -> HAS_QUESTION -> {question_node['name']}")
        elif entity["label"] == "Step":
            step_node = node_dict[entity["name"]]
            relationship = Relationship(action_node, "HAS_STEP", step_node)
            graph.create(relationship)
            print(f"Created relationship: {action_node['name']} -> HAS_STEP -> {step_node['name']}")
        elif entity["label"] == "Requirement":
            requirement_node = node_dict[entity["name"]]
            relationship = Relationship(action_node, "HAS_REQUIREMENT", requirement_node)
            graph.create(relationship)
            print(f"Created relationship: {action_node['name']} -> HAS_REQUIREMENT -> {requirement_node['name']}")
        elif entity["label"] == "Keys":
            key_node = node_dict[entity["name"]]
            relationship = Relationship(action_node, "HAS_KEY", key_node)
            graph.create(relationship)
            print(f"Created relationship: {action_node['name']} -> HAS_KEY -> {key_node['name']}")    

# Insert data into Neo4j
for data in transformed_data:
    create_nodes_and_relationships(data)