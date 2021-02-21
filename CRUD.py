import pymongo
import json
import os
from pymongo import MongoClient

client = MongoClient("mongodb://127.0.0.1:27017/?directConnection=true")
db = client["test_db"]
collection = db ["test_col"]

#############################CREATE#############################
def create_one(file):
    JSON = json.loads(open(file, "r").read())
    x = collection.insert_one(JSON)
    print(x.inserted_id)

def create_multiple(folder):
    docs=[]
    for file in os.listdir(folder):
        path=folder+"/"+file
        JSON_FILE = json.loads(open(path, "r").read())
        docs.append(JSON_FILE)
    x = collection.insert_many(docs)
    print(x.inserted_ids)

#############################READ#############################
def readAll():
    res = collection.find()
    for x in res:
        print(x)

def read():
    res = collection.find_one({"Emp_ID":1})
    print(res)

def readCondition():
    query = {
        "Emp_ID": {"$gt":1}
    }

    res=collection.find(query)
    for x in res:
        print(x)

def readCondition2():
    query = {
        "$and":[
        {"Emp_ID": {"$gt":1}},
        {"Personal_details.Last_Name":"Hörich"}
        ]
    }

    res=collection.find(query)
    for x in res:
        print(x)
#############################UPDATE#############################
def update():
    collection.update_one({"Emp_ID":1},{"$set":{"Contact.phone":""}})

#############################DELETE#############################

def delete():
    collection.delete_one({"Emp_ID":1})
    

#create_one("JSON/testdata0.json")
#create_multiple("JSON")
#readAll()
#read()
#readCondition()
#readCondition2()
#update()
#delete()

