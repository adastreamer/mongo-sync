import os
from pymongo import MongoClient

MONGO_SOURCE_URL = os.environ.get('MONGO_SOURCE_URL')
MONGO_TARGET_URL = os.environ.get('MONGO_TARGET_URL')

mc_source = MongoClient(MONGO_SOURCE_URL, connect=False)
db_source = mc_source.get_default_database()

mc_target = MongoClient(MONGO_TARGET_URL, connect=False)
db_target = mc_target.get_default_database()

for col in db_source.collection_names():
    records = db_source[col].find()
    for index, rec in enumerate(records):
        target = db_target[col].find_one(rec)
        if not target:
            print("inserting {}[{}]".format(col, index))
            db_target[col].insert_one(rec)
        else:
            print("skipping {}[{}]".format(col, index))

