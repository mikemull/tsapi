from bson import ObjectId

import motor.motor_asyncio


class MongoClient:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
        self.db = self.client['tsapidb']

    async def insert_dataset(self, dataset):
        result = await self.db.datasets.insert_one(dataset)
        return repr(result.inserted_id)

    async def get_datasets(self):
        cursor = self.db.datasets.find({})
        docs = [doc for doc in await cursor.to_list(length=100)]
        for doc in docs:
            doc['id'] = str(doc['_id'])
        return docs

    async def get_dataset(self, dataset_id):
        doc = await self.db.datasets.find_one({"_id": ObjectId(dataset_id)})
        doc['id'] = str(doc['_id'])
        return doc
