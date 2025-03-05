from bson import ObjectId

import motor.motor_asyncio


class MongoClient:
    def __init__(self, settings):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(settings.mdb_url)
        self.db = self.client[settings.mdb_name]

    async def insert_dataset(self, dataset):
        result = await self.db.datasets.insert_one(dataset)
        return str(result.inserted_id)

    async def get_datasets(self):
        cursor = self.db.datasets.find({})
        docs = [doc for doc in await cursor.to_list(length=100)]
        for doc in docs:
            doc['id'] = str(doc['_id'])
            doc['ops'] = await self.get_opsets_for_dataset(doc['id'])
        return docs

    async def get_dataset(self, dataset_id):
        doc = await self.db.datasets.find_one({"_id": ObjectId(dataset_id)})
        doc['id'] = str(doc['_id'])
        return doc

    async def get_dataset_by_name(self, name):
        doc = await self.db.datasets.find_one({"name": name})
        doc['id'] = str(doc['_id'])
        return doc

    async def delete_dataset(self, dataset_id):
        _ = await self.db.opsets.delete_many({"dataset_id": dataset_id})
        result = await self.db.datasets.delete_one({"_id": ObjectId(dataset_id)})
        return result.deleted_count

    async def insert_opset(self, opset):
        result = await self.db.opsets.insert_one(opset)
        return str(result.inserted_id)

    async def get_opset(self, opset_id):
        doc = await self.db.opsets.find_one({"_id": ObjectId(opset_id)})

        if doc['id'] is None or doc['id'] == '0':
            doc['id'] = str(doc['_id'])
        return doc

    async def get_opsets_for_dataset(self, dataset_id):
        cursor = self.db.opsets.find({"dataset_id": dataset_id})
        opsets = [doc for doc in await cursor.to_list(length=100)]
        for ops in opsets:
            if ops['id'] is None or ops['id'] == '0':
                ops['id'] = str(ops['_id'])
        return opsets

    async def update_opset(self, opset_id, opset):
        result = await self.db.opsets.replace_one({"_id": ObjectId(opset_id)}, opset)
        if result.matched_count == 0:
            return None
        return await self.get_opset(opset_id)
