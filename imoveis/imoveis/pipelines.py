# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

# db.imoveis.find({'history_price': {$exists: true}, $where: 'this.history_price.length>1'})
# db.imoveis.find().sort({'last_updated':1})
# db.imoveis.find({'history_price': {$exists: true}, $where: 'this.history_price.length>1'}).sort({'last_updated':1}).pretty()
# db.imoveis.aggregate([{   '$group': {     '_id': {'id': '$id'},      'count': {'$sum': 1},      'data': {'$addToSet': '$$ROOT'}} }, {   '$match': {     'count': {'$gt': 1} }},  { $project: { _id: 0,                   id: "$_id.id",                   count: 1}} ])
from pymongo import MongoClient
from scrapy.exceptions import DropItem


class ImoveisPipeline:
    def process_item(self, item, spider):
        history_price = {
            'price': item.get('price'),
            'last_updated': item.get('last_updated')
        }
        document = self.db.imoveis.find_one({'source_id': item.get('source_id')})
        if document is not None:
            if document.get('price') != item.get('price'):
                self.db.imoveis.update_one(
                    {'source_id': item.get('source_id')},
                    {
                        '$set': {
                            'price': item.get('price'),
                            'url': item.get('url'),
                            'last_updated': item.get('last_updated')
                        },
                        '$push': {'history_price': history_price}
                    }
                )
            else:
                raise DropItem("Repeated %s" % item['url'])
        else:
            new_document = dict(item)
            new_document['history_price'] = [history_price]
            self.db.imoveis.insert(new_document)
        return item

    def open_spider(self, spider):
        self.client = MongoClient()
        self.db = self.client.dbimoveis

    def close_spider(self, spider):
        self.client.close()
