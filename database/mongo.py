from pymongo import MongoClient

from config.settings import Mongo

class Conn(object):
    def __init__(self):
        mongo_conn = MongoClient(Mongo.HOST)
    #     self.mongo_db = mongo_conn.marginbot
    #
    # def mongo_ins_msg(self, data):
    #     collection = self.mongo_db['send_telegram']
    #     obj = collection.insert(data)
    #     return obj
    #
    # def mongo_ins_error(self, data):
    #     collection = self.mongo_db['error_usersocket']
    #     obj = collection.insert(data)
    #     return obj
    #
    # def mongo_ins_info(self, data):
    #     collection = self.mongo_db['service_info']
    #     obj = collection.insert(data)
    #     return obj

