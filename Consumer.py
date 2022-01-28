import json
from time import sleep
from kafka import KafkaConsumer
from pymongo import MongoClient

consumer = KafkaConsumer(group_id='my-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         consumer_timeout_ms=10000)

list_of_product_user_topics = ['products-topic', 'users-topic']
consumer.subscribe(list_of_product_user_topics)

client = MongoClient('localhost:27017',
                     username='root',
                     password='Qwerty123!',
                     authMechanism='SCRAM-SHA-256')

database = client['hua-python']
collection = database['user']

# Deleting all users in the db collection as the very first step.
mongo_response = collection.delete_many({})
print('Deleted total {} users in user collection.'.format(mongo_response.deleted_count))
list_of_name_and_product_ids = []
products_list = {}

for message in consumer:

    # print('topic: {}'.format(message.topic))
    # print('partition: {}'.format(message.partition))
    # print('offset: {}'.format(message.offset))
    # print('message value: {}'.format(message.value))

    # users-topic e.g: {'name': 'Giorgos', 'age': 21, 'height': 1.78, 'productID': [4,3]}
    # products-topic e.g: {'productID': 4, 'name': 'How Google Works', 'price': 13.16, 'rating': 4.06}
    data = message.value

    if message.topic == 'users-topic':
        created_user = {
            'name': data.get('name'),
            'age': data.get('age'),
            'height': data.get('height'),
            'products': []
        }

        name_and_product = {
            'name': data.get('name'),
            'products': data.get('productID')
        }

        list_of_name_and_product_ids.append(name_and_product)

        collection.insert_one(created_user)
        print('User: {} created successfully.'.format(data.get('name')))

    elif message.topic == 'products-topic':
        products_list[data.get('productID')] = data

        for count, product in enumerate(products_list.keys()):
            for user in list_of_name_and_product_ids:
                allProductsFromUser = user.get('products')
                for productIfFromUser in allProductsFromUser:
                    if productIfFromUser == product:
                        result = products_list.get(product)
                        print('result: {}'.format(result))

                        find_by_name = {'name': user.get('name')}
                        list_of_result = [result]

                        unique_product = {'$addToSet': {'products': {'$each': list_of_result}}}
                        collection.update_one(find_by_name, unique_product)
                        print('User: {} updated successfully.'.format(user.get('name')))

sleep(1000)
