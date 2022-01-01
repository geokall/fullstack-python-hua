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

# Always delete all data in mongo db as a first step
# print(collection.count_documents(), "existed in user collection.")
mongo_response = collection.delete_many({})
print(mongo_response.deleted_count, " deleted in user collection.")
####################################################################################
users_left = []
products_list = {}


def fusion(product_list, user):
    list_of_inserted = []
    list_of_not_inserted = []
    is_found = False

    for productID in user['productID']:
        print('h lista twn products')
        print(product_list.keys())
        if productID in product_list.keys():
            product = product_list.get(productID)
            list_of_inserted.append(product)
            is_found = True
        else:
            list_of_not_inserted.append(productID)

    data_fusion_user = {
        'name': user['name'],
        'age': user['age'],
        'height': user['height'],
        'products': list_of_inserted
    }

    return {'inserted': data_fusion_user, 'not_inserted': list_of_not_inserted, 'is_found': is_found}


for message in consumer:

    # print('topic: ' + message.topic)
    # print('partition: ' + message.partition)
    # print('offset: ' + message.offset)
    # print('value: ' + message.value)
    print('message value')
    data = message.value
    print(data)
    print('ti topic einai')
    print(message.topic)

    if message.topic == 'products-topic':
        products_list[data.get('productID')] = data
        print('Saving product to list.')
        print('-----U S E R S -- L E F T----------------------------------------')
        print(users_left)

        for index, user in enumerate(users_left.copy()):
            fuse = fusion(products_list, user)
            print('fusion products-topic')
            print(fuse.get('inserted'))
            print(fuse.get('not_inserted'))
            print(fuse.get('is_found'))

            if fuse.get('is_found'):
                query = {'name': user['name']}

                if len(fuse.get('inserted').get('products')) == 0:
                    raise Exception('Something went wrong ...')

                new_product = {'$push': {'products': {'$each': fuse.get('inserted').get('products')}}}
                print('neo proion')
                print(new_product)

                collection.update_one(query, new_product)

                # dhladh inserted
                if not fuse.get('not_inserted'):
                    users_left.remove(user)

                else:
                    print(fuse.get('inserted'), index)
                    users_left.remove(user)
                    users_left.append({
                        'name': user['name'],
                        'age': user['age'],
                        'height': user['height'],
                        'productID': fuse.get('not_inserted')
                    })

        print('-----------------------------------------------------------------')
        print(users_left)

    # users-topic
    else:
        fuse = fusion(products_list, data)

        print('fusion users-topic')
        print(fuse.get('inserted'))
        print(fuse.get('not_inserted'))
        print(fuse.get('is_found'))

        collection.insert_one(fuse.get('inserted'))
        print('User added.')

        if fuse.get('not_inserted'):
            data['productID'] = fuse.get('not_inserted')
            users_left.append(data)

print('THE END')
print(users_left)
sleep(1000)
