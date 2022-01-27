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
total_users_fetched = []
products_list = {}


def data_fusion(product_list, user):
    list_of_exist_in_keys = []
    list_of_not_exist_in_keys = []
    is_found = False

    print('list of products keys: {}'.format(product_list.keys()))

    # array of product ids
    for productID in user['productID']:

        if productID in product_list.keys():
            product = product_list.get(productID)
            list_of_exist_in_keys.append(product)
            is_found = True
        else:
            list_of_not_exist_in_keys.append(productID)

    data_fusion_user = {
        'name': user['name'],
        'age': user['age'],
        'height': user['height'],
        'products': list_of_exist_in_keys
    }

    return {'exist': data_fusion_user, 'not_exist': list_of_not_exist_in_keys, 'is_found': is_found}


for message in consumer:

    print('topic: {}'.format(message.topic))
    print('partition: {}'.format(message.partition))
    print('offset: {}'.format(message.offset))
    print('message value: {}'.format(message.value))

    # contains array of product ids and product data (name,age..)
    data = message.value

    if message.topic == 'products-topic':
        products_list[data.get('productID')] = data
        print('Saving product to products list')
        print('-----U S E R S -- L E F T----------------------------------------')
        print(total_users_fetched.copy())

        for count, user in enumerate(total_users_fetched.copy()):
            fuse = data_fusion(products_list, user)
            print('fusion products-topic')
            print(fuse.get('exist'))
            print(fuse.get('not_exist'))
            print(fuse.get('is_found'))

            if fuse.get('is_found'):
                query = {'name': user['name']}

                if len(fuse.get('exist').get('products')) == 0:
                    raise Exception('Something went wrong ...')

                new_product = {'$push': {'products': {'$each': fuse.get('exist').get('products')}}}
                print('neo proion')
                print(new_product)

                collection.update_one(query, new_product)

                # this case is about those who have been exist
                if not fuse.get('not_exist'):
                    total_users_fetched.remove(user)

                else:
                    print(fuse.get('exist'), count)
                    total_users_fetched.remove(user)
                    total_users_fetched.append({
                        'name': user['name'],
                        'age': user['age'],
                        'height': user['height'],
                        'productID': fuse.get('not_exist')
                    })

        print('-----------------------------------------------------------------')
        print(total_users_fetched)

    # users-topic
    else:
        fuse = data_fusion(products_list, data)

        print('fusion users-topic')
        print(fuse.get('exist'))
        print(fuse.get('not_exist'))
        print(fuse.get('is_found'))

        collection.insert_one(fuse.get('exist'))
        print('User added.')

        if fuse.get('not_exist'):
            data['productID'] = fuse.get('not_exist')
            total_users_fetched.append(data)

print('THE END')
print(total_users_fetched)
sleep(1000)
