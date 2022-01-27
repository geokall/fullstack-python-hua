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

list_of_users_to_save = []
list_of_product_ids = {}


def fusion(list_of_product_id, user):
    list_of_exist_in_keys = []
    list_of_not_exist_in_keys = []
    is_found = False

    print('list of products keys: {}'.format(list_of_product_id.keys()))

    # array of product ids
    for productID in user['productID']:
        if productID in list_of_product_id.keys():
            product = list_of_product_id.get(productID)
            list_of_exist_in_keys.append(product)
            is_found = True
        else:
            print('ID {} does not exist in products list'.format(productID))
            list_of_not_exist_in_keys.append(productID)

    data_fusion_user = {
        'name': user['name'],
        'age': user['age'],
        'height': user['height'],
        'products': list_of_exist_in_keys
    }

    return {'inserted': data_fusion_user, 'not_inserted': list_of_not_exist_in_keys, 'is_found': is_found}


for message in consumer:

    # print('topic: {}'.format(message.topic))
    # print('partition: {}'.format(message.partition))
    # print('offset: {}'.format(message.offset))
    print('message value: {}'.format(message.value))

    # users-topic e.g: {'name': 'Giorgos', 'age': 21, 'height': 1.78, 'products': [4]}
    # products-topic e.g: {'productID': 4, 'name': 'How Google Works', 'price': 13.16, 'rating': 4.06}
    data = message.value

    if message.topic == 'products-topic':
        list_of_product_ids[data.get('productID')] = data

        # enumerate to retrieve count
        for count, user in enumerate(list_of_users_to_save.copy()):
            fuse = fusion(list_of_product_ids, user)
            print('Data fusion on products-topic')
            print(fuse.get('inserted'))
            print(fuse.get('not_inserted'))
            print('User found: {}'.format(fuse.get('is_found')))

            if fuse.get('is_found'):
                saved_user_in_db_by_name = {'name': user['name']}

                if len(fuse.get('inserted').get('products')) == 0:
                    raise Exception('Something went wrong, user must have at least one product')

                new_product = {'$push': {'products': {'$each': fuse.get('inserted').get('products')}}}
                print(new_product)

                collection.update_one(saved_user_in_db_by_name, new_product)
                print('User updated in mongoDB successfully.')

                # this case is about those who have been inserted
                if not fuse.get('not_inserted'):
                    print('Removed from users: {}'.format(user))
                    list_of_users_to_save.remove(user)

                else:
                    print(fuse.get('inserted'), count)
                    print('Removed from users: {}'.format(user))
                    list_of_users_to_save.remove(user)
                    list_of_users_to_save.append({
                        'name': user['name'],
                        'age': user['age'],
                        'height': user['height'],
                        'productID': fuse.get('not_inserted')
                    })

        print('-----///-----')
        print(list_of_users_to_save)

    # users-topic
    else:
        print('Data fusion on users-topic')

        fuse = fusion(list_of_product_ids, data)

        print(fuse.get('inserted'))
        print(fuse.get('not_inserted'))
        print('User found: {}'.format(fuse.get('is_found')))

        collection.insert_one(fuse.get('inserted'))
        print('User created in mongoDB successfully.')

        if fuse.get('not_inserted'):
            data['productID'] = fuse.get('not_inserted')
            list_of_users_to_save.append(data)

print('Final result should be empty array.')
print(list_of_users_to_save)
sleep(2000)
