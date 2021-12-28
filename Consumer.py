import json
from time import sleep
from kafka import KafkaConsumer
from pymongo import MongoClient

# To consume latest messages and auto-commit offsets ##############################
consumer = KafkaConsumer(group_id='my-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         consumer_timeout_ms=10000)

topics_list = ['products-topic', 'users-topic']
consumer.subscribe(topics_list)
###################################################################################

# Connect to MongoDB ##############################################################
client = MongoClient('localhost:27017',
                     username='root',
                     password='Qwerty123!',
                     authMechanism='SCRAM-SHA-256')

database = client['usersDB']
collection = database['users']

# Delete everything in mongo database
x = collection.delete_many({})
print(x.deleted_count, " documents deleted.")
####################################################################################
users_left = []
products_list = {}


def fusion(product_list, usr):
    result = []
    not_inserted = []
    flag = False
    for productID in usr['productID']:
        if productID in product_list.keys():
            result.append(product_list.get(productID))
            flag = True
        else:
            not_inserted.append(productID)
    fused_user = {
        'name': usr['name'],
        'age': usr['age'],
        'purchased': usr['purchased'],
        'products': result
    }

    return {'fusion_user': fused_user, 'not_inserted': not_inserted, 'found': flag}


for message in consumer:

    print('')
    print("[NEW MESSAGE] ========> %s:%d:%d: value=%s" % (message.topic,
                                                          message.partition,
                                                          message.offset,
                                                          message.value))
    data = message.value
    if message.topic == 'products-topic':
        products_list[data.get('productID')] = data
        print('Saving product to list.')
        print('-----U S E R S -- L E F T----------------------------------------')
        print(users_left)
        for index, user in enumerate(users_left.copy()):
            fuse = fusion(products_list, user)

            if fuse.get('found'):
                query = {'name': user['name']}
                if len(fuse.get('fusion_user').get('products')) == 0:
                    raise Exception('Something went wrong ...')

                new_product = {'$push': {'products': {'$each': fuse.get('fusion_user').get('products')}}}

                print(new_product)
                collection.update_one(query, new_product)
                print('____Database updated.')

                if not fuse.get('not_inserted'):
                    users_left.remove(user)

                else:
                    print(fuse.get('fusion_user'), index)
                    users_left.remove(user)
                    users_left.append({
                        'name': user['name'],
                        'age': user['age'],
                        'purchased': user['purchased'],
                        'productID': fuse.get('not_inserted')
                    })

        print('-----------------------------------------------------------------')
        print(users_left)

    else:
        fuse = fusion(products_list, data)
        print(fuse.get('fusion_user'))
        collection.insert_one(fuse.get('fusion_user'))
        print('User added.')

        if fuse.get('not_inserted'):
            data['productID'] = fuse.get('not_inserted')
            users_left.append(data)

print('THE END')
print(users_left)
sleep(1000)
