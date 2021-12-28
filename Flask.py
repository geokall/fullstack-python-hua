from flask import Flask
from markupsafe import escape
from pymongo import MongoClient

app_flask = Flask(__name__)

client = MongoClient('localhost:27017',
                     username='root',
                     password='Qwerty123!',
                     authMechanism='SCRAM-SHA-256')

database = client['usersDB']
collection = database['users']


@app_flask.route('/user/<name>', methods=['GET'])
def find_products_by_username(name):
    username = escape(name)

    response = collection.find({'name': username}, {"products": 1})

    response_list = list(response)

    if len(response_list) == 0:
        return {'notFoundError': 'None users found with this username'}
    elif len(response_list) > 1:
        return {'duplicateError': 'In DB exists more than one users with the same username'}
    elif len(response_list) == 1:
        return {'products': response_list[0].get('products')}
    else:
        return {'error': 'Generic error'}
