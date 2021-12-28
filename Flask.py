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
def show_user_profile(name):
    username = escape(name)
    cursor = collection.find({'name': username}, {"products": 1})
    list_cur = list(cursor)
    print(list_cur)
    if len(list_cur) == 0:
        return {'error': 'no users found'}
    elif len(list_cur) > 1:
        return {'error': 'more than 1 user found'}
    elif len(list_cur) == 1:
        return {'products': list_cur[0].get('products')}
    else:
        return {'error': 'something went wrong'}
