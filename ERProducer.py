import json
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
import mysql.connector
from mysql.connector import errorcode

try:
    cnx = mysql.connector.connect(user='root', password='Qwerty123!',
                                  database='hua-python')
    cursor = cnx.cursor()

    products_from_db = "SELECT * FROM Product;"
    cursor.execute(products_from_db)

    # retrieve all column names
    column_names = [x[0] for x in cursor.description]
    all_rows = cursor.fetchall()

    list_of_json_data = []

    # using dict in order to format JSON string from list of tuples
    # zip to pair together each item
    for row in all_rows:
        list_of_json_data.append(dict(zip(column_names, row)))

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    products_length = len(products_from_db)

    for i in range(products_length):
        print('Sending data to products-topic: ', list_of_json_data[i])
        kafka_producer = producer.send('products-topic', list_of_json_data[i])
        # waiting 2 seconds per json data, will give the result 10 elements per 20 seconds
        sleep(2)
        try:
            response_producer = kafka_producer.get(timeout=5)

            # Successful result returns assigned partition and offset
            print(response_producer.topic)
            print(response_producer.partition)
            print(response_producer.offset)
        except KafkaError as e:
            print('[ERROR] ' + e.__str__())

    producer.flush()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Wrong credentials")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
