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

    allProductsFromDB = "SELECT * FROM Product;"
    cursor.execute(allProductsFromDB)

    row_headers = [x[0] for x in cursor.description]
    allRows = cursor.fetchall()

    listOfJsonData = []

    # using dict in order to format JSON string from list of tuples
    # zip to pair together each item
    for row in allRows:
        listOfJsonData.append(dict(zip(row_headers, row)))

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    print(len(allProductsFromDB))

    for i in range(len(allProductsFromDB)):
        print('Sending data: ', listOfJsonData[i])
        future = producer.send('products-topic', listOfJsonData[i])
        sleep(2)
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)

            # Successful result returns assigned partition and offset
            print(record_metadata.topic)
            print(record_metadata.partition)
            print(record_metadata.offset)
        except KafkaError as e:
            # Decide what to do if produce request failed...
            print('[ERROR] ' + e.__str__())

    # block until all async messages are sent
    producer.flush()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Wrong credentials")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
