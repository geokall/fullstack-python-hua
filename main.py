import os
import csv

# from Flask import app_flask
from MySQL import insertToMySQL
# from Neo4j import insertToNeo4j

if __name__ == '__main__':

    filename = open('books.csv', 'r')
    file = csv.DictReader(filename)

    ratings = []
    titles = []
    types = []
    prices = []
    counter = 0
    for column in file:
        ratings.append(column['Rating'])
        titles.append(column['Book_title'])
        types.append(column['Type'])
        prices.append(column['Price'])

        counter += 1
        if counter == 51:
            break

    # Connect to MySql database, create table Product and insert 50 books
    insertToMySQL(ratings, titles, types, prices)
    # Connect to Neo4j database, create users and relationships and insert 15 users
    # insertToNeo4j()
    #
    # # delete existing topics
    # os.system("sudo -S docker exec kafka tmp/deleteTopic.sh")
    #
    # # start the consumer
    # os.system("gnome-terminal -- python Consumer.py")
    #
    # # start the producers
    # os.system("gnome-terminal -- python ERProducer.py")
    # os.system("gnome-terminal -- python GraphProducer.py")
    #
    # # start the flask
    # app_flask.run()
