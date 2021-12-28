import os
import csv

from Flask import app_flask
from MySQL import insert_data_on_my_sql
from Neo4j import insertToNeo4j

if __name__ == '__main__':

    # Connect to MySql database, create table Product and insert 50 books
    insert_data_on_my_sql()
    # Connect to Neo4j database, create users and relationships and insert 15 users
    insertToNeo4j()
    #
    # delete existing topics
    os.system("sudo -S docker exec kafka tmp/deleteTopic.sh")

    # start the consumer
    os.system("gnome-terminal -- python Consumer.py")

    # # start the producers
    os.system("gnome-terminal -- python ERProducer.py")
    os.system("gnome-terminal -- python GraphProducer.py")

    # # start the flask
    app_flask.run()
