import os

from Flask import app_flask
from MySQL import insert_data_on_my_sql
from Neo4j import insert_data_to_neo4j

if __name__ == '__main__':

    # Creating Table Product on MySQL with 50 books-products
    insert_data_on_my_sql()

    # Creating users and friendship relationships
    insert_data_to_neo4j()

    # delete existing topics __consumer_offsets, products-topic, users-topic
    os.system("sudo -S docker exec kafka tmp/deleteTopic.sh")

    # Running on terminal these scripts
    os.system("gnome-terminal -- python ERProducer.py")
    os.system("gnome-terminal -- python GraphProducer.py")
    os.system("gnome-terminal -- python Consumer.py")

    app_flask.run()
