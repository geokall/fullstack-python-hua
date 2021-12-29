import json
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from neo4j import GraphDatabase


def fetch_all_nodes():
    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "hua-neo4j"))
    with driver.session() as session:
        result = session.read_transaction(retrieve_all_nodes_from_neo4j)
        for record in result:
            return record


def retrieve_all_nodes_from_neo4j(tx):
    query = (
        "MATCH (n) "
        "RETURN n "
    )
    result = tx.run(query)
    print("Returned all nodes in database.")
    return [result.data()]


neo4j_list = fetch_all_nodes()
filtered_neo4j_nodes_list = []

for obj in range(len(neo4j_list)):
    # Removing {'n':}
    filtered_neo4j_nodes_list.append(neo4j_list[obj]["n"])

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))
for i in range(len(filtered_neo4j_nodes_list)):
    print('Sending data to users-topic: ', filtered_neo4j_nodes_list[i])
    future = producer.send('users-topic', filtered_neo4j_nodes_list[i])
    sleep(2)
    # Block for 'synchronous' sends
    try:
        response_producer = future.get(timeout=5)

        # Successful result returns assigned partition and offset
        print(response_producer.topic)
        print(response_producer.partition)
        print(response_producer.offset)
    except KafkaError as e:
        print('[ERROR] ' + e.__str__())

producer.flush()
