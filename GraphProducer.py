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
    print("Returned all nodes from neo4j.")
    return [result.data()]


all_neo4j_nodes = fetch_all_nodes()
filtered_neo4j_nodes_list = []

for obj in range(len(all_neo4j_nodes)):
    # Removing {'n':}
    filtered_neo4j_nodes_list.append(all_neo4j_nodes[obj]["n"])

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))

print('Sending total nodes: {}'.format(len(filtered_neo4j_nodes_list)))

for i in range(len(filtered_neo4j_nodes_list)):
    print('Sending data to users-topic: ', filtered_neo4j_nodes_list[i])
    graph_producer = producer.send('users-topic', filtered_neo4j_nodes_list[i])
    sleep(4)
    # Waiting 4 seconds per row, will provide the required result: 5 elements per 20 seconds
    try:
        # response_producer = graph_producer.get(timeout=2)
        # in case we wanted to retrieve topic, partition,offset

        print('Successfully published the message to users-topic')
    except KafkaError as e:
        print('[ERROR] ' + e.__str__())

producer.flush()
