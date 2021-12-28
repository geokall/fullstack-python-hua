import json
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from neo4j import GraphDatabase


def return_nodes():
    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "hua-neo4j"))
    with driver.session() as session:
        result = session.read_transaction(return_all_nodes)
        for record in result:
            return record


def return_all_nodes(tx):
    query = (
        "MATCH (n) "
        "RETURN n "
    )
    result = tx.run(query)
    print("Returned all nodes in database.")
    # print(result.data())
    return [result.data()]


neo4j_list = return_nodes()
neo4j_nodes = []
for obj in range(len(neo4j_list)):
    neo4j_nodes.append(neo4j_list[obj]["n"])
    print(neo4j_list[obj]["n"])

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))

for i in range(15):
    print('Sending data: ', neo4j_nodes[i])
    future = producer.send('users-topic', neo4j_nodes[i])
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
