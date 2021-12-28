import logging
import random
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


def retrieve_random_height_rounded():
    return round(random.uniform(1.60, 1.90), 2)


def insert_data_to_neo4j():
    scheme = "neo4j"
    host_name = "localhost"
    port = 7687

    url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
    user = "neo4j"
    password = "hua-neo4j"

    app = App(url, user, password)
    app.delete_nodes()
    app.create_friendship_between_two("Giorgos", "Lydia")
    app.create_friendship_between_two("Christos", "Pantelis")

    app.add_friendship_between_two(app.find_person_by_name("Giorgos"), "Nikolas")
    app.add_friendship_between_two(app.find_person_by_name("Giorgos"), "Dimitris")
    app.add_friendship_between_two(app.find_person_by_name("Giorgos"), "Panayiotis")
    app.add_friendship_between_two(app.find_person_by_name("Nikolas"), "Dionysia")
    app.add_friendship_between_two(app.find_person_by_name("Nikolas"), "Stefania")
    app.add_friendship_between_two(app.find_person_by_name("Nikolas"), "Giannis")
    #
    app.match_friendship_between_two(app.find_person_by_name("Giorgos"), app.find_person_by_name("Pantelis"))
    app.match_friendship_between_two(app.find_person_by_name("Stefania"), app.find_person_by_name("Christos"))

    app.close()


class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # We need to close the connection manually
        self.driver.close()

    def create_friendship_between_two(self, name1, name2):
        with self.driver.session() as session:

            product_list1 = []
            product_list2 = []

            random_product_list1 = random.randint(1, 5)
            random_product_list2 = random.randint(1, 5)

            random_age1 = random.randint(18, 40)
            random_age2 = random.randint(18, 40)

            random_height1 = retrieve_random_height_rounded()
            random_height2 = retrieve_random_height_rounded()

            for x in range(random_product_list1):
                product_list1.append(random.randint(1, 50))

            for x in range(random_product_list2):
                product_list2.append(random.randint(1, 50))

            result = session.write_transaction(
                self._create_friendship_between_two,
                name1,
                name2,
                random_age1,
                random_age2,
                random_height1,
                random_height2,
                product_list1,
                product_list2,
            )

            for record in result:
                print("Created friendship between: {p1}, {p2}".format(
                    p1=record['p1'], p2=record['p2']))

    @staticmethod
    def _create_friendship_between_two(tx, name1, name2, age1, age2, height1, height2, product_list1, product_list2, ):

        query = (
            "CREATE (p1:Person { name: $name1, age: $age1, height: $height1, productID: $product_list1 }) "
            "CREATE (p2:Person { name: $name2, age: $age2, height: $height2, productID: $product_list2 }) "
            "CREATE (p1)-[:FRIENDS_WITH]->(p2) -[:FRIENDS_WITH]->(p1)"
            "RETURN p1, p2"
        )

        result = tx.run(query,
                        name1=name1,
                        name2=name2,
                        age1=age1,
                        age2=age2,
                        height1=height1,
                        height2=height2,
                        product_list1=product_list1,
                        product_list2=product_list2,
                        )
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def add_friendship_between_two(self, name1, name2):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            product_list = []

            number_of_products = random.randint(1, 5)

            for x in range(number_of_products):
                product_list.append(random.randint(1, 50))

            random_age = random.randint(18, 40)

            random_height = retrieve_random_height_rounded()

            result = session.write_transaction(
                self._create_friendship_of_existing,
                name1,
                name2,
                product_list,
                random_age,
                random_height)
            for record in result:
                print("Created friendship between: {p1}, {p2}".format(
                    p1=record['p1'], p2=record['p2']))

    @staticmethod
    def _create_friendship_of_existing(tx, name1, name2, product_list, age, height):

        query = (
            "MATCH (p1:Person WHERE p1.name= $name1) "
            "CREATE (p2:Person { name: $name2, age: $age, height: $height, productID: $product_list}) "
            "CREATE (p1)-[:FRIENDS_WITH]->(p2) -[:FRIENDS_WITH]->(p1)"
            "RETURN p1, p2"
        )

        result = tx.run(query,
                        name1=name1,
                        name2=name2,
                        age=age,
                        height=height,
                        product_list=product_list,
                        )
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def match_friendship_between_two(self, name1, name2):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_friendship_between_two_matched,
                name1,
                name2,
            )
            for record in result:
                print("Created friendship between: {p1}, {p2}".format(
                    p1=record['p1'], p2=record['p2']))

    @staticmethod
    def _create_friendship_between_two_matched(tx, name1, name2):

        query = (
            "MATCH (p1:Person WHERE p1.name= $name1) "
            "MATCH (p2:Person WHERE p2.name= $name2) "
            "CREATE (p1)-[:FRIENDS_WITH]->(p2) -[:FRIENDS_WITH]->(p1)"
            "RETURN p1, p2"
        )

        result = tx.run(query, name1=name1, name2=name2)
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person_by_name(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person_name, person_name)
            for record in result:
                # print("Found person: {record}".format(record=record))
                return record

    @staticmethod
    def _find_and_return_person_name(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)

        return [record["name"] for record in result]

    def delete_nodes(self):
        with self.driver.session() as session:
            result = session.write_transaction(self.delete_all_nodes)
            for record in result:
                return record

    @staticmethod
    def delete_all_nodes(tx):
        query = (
            "MATCH (n) "
            "DETACH DELETE n "
        )
        result = tx.run(query)

        print("Deleted all existing nodes in database.")

        return [result]
