import logging
import random
import datetime

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


def date_of_purchase():
    start_date = datetime.date(2021, 12, 25)
    end_date = datetime.date(2022, 1, 6)

    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return str(random_date)


def insertToNeo4j():
    scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
    host_name = "localhost"
    port = 7687
    url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
    user = "neo4j"
    password = "hua-neo4j"
    app = App(url, user, password)
    app.delete_nodes()
    app.create_friendship("Giorgos", "Lydia")

    app.add_friendship(app.find_person("Giorgos"), "Nikolas")
    app.add_friendship(app.find_person("Giorgos"), "Dimitris")
    app.add_friendship(app.find_person("Giorgos"), "Panayiotis")
    app.add_friendship(app.find_person("Giorgos"), "Christos")
    app.add_friendship(app.find_person("Nikolas"), "Panayiotis")
    app.add_friendship(app.find_person("Nikolas"), "Lydia")
    app.add_friendship(app.find_person("Nikolas"), "Spyros")
    app.add_friendship(app.find_person("Christos"), "Pantelis")
    app.add_friendship(app.find_person("Christos"), "Dimitris")
    app.add_friendship(app.find_person("Lydia"), "Nikolas")
    app.add_friendship(app.find_person("Lydia"), "DImitris")

    app.match_friendship(app.find_person("Giorgos"), app.find_person("Lydia"))
    app.match_friendship(app.find_person("Giorgos"), app.find_person("Nikolas"))
    app.match_friendship(app.find_person("Marcia"), app.find_person("Arianna"))
    app.match_friendship(app.find_person("Alexandra"), app.find_person("Sofia"))
    app.match_friendship(app.find_person("Arianna"), app.find_person("Sofia"))
    app.match_friendship(app.find_person("Marcia"), app.find_person("Sofia"))
    app.match_friendship(app.find_person("Spyros"), app.find_person("Maximos"))

    app.close()


class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            product_list1 = []
            product_list2 = []
            num_of_products1 = random.randint(1, 5)
            num_of_products2 = random.randint(1, 5)
            age1 = random.randint(18, 70)
            age2 = random.randint(18, 70)
            date1 = date_of_purchase()
            date2 = date_of_purchase()
            for x in range(num_of_products1):
                product_list1.append(random.randint(1, 50))

            for x in range(num_of_products2):
                product_list2.append(random.randint(1, 50))

            result = session.write_transaction(
                self._create_and_return_friendship,
                person1_name,
                person2_name,
                product_list1,
                product_list2,
                age1,
                age2,
                date1,
                date2)
            for record in result:
                print("Created friendship between: {p1}, {p2}".format(
                    p1=record['p1'], p2=record['p2']))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name, product_list1, product_list2, age1, age2, date1,
                                      date2):

        query = (
            "CREATE (p1:Person { name: $person1_name, age: $age1, productID: $product_list1, purchased: $date1 }) "
            "CREATE (p2:Person { name: $person2_name, age: $age2, productID: $product_list2, purchased: $date2 }) "
            "CREATE (p1)-[:FRIENDS_WITH]->(p2) -[:FRIENDS_WITH]->(p1)"
            "RETURN p1, p2"
        )
        result = tx.run(query,
                        person1_name=person1_name,
                        person2_name=person2_name,
                        product_list1=product_list1,
                        product_list2=product_list2,
                        age1=age1,
                        age2=age2,
                        date1=date1,
                        date2=date2)
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def add_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            product_list = []
            num_of_products = random.randint(1, 5)
            for x in range(num_of_products):
                product_list.append(random.randint(1, 50))
            age = random.randint(18, 70)
            date = date_of_purchase()
            result = session.write_transaction(
                self._create_and_return_friendship_of_existing,
                person1_name,
                person2_name,
                product_list,
                age,
                date)
            for record in result:
                print("Created friendship between: {p1}, {p2}".format(
                    p1=record['p1'], p2=record['p2']))

    @staticmethod
    def _create_and_return_friendship_of_existing(tx, person1_name, person2_name, product_list, age, date):

        query = (
            "MATCH (p1:Person WHERE p1.name= $person1_name) "
            "CREATE (p2:Person { name: $person2_name, age: $age, productID: $product_list, purchased: $date }) "
            "CREATE (p1)-[:FRIENDS_WITH]->(p2) -[:FRIENDS_WITH]->(p1)"
            "RETURN p1, p2"
        )
        result = tx.run(query,
                        person1_name=person1_name,
                        person2_name=person2_name,
                        product_list=product_list,
                        age=age,
                        date=date)
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def match_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship_of_matched,
                person1_name,
                person2_name,
            )
            for record in result:
                print("Created friendship between: {p1}, {p2}".format(
                    p1=record['p1'], p2=record['p2']))

    @staticmethod
    def _create_and_return_friendship_of_matched(tx, person1_name, person2_name):

        query = (
            "MATCH (p1:Person WHERE p1.name= $person1_name) "
            "MATCH (p2:Person WHERE p2.name= $person2_name) "
            "CREATE (p1)-[:FRIENDS_WITH]->(p2) -[:FRIENDS_WITH]->(p1)"
            "RETURN p1, p2"
        )
        result = tx.run(query,
                        person1_name=person1_name,
                        person2_name=person2_name)
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for record in result:
                # print("Found person: {record}".format(record=record))
                return record

    @staticmethod
    def _find_and_return_person(tx, person_name):
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
