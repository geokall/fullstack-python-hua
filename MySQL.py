import mysql.connector
from mysql.connector import errorcode
import csv


def insert_data_on_my_sql():
    ratings, titles, types, prices = return_from_csv_columns()

    try:
        cnx = mysql.connector.connect(host='localhost', user='root', password='Qwerty123!', database='hua-python')
        cursor = cnx.cursor()

        table = ("CREATE TABLE IF NOT EXISTS `hua-python`.`Product` ("
                 "`productID` INT NOT NULL,"
                 "`name` VARCHAR(150) NOT NULL,"
                 "`price` DOUBLE NOT NULL,"
                 "`rating` DOUBLE NOT NULL,"
                 "`type` varchar(40) NOT NULL,"
                 " PRIMARY KEY (`productID`),"
                 "UNIQUE INDEX `productID_UNIQUE` (`productID` ASC) VISIBLE);")

        try:
            print("Trying to create Table Product")
            cursor.execute(table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table Product, already exists.")
            else:
                print(err.msg)
        else:
            print("Table Product is successfully created")

        try:
            for number in range(1, 51):
                add_product = ("INSERT INTO Product "
                               "(productID, rating, name, type, price) "
                               "VALUES (%s, %s, %s, %s, %s)")
                prices_string_to_float = float(prices[number])
                rounded_prices = round(prices_string_to_float, 2)
                values = (number, ratings[number], titles[number], types[number], rounded_prices)
                cursor.execute(add_product, values)
                cnx.commit()
                print("Insert was done successfully")

        except mysql.connector.Error as error:
            print("Failed to insert into MySQL table {}".format(error))

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Wrong credentials")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor.close()
        cnx.close()
        print("MySQL connection is closed")


def return_from_csv_columns():
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

    return ratings, titles, types, prices
