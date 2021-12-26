import mysql.connector
from mysql.connector import errorcode


def insertToMySQL(ratings, titles, types, prices):
    try:
        cnx = mysql.connector.connect(user='root', password='Qwerty123!',
                                      database='full_stack')
        cursor = cnx.cursor()
        table = ("CREATE TABLE `full_stack`.`Product` ("
                 "`productID` INT NOT NULL,"
                 "`name` VARCHAR(150) NULL,"
                 "`price` DOUBLE NULL,"
                 "`rating` DOUBLE NULL,"
                 "`type` varchar(40) NULL,"
                 " PRIMARY KEY (`productID`),"
                 "UNIQUE INDEX `productID_UNIQUE` (`productID` ASC) VISIBLE);")

        try:
            print("Creating table Products")
            cursor.execute(table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

        try:
            for number in range(1, 51):
                addProduct = ("INSERT INTO Product "
                              "(productID, rating, name, type, price) "
                              "VALUES (%s, %s, %s, %s, %s)")
                values = (number, ratings[number], titles[number], types[number], prices[number])
                cursor.execute(addProduct, values)
                cnx.commit()
            print("Insert was done successfully")

        except mysql.connector.Error as error:
            print("Failed to insert into MySQL table {}".format(error))

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor.close()
        cnx.close()
        print("MySQL connection is closed")
