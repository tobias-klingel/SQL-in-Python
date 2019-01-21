import pymysql
import time

import unicodedata


class MySQLDBUse():
    host = "127.0.0.1"
    port = 3306
    user = "root"
    password = "xxxxxxxxxx"

    #Init a connection to the server to a certain database which to get hand over
    def __init__(self,dbname):
        self._db_connection = pymysql.connect(user=self.user,port=self.port, passwd=self.password, db=dbname)
        self._db_cursor = self._db_connection.cursor()

    #Send a sql query to the database
    def query(self, sqlQuery):
       try:
          return self._db_cursor.execute(sqlQuery)
          self._db_connection.commit()
       except (pymysql.DatabaseError) as error:
          print(error)
          return error

    #Get all rows of a database
    def fetchall(self):
        return self._db_cursor.fetchall()
