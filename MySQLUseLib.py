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

##########################################################################################################################################################
# General
#################
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

##########################################################################################################################################################
# Creating Tabels
#################

    # Create table with id, column1(Char), column2(INT), time stamp
    def createNew_2column_Table(self, tabelName):
        db = MyDB("DATABASENAME") #<------Please change
        createNewHastagTabel = "CREATE TABLE " + tabelName + " ( \
       id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
       column1 CHAR(50) NOT NULL, \
       column2 INT (50) NOT NULL, \
       reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
       )"
        try:
            print self._db_cursor.execute(createNewHastagTabel)
        except (pymysql.DatabaseError) as error:
            print(error)
            print "Error in creating: " + tabelName
        return error

##########################################################################################################################################################
#Write in Tables
################
    #Write 2 cloum in created table
    def insertNew_2Colum_InDB(self, tableName, column1, column2):
         sqlQuery = "insert into " + tableName +  " VALUES(null, '%s', '%i', NOW())"% \
              (column1, column2)
         self.query(sqlQuery)
         self._db_connection.commit()