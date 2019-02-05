import pymysql


class MySQLDBUse():
    host = "127.0.0.1"
    port = 3306
    user = "root"
    password = "xxxxxxxxxx"

    #Init a connection to the server to a certain database which to get hand over
    def __init__(self,dbname):
        self._db_connection = pymysql.connect(user=self.user,port=self.port, passwd=self.password, db=dbname)
        self._db_cursor = self._db_connection.cursor()

    # Closes connection to database of object get destroyed
    def __del__(self):
        self._db_cursor.close()

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
    
    #Return number of tabels in database
    def numberOfTabels(self,dbname):
        tabels = self.getAllTables(dbname)
        return len(tabels)    

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
    #Write 2 entries in created table
    def insertNew_2Colum_InDB(self, tableName, column1, column2):
         sqlQuery = "insert into " + tableName +  " VALUES(null, '%s', '%i', NOW())"% \
              (column1, column2)
         self.query(sqlQuery)
         self._db_connection.commit()
         
##########################################################################################################################################################
# Read Tabels
##################
    #Get all tabel of a database
    def getAllTables(self,dbname):
        self.__init__(dbname)
        numberOfTables= db.query("SHOW TABLES")
        return db.fetchall()

    #Return column(columName) from table(tableName)
    def readColumNameFromTable(self, columName ,tableName):
        sqlQuery = ("SELECT {} FROM ".format(columName) + tableName + " ")
        self.query(sqlQuery)
        return self.fetchall()

    #Return top 5 similar entries from table(tableName) where values of columnName2 >250
    def readTop5ofTable(self, columnName1, columnName2, tableName):
        sqlQuery = ("SELECT {}, {} FROM ".format(columnName1,columnName2) +
                    tableName + " Where '{}' >250 ORDER BY {} DESC LIMIT 5".format(columnName2, columnName2))
        self.query(sqlQuery)
        return self.fetchall()

    # Return top 5 list similar entries from ALL tables where values of columnName2 >250
    def readTop5ofAllTables(self, dbname, columnName1, columnName2):
        allTabels = self.getAllTables(dbname)
        result = []
        for table in allTabels:
            sqlQuery = ("SELECT {}, {} FROM ".format(columnName1,columnName2) + table[0] + " Where '{}' >250 ORDER BY {} DESC LIMIT 5".format(columnName2, columnName2))
            self.query(sqlQuery)
            result.append(self.fetchall())
        return result
        
##########################################################################################################################################################
# Delete func.
#################
    # Delete empty Tabels
    def deleteEmptyTabels(self,dbname):
        tabels = db.getAllTables(dbname)
        for tablename in tabels:
            sqlq = "SELECT * FROM " + tablename[0] + " LIMIT 1;"
            if not db.query(sqlq):
                sqlqDelete = "DROP TABLE " + tablename[0]
                print db.query(sqlqDelete)
                print "Delete " + tablename[0]
                
    # Delete all tabels with wrong colum name
    def deleteTabelsWithWrongColum(self,dbname, columnName):
        tabels = db.getAllTables(dbname)
        for tablename in tabels:
            sqlq = "SELECT *    FROM    INFORMATION_SCHEMA.COLUMNS    WHERE    TABLE_NAME = '" + tablename[0] + "'    AND    COLUMN_NAME = '" + columnName + "'"
            result = db.query(sqlq)
            if result == 0:
                sqlqDelete = "DROP TABLE " + tablename[0]
                print db.query(sqlqDelete)
                print "Delete " + tablename[0]
                print "--------------------"
