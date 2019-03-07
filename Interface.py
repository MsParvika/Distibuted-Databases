import psycopg2
DATABASE_NAME = 'distributedDb'

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")



def create_db(dbname):
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute('Select count(*) from pg_catalog.pg_database where datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('Create database %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)
    cur.close()
    con.close()


#load ratings.dat or any other file_name, file into table_name sepcified by the user
def loadratings(tableName, ratingsFile , openconnection):
    cur = openconnection.cursor()

    cur.execute("Drop table if exists "+ tableName)

    cur.execute("Create table "+tableName +" (UserID integer,rubbish1 char, MovieID integer,rubbish2 char, Rating float,rubbish3 char, timestamp int)")
    fileOpen = open(ratingsFile,'r')
    cur.copy_from(fileOpen, tableName, sep=':')
    cur.execute("Alter table "+tableName+" drop column rubbish1, drop column rubbish2, drop column rubbish3, drop column timestamp")

#Creates range partition for the the data on the givel global range 0 to 5  (if you want to create particular range then uncomment the min(res) and mx(res) and respective code too
def rangepartition(tableName, numberOfPartitions, openconnection):
    cur = openconnection.cursor()

    cur.execute("Select rating from "+tableName)
    res = cur.fetchall()
    if len(res)== 0:
        print(tableName+" Table is Empty")
        return

    cur.execute("Select count(*) from information_schema.tables where table_name like 'range_part%'")
    deletingTables = int(cur.fetchone()[0])
    index = 0
    while index < deletingTables:
        cur.execute("Drop table if exists range_part" + str(index))
        index += 1

    min_val = 0#min(res)
    max_val = 5 #max(res)
    rangeOfRatings= 5 - 0 #max_val[0]-min_val[0]
    eachRange= rangeOfRatings/numberOfPartitions
    index = 1
    firstIterationFlag= True
    while index <= numberOfPartitions:
        if(firstIterationFlag):
            cur.execute("Create table range_part" + str(index-1) + " as select * from " + tableName + " where Rating >= " + str(min_val + eachRange*(index-1)) + " and Rating <= " + str(min_val + eachRange*index) + ";")
            firstIterationFlag= False
        else:
            cur.execute("Create table range_part" + str(index-1) + " as select * from " + tableName + " where Rating > " + str(min_val + eachRange*(index-1)) + " and Rating <= " + str(min_val + eachRange*index) + ";")
        index += 1

#create round robin partition for the input data ( it will insert records one by one to the partitioned tables)
def roundrobinpartition(tableName, numberOfPartitions, openconnection):
    cur = openconnection.cursor()

    cur.execute("Select * from " +tableName)
    allRecords = cur.fetchall()
    if len(allRecords) == 0:
        print(tableName+ " Table is Empty")
        return

    cur.execute("Select count(*) from information_schema.tables where table_name like 'rrobin_part%'")
    deletingTables = int(cur.fetchone()[0])
    index = 0
    while index < deletingTables:
        cur.execute("Drop table if exists rrobin_part" + str(index))
        index += 1

    index=1
    while index<=numberOfPartitions:
        cur.execute("Drop table if exists rrobin_part"+str(index-1))
        cur.execute("Create table rrobin_part" + str(index-1) + " as select UserId, MovieID, Rating from (Select UserId, MovieID, Rating, Row_Number() over() rn from "+tableName+" ) x where  rn % " + str(numberOfPartitions) +" = " + str(index % numberOfPartitions))
        index += 1

#this will insert the record in the next table which has the least data (as round robin works on disrtibuting the records one by one to the tables)
def roundrobininsert(tableName, userId, itemId, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("Insert into " + tableName + " (UserId, MovieId, Rating) values (%d, %d, %f)" % (userId, itemId, rating))
    cur.execute("Select count(*) from information_schema.tables where table_name like 'rrobin_part%'")
    numberOfPartitions = int(cur.fetchone()[0])
    if numberOfPartitions == 0:
        print(tableName + " Table has not been Round Robin Partitioned !!!")
        return

    firstPartionNumber= 0
    cur.execute("Select count(*) from rrobin_part" + str(firstPartionNumber))
    thisTotalRecords = int(cur.fetchone()[0])
    nextPartitionNumber = firstPartionNumber + 1
    while nextPartitionNumber < numberOfPartitions:
        prevTotalRecords = thisTotalRecords
        cur.execute("Select count(*) from rrobin_part"+str(nextPartitionNumber))
        thisTotalRecords = int(cur.fetchone()[0])
        if(thisTotalRecords < prevTotalRecords):
            cur.execute("Insert into rrobin_part"+str(nextPartitionNumber)+"(UserId, MovieId, Rating) values (%d, %d, %f)" %(userId, itemId, rating))
            return
        nextPartitionNumber += 1
    cur.execute("Insert into rrobin_part" + str(firstPartionNumber) + "(UserId, MovieId, Rating) values (%d, %d, %f)" %(userId, itemId, rating))


#ranges are mutually exclusive and are in float so you might want to see the exact rating value to see which partition it belongs to.
def rangeinsert(tableName, userId, itemId, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("Insert into " + tableName + " (UserId, MovieId, Rating) values (%d, %d, %f)" % (userId, itemId, rating))

    cur.execute("Select count(*) from information_schema.tables where table_name like 'range_part%'")
    numberOfPartitions = int(cur.fetchone()[0])
    if numberOfPartitions==0:
        print(tableName+" Table has not been Range Partitioned !!!")
        return

    rangeOfRatings = 5  # used global range of dataset here as 0-5 thus range becomes 5
    eachRange = float(rangeOfRatings) / numberOfPartitions

    while numberOfPartitions > 0:
        if numberOfPartitions - 1 == 0 and rating >= 0 and rating <= eachRange * numberOfPartitions:
            cur.execute("Insert into range_part" + str(numberOfPartitions - 1) + " (UserId, MovieId, Rating) values (%d, %d, %f)" % (userId, itemId, rating))
        elif rating > eachRange * (numberOfPartitions - 1) and rating <= eachRange * numberOfPartitions:
            cur.execute("Insert into range_part" + str(numberOfPartitions - 1) + " (UserId, MovieId, Rating) values (%d, %d, %f)" % (userId, itemId, rating))
        numberOfPartitions -= 1
