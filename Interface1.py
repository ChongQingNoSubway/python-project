# author: wenhui zhu 
# Start time : 2020/09/07 10:41:23
# Last updata ： 2020/09/14 22:18:16

import psycopg2
import os
import sys

# def getOpenConnection(user='postgres', password='zwhqewr654', dbname='postgres'): test

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    print('Openned database successfully')
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")



def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    print("loadingRatings")
    current = openconnection.cursor()
    current.execute("DROP TABLE IF EXISTS "+ratingstablename)

    current.execute("CREATE TABLE "+ ratingstablename +"(userid INT, space1 char, movieid INT, space2 char, rating float, space3 char, Timestamp INT)")
    print(ratingsfilepath)

    current.copy_from(open(ratingsfilepath),ratingstablename,sep=':')

    current.execute("ALTER TABLE "+ ratingstablename+ " DROP COLUMN space1, DROP COLUMN space2, DROP COLUMN space3, DROP COLUMN Timestamp")

    current.close()

    openconnection.commit()

    #print("created this table") test



def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 1: 
        return
    
    # get the interval of rangepartition from numberofpartitions
    interval = 5.0/numberofpartitions

    current = openconnection.cursor()

    #take all the data from ratings table
    # create the ranger_ratings_part table
    #print(interval)
    for i in range(numberofpartitions):
        l = i * interval
        r = l + interval
        table_name = 'range_ratings_part' + str(i)
        current.execute("CREATE TABLE " + table_name + '(userid INT, movieid INT, rating float)')

        if i == 0:
            current.execute('Insert into ' + table_name + '(userid, movieid, rating) select userid, movieid, rating from ' + ratingstablename + ' where rating >= ' + str(l) + ' and rating <= ' + str(r) + ';' )
        else : 
            current.execute('Insert into ' + table_name + '(userid, movieid, rating) select userid, movieid, rating from ' + ratingstablename + ' where rating > ' + str(l) + ' and rating <= ' + str(r) + ';')        
    
    current.close()
    #print("FINISH rangePartition") test
    openconnection.commit()



def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 1: 
        return
    
    current = openconnection.cursor()

    temp = 0 
    #print('roundRobinPartition!!!!') test

    #take all the data from ratings table
    # create the round_robin_ratings table
    for i in range(0, numberofpartitions):

        table_name = 'round_robin_ratings_part' + str(i)
        current.execute('Create table ' + table_name + ' (userid INT, movieid INT, rating float);')
        if(i != (numberofpartitions -1 )):
            temp = i +1
            current.execute("INSERT INTO {0} SELECT {1},{2},{3} FROM (SELECT ROW_NUMBER() OVER() as row_number,* FROM {4}) as foo WHERE MOD(row_number,{5}) = cast ('{6}' as bigint) ".format(table_name,'userid', 'movieid', 'rating' , ratingstablename, numberofpartitions, temp))
        else: 
            temp = 0
            current.execute("INSERT INTO {0} SELECT {1},{2},{3} FROM (SELECT ROW_NUMBER() OVER() as row_number,* FROM {4}) as foo WHERE MOD(row_number,{5}) = cast ('{6}' as bigint) ".format(table_name,'userid', 'movieid', 'rating' , ratingstablename, numberofpartitions, temp))

    current.close()
    openconnection.commit()



def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    current = openconnection.cursor()

    #get  partition number[]
    current.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format('round_robin_ratings_part'))
    number_partition = current.fetchone()[0]

    #get count of row 
    current.execute('select count(*) from' + ratingstablename + ';')
    rating_row = current.fetchone()[0]

    #get number of partition 
    #(current row count + additional row) % number of partitions.
    partitionNumber = (rating_row-1)%number_partition
    current.execute('Insert Into '+ ratingstablename + ' (userid, movieid, rating) values (' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');')
    current.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format('round_robin_ratings_part' + str(partitionNumber), str(userid), str(itemid), str(rating)))
    
    current.close()
    openconnection.commit()



def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    current = openconnection.cursor()

    current.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format('range_ratings_part'))
    
    #Number of paritions is equal to the number of range partition tables.
    number_partition = current.fetchone()[0]
    
    #get the interval of 
    temp = 0.0
    interval = 5.0/number_partition
    
    #Inserting in ratings table.
    current.execute('Insert Into '+ ratingstablename + ' (userid, movieid, rating) values (' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');')

    #handle rating == 0.0 explicitly because it also includes the lower limit.
    if (rating==0.0):
        current.execute('Insert Into '+ 'range_ratings_part' + ' (userid, movieid, rating) values (' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');')
    else:

        for i in range(number_partition):

            if(rating > temp and rating <= temp + interval):
                current.execute('INSERT Into {0} VALUES ({1}, {2}, {3})'.format( 'range_ratings_part'+str(i), str(userid), str(itemid), str(rating)))
                break

            temp += interval

    #Committing the changes just in case if the autocommit is not on in the calling script.
    openconnection.commit()       
    current.close()



def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    current = openconnection.cursor()

    result_list = []

    # according to the ratingMinvalue and rating Maxvalue
    # get number of partition from range-table and oganization prepare sql execute into text
    current.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format('range_ratings_part'))
    number_range = current.fetchone()[0]

    for i in range(number_range):
        result_list.append("SELECT 'range_ratings_part" + str(i) +"' AS tablename, userid, movieid, rating FROM range_ratings_part" + str(i) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))

    # get nomber of partition from robin-table and oganization prepare sql execute into text 
    current.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format('round_robin_ratings_part'))
    number_partition = current.fetchone()[0]

    for i in range(number_partition):
        result_list.append("SELECT 'round_robin_ratings_part" + str(i) +"' AS tablename, userid, movieid, rating FROM round_robin_ratings_part" + str(i) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))
    
    #prepare the sql execute and write the data into text
    query = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(result_list))
    result_file = open(outputPath, 'w+')
    # make sure your path only include english!!!!!!!!

    #print(outputPath) test
    sql_execute = "COPY (" + query + ") TO '" + os.path.abspath(result_file.name) + "' (FORMAT text, DELIMITER ',')"
    current.execute(sql_execute)


    current.close()
    result_file.close()
   


def pointQuery(ratingValue, openconnection, outputPath):
    current = openconnection.cursor()
    result_list = []

    # according to the ratingvalue
    # get number of partition from range-table and oganization prepare sql execute into text 
    current.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format('range_ratings_part'))
    number_range = current.fetchone()[0]
    for i in range(number_range):
        result_list.append("SELECT 'range_ratings_part" + str(i) +"' AS tablename, userid, movieid, rating FROM range_ratings_part" + str(i) + " WHERE rating = " + str(ratingValue))


    # get nomber of partition from robin-table and oganization prepare sql execute into text 
    current.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format('round_robin_ratings_part'))
    number_partition = current.fetchone()[0]
    for i in range(number_partition):
        result_list.append("SELECT 'round_robin_ratings_part" + str(i) +"' AS tablename, userid, movieid, rating FROM round_robin_ratings_part" + str(i) + " WHERE rating = " + str(ratingValue))
    

    #prepare the sql execute and write the data into text
    query = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(result_list))

    # make sure your path only include english , can not be any other language!!!!
    result_file = open(outputPath, 'w+')

    sql_execute = "COPY (" + query + ") TO '" + os.path.abspath(result_file.name) + "' (FORMAT text, DELIMITER ',')"
    current.execute(sql_execute)


    current.close()
    result_file.close()



def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
