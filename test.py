import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    print('Openned database successfully')
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    print("loadingRatings")
    current = openconnection.cursor()
    current.execute("DROP TABLE IF EXISTS "+ratingstablename)
    create_table = "CREATE TABLE "+ ratingstablename +"(userid INT, space1 char, movieid INT, space2 char, rating float, space3 char, Timestamp INT)"
    current.execute(create_table)
    current.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
    alert_table = "ALTER TABLE "+ ratingstablename+ " DROP COLUMN space1, DROP COLUMN space2, DROP COLUMN space3, DROP COLUMN Timestamp"
    current.execute(alert_table)
    current.close()
    openconnection.commit()
    print("created this table")


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 1: 
        return
    interval = 5.0/numberofpartitions
    current = openconnection.cursor()
    for i in range(numberofpartitions):
        left = i * interval
        right = left + interval
        table_name = 'range_part' + str(i)
        current.execute("CREATE TABLE " + table_name + '(userid INT, movieid INT, rating float)')

        if i == 0:
            current.execute('Insert into ' + table_name + '(userid, movieid, rating) select userid, movieid, rating from ' + ratingstablename + ' where rating >= ' + str(left) + ' and rating <= ' + str(right) + ';' )
        else : 
            current.execute('Insert into ' + table_name + '(userid, movieid, rating) select userid, movieid, rating from ' + ratingstablename + ' where rating > ' + str(left) + ' and rating <= ' + str(right) + ';')        
    current.close()
    print("FINISH rangePartition")
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 1: 
        return
    current = openconnection.cursor()
    temp = 0 
    print('roundRobinPartition!!!!')
    for i in range(0, numberofpartitions):
        table_name = 'rrobin_part' + str(i)
        current.execute('Create table ' + table_name + ' (userid INT, movieid INT, rating float);')
        if(i != (numberofpartitions -1 )):
            temp = i +1
        else: 
            temp = 0
        try:
            current.execute("INSERT INTO {0} SELECT {1},{2},{3} FROM (SELECT ROW_NUMBER() OVER() as row_number,* FROM {4}) as foo WHERE MOD(row_number,{5}) = cast ('{6}' as bigint) ".format(table_name,'userid', 'movieid', 'rating' , ratingstablename, numberofpartitions, temp))
            openconnection.commit()
        except Exception as e:
            print(e)    


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    current = openconnection.cursor()
    current.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format('rrobin_part'))
    number_partition = current.fetchone()[0]
    current.execute('select count(*) from' + ratingstablename + ';')
    rating_row = current.fetchone()[0]
    partitionNumber = (rating_row)%number_partition

    current.execute('Insert Into '+ ratingstablename + ' (userid, movieid, rating) values (' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');')
    current.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format('rrobin_part' + str(partitionNumber), str(userid), str(itemid), str(rating)))

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format('range_part'))
    #Number of paritions is equal to the number of range partition tables.
    partition = cur.fetchone()[0]
    
    start = 0.0
    interval = 5.0/partition
    
    #Inserting in ratings table.
    cur.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format( ratingstablename, str(userid), str(itemid), str(rating)))

    #Need to handle rating == 0.0 explicitly because it also includes the lower limit.
    if (rating==0.0):
        cur.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format( 'range_part'+'0', str(userid), str(itemid), str(rating)))
    else:
        for p in range(partition):
            if(rating > start and rating <= start + interval):
                cur.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format( 'range_part'+str(p), str(userid), str(itemid), str(rating)))
                #Break from the loop because we already found the desired range.
                break
            start += interval

    #Committing the changes just in case if the autocommit is not on in the calling script.
    openconnection.commit()       
    cur.close()

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    pass #Remove this once you are done with implementation


def pointQuery(ratingValue, openconnection, outputPath):
    pass # Remove this once you are done with implementation


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
