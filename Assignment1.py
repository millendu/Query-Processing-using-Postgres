#!/usr/bin/python2.7
#
# ASSIGNMENT-1
# AUTHOR: MANIDEEP ILLENDULA(ASU ID:1208825003)
# Referred to various SQL statements for execution of the assignment

import psycopg2
import fileinput
import time
import math

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Function to load all the data from file to tables in database by preprocessing the data for effecient use
# REFERENCE: http://stackoverflow.com/questions/5453267/is-it-possible-to-modify-lines-in-a-file-in-place
# REFERENCE: http://initd.org/psycopg/docs/cursor.html
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    curs = openconnection.cursor()
    statement1 = "create table " + str(ratingstablename) + "(userid integer, movieid integer, rating real, timestamp integer)"
    curs.execute(statement1)
    for line in fileinput.FileInput(ratingsfilepath, inplace=1):
        print line.replace('::', ':').rstrip()
    fileopen = open(ratingsfilepath, 'r')
    curs.copy_from(fileopen, ratingstablename, sep = ':', size = 1000000, columns = ('userid', 'movieid', 'rating', 'timestamp'))
    statement2 = "alter table " + str(ratingstablename) + " drop column timestamp"
    curs.execute(statement2)
    curs.close()
    pass

# Function to partition the table based on the rating values and insert them respectively
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    curs = openconnection.cursor()
    statement = "create table rangepartitions(number integer)"
    curs.execute(statement)
    statement_insert = "insert into rangepartitions(number) values (" + str(numberofpartitions) +")"
    curs.execute(statement_insert)
    rangeofpartition = "%.2f" % (float(5) / float(numberofpartitions))
    for i in range(numberofpartitions):
        statement1 = "create table range_part" + str(i) + "(userid integer, movieid integer, rating real)"
        curs.execute(statement1)
    for i in range(numberofpartitions):
        if(i == 0):
            statement3 = "insert into range_part" + str(i) + "(select * from " + str(ratingstablename) + " where (rating >= " + str(0) + " and rating <=" + str(rangeofpartition) + ") order by userid asc, movieid asc)"
            curs.execute(statement3)
        elif(i == numberofpartitions - 1):
            lower_p = float(rangeofpartition) * i
            statement3 = "insert into range_part" + str(i) + "(select * from " + str(ratingstablename) + " where (rating > " + str(lower_p) + " and rating <=" + str(5) + ") order by userid asc, movieid asc)"
            curs.execute(statement3)
        else:
            lower_p = float(rangeofpartition) * i
            upper_p = float(rangeofpartition) * (i+1)
            statement3 = "insert into range_part" + str(i) + "(select * from " + str(ratingstablename) + " where (rating > " + str(lower_p) + " and rating <=" + str(upper_p) + ") order by userid asc, movieid asc)"
            curs.execute(statement3)
    curs.close()
    pass

# Function to partition the table as per roundrobin principle as insert data respectively
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    curs = openconnection.cursor()
    statement = "create table rrobinpartitions(number integer)"
    curs.execute(statement)
    statement_insert = "insert into rrobinpartitions(number) values (" + str(numberofpartitions) +")"
    curs.execute(statement_insert)
    for i in range(numberofpartitions):
        statement2 = "create table rrobin_part" + str(i) + "(userid integer, movieid integer, rating real)"
        curs.execute(statement2)
    for j in range(numberofpartitions):
        statement3 = "insert into rrobin_part" + str(j) + "(select userid,movieid,rating from (select *,row_number() over () as rownum from " + str(ratingstablename) + " order by userid asc, movieid asc) as stats where mod(rownum - " + str(1) + "," + str(numberofpartitions) + ") = " + str(j) + ")"
        curs.execute(statement3)
    curs.close()
    pass

# Function to insert the new tuple based on the principle of roundrobin
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    curs = openconnection.cursor()
    statement_rrobin = "select * from rrobinpartitions"
    curs.execute(statement_rrobin)
    numberofroundrobinpartitions = int(curs.fetchone()[0])
    count = 0
    flag = 0
    for i in range(numberofroundrobinpartitions):
        statement = "select count(*) from rrobin_part" + str(i)
        curs.execute(statement)
        out = curs.fetchone()[0]
        count = count + out
    flag = ((count) % numberofroundrobinpartitions)
    statement2 = "insert into rrobin_part" + str(flag) + "(userid, movieid, rating) values ('" + str(userid) + "' , '" + str(itemid) + "' , '" + str(rating) + "')"
    curs.execute(statement2)
    curs.close()
    pass

# Function to insert the new tuple into the partition based on the range
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    curs = openconnection.cursor()
    statement_range = "select * from rangepartitions"
    curs.execute(statement_range)
    numberofrangepartitions = int(curs.fetchone()[0])
    rangeofpartition = "%.2f" % (float(5) / float(numberofrangepartitions))
    if(float(rating) > ((float(numberofrangepartitions) * float(rangeofpartition)))):
        partitionnumber = numberofrangepartitions - 1
    else:
        partitionnumber = int(math.ceil(float(rating) / float(rangeofpartition)))
        partitionnumber = partitionnumber - 1
    statement1 = "insert into range_part" + str(partitionnumber) + "(userid, movieid, rating) values ('" + str(userid) + "' , '" + str(itemid) + "' , '" + str(rating) + "')"
    curs.execute(statement1)
    curs.close()
    pass

# Delete all the partitions except the mastertable
def deletepartitionsandexit(openconnection):
    curs = openconnection.cursor()
    statement_range = "select * from rangepartitions"
    curs.execute(statement_range)
    numberofrangepartitions = int(curs.fetchone()[0])
    for i in range(numberofrangepartitions):
        statement1 = "drop table range_part" + str(i)
        curs.execute(statement1)
    statement_rrobin = "select * from rrobinpartitions"
    curs.execute(statement_rrobin)
    numberofroundrobinpartitions = int(curs.fetchone()[0])
    for j in range(numberofroundrobinpartitions):
        statement2 = "drop table rrobin_part" + str(j)
        curs.execute(statement2)
    statement_rangedelete = "drop table rangepartitions"
    curs.execute(statement_rangedelete)
    statement_rrobindelete = "drop table rrobinpartitions"
    curs.execute(statement_rrobindelete)
    curs.close()
    pass

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            #time_1 = time.time()
            loadratings('sampleratings', 'sampleratings.dat', con)
            #time_2 = time.time()
            #print(time_2 - time_1)
            rangepartition('sampleratings', 7, con)
            #time_3 = time.time()
            #print(time_3 - time_2)
            print("HIone")
            roundrobinpartition('sampleratings', 5 , con)
            #time_4 = time.time()
            #print(time_4 - time_3)
            print("HItwo")
            rangeinsert('sampleratings', '1', '147', '5.0', con)
            rangeinsert('sampleratings', '1', '147', '0.71', con)
            rangeinsert('sampleratings', '1', '147', '1.5', con)
            rangeinsert('sampleratings', '1', '147', '2.5', con)
            rangeinsert('sampleratings', '1', '147', '3.0', con)
            rangeinsert('sampleratings', '1', '147', '3.8', con)
            rangeinsert('sampleratings', '1', '147', '4.98', con)
            rangeinsert('sampleratings', '1', '147', '4.971', con)
            #time_5 = time.time()
            #print(time_5 - time_4)
            print("Hithree")
            roundrobininsert('sampleratings', '1', '147', '5.0', con)
            roundrobininsert('sampleratings', '1', '147', '3.0', con)
            roundrobininsert('sampleratings', '1', '147', '2.0', con)
            roundrobininsert('sampleratings', '1', '147', '0.5', con)
            roundrobininsert('sampleratings', '1', '147', '1.0', con)
            print("HIfour")

            #time_6 = time.time()
            #print(time_6 - time_5)
            #deletepartitionsandexit(con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print("OOPS! This is the error ==> ", detail)
