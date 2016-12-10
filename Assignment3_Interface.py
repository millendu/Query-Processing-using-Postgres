#!/usr/bin/python2.7
#AUTHOR: MANIDEEP ILLENDULA
#ID: 1208825003
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading
import math
##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    curs = openconnection.cursor()
    statement_max = "select MAX(" + str(SortingColumnName) + ") from " + str(InputTable)
    curs.execute(statement_max)
    maximum = float(curs.fetchone()[0])
    statement_min = "select MIN(" + str(SortingColumnName) + ") from " + str(InputTable)
    curs.execute(statement_min)
    minimum = float(curs.fetchone()[0])
    rangepartition(InputTable, 5, SortingColumnName, maximum, minimum, openconnection)

    statement = "create table " + str(OutputTable) + " (like " + str(InputTable) +"_part0)"
    curs.execute(statement)

    table1 = InputTable + "_part0"
    table2 = InputTable + "_part1"
    table3 = InputTable + "_part2"
    table4 = InputTable + "_part3"
    table5 = InputTable + "_part4"

    temp = str(InputTable) + "_" + str(SortingColumnName)

    SortingColumnName = temp
    t1 = threading.Thread(Sort(table1, SortingColumnName, OutputTable, openconnection))
    t2 = threading.Thread(Sort(table2, SortingColumnName, OutputTable, openconnection))
    t3 = threading.Thread(Sort(table3, SortingColumnName, OutputTable, openconnection))
    t4 = threading.Thread(Sort(table4, SortingColumnName, OutputTable, openconnection))
    t5 = threading.Thread(Sort(table5, SortingColumnName, OutputTable, openconnection))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    openconnection.commit()
    pass #Remove this once you are done with implementation

def Sort(inputTable, column, outputTable, openconnection):
    curs = openconnection.cursor()
    statement = "insert into " + str(outputTable) + " (select * from " + str(inputTable) + " order by " + str(column) +")"
    curs.execute(statement)
    pass

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    curs = openconnection.cursor()
    statement_max1 = "select MAX(" + str(Table1JoinColumn) + ") from " + str(InputTable1)
    curs.execute(statement_max1)
    maximum1 = float(curs.fetchone()[0])
    statement_min1 = "select MIN(" + str(Table1JoinColumn) + ") from " + str(InputTable1)
    curs.execute(statement_min1)
    minimum1 = float(curs.fetchone()[0])
    statement_max2 = "select MAX(" + str(Table2JoinColumn) + ") from " + str(InputTable2)
    curs.execute(statement_max2)
    maximum2 = float(curs.fetchone()[0])
    statement_min2 = "select MIN(" + str(Table2JoinColumn) + ") from " + str(InputTable2)
    curs.execute(statement_min2)
    minimum2 = float(curs.fetchone()[0])

    diff1 = maximum1 - minimum1
    diff2 = maximum2 - minimum2

    if diff1 < diff2:
        maximum = maximum1
        minimum = minimum1
    else:
        maximum = maximum2
        minimum = minimum2

    rangepartition(InputTable1, 5, Table1JoinColumn, maximum, minimum, openconnection)
    rangepartition(InputTable2, 5, Table2JoinColumn, maximum, minimum, openconnection)

    temp1 = str(InputTable1) + "_" + str(Table1JoinColumn)
    temp2 = str(InputTable2) + "_" + str(Table2JoinColumn)

    Table1JoinColumn = temp1
    Table2JoinColumn = temp2

    statement = "create table " + str(OutputTable) + " as (select * from " + str(InputTable1) + "_part0, " + str(InputTable2) +"_part0 where true = false)"
    curs.execute(statement)

    table1 = InputTable1 + "_part0"
    table2 = InputTable1 + "_part1"
    table3 = InputTable1 + "_part2"
    table4 = InputTable1 + "_part3"
    table5 = InputTable1 + "_part4"

    table_1 = InputTable2 + "_part0"
    table_2 = InputTable2 + "_part1"
    table_3 = InputTable2 + "_part2"
    table_4 = InputTable2 + "_part3"
    table_5 = InputTable2 + "_part4"


    t1 = threading.Thread(Join(table1, table_1, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t2 = threading.Thread(Join(table2, table_2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t3 = threading.Thread(Join(table3, table_3, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t4 = threading.Thread(Join(table4, table_4, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t5 = threading.Thread(Join(table5, table_5, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()

    openconnection.commit()
    pass # Remove this once you are done with implementation

def Join(inputTable1, inputTable2, column1, column2, outputTable, openconnection):
    curs = openconnection.cursor()
    statement = "insert into " + str(outputTable) + " (select * from " + str(inputTable1) + ", " + str(inputTable2) + " where " + str(inputTable1) + "." + str(column1) + " = " + str(inputTable2) + "." + str(column2) +")"
    curs.execute(statement)
    pass

# This function is taken from first assignment submitted by me and has been modified little as per the requirment in this assignment
def rangepartition(ratingstablename, numberofpartitions, sortingcolumnname, maximum, minimum, openconnection):
    curs = openconnection.cursor()
    rangeofpartition = "%.2f" % (float(maximum - minimum) / float(numberofpartitions))
    for i in range(numberofpartitions):
        statement1 = "create table "+ str(ratingstablename) + "_part" + str(i) + "(like " + str(ratingstablename) + ")"
        curs.execute(statement1)
    for i in range(numberofpartitions):
        if(i == 0):
            upper_p = float(minimum) + float(rangeofpartition)
            statement3 = "insert into " + str(ratingstablename) + "_part" + str(i) + "(select * from " + str(ratingstablename) + " where (" + str(sortingcolumnname) + " >= " + str(minimum) + " and " + str(sortingcolumnname) + " <=" + str(upper_p) + "))"
            curs.execute(statement3)
            print statement3
        elif(i == numberofpartitions - 1):
            lower_p = float(minimum) + float(rangeofpartition) * i
            statement3 = "insert into " + str(ratingstablename) + "_part" + str(i) + "(select * from " + str(ratingstablename) + " where (" + str(sortingcolumnname) + " > " + str(lower_p) + " and " + str(sortingcolumnname) + " <=" + str(maximum) + "))"
            curs.execute(statement3)
            print statement3
        else:
            lower_p = float(minimum) + float(rangeofpartition) * i
            upper_p = float(minimum) + float(rangeofpartition) * (i+1)
            statement3 = "insert into " + str(ratingstablename) + "_part" + str(i) + "(select * from " + str(ratingstablename) + " where (" + str(sortingcolumnname) + " > " + str(lower_p) + " and " + str(sortingcolumnname) + " <=" + str(upper_p) + "))"
            curs.execute(statement3)
            print(statement3)
    statement_table1 = "select column_name from information_schema.columns where table_name = '" + str(ratingstablename) + "_part0'"
    curs.execute(statement_table1)
    l1 = curs.fetchall()

    l1temp = []

    l1t = []
    for i in l1:
        var = str(ratingstablename) + "_" + str(i[0])
        l1temp.append(var)
        l1t.append(str(i[0]))

    for i in range(numberofpartitions):
        for col in range(0,len(l1t)):
            statement_alter = "alter table " + str(ratingstablename) + "_part" + str(i) + " rename column " + l1t[col] + " to " + l1temp[col]
            curs.execute(statement_alter)

    curs.close()
    pass

################### DO NOT CHANGE ANYTHING BELOW THIS #############################

# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
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
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment2"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment2 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
