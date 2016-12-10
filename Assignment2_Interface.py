#!/usr/bin/python2.7
#
# ASSIGNMENT-2
# AUTHOR: MANIDEEP ILLENDULA(ASU ID:1208825003)
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    rangePart = "RangeRatingsPart"
    roundRobinPart = "RoundRobinRatingsPart"
    cursor = openconnection.cursor()
    target = open('RangeQueryOut.txt' , 'w')
    if ratingMinValue == 0:
        statement1 = "select partitionnum from range" + str(ratingsTableName) + "metadata where minrating <= " + str(ratingMinValue) + " and maxrating >= " + str(ratingMinValue)
        cursor.execute(statement1)
        minimum = cursor.fetchone()[0]
    elif ratingMinValue > 5.0:
        minimum = 4
    else:
        statement1 = "select partitionnum from range" + str(ratingsTableName) + "metadata where minrating < " + str(ratingMinValue) + " and maxrating >= " + str(ratingMinValue)
        cursor.execute(statement1)
        minimum = cursor.fetchone()[0]
    if ratingMaxValue == 0:
        statement2 = "select partitionnum from range" + str(ratingsTableName) + "metadata where minrating <= " + str(ratingMaxValue) + " and maxrating >= " + str(ratingMaxValue)
        cursor.execute(statement2)
        maximum = cursor.fetchone()[0]
    elif ratingMaxValue > 5.0:
        maximum = 4
    else:
        statement2 = "select partitionnum from range" + str(ratingsTableName) + "metadata where minrating < " + str(ratingMaxValue) + " and maxrating >= " + str(ratingMaxValue)
        cursor.execute(statement2)
        maximum = cursor.fetchone()[0]
    if minimum == maximum:
        statement3 = "select * from range" + str(ratingsTableName) + "part" + str(minimum) + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue)
        cursor.execute(statement3)
        row = cursor.fetchone()
        if row != None:
            result = rangePart + str(minimum) + ","
            result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
            target.write(result)
            target.write("\n")
        while row:
            row = cursor.fetchone()
            if row != None:
                result = rangePart + str(minimum) + ","
                result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
                target.write(result)
                target.write("\n")
    else:
        for x in range(minimum,maximum + 1):
            if x == minimum:
                statement3 = "select * from range" + str(ratingsTableName) + "part" + str(x) + " where rating >= " + str(ratingMinValue)
            elif x == maximum:
                statement3 = "select * from range" + str(ratingsTableName) + "part" + str(x) + " where rating <= " + str(ratingMaxValue)
            else:
                statement3 = "select * from range" + str(ratingsTableName) + "part" + str(x)
            cursor.execute(statement3)
            row = cursor.fetchone()
            if row != None:
                result = rangePart + str(x) + ","
                result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
                target.write(result)
                target.write("\n")
            while row:
                row = cursor.fetchone()
                if row != None:
                    result = rangePart + str(x) + ","
                    result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
                    target.write(result)
                    target.write("\n")

    statement4 = "select partitionnum from roundrobin" + str(ratingsTableName) +"metadata"
    cursor.execute(statement4)
    numberofPartitions = cursor.fetchone()[0]
    for i in range(numberofPartitions):
        statement5 = "select * from roundrobin" + str(ratingsTableName) + "part" + str(i) + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue)
        cursor.execute(statement5)
        row = cursor.fetchone()
        if row != None:
            result = roundRobinPart + str(i) + ","
            result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
            target.write(result)
            target.write("\n")
        while row:
            row = cursor.fetchone()
            if row != None:
                result = roundRobinPart + str(i) + ","
                result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
                target.write(result)
                target.write("\n")


def PointQuery(ratingsTableName, ratingValue, openconnection):
    rangePart = "RangeRatingsPart"
    roundRobinPart = "RoundRobinRatingsPart"
    cursor = openconnection.cursor()
    target2 = open('PointQueryOut.txt','w')
    if ratingValue == 0:
        statement1 = "select partitionnum from range" + str(ratingsTableName) + "metadata where minrating <= " + str(ratingValue) + " and maxrating >= " + str(ratingValue)
        cursor.execute(statement1)
        partition = cursor.fetchone()[0]
    elif ratingValue > 5.0:
        partition = 4
    else:
        statement1 = "select partitionnum from range" + str(ratingsTableName) + "metadata where minrating < " + str(ratingValue) + " and maxrating >= " + str(ratingValue)
        cursor.execute(statement1)
        partition = cursor.fetchone()[0]
    statement2 = "select * from range" + str(ratingsTableName) + "part" + str(partition) + " where rating = " + str(ratingValue)
    cursor.execute(statement2)
    row = cursor.fetchone()
    if row != None:
        result = rangePart + str(partition) + ","
        result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
        target2.write(result)
        target2.write("\n")
    while row:
        row = cursor.fetchone()
        if row != None:
            result = rangePart + str(partition) + ","
            result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ","")
            target2.write(result)
            target2.write("\n")

    statement4 = "select partitionnum from roundrobin" + str(ratingsTableName) +"metadata"
    cursor.execute(statement4)
    numberofPartitions = cursor.fetchone()[0]
    for i in range(numberofPartitions):
        statement5 = "select * from roundrobin" + str(ratingsTableName) + "part" + str(i) + " where rating = " + str(ratingValue)
        cursor.execute(statement5)
        row = cursor.fetchone()
        if row != None:
            result = roundRobinPart + str(i) + ","
            result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
            target2.write(result)
            target2.write("\n")
        while row:
            row = cursor.fetchone()
            if row != None:
                result = roundRobinPart + str(i) + ","
                result = (result + (str(row).replace("("," ")).replace(")"," ")).replace(" ", "")
                target2.write(result)
                target2.write("\n")
