##
## Noah Jackson
## CS351 -- Homework 5
##

import mysql.connector
import pandas as pd


# creates relations in mySQL workbench for the supplied data
def create_relations():
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='Tricky123!',
        database="Homework5"
    )

    print(mydb)

    df = pd.read_csv('/home/noahj734/Desktop/PycharmProjects/Databases/DBHW5/tmdb_5000_movies.csv/')

    print(df.shape)

    # print(df.columns.values)     # example how to print column names

    my_cursor = mydb.cursor()

    # creating relations inside of mySQL workbench
    # my_cursor.execute("CREATE DATABASE Homework5")
    # my_cursor.execute("SHOW DATABASES")
    # my_cursor.execute("SHOW TABLES")
    # my_cursor.execute("CREATE TABLE Genre (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Keyword (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Production_Companies (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Production_Countries (iso_3166_1 VARCHAR(255), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Spoken_Languages (iso_369_1 VARCHAR(255), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Spoken_Languages (iso_369_1 VARCHAR(255), name VARCHAR(255))")

    fill_relations(mydb, df)

    return


# Fills relations with data provided
def fill_relations(mydb, df):
    idArray = []
    nameArray = []
    """ i = 0
     while i < df.shape[0]:

         rand = df['genres'].iloc[i]
         rand = ''.join(c for c in rand if c not in '(){}<>,":')
         print(rand)


         i += 1
       """
    randId = ""
    rand = df['genres'].iloc[0]
    rand = ''.join(c for c in rand if c not in '(){}<>,":')
    print(rand)
    randLength = len(rand)
    print("len(rand): %d\n" % randLength)
    spaceCount = 0

    j = 4

    while j < randLength:
        if rand[j] == " ":
            print("breaking\n")
            break

        else:
            print("char: %c\n" % rand[j])
            randId += rand[j]
            j += 1

    print(randId)
    sqlFormula = "INSERT INTO Genre ( id, name) VALUES ( %s, %s)"

    return


create_relations()


'''
##
## Noah Jackson
## CS351 -- Homework 5
##

import mysql.connector
import pandas as pd


# creates relations in mySQL workbench for the supplied data
def create_relations():
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='Tricky123!',
        database="Homework5"
    )

    print(mydb)

    df = pd.read_csv('/home/noahj734/Desktop/PycharmProjects/Databases/tmdb_5000_movies.csv')
    print(df.shape)

    # print(df.columns.values)     # example how to print column names

    my_cursor = mydb.cursor()

    # creating relations inside of mySQL workbench
    # my_cursor.execute("CREATE DATABASE Homework5")
    # my_cursor.execute("SHOW DATABASES")
    # my_cursor.execute("SHOW TABLES")
    # my_cursor.execute("CREATE TABLE Genre (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Keyword (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Production_Companies (id INTEGER(10), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Production_Countries (iso_3166_1 VARCHAR(255), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Spoken_Languages (iso_369_1 VARCHAR(255), name VARCHAR(255))")
    # my_cursor.execute("CREATE TABLE Spoken_Languages (iso_369_1 VARCHAR(255), name VARCHAR(255))")

    fill_relations(mydb, df)

    return


# Fills relations with data provided
def fill_relations(mydb, df):
    idArray = []
    nameArray = []
    i = 0
    while i < df.shape[0]:

        randId = ""
        rand = df['genres'].iloc[i]
        rand = ''.join(c for c in rand if c not in '(){}<>,":')
        print("rand: %s\n" % rand)
        randLength = len(rand)

        spaceCount = 0

        j = 4
        while j < randLength:
            if rand[j] == " ":
                print("breaking\n")
                break

            else:
                print("char: %c\n" % rand[j])
                randId += rand[j]
                j += 1
        print("randId: %s\n" % randId)
        i += 1

    sqlFormula = "INSERT INTO Genre ( id, name) VALUES ( %s, %s)"

    return


create_relations()
'''