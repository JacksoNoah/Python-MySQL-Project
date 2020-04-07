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
    # setting up all data frames for data parsing/extraction
    genreColumns = ['movieID', 'genreID', 'genreName']
    genreDF = pd.DataFrame(columns=genreColumns)

    keywordColumns = ['movieID', 'keywordID', 'keywordName']
    keywordDF = pd.DataFrame(columns=keywordColumns)

    prodCompaniesColumns = ['movieID', 'prodCompanyID', 'prodCompanyName']
    prodCompaniesDF = pd.DataFrame(columns=prodCompaniesColumns)

    prodCountriesColumns = ['movieID', 'prodCountryID', 'prodCountryName']
    prodCountriesDF = pd.DataFrame(columns=prodCountriesColumns)

    spokenLangColumns = ['movieID', 'spokenLangID', 'spokenLangName']
    spokenLangDF = pd.DataFrame(columns=spokenLangColumns)

    dfArray = []
    dfArray.append(genreDF)
    dfArray.append(keywordDF)
    dfArray.append(prodCompaniesDF)
    dfArray.append(prodCountriesDF)
    dfArray.append(spokenLangDF)

    # get_data(df, genreDF, 'genres')
    # get_data(df, genreDF, 'keywords')
    # get_data(df, genreDF, 'production_companies')
    # get_data(df, genreDF, 'production_countries')
    get_data(df, genreDF, 'spoken_languages')

    # print(df.iloc[0])
    sqlFormula = "INSERT INTO Genre ( id, name) VALUES ( %s, %s)"

    return


# parses/extracts data of a specific attribute, "attr", of the DataFrame "df"
def get_data(df, newDF, attr):
    i = 0
    while i < df.shape[0]:

        evenOddFlag = 0  # if 0, on odd space, on even space in rows data

        randDataName = ""
        randDataId = ""
        randData = df[attr].iloc[i]
        randData = ''.join(c for c in randData if c not in '(){}<>,":[]')
        # print("randData: %s\n" % randData)
        randLength = len(randData)
        print(newDF.columns[0])
        spaceCount = 0

        j = 0
        # while j is less than length of row
        while j < randLength:
            if randData[j] == " ":

                # print("breaking\n")
                break

            else:
                # print("char: %c\n" % randData[j])
                randDataId += randData[j]
                j += 1
        # print("randDataId: %s\n" % randDataId)
        i += 1

    print("i: %d\n" % i)


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