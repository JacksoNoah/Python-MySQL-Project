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

    get_data(df, genreDF, 'genres')
    # get_data(df, genreDF, 'keywords')
    # get_data(df, genreDF, 'production_companies')
    # get_data(df, genreDF, 'production_countries')
    # get_data(df, genreDF, 'spoken_languages')

    # print(df['genres'].iloc[0])
    sqlFormula = "INSERT INTO Genre ( id, name) VALUES ( %s, %s)"

    return


# parses/extracts data of a specific attribute, "attr", of the DataFrame "df"
def get_data(df, newDF, attr):
    # get column names of data frame being inserted into
    colOneName = newDF.columns[1]
    colTwoName = newDF.columns[2]

    i = 0
    while i < df.shape[0]:

        evenOddFlag = 0  # is 0 when char is odd numbered space, and 1 when char is even space in row
        dataFlag = 0  # if 1 then data element is id, or newDF.columns[1], if 2 then newDF.columns[2]
        commaCount = 0  # keep track of which space

        randDataName = ""  # used to store name attribute from data row i
        randDataId = ""  # used to store id attribute from data row i

        # get data from row i
        randData = df[attr].iloc[i]

        # remove all chars in '(){}<>,":[]' from the rows data to help with parsing
        # randData = ''.join(c for c in randData if c not in '(){}<>,":[]')
        randData = ''.join(c for c in randData if c not in '{}[]<>":')
        randData = randData.replace("id", "")
        randData = randData.replace("name", "")
        # randData = randData.replace(" ", "")
        print("randData: %s\n" % randData)
        randLength = len(randData)
        # print(newDF.columns[0])
        commaCount = 0

        j = 1
        # while j is less than length of row
        count = 0
        while j < randLength:

            # print(count)
            count += 1

            # if char is a space then we have reached end of word then set id attribute in newDF
            if randData[j] == "," and (commaCount % 2) == 1:
                newDF[colOneName] = randDataId
                commaCount += 1
                randDataId = ""  # used to store id attribute from data row i
                evenOddFlag = 0

                # print("breaking\n")

            # if char is a space then we have reached end of word then set name attribute in newDF
            elif randData[j] == "," and (commaCount % 2) == 0:
                newDF[colTwoName] = randDataName
                commaCount += 1
                randDataName = ""  # used to store name attribute from data row i
                evenOddFlag = 1

                # print("breaking\n")

            # if char is first space reached in row its id attributes data
            elif randData[j] != "," and randData[j] != " ":

                # we know characters are now id data
                if evenOddFlag == 0:
                    randDataId += randData[j]

                # we know characters are now name data
                if evenOddFlag == 1:
                    randDataName += randData[j]


            j += 1

        print("randDataId: %s\nrandDataName: %s\n" % (randDataId, randDataName))
        i += 1

    # print("i: %d\n" % i)


create_relations()
